// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package trace

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"slices"
	"sort"
)

type AdmissionTrace struct {
	TicketScope       string          `json:"ticket_scope"`
	WALStage          string          `json:"wal_stage"`
	Waits             int             `json:"waits"`
	Retirements       map[int]int     `json:"retirements_per_wait"`
	FullWaveTickets   int             `json:"full_wave_tickets"`
	FullWaveWaits     int             `json:"full_wave_waits"`
	FullWaveWALIdleMS float64         `json:"full_wave_wal_idle_ms"`
	Longest           []AdmissionWait `json:"longest_waits"`
}

type AdmissionWait struct {
	Batch             int     `json:"batch"`
	BeginNS           int64   `json:"begin_ns"`
	EndNS             int64   `json:"admitted_ns"`
	Retired           int     `json:"retired"`
	FirstRetirementNS int64   `json:"first_retirement_ns,omitempty"`
	LastRetirementNS  int64   `json:"last_retirement_ns,omitempty"`
	LastWALNS         int64   `json:"last_wal_completion_ns,omitempty"`
	WALIdleMS         float64 `json:"wal_idle_after_retirement_ms"`
}

// AnalyzeAdmissionTrace correlates blocked SETs with ticket retirements and WAL progress.
func AnalyzeAdmissionTrace(timeline io.Reader, batch, fullWaveTickets, limit int) (AdmissionTrace, error) {
	return AnalyzeAdmissionTraceWithScope(timeline, batch, fullWaveTickets, limit, "thread")
}

// AnalyzeAdmissionTraceWithScope counts ticket retirements on the waiting thread or the entire leader.
func AnalyzeAdmissionTraceWithScope(timeline io.Reader, batch, fullWaveTickets, limit int, scope string) (AdmissionTrace, error) {
	var result AdmissionTrace
	if scope != "thread" && scope != "leader" {
		return result, errors.New("admission-wait scope must be thread or leader")
	}
	if batch < 0 || fullWaveTickets <= 0 || limit < 0 {
		return result, errors.New("admission analysis requires a positive ticket count and nonnegative batch and limit")
	}
	events, err := readTraceEvents(timeline)
	if err != nil {
		return result, err
	}
	var batches, waits, wal, writes []Event
	retirements := map[int64][]int64{}
	ticketGroup := func(tid int64) int64 {
		if scope == "leader" {
			return 0
		}
		return tid
	}
	leader := ""
	for _, e := range events {
		if e.Kind != "X" {
			continue
		}
		if e.Cat == "client" && e.Name == "client.pipeline" {
			batches = append(batches, e)
		}
		if e.Cat != "leader" {
			continue
		}
		if leader != "" && e.Node != leader {
			return result, errors.New("admission analysis requires one leader")
		}
		leader = e.Node
		switch e.Name {
		case "raft.set.admission.wait":
			waits = append(waits, e)
		case "raft.set.ticket_lifetime":
			group := ticketGroup(e.TID)
			retirements[group] = append(retirements[group], traceEnd(e))
		case "raft.wal.append.complete":
			wal = append(wal, e)
		case "raft.folly.write":
			writes = append(writes, e)
		}
	}
	if len(wal) == 0 {
		wal = writes
	}
	if len(batches) == 0 || len(wal) == 0 || len(retirements) == 0 {
		return result, errors.New("admission analysis requires pipeline, ticket and WAL spans")
	}
	if batch > len(batches) {
		return result, fmt.Errorf("batch must be between 1 and %d", len(batches))
	}
	slices.SortFunc(batches, func(a, b Event) int { return cmp.Compare(a.Time, b.Time) })
	for i, b := range batches {
		if i > 0 && traceEnd(batches[i-1]) > traceNS(b.Time) {
			return result, errors.New("admission analysis requires nonoverlapping client batches")
		}
	}
	slices.SortFunc(wal, func(a, b Event) int { return cmp.Compare(traceEnd(a), traceEnd(b)) })
	for _, ends := range retirements {
		slices.Sort(ends)
	}
	result = AdmissionTrace{
		TicketScope: scope, WALStage: wal[0].Name, FullWaveTickets: fullWaveTickets, Retirements: map[int]int{},
		Longest: []AdmissionWait{},
	}
	for _, span := range waits {
		begin, end := traceNS(span.Time), traceEnd(span)
		i := sort.Search(len(batches), func(i int) bool { return traceEnd(batches[i]) >= end })
		if i == len(batches) || traceNS(batches[i].Time) > begin || end < begin {
			return result, errors.New("admission wait lies outside a client batch")
		}
		if batch > 0 && batch != i+1 {
			continue
		}
		tickets := retirements[ticketGroup(span.TID)]
		first, _ := slices.BinarySearch(tickets, begin)
		past := sort.Search(len(tickets), func(i int) bool { return tickets[i] > end })
		wait := AdmissionWait{Batch: i + 1, BeginNS: begin, EndNS: end, Retired: past - first}
		if wait.Retired > 0 {
			wait.FirstRetirementNS, wait.LastRetirementNS = tickets[first], tickets[past-1]
		}
		lastWAL := sort.Search(len(wal), func(i int) bool { return traceEnd(wal[i]) > end }) - 1
		if lastWAL >= 0 && traceEnd(wal[lastWAL]) >= traceNS(batches[i].Time) {
			wait.LastWALNS = traceEnd(wal[lastWAL])
			if wait.Retired > 0 {
				wait.WALIdleMS = float64(end-max(wait.FirstRetirementNS, wait.LastWALNS)) / 1e6
			}
		}
		result.Waits++
		result.Retirements[wait.Retired]++
		if wait.Retired == fullWaveTickets {
			result.FullWaveWaits++
			result.FullWaveWALIdleMS += wait.WALIdleMS
		}
		result.Longest = append(result.Longest, wait)
	}
	slices.SortStableFunc(result.Longest, func(a, b AdmissionWait) int {
		return cmp.Compare(b.EndNS-b.BeginNS, a.EndNS-a.BeginNS)
	})
	result.Longest = result.Longest[:min(limit, len(result.Longest))]
	return result, nil
}
