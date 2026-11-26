// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package trace

import (
	"cmp"
	"errors"
	"io"
	"math"
	"slices"
	"sort"

	"dragonflydb.io/profile/pkg/otel"
)

type ApplyConfirmation struct {
	Confirmations      int              `json:"confirmations"`
	Matched            int              `json:"matched"`
	ApplyBatches       int              `json:"apply_batches"`
	MinBatchWrites     int              `json:"min_batch_writes"`
	MaxBatchWrites     int              `json:"max_batch_writes"`
	PostApply          ConfirmationTime `json:"post_apply"`
	PipelineWindows    int              `json:"pipeline_windows"`
	PipelineTails      int              `json:"pipeline_tails"`
	PipelineTailSumUS  float64          `json:"pipeline_tail_sum_us"`
	PipelineTailMaxUS  float64          `json:"pipeline_tail_max_us"`
	WorkerQueueInApply float64          `json:"worker_queue_in_apply_ms"`
	Tickets            int              `json:"tickets"`
	PeakInFlight       int              `json:"peak_in_flight"`
}

type ConfirmationTime struct {
	P50MS   float64 `json:"p50_ms"`
	P95MS   float64 `json:"p95_ms"`
	MaxMS   float64 `json:"max_ms"`
	UnionMS float64 `json:"union_ms"`
	Over1MS int     `json:"over_1ms"`
}

type offsetInterval struct {
	otel.Interval
	first, last int64
}

// AnalyzeApplyConfirmation joins leader confirmations to the apply range covering each offset.
func AnalyzeApplyConfirmation(timeline io.Reader) (ApplyConfirmation, error) {
	events, err := readTraceEvents(timeline)
	if err != nil {
		return ApplyConfirmation{}, err
	}
	var applies, confirmations, pipeline []offsetInterval
	var applyTimes, workerQueue, tickets []otel.Interval
	for _, event := range events {
		if event.Kind != "X" || (event.Cat != "leader" && event.Cat != "client") {
			continue
		}
		timing := otel.Interval{
			Begin: traceNS(event.Time),
			End:   traceEnd(event),
		}
		if timing.Begin < 0 || timing.End < timing.Begin {
			return ApplyConfirmation{}, errors.New("invalid timeline interval")
		}
		arg0, hasArg0 := event.Args["arg0"]
		arg1, hasArg1 := event.Args["arg1"]
		if event.Cat == "client" {
			if event.Name == "client.pipeline" {
				if !hasArg0 || !hasArg1 || arg0 < 0 || arg1 <= 0 || arg0 > math.MaxInt64-arg1 {
					return ApplyConfirmation{}, errors.New("invalid client pipeline write range")
				}
				pipeline = append(pipeline, offsetInterval{timing, arg0, arg0 + arg1})
			}
			continue
		}
		switch event.Name {
		case "raft.apply":
			if !hasArg0 || !hasArg1 || arg1 <= arg0 {
				return ApplyConfirmation{}, errors.New("invalid leader apply offset range")
			}
			applies = append(applies, offsetInterval{timing, arg0, arg1})
			applyTimes = append(applyTimes, timing)
		case "raft.set.await_apply":
			if !hasArg0 {
				return ApplyConfirmation{}, errors.New("missing confirmation offset")
			}
			confirmations = append(confirmations, offsetInterval{timing, arg0, arg0})
		case "raft.worker.queue":
			workerQueue = append(workerQueue, timing)
		case "raft.set.ticket_lifetime":
			tickets = append(tickets, timing)
		}
	}
	if len(applies) == 0 || len(confirmations) == 0 {
		return ApplyConfirmation{}, errors.New("timeline has no leader apply or confirmation events")
	}
	slices.SortFunc(applies, func(a, b offsetInterval) int { return cmp.Compare(a.last, b.last) })
	slices.SortFunc(confirmations, func(a, b offsetInterval) int { return cmp.Compare(a.last, b.last) })
	for i := 1; i < len(applies); i++ {
		if applies[i-1].last > applies[i].first {
			return ApplyConfirmation{}, errors.New("overlapping leader apply offset ranges")
		}
	}
	for i := 1; i < len(confirmations); i++ {
		if confirmations[i-1].last == confirmations[i].last {
			return ApplyConfirmation{}, errors.New("duplicate confirmation offset")
		}
	}

	result := ApplyConfirmation{
		Confirmations: len(confirmations), PipelineWindows: len(pipeline), Tickets: len(tickets),
		WorkerQueueInApply: otel.IntersectionMS(workerQueue, applyTimes), PeakInFlight: peakOverlaps(tickets),
	}
	batchWrites := make(map[int]int)
	postApply := make([]otel.Interval, 0, len(confirmations))
	delays := make([]int64, 0, len(confirmations))
	for _, confirmation := range confirmations {
		index := sort.Search(len(applies), func(i int) bool { return applies[i].last >= confirmation.last })
		if index == len(applies) || confirmation.last <= applies[index].first || confirmation.End < applies[index].End {
			continue
		}
		batchWrites[index]++
		delay := confirmation.End - applies[index].End
		delays = append(delays, delay)
		postApply = append(postApply, otel.Interval{Begin: applies[index].End, End: confirmation.End})
	}
	if len(delays) == 0 {
		return ApplyConfirmation{}, errors.New("no confirmations matched a completed apply range")
	}
	result.Matched, result.ApplyBatches = len(delays), len(batchWrites)
	result.MinBatchWrites = result.Matched
	for _, writes := range batchWrites {
		result.MinBatchWrites = min(result.MinBatchWrites, writes)
		result.MaxBatchWrites = max(result.MaxBatchWrites, writes)
	}
	if result.Matched == result.Confirmations {
		result.measurePipelineTails(pipeline, confirmations, delays)
	}
	slices.Sort(delays)
	p95 := int(math.Ceil(.95*float64(len(delays)))) - 1
	result.PostApply = ConfirmationTime{
		P50MS:   float64(delays[(len(delays)-1)/2]) / 1e6,
		P95MS:   float64(delays[p95]) / 1e6,
		MaxMS:   float64(delays[len(delays)-1]) / 1e6,
		UnionMS: otel.UnionMS(postApply),
		Over1MS: len(delays) - sort.Search(len(delays), func(i int) bool { return delays[i] > 1e6 }),
	}
	return result, nil
}

func (r *ApplyConfirmation) measurePipelineTails(pipeline, confirmations []offsetInterval, delays []int64) {
	slices.SortFunc(pipeline, func(a, b offsetInterval) int { return cmp.Compare(a.first, b.first) })
	if len(pipeline) == 0 || pipeline[len(pipeline)-1].last != int64(len(confirmations)) {
		return
	}
	for i, confirmation := range confirmations {
		if i > 0 && confirmation.last-1 != confirmations[i-1].last {
			return
		}
	}
	var cursor int64
	for _, batch := range pipeline {
		if batch.first != cursor {
			return
		}
		cursor = batch.last
	}
	for _, batch := range pipeline {
		index := int(batch.last - 1)
		end := confirmations[index].End
		if batch.Begin <= end && end <= batch.End {
			r.PipelineTails++
			micros := float64(delays[index]) / 1000
			r.PipelineTailSumUS += micros
			r.PipelineTailMaxUS = max(r.PipelineTailMaxUS, micros)
		}
	}
}

func peakOverlaps(intervals []otel.Interval) int {
	var starts, ends []int64
	for _, interval := range intervals {
		if interval.Begin < interval.End {
			starts = append(starts, interval.Begin)
			ends = append(ends, interval.End)
		}
	}
	slices.Sort(starts)
	slices.Sort(ends)
	peak, finished := 0, 0
	for i, start := range starts {
		for ends[finished] <= start {
			finished++
		}
		peak = max(peak, i+1-finished)
	}
	return peak
}
