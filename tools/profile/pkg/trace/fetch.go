// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package trace

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"slices"
)

type FetchCounts struct {
	Fetches               int `json:"fetches"`
	Partial               int `json:"partial"`
	AwaitFsyncOverlaps    int `json:"await_fsync_overlaps"`
	GapsOver1MS           int `json:"gaps_gt_1ms"`
	PartialBlockedByFlush int `json:"partial_blocked_by_flush"`
}

type FetchDelayReport struct {
	FetchCounts
	Batch  int          `json:"batch,omitempty"`
	Delays []FetchDelay `json:"delays"`
}

type FetchDelay struct {
	Node                  string  `json:"node"`
	FirstOffset           int64   `json:"first_offset"`
	LastOffset            int64   `json:"last_offset"`
	RPCReturnNS           int64   `json:"rpc_return_ns"`
	ReceiveStartNS        int64   `json:"receive_start_ns"`
	RPCToReceiveMS        float64 `json:"rpc_to_receive_ms"`
	PayloadBytes          int64   `json:"payload_bytes"`
	AppendedBytes         int64   `json:"appended_bytes"`
	RPC                   Event   `json:"rpc"`
	Receive               Event   `json:"receive"`
	AwaitIO               []Event `json:"await_io"`
	Fsync                 []Event `json:"fsync"`
	AwaitFsync            bool    `json:"await_io_overlaps_fsync"`
	PartialBlockedByFlush bool    `json:"partial_blocked_by_flush"`
}

// AnalyzeFetchDelay ranks RPC-to-receive delays for Fetches that cover the selected batch.
func AnalyzeFetchDelay(timeline io.Reader, batch, limit int) (FetchDelayReport, error) {
	if batch < 0 || limit < 0 {
		return FetchDelayReport{}, errors.New("batch and limit must be nonnegative")
	}
	events, err := readTraceEvents(timeline)
	if err != nil {
		return FetchDelayReport{}, err
	}
	return analyzeFetchDelay(events, batch, limit)
}

// AnalyzeRoundFetchDelay ranks all data-bearing Fetches, including those outside the slowest batch.
func AnalyzeRoundFetchDelay(timeline io.Reader, limit int) (FetchDelayReport, error) {
	if limit < 0 {
		return FetchDelayReport{}, errors.New("limit must be nonnegative")
	}
	events, err := readTraceEvents(timeline)
	if err != nil {
		return FetchDelayReport{}, err
	}
	return rankFetchDelays(events, 0, limit, true)
}

func analyzeFetchDelay(events []Event, batch, limit int) (FetchDelayReport, error) {
	return rankFetchDelays(events, batch, limit, false)
}

func rankFetchDelays(events []Event, batch, limit int, round bool) (FetchDelayReport, error) {
	var batches, wal []Event
	follower := map[string][]Event{}
	for _, e := range events {
		if e.Kind != "X" {
			continue
		}
		switch {
		case e.Cat == "client" && e.Name == "client.pipeline":
			batches = append(batches, e)
		case e.Cat == "leader" && e.Name == "raft.wal.append.complete":
			wal = append(wal, e)
		case e.Cat == "follower":
			follower[e.Name] = append(follower[e.Name], e)
		}
	}
	appends := follower["raft.fetch.appended"]
	if len(appends) == 0 {
		return FetchDelayReport{}, errors.New("Fetch analysis requires follower raft.fetch.appended offset events")
	}
	var first, last int64
	if !round {
		if len(batches) == 0 || len(wal) == 0 {
			return FetchDelayReport{}, errors.New("batch Fetch analysis requires pipeline and WAL spans")
		}
		slices.SortFunc(batches, func(a, b Event) int { return cmp.Compare(a.Time, b.Time) })
		selected, err := selectTraceBatch(batches, batch)
		if err != nil {
			return FetchDelayReport{}, err
		}
		batch = selected
		base := slices.MinFunc(wal, func(a, b Event) int { return cmp.Compare(a.Time, b.Time) }).Args["arg0"]
		first = base + batches[batch-1].Args["arg0"] + 1
		last = first + batches[batch-1].Args["arg1"] - 1
	}

	report := FetchDelayReport{Batch: batch, Delays: []FetchDelay{}}
	for _, app := range appends {
		if !round && (app.Args["arg0"] >= last || app.Args["arg1"] < first) {
			continue
		}
		rpc, rx, err := fetchPair(app, follower["raft.fetch.receive"], follower["raft.fetch.rpc"])
		if err != nil {
			return report, err
		}
		logAppend, hasAppend := nestedFetchEvent(follower["raft.log.append"], rx)
		write, hasWrite := nestedFetchEvent(follower["raft.log.pwrite"], logAppend)
		if !hasAppend || !hasWrite {
			return report, fmt.Errorf("missing Follower WAL append or write at offset %d", app.Args["arg0"]+1)
		}
		delay := measureFetchDelay(rpc, rx, follower["raft.await_io"], follower["raft.folly.fsync"])
		delay.FirstOffset, delay.LastOffset = app.Args["arg0"]+1, app.Args["arg1"]
		delay.recordWrite(logAppend.Args["arg0"], write.Args["arg0"])
		if delay.AppendedBytes < delay.PayloadBytes {
			report.Partial++
		}
		if delay.AwaitFsync {
			report.AwaitFsyncOverlaps++
		}
		if delay.ReceiveStartNS-delay.RPCReturnNS > 1_000_000 {
			report.GapsOver1MS++
			if delay.PartialBlockedByFlush {
				report.PartialBlockedByFlush++
			}
		}
		report.Delays = append(report.Delays, delay)
	}
	report.Fetches = len(report.Delays)
	slices.SortStableFunc(report.Delays, func(a, b FetchDelay) int { return cmp.Compare(b.RPCToReceiveMS, a.RPCToReceiveMS) })
	report.Delays = report.Delays[:min(limit, len(report.Delays))]
	return report, nil
}

func measureFetchDelay(rpc, rx Event, awaits, syncs []Event) FetchDelay {
	begin, end := traceEnd(rpc), traceNS(rx.Time)
	delay := FetchDelay{
		Node: rpc.Node, RPC: rpc, Receive: rx, RPCReturnNS: begin, ReceiveStartNS: end,
		RPCToReceiveMS: float64(end-begin) / 1e6,
	}
	for _, e := range awaits {
		if e.Node == rpc.Node && e.TID == rpc.TID && traceNS(e.Time) < end && traceEnd(e) > begin {
			delay.AwaitIO = append(delay.AwaitIO, e)
		}
	}
	for _, e := range syncs {
		if e.Node != rpc.Node || traceNS(e.Time) >= end || traceEnd(e) <= begin {
			continue
		}
		delay.Fsync = append(delay.Fsync, e)
		for _, wait := range delay.AwaitIO {
			if max(begin, traceNS(wait.Time), traceNS(e.Time)) < min(end, traceEnd(wait), traceEnd(e)) {
				delay.AwaitFsync = true
			}
		}
	}
	return delay
}

func (d *FetchDelay) recordWrite(payload, written int64) {
	d.PayloadBytes, d.AppendedBytes = payload, written

	// The historical query tests both events against the gap independently.
	d.PartialBlockedByFlush = d.ReceiveStartNS-d.RPCReturnNS > 1_000_000 && written < payload &&
		len(d.AwaitIO) > 0 && len(d.Fsync) > 0
}

func nextFetchReport(app, rpc Event, reports []Event) *Event {
	var next *Event
	for i := range reports {
		e := &reports[i]
		if e.Node == app.Node && e.Args["arg1"] == rpc.Args["arg0"] && e.Args["arg0"] >= app.Args["arg1"] &&
			e.Time >= app.Time && (next == nil || e.Time < next.Time) {
			next = e
		}
	}
	return next
}

func fetchPair(app Event, receives, rpcs []Event) (Event, Event, error) {
	var rx, rpc Event
	for _, e := range receives {
		if e.Node == app.Node && e.TID == app.TID && traceEnd(e) <= traceNS(app.Time) &&
			(rx.Name == "" || traceEnd(e) > traceEnd(rx)) {
			rx = e
		}
	}
	if rx.Name == "" || rx.Args["arg0"] != app.Args["arg0"] {
		return rpc, rx, fmt.Errorf("no receive span matches the Fetch at offset %d", app.Args["arg0"]+1)
	}
	for _, e := range rpcs {
		if e.Node == rx.Node && e.Args["arg1"] == rx.Args["arg1"] && traceEnd(e) <= traceNS(rx.Time) &&
			(rpc.Name == "" || traceEnd(e) > traceEnd(rpc)) {
			rpc = e
		}
	}
	if rpc.Name == "" {
		return rpc, rx, fmt.Errorf("missing Fetch RPC for receive at offset %d", app.Args["arg0"]+1)
	}
	return rpc, rx, nil
}

func nestedFetchEvent(events []Event, parent Event) (Event, bool) {
	for _, e := range events {
		if e.Node == parent.Node && e.TID == parent.TID && e.Time >= parent.Time && traceEnd(e) <= traceEnd(parent) {
			return e, true
		}
	}
	return Event{}, false
}
