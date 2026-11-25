// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package trace

import (
	"cmp"
	"io"
	"math"
	"slices"
)

type FetchRequest struct {
	NextReport *Event `json:"next_report,omitempty"`
	FetchDelay
	Number        int    `json:"number"`
	Append        *Event `json:"append,omitempty"`
	Write         *Event `json:"write,omitempty"`
	Appended      *Event `json:"appended,omitempty"`
	ClientBatches []int  `json:"client_batches,omitempty"`
}

// AnalyzeFetchRequests pairs data-group Fetch RPCs and receive spans in one capture.
// Only unambiguous pairs from the same follower are considered processed requests.
func AnalyzeFetchRequests(timeline io.Reader) ([]FetchRequest, error) {
	events, err := readTraceEvents(timeline)
	if err != nil {
		return nil, err
	}
	requests, _ := collectFetchRequests(events)
	return requests, nil
}

func collectFetchRequests(events []Event) ([]FetchRequest, []Event) {
	var batches []Event
	var firstWAL Event
	nodes := map[string]map[string][]Event{}
	for _, e := range events {
		if e.Kind != "X" || e.Dur < 0 {
			continue
		}
		switch {
		case e.Cat == "client" && e.Name == "client.pipeline":
			batches = append(batches, e)
		case e.Cat == "leader" && e.Name == "raft.wal.append.complete":
			if firstWAL.Name == "" || e.Time < firstWAL.Time {
				firstWAL = e
			}
		case e.Cat == "follower":
			switch e.Name {
			case "raft.fetch.rpc", "raft.fetch.receive", "raft.fetch.appended", "raft.fetch.report",
				"raft.log.append", "raft.log.pwrite", "raft.await_io", "raft.folly.fsync":
				if nodes[e.Node] == nil {
					nodes[e.Node] = map[string][]Event{}
				}
				nodes[e.Node][e.Name] = append(nodes[e.Node][e.Name], e)
			}
		}
	}
	slices.SortStableFunc(batches, compareEventStart)
	requests := []FetchRequest{}
	for _, node := range nodes {
		requests = appendFetchRequests(requests, node, firstWAL, batches)
	}
	slices.SortFunc(requests, func(a, b FetchRequest) int {
		if n := compareEventStart(a.RPC, b.RPC); n != 0 {
			return n
		}
		return cmp.Compare(a.Node, b.Node)
	})
	for i := range requests {
		requests[i].Number = i + 1
	}
	return requests, batches
}

func compareEventStart(a, b Event) int {
	return cmp.Compare(a.Time, b.Time)
}

func appendFetchRequests(requests []FetchRequest, node map[string][]Event, firstWAL Event, batches []Event) []FetchRequest {
	rpcs, receives := node["raft.fetch.rpc"], node["raft.fetch.receive"]
	slices.SortStableFunc(rpcs, compareEventStart)
	slices.SortStableFunc(receives, compareEventStart)
	rpcCounts := make(map[int64]int)
	receivesBySequence := make(map[int64][]int)
	for _, rpc := range rpcs {
		rpcCounts[rpc.Args["arg1"]]++
	}
	for i, rx := range receives {
		receivesBySequence[rx.Args["arg1"]] = append(receivesBySequence[rx.Args["arg1"]], i)
	}
	var cursor int
	var previousEnd int64 = math.MinInt64
	for i, rpc := range rpcs {
		begin, end := traceNS(rpc.Time), traceEnd(rpc)
		overlapsPrevious := begin < previousEnd
		previousEnd = max(previousEnd, end)
		next := int64(math.MaxInt64)
		if i+1 < len(rpcs) {
			next = traceNS(rpcs[i+1].Time)
		}
		var rx Event
		if seq := rpc.Args["arg1"]; seq > 0 {
			indices := receivesBySequence[seq]
			if rpcCounts[seq] != 1 || len(indices) != 1 {
				continue
			}
			pos := indices[0]
			rx = receives[pos]
			if traceNS(rx.Time) < end {
				continue
			}
			next = math.MaxInt64
			if pos+1 < len(receives) {
				next = traceNS(receives[pos+1].Time)
			}
		} else {
			if overlapsPrevious || end >= next {
				continue
			}
			for cursor < len(receives) && traceNS(receives[cursor].Time) < end {
				cursor++
			}
			if cursor == len(receives) {
				continue
			}
			rx = receives[cursor]
			if rx.Args["arg1"] != 0 || traceNS(rx.Time) >= next || traceEnd(rx) > next ||
				(cursor+1 < len(receives) && traceNS(receives[cursor+1].Time) < next) {
				continue
			}
			cursor++
		}

		request := FetchRequest{
			FetchDelay: measureFetchDelay(rpc, rx, node["raft.await_io"], node["raft.folly.fsync"]),
		}
		if appendEvent, ok := nestedFetchEvent(node["raft.log.append"], rx); ok {
			request.Append = &appendEvent
			request.PayloadBytes = appendEvent.Args["arg0"]
			if write, ok := nestedFetchEvent(node["raft.log.pwrite"], appendEvent); ok {
				request.Write = &write
				request.recordWrite(request.PayloadBytes, write.Args["arg0"])
			}
		}
		request.Fsync = request.Fsync[:0]
		for _, sync := range node["raft.folly.fsync"] {
			if traceNS(sync.Time) < traceEnd(rx) && traceEnd(sync) > begin {
				request.Fsync = append(request.Fsync, sync)
			}
		}
		request.Appended = fetchAppendMarker(rx, next, node["raft.fetch.appended"])
		if request.Appended != nil {
			request.NextReport = nextFetchReport(*request.Appended, rpc, node["raft.fetch.report"])
			request.FirstOffset = request.Appended.Args["arg0"] + 1
			request.LastOffset = request.Appended.Args["arg1"]
		}
		base, hasBase := firstWAL.Args["arg0"]
		if hasBase && request.FirstOffset > 0 {
			for i, batch := range batches {
				start, count := batch.Args["arg0"], batch.Args["arg1"]
				if base >= 0 && start >= 0 && count > 0 && start <= math.MaxInt64-base &&
					count <= math.MaxInt64-base-start && base+start < request.LastOffset &&
					base+start+count >= request.FirstOffset {
					request.ClientBatches = append(request.ClientBatches, i+1)
				}
			}
		}
		requests = append(requests, request)
	}
	return requests
}

func fetchAppendMarker(receive Event, nextRPC int64, appends []Event) *Event {
	start, ok := receive.Args["arg0"]
	if !ok || start < 0 {
		return nil
	}
	var match *Event
	for _, marker := range appends {
		when := traceNS(marker.Time)
		base, hasBase := marker.Args["arg0"]
		if marker.TID != receive.TID || when < traceEnd(receive) || when >= nextRPC ||
			!hasBase || base != start || marker.Args["arg1"] <= start {
			continue
		}
		if match != nil {
			return nil
		}
		match = &marker
	}
	return match
}
