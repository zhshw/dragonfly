// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package trace

import (
	"cmp"
	"errors"
	"io"
	"slices"
	"strconv"

	"dragonflydb.io/profile/pkg/otel"
)

type FetchWait struct {
	FetchRequest
	IOMS          float64         `json:"io_ms"`
	SyncOverlapMS float64         `json:"sync_overlap_ms"`
	SyncOverlap   []otel.Interval `json:"sync_overlap_ns,omitempty"`
	Offset        int64           `json:"offset,omitempty"`
	ReplyGap      *ReplyGap       `json:"reply_gap,omitempty"`
	LeaderWAL     *Event          `json:"leader_wal,omitempty"`
	LeaderHW      *Event          `json:"leader_hw,omitempty"`
	LeaderReply   *Event          `json:"leader_reply,omitempty"`
}

type FetchWaitReport struct {
	Round        *FetchCounts `json:"round,omitempty"`
	RoundError   string       `json:"round_error,omitempty"`
	Order        string       `json:"order"`
	HasReplyGaps bool         `json:"has_reply_gaps"`
	Scope        string       `json:"scope"`
	BatchAligned bool         `json:"batch_aligned"`
	Batch        int          `json:"batch,omitempty"`
	BatchMS      float64      `json:"batch_ms,omitempty"`
	Matched      int          `json:"matched"`
	Requests     []FetchWait  `json:"requests"`
}

// AnalyzeFetchWaits ranks data-bearing Fetches in the full round or the slowest batch.
// Without offset telemetry only the round-wide, same-follower timing can be attributed.
func AnalyzeFetchWaits(timeline io.Reader, scope, order string) (FetchWaitReport, error) {
	report := FetchWaitReport{Scope: scope, Order: order, Requests: []FetchWait{}}
	if (scope != "slowest" && scope != "round") || !slices.Contains([]string{"gap", "io", "reply", "overlap"}, order) {
		return report, errors.New("scope must be slowest or round and sort must be gap, io, reply or overlap")
	}
	events, err := readTraceEvents(timeline)
	if err != nil {
		return report, err
	}
	round, roundErr := rankFetchDelays(events, 0, 0, true)
	if roundErr != nil {
		report.RoundError = roundErr.Error()
	} else {
		report.Round = &round.FetchCounts
	}
	requests, batches := collectFetchRequests(events)
	report.BatchAligned = slices.ContainsFunc(requests, func(r FetchRequest) bool { return len(r.ClientBatches) > 0 })
	var gaps []ReplyGap
	if scope == "slowest" && report.BatchAligned {
		report.Batch, _ = selectTraceBatch(batches, 0)
		report.BatchMS = batches[report.Batch-1].Dur / 1000

		// Reply gaps are independent evidence. Older captures can lack the milestones they need.
		path, err := analyzeRequestTrace(events, RequestTraceQuery{FetchPhases: true, Slow: true, Batch: report.Batch, Limit: len(events)})
		if err == nil {
			gaps = path.Slow.ReplyGaps
		}
	} else {
		report.Scope = "round"
	}
	report.HasReplyGaps = len(gaps) > 0
	if order == "reply" && !report.HasReplyGaps {
		report.Order = "gap"
	}
	for _, r := range requests {
		if (r.Appended == nil && r.Append == nil) || (report.Batch != 0 && !slices.Contains(r.ClientBatches, report.Batch)) {
			continue
		}
		wait := FetchWait{FetchRequest: r, Offset: r.FirstOffset}
		wait.measureIO()
		if order == "io" && wait.IOMS == 0 {
			continue
		}
		for i := range gaps {
			gap := &gaps[i]
			if r.FirstOffset <= gap.Offset && gap.Offset <= r.LastOffset {
				wait.Offset, wait.ReplyGap = gap.Offset, gap
				break
			}
		}
		report.Requests = append(report.Requests, wait)
	}
	slices.SortStableFunc(report.Requests, func(a, b FetchWait) int {
		if order == "overlap" && a.SyncOverlapMS != b.SyncOverlapMS {
			return cmp.Compare(b.SyncOverlapMS, a.SyncOverlapMS)
		}
		if report.Order == "reply" {
			if n := cmp.Compare(replyGapNS(b.ReplyGap), replyGapNS(a.ReplyGap)); n != 0 {
				return n
			}
		}
		if order == "io" && a.IOMS != b.IOMS {
			return cmp.Compare(b.IOMS, a.IOMS)
		}
		return cmp.Compare(b.RPCToReceiveMS, a.RPCToReceiveMS)
	})
	report.Matched = len(report.Requests)
	report.Requests = report.Requests[:min(10, report.Matched)]
	for i := range report.Requests {
		report.Requests[i].matchLeader(events)
	}
	return report, nil
}

func replyGapNS(g *ReplyGap) int64 {
	if g == nil {
		return 0
	}
	return g.EndNS - g.BeginNS
}

func (r *FetchWait) measureIO() {
	var ioWaits, overlaps []otel.Interval
	for _, wait := range r.AwaitIO {
		start := max(r.RPCReturnNS, traceNS(wait.Time))
		end := min(r.ReceiveStartNS, traceEnd(wait))
		ioWaits = append(ioWaits, otel.Interval{Begin: start, End: end})
		for _, sync := range r.Fsync {
			lo, hi := max(start, traceNS(sync.Time)), min(end, traceEnd(sync))
			if lo < hi {
				overlaps = append(overlaps, otel.Interval{Begin: lo, End: hi})
			}
		}
	}
	r.IOMS = otel.UnionMS(ioWaits)
	r.SyncOverlap = otel.Merged(overlaps)
	r.SyncOverlapMS = otel.UnionMS(r.SyncOverlap)
}

func (r *FetchWait) matchLeader(events []Event) {
	peer, ok := r.RPC.Args["arg0"]
	if !ok || r.Appended == nil {
		return
	}
	leader := strconv.FormatInt(peer, 10)
	var wal, hw, replies []*Event
	for i := range events {
		e := &events[i]
		if e.Cat != "leader" || e.Node != leader || e.Kind != "X" || e.Dur < 0 {
			continue
		}
		base, ok := e.Args["arg0"]
		if !ok || base >= r.Offset || e.Args["arg1"] < r.Offset {
			continue
		}
		switch e.Name {
		case "raft.wal.append.complete":
			if traceEnd(*e) <= r.RPCReturnNS {
				wal = append(wal, e)
			}
		case "raft.hw.advance":
			if traceNS(e.Time) >= traceEnd(*r.Appended) {
				hw = append(hw, e)
			}
		case "raft.reply.advance":
			if traceNS(e.Time) >= traceEnd(*r.Appended) {
				replies = append(replies, e)
			}
		}
	}
	if len(wal) == 1 {
		r.LeaderWAL = wal[0]
	}
	if len(hw) == 1 {
		r.LeaderHW = hw[0]
	}
	if len(replies) == 1 && (r.LeaderHW == nil || traceNS(replies[0].Time) >= traceEnd(*r.LeaderHW)) {
		r.LeaderReply = replies[0]
	}
}
