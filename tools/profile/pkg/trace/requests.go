// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package trace

import (
	"cmp"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
	"sort"
	"strings"
)

var requestPhases = [...]string{
	"send-to-group", "group-to-wal", "wal-to-follower-append",
	"follower-append-to-hw", "hw-to-reply", "reply-to-client",
}
var requestPhasesWithoutWatermarks = []string{"send-to-group", "group-to-reply", "reply-to-client"}

var replicationPhases = [...]string{
	"send-to-group", "group-to-wal", "wal-to-rpc-start", "rpc-remaining", "rpc-end-to-receive",
	"receive-to-append", "append-to-report", "report-to-hw", "hw-to-reply", "reply-to-client",
}

type RequestTraceQuery struct {
	FetchPhases bool
	Slow        bool
	ByWrites    bool
	Batch       int
	Limit       int
	Role, Stage string
}

type RequestPhase struct {
	Name         string  `json:"phase"`
	MS           float64 `json:"ms"`
	Percent      float64 `json:"percent"`
	Writes       int     `json:"writes"`
	WritePercent float64 `json:"write_percent"`
}

type RequestTrace struct {
	Batches int             `json:"batches"`
	Writes  int             `json:"writes"`
	SetMS   float64         `json:"set_ms"`
	WaitMS  *float64        `json:"wait_ms,omitempty"`
	Phases  []RequestPhase  `json:"phases"`
	Slow    *SlowBatchTrace `json:"slow_path,omitempty"`

	replyGapStats ReplyGapStats
}

type SlowBatchTrace struct {
	Batch       int        `json:"batch"`
	BeginNS     int64      `json:"begin_ns"`
	EndNS       int64      `json:"end_ns"`
	ReplyGaps   []ReplyGap `json:"reply_gaps"`
	SpanMatches int        `json:"span_matches,omitempty"`
	Spans       []Event    `json:"spans,omitempty"`
}

type RequestFetch struct {
	FirstOffset    int64 `json:"first_offset"`
	LastOffset     int64 `json:"last_offset"`
	RPCStartNS     int64 `json:"rpc_start_ns"`
	RPCReturnNS    int64 `json:"rpc_return_ns"`
	ReceiveStartNS int64 `json:"receive_start_ns"`
	ReportNS       int64 `json:"report_ns"`
}

type ReplyGap struct {
	Fetch   *RequestFetch  `json:"fetch,omitempty"`
	Offset  int64          `json:"offset"`
	BeginNS int64          `json:"begin_ns"`
	EndNS   int64          `json:"end_ns"`
	Phases  []RequestPhase `json:"phases"`
}

type phaseTime struct {
	ns     int64
	writes int
}

type phaseTimes [len(replicationPhases)]phaseTime

func (p phaseTimes) rows(names []string, totalNS int64, writes int) []RequestPhase {
	rows := make([]RequestPhase, 0, len(p))
	for i, name := range names {
		phase := p[i]
		if phase.ns > 0 {
			rows = append(rows, RequestPhase{
				Name: name, MS: float64(phase.ns) / 1e6,
				Percent: 100 * float64(phase.ns) / float64(totalNS), Writes: phase.writes,
				WritePercent: 100 * float64(phase.writes) / float64(writes),
			})
		}
	}
	return rows
}

// AnalyzeRequestTrace measures the wait for successive ordered SET replies.
// Each interval is divided at the awaited write's milestones, so pipelining does not double count time.
func AnalyzeRequestTrace(timeline io.Reader, q RequestTraceQuery) (RequestTrace, error) {
	if q.Batch < 0 || q.Limit < 0 {
		return RequestTrace{}, errors.New("batch and limit must be nonnegative")
	}
	events, err := readTraceEvents(timeline)
	if err != nil {
		return RequestTrace{}, err
	}
	return analyzeRequestTrace(events, q)
}

func analyzeRequestTrace(events []Event, q RequestTraceQuery) (RequestTrace, error) {
	stages := map[string][]Event{}
	for _, e := range events {
		if e.Kind == "X" {
			stages[e.Cat+"/"+e.Name] = append(stages[e.Cat+"/"+e.Name], e)
		}
	}
	batches := stages["client/client.pipeline"]
	ready := stages["leader/raft.set.admission_queue"]
	reply := stages["leader/raft.reply.advance"]
	ranges := [][]Event{
		stages["leader/raft.wal.append.complete"], stages["follower/raft.fetch.appended"],
		stages["leader/raft.hw.advance"], reply,
	}
	if len(batches) == 0 || len(ready) == 0 || len(reply) == 0 {
		return RequestTrace{}, errors.New("request analysis requires Raft pipeline, admission and reply spans")
	}
	names := requestPhases[:]
	var fetches map[int64]*RequestFetch
	if slices.ContainsFunc(ranges, func(events []Event) bool { return len(events) == 0 }) {
		ranges = [][]Event{reply}
		names = requestPhasesWithoutWatermarks
	} else if q.FetchPhases && len(stages["follower/raft.fetch.report"]) > 0 {
		var err error
		fetches, err = requestFetchPaths(ranges[1], stages)
		if err != nil {
			return RequestTrace{}, err
		}
		names = replicationPhases[:]
	}
	slices.SortFunc(batches, func(a, b Event) int { return cmp.Compare(a.Time, b.Time) })
	slices.SortFunc(ready, func(a, b Event) int { return cmp.Compare(traceEnd(a), traceEnd(b)) })
	for _, events := range ranges {
		slices.SortFunc(events, func(a, b Event) int { return cmp.Compare(a.Args["arg1"], b.Args["arg1"]) })
		for i, e := range events {
			if e.Args["arg0"] < 0 || e.Args["arg1"] <= e.Args["arg0"] ||
				(i > 0 && events[i-1].Args["arg1"] > e.Args["arg0"]) {
				return RequestTrace{}, fmt.Errorf("invalid or overlapping %s offsets", e.Name)
			}
		}
	}
	base := ranges[0][0].Args["arg0"]
	if base > math.MaxInt64-int64(len(ready)) {
		return RequestTrace{}, errors.New("invalid WAL offset base")
	}
	var rank, lastEnd int64
	for _, batch := range batches {
		count := batch.Args["arg1"]
		if count <= 0 || batch.Args["arg0"] != rank || count > int64(len(ready))-rank ||
			traceNS(batch.Time) < lastEnd || traceEnd(batch) <= traceNS(batch.Time) {
			return RequestTrace{}, errors.New("request analysis requires one ordered SET pipeline")
		}
		rank += count
		lastEnd = traceEnd(batch)
	}
	if rank != int64(len(ready)) {
		return RequestTrace{}, errors.New("client writes and admission spans differ")
	}
	var selected int
	if q.Slow {
		batch, err := selectTraceBatch(batches, q.Batch)
		if err != nil {
			return RequestTrace{}, err
		}
		selected = batch
	}

	var totals phaseTimes
	var totalNS int64
	var replyGaps replyGapAccumulator
	result := RequestTrace{}
	for i, b := range batches {
		if q.Slow && i+1 != selected {
			continue
		}
		gaps, times, err := measureRequestBatch(b, base, ready, ranges, names, fetches)
		if err != nil {
			return RequestTrace{}, err
		}
		for i, p := range times {
			totals[i].ns += p.ns
			totals[i].writes += p.writes
		}
		begin, end := traceNS(b.Time), traceEnd(b)
		totalNS += end - begin
		result.Batches++
		result.Writes += int(b.Args["arg1"])
		if q.Slow {
			slices.SortStableFunc(gaps, func(a, b ReplyGap) int {
				return cmp.Compare(b.EndNS-b.BeginNS, a.EndNS-a.BeginNS)
			})
			result.Slow = &SlowBatchTrace{
				Batch: i + 1, BeginNS: begin, EndNS: end, ReplyGaps: gaps[:min(q.Limit, len(gaps))],
			}
			if q.Role != "" || q.Stage != "" {
				result.Slow.selectSpans(events, q)
			}
		} else {
			replyGaps.addBatch(gaps)
		}
	}
	result.replyGapStats = replyGaps.result()
	result.SetMS = float64(traceEnd(batches[len(batches)-1])-traceNS(batches[0].Time)) / 1e6
	if waits := stages["client/client.final_wait"]; len(waits) == 1 {
		ms := float64(traceEnd(waits[0])-traceNS(waits[0].Time)) / 1e6
		result.WaitMS = &ms
	}
	result.Phases = totals.rows(names, totalNS, result.Writes)
	slices.SortStableFunc(result.Phases, func(a, b RequestPhase) int {
		if q.ByWrites && a.Writes != b.Writes {
			return cmp.Compare(b.Writes, a.Writes)
		}
		return cmp.Compare(b.MS, a.MS)
	})
	return result, nil
}

func requestFetchPaths(appends []Event, stages map[string][]Event) (map[int64]*RequestFetch, error) {
	paths := make(map[int64]*RequestFetch, len(appends))
	for _, app := range appends {
		rpc, rx, err := fetchPair(app, stages["follower/raft.fetch.receive"], stages["follower/raft.fetch.rpc"])
		if err != nil {
			return nil, err
		}
		next := nextFetchReport(app, rpc, stages["follower/raft.fetch.report"])
		if next == nil {
			return nil, fmt.Errorf("missing Follower Fetch report for offset %d", app.Args["arg1"])
		}
		paths[app.Args["arg1"]] = &RequestFetch{
			FirstOffset: app.Args["arg0"] + 1, LastOffset: app.Args["arg1"],
			RPCStartNS: traceNS(rpc.Time), RPCReturnNS: traceEnd(rpc),
			ReceiveStartNS: traceNS(rx.Time), ReportNS: traceNS(next.Time),
		}
	}
	return paths, nil
}

func measureRequestBatch(batch Event, base int64, ready []Event, ranges [][]Event, names []string, fetches map[int64]*RequestFetch) ([]ReplyGap, phaseTimes, error) {
	var totals phaseTimes
	var gaps []ReplyGap
	begin, end := traceNS(batch.Time), traceEnd(batch)
	cursor := begin
	first, count := batch.Args["arg0"], batch.Args["arg1"]
	for rank := first; rank < first+count; rank++ {
		offset := base + rank + 1
		var times [len(replicationPhases) - 1]int64
		var fetch *RequestFetch
		milestones := times[:1]
		milestones[0] = traceEnd(ready[rank])
		for _, events := range ranges {
			j := sort.Search(len(events), func(j int) bool { return events[j].Args["arg1"] >= offset })
			if j == len(events) || events[j].Args["arg0"] >= offset {
				return nil, totals, fmt.Errorf("missing %s milestone for offset %d", events[0].Name, offset)
			}
			e := events[j]
			if e.Name == "raft.fetch.appended" && fetches != nil {
				fetch = fetches[e.Args["arg1"]]
				wal := milestones[len(milestones)-1]
				milestones = append(milestones, max(wal, fetch.RPCStartNS), fetch.RPCReturnNS,
					fetch.ReceiveStartNS, traceNS(e.Time), fetch.ReportNS)
			} else {
				milestones = append(milestones, traceNS(e.Time))
			}
		}
		if milestones[0] < begin || milestones[len(milestones)-1] > end ||
			milestones[len(milestones)-1] < cursor || !slices.IsSorted(milestones) {
			return nil, totals, fmt.Errorf("out-of-order request milestones at offset %d", offset)
		}
		start := cursor
		var phases phaseTimes
		for i, ts := range milestones {
			if ts > cursor {
				phases[i] = phaseTime{ns: ts - cursor, writes: 1}
				totals[i].ns += ts - cursor
				totals[i].writes++
				cursor = ts
			}
		}
		if cursor > start {
			gaps = append(gaps, ReplyGap{Fetch: fetch, Offset: offset, BeginNS: start, EndNS: cursor, Phases: phases.rows(names, cursor-start, 1)})
		}
	}
	if end > cursor {
		totals[len(names)-1] = phaseTime{ns: end - cursor, writes: 1}
	}
	return gaps, totals, nil
}

func (b *SlowBatchTrace) selectSpans(events []Event, q RequestTraceQuery) {
	for _, e := range events {
		if e.Kind == "X" && (q.Role == "" || e.Cat == q.Role) && strings.HasPrefix(e.Name, q.Stage) &&
			traceNS(e.Time) <= b.EndNS && traceEnd(e) >= b.BeginNS {
			b.Spans = append(b.Spans, e)
		}
	}
	slices.SortStableFunc(b.Spans, func(a, c Event) int { return cmp.Compare(b.spanOverlap(c), b.spanOverlap(a)) })
	b.SpanMatches = len(b.Spans)
	b.Spans = b.Spans[:min(q.Limit, len(b.Spans))]
	slices.SortStableFunc(b.Spans, func(a, c Event) int { return cmp.Compare(a.Time, c.Time) })
}

func (b *SlowBatchTrace) spanOverlap(e Event) int64 {
	return min(b.EndNS, traceEnd(e)) - max(b.BeginNS, traceNS(e.Time))
}
