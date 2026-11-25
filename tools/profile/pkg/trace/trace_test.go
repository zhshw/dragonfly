// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package trace

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func TestAnalyzeApplyConfirmation(t *testing.T) {
	const timeline = `{"traceEvents":[
  {"name":"client.pipeline","cat":"client","ph":"X","ts":0,"dur":600,"args":{"arg0":0,"arg1":2}},
  {"name":"client.pipeline","cat":"client","ph":"X","ts":900,"dur":300,"args":{"arg0":2,"arg1":1}},
  {"name":"raft.apply","cat":"leader","ph":"X","ts":100,"dur":100,"args":{"arg0":9,"arg1":11}},
  {"name":"raft.apply","cat":"leader","ph":"X","ts":1000,"dur":100,"args":{"arg0":11,"arg1":12}},
  {"name":"raft.set.await_apply","cat":"leader","ph":"X","ts":50,"dur":450,"args":{"arg0":10}},
  {"name":"raft.set.await_apply","cat":"leader","ph":"X","ts":60,"dur":165,"args":{"arg0":11}},
  {"name":"raft.set.await_apply","cat":"leader","ph":"X","ts":900,"dur":230,"args":{"arg0":12}},
  {"name":"raft.worker.queue","cat":"leader","ph":"X","ts":150,"dur":100},
  {"name":"raft.set.ticket_lifetime","cat":"leader","ph":"X","ts":50,"dur":450},
  {"name":"raft.set.ticket_lifetime","cat":"leader","ph":"X","ts":60,"dur":165},
  {"name":"raft.set.ticket_lifetime","cat":"leader","ph":"X","ts":900,"dur":230}
]}`
	got, err := AnalyzeApplyConfirmation(strings.NewReader(timeline))
	if err != nil {
		t.Fatal(err)
	}
	want := ApplyConfirmation{
		Confirmations: 3, Matched: 3, ApplyBatches: 2, MinBatchWrites: 1, MaxBatchWrites: 2,
		PostApply:       ConfirmationTime{P50MS: .030, P95MS: .300, MaxMS: .300, UnionMS: .330},
		PipelineWindows: 2, PipelineTails: 2, PipelineTailSumUS: 55, PipelineTailMaxUS: 30,
		WorkerQueueInApply: .050, Tickets: 3, PeakInFlight: 2,
	}
	if got != want {
		t.Fatalf("analysis = %+v, want %+v", got, want)
	}

	t.Run("noncontiguous offsets omit pipeline attribution", func(t *testing.T) {
		gap := strings.ReplaceAll(timeline, `:12}`, `:13}`)
		got, err := AnalyzeApplyConfirmation(strings.NewReader(gap))
		if err != nil || got.Matched != 3 || got.PipelineTails != 0 || got.PipelineWindows != 2 {
			t.Fatalf("analysis = %+v, err = %v", got, err)
		}
	})
}

func TestAnalyzeRequestTrace(t *testing.T) {
	const timeline = `{"traceEvents":[
  {"name":"client.pipeline","cat":"client","ph":"X","ts":0,"dur":1000,"args":{"arg0":0,"arg1":2}},
  {"name":"client.final_wait","cat":"client","ph":"X","ts":1000,"dur":50},
  {"name":"raft.set.admission_queue","cat":"leader","ph":"X","ts":0,"dur":100},
  {"name":"raft.set.admission_queue","cat":"leader","ph":"X","ts":0,"dur":140},
  {"name":"raft.wal.append.complete","cat":"leader","ph":"X","ts":200,"args":{"arg0":10,"arg1":11}},
  {"name":"raft.wal.append.complete","cat":"leader","ph":"X","ts":250,"args":{"arg0":11,"arg1":12}},
  {"name":"raft.fetch.appended","cat":"follower","ph":"X","ts":400,"args":{"arg0":10,"arg1":11}},
  {"name":"raft.fetch.appended","cat":"follower","ph":"X","ts":450,"args":{"arg0":11,"arg1":12}},
  {"name":"raft.hw.advance","cat":"leader","ph":"X","ts":500,"args":{"arg0":10,"arg1":11}},
  {"name":"raft.hw.advance","cat":"leader","ph":"X","ts":550,"args":{"arg0":11,"arg1":12}},
  {"name":"raft.reply.advance","cat":"leader","ph":"X","ts":700,"args":{"arg0":10,"arg1":11}},
  {"name":"raft.reply.advance","cat":"leader","ph":"X","ts":900,"args":{"arg0":11,"arg1":12}}
]}`
	report, err := AnalyzeRequestTrace(strings.NewReader(timeline), RequestTraceQuery{
		Slow: true, Limit: 2, Role: "follower", Stage: "raft.fetch.",
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.SetMS != 1 || report.WaitMS == nil || *report.WaitMS != .05 ||
		report.Batches != 1 || report.Writes != 2 {
		t.Fatalf("unexpected request summary: %+v", report)
	}
	top := RequestPhase{Name: "hw-to-reply", MS: .4, Percent: 40, Writes: 2, WritePercent: 100}
	if report.Phases[0] != top {
		t.Fatalf("top phase = %+v, want %+v", report.Phases[0], top)
	}
	var percent float64
	for _, phase := range report.Phases {
		percent += phase.Percent
	}
	if percent != 100 {
		t.Fatalf("phases account for %.2f%% of batch time", percent)
	}
	if report.Slow.Batch != 1 || report.Slow.SpanMatches != 2 || len(report.Slow.ReplyGaps) != 2 {
		t.Fatalf("unexpected slow path: %+v", report.Slow)
	}
	second := report.Slow.ReplyGaps[1]
	if second.Offset != 12 || second.BeginNS != 700000 || second.EndNS != 900000 || len(second.Phases) != 1 {
		t.Fatalf("overlapping work was counted in the second reply gap: %+v", second)
	}

	missing := strings.Replace(timeline, "raft.hw.advance", "missing", 1)
	if _, err := AnalyzeRequestTrace(strings.NewReader(missing), RequestTraceQuery{}); err == nil {
		t.Fatal("expected missing high watermark to fail")
	}

	t.Run("reply gaps across batches", func(t *testing.T) {
		both := strings.TrimSuffix(timeline, "\n]}") + `,
  {"name":"client.pipeline","cat":"client","ph":"X","ts":1200,"dur":800,"args":{"arg0":2,"arg1":2}},
  {"name":"raft.set.admission_queue","cat":"leader","ph":"X","ts":1200,"dur":100},
  {"name":"raft.set.admission_queue","cat":"leader","ph":"X","ts":1200,"dur":150},
  {"name":"raft.wal.append.complete","cat":"leader","ph":"X","ts":1500,"args":{"arg0":12,"arg1":14}},
  {"name":"raft.fetch.appended","cat":"follower","ph":"X","ts":1600,"args":{"arg0":12,"arg1":14}},
  {"name":"raft.hw.advance","cat":"leader","ph":"X","ts":1700,"args":{"arg0":12,"arg1":14}},
  {"name":"raft.reply.advance","cat":"leader","ph":"X","ts":1800,"args":{"arg0":12,"arg1":14}}
]}`
		stats, err := AnalyzeReplyGapStats(strings.NewReader(both))
		if err != nil {
			t.Fatal(err)
		}
		if stats.Batches != 2 || stats.Count != 3 || stats.MaxMS != .7 || stats.MinMS != .2 ||
			math.Abs(stats.MeanMS-.5) > 1e-12 || math.Abs(stats.VarianceMS2-.14/3) > 1e-12 {
			t.Fatalf("unexpected reply gap statistics: %+v", stats)
		}
		if len(stats.BatchMaxMS) != 2 || stats.BatchMaxMS[0] == nil || stats.BatchMaxMS[1] == nil ||
			*stats.BatchMaxMS[0] != .7 || *stats.BatchMaxMS[1] != .6 {
			t.Fatalf("unexpected maximum per batch: %v", stats.BatchMaxMS)
		}
	})
}

func TestAnalyzeAdmissionTrace(t *testing.T) {
	const timeline = `{"traceEvents":[
  {"name":"client.pipeline","cat":"client","ph":"X","ts":0,"dur":100},
  {"name":"raft.set.admission.wait","cat":"leader","ph":"X","pid":"1","tid":5,"ts":10,"dur":80},
  {"name":"raft.set.ticket_lifetime","cat":"leader","ph":"X","pid":"1","tid":5,"ts":0,"dur":30},
  {"name":"raft.set.ticket_lifetime","cat":"leader","ph":"X","pid":"1","tid":5,"ts":0,"dur":80},
  {"name":"raft.set.ticket_lifetime","cat":"leader","ph":"X","pid":"1","tid":9,"ts":0,"dur":50},
  {"name":"raft.folly.write","cat":"leader","ph":"X","pid":"1","tid":7,"ts":15,"dur":25}
]}`
	report, err := AnalyzeAdmissionTrace(strings.NewReader(timeline), 1, 2, 2)
	if err != nil {
		t.Fatal(err)
	}
	if report.WALStage != "raft.folly.write" || report.Waits != 1 ||
		report.FullWaveWaits != 1 || report.FullWaveWALIdleMS != .05 {
		t.Fatalf("unexpected admission report: %+v", report)
	}
	wait := report.Longest[0]
	if wait.Retired != 2 || wait.FirstRetirementNS != 30000 || wait.LastWALNS != 40000 || wait.EndNS != 90000 {
		t.Fatalf("wrong admission boundaries: %+v", wait)
	}
	leader, err := AnalyzeAdmissionTraceWithScope(strings.NewReader(timeline), 1, 3, 1, "leader")
	if err != nil || leader.FullWaveWaits != 1 || leader.Longest[0].Retired != 3 || leader.FullWaveWALIdleMS != .05 {
		t.Fatalf("cross-thread retirement missing: %+v, %v", leader, err)
	}
}

func TestAnalyzeFetchDelay(t *testing.T) {
	const timeline = `{"traceEvents":[
  {"name":"client.pipeline","cat":"client","ph":"X","ts":0,"dur":2000,"args":{"arg0":0,"arg1":2}},
  {"name":"raft.wal.append.complete","cat":"leader","pid":"1","ph":"X","ts":10,"args":{"arg0":10,"arg1":12}},
  {"name":"raft.fetch.rpc","cat":"follower","ph":"X","pid":"2","tid":3,"ts":20,"dur":100,"args":{"arg0":1}},
  {"name":"raft.await_io","cat":"follower","ph":"X","pid":"2","tid":4,"ts":140,"dur":530},
  {"name":"raft.await_io","cat":"follower","ph":"X","pid":"2","tid":3,"ts":160,"dur":490},
  {"name":"raft.folly.fsync","cat":"follower","ph":"X","pid":"2","tid":7,"ts":80,"dur":600},
  {"name":"raft.fetch.receive","cat":"follower","ph":"X","pid":"2","tid":7,"ts":700,"dur":50,"args":{"arg0":10}},
  {"name":"raft.log.append","cat":"follower","ph":"X","pid":"2","tid":7,"ts":706,"dur":30,"args":{"arg0":500}},
  {"name":"raft.log.pwrite","cat":"follower","ph":"X","pid":"2","tid":7,"ts":710,"dur":20,"args":{"arg0":450}},
  {"name":"raft.fetch.appended","cat":"follower","ph":"X","pid":"2","tid":7,"ts":750,"args":{"arg0":10,"arg1":12}}
]}`
	report, err := AnalyzeFetchDelay(strings.NewReader(timeline), 0, 1)
	if err != nil {
		t.Fatal(err)
	}
	if report.Batch != 1 || report.Fetches != 1 || report.AwaitFsyncOverlaps != 1 || len(report.Delays) != 1 {
		t.Fatalf("unexpected Fetch report: %+v", report)
	}
	delay := report.Delays[0]
	if delay.FirstOffset != 11 || delay.LastOffset != 12 || delay.RPCToReceiveMS != .58 ||
		delay.PayloadBytes != 500 || delay.AppendedBytes != 450 || !delay.AwaitFsync || len(delay.AwaitIO) != 1 {
		t.Fatalf("unexpected Fetch delay: %+v", delay)
	}

	requests, err := AnalyzeFetchRequests(strings.NewReader(timeline))
	if err != nil || len(requests) != 1 {
		t.Fatalf("unexpected Fetch requests: %+v, %v", requests, err)
	}
	request := requests[0]
	if request.Number != 1 || request.FirstOffset != 11 || request.LastOffset != 12 ||
		request.RPCToReceiveMS != delay.RPCToReceiveMS || !request.AwaitFsync ||
		request.Append == nil || request.Write == nil || request.AppendedBytes != 450 ||
		len(request.ClientBatches) != 1 || request.ClientBatches[0] != 1 {
		t.Fatalf("unexpected Fetch request: %+v", request)
	}

	withReplies := strings.TrimSuffix(timeline, "\n]}") + `,
  {"name":"raft.set.admission_queue","cat":"leader","pid":"1","ph":"X","ts":0,"dur":4},
  {"name":"raft.set.admission_queue","cat":"leader","pid":"1","ph":"X","ts":0,"dur":8},
  {"name":"raft.hw.advance","cat":"leader","pid":"1","ph":"X","ts":900,"args":{"arg0":10,"arg1":12}},
  {"name":"raft.reply.advance","cat":"leader","pid":"1","ph":"X","ts":1100,"args":{"arg0":10,"arg1":12}}
]}`
	path, err := AnalyzeRequestPath(strings.NewReader(withReplies), 1)
	if err != nil || len(path.ReplyGaps) != 1 || path.ReplyGaps[0].Offset != 11 ||
		len(path.Fetches) != 1 || path.Fetches[0].RPC.TID != 3 || path.Fetches[0].Receive.Time != 700 {
		t.Fatalf("unexpected combined request path: %+v, %v", path, err)
	}
	waits, err := AnalyzeFetchWaits(strings.NewReader(withReplies), "slowest", "gap")
	if err != nil || waits.Batch != report.Batch || waits.Matched != 1 || len(waits.Requests) != 1 {
		t.Fatalf("unexpected ranked Fetches: %+v, %v", waits, err)
	}
	row := waits.Requests[0]
	if row.Offset != 11 || row.ReplyGap == nil || row.ReplyGap.Offset != row.Offset ||
		row.RPCToReceiveMS != delay.RPCToReceiveMS || row.IOMS != .49 || row.SyncOverlapMS != .49 ||
		row.LeaderWAL == nil || row.LeaderHW == nil || row.LeaderReply == nil {
		t.Fatalf("incomplete offset-aligned wait: %+v", row)
	}

}

func TestFetchRequestsKeepTimingWithoutOffsetSpans(t *testing.T) {
	const timeline = `{"traceEvents":[
  {"name":"raft.fetch.rpc","cat":"follower","ph":"X","pid":"2","tid":3,"ts":10,"dur":10},
  {"name":"raft.fetch.rpc","cat":"follower","ph":"X","pid":"2","tid":4,"ts":12,"dur":12},
  {"name":"raft.fetch.receive","cat":"follower","ph":"X","pid":"2","tid":5,"ts":30,"dur":1},
  {"name":"raft.fetch.rpc","cat":"follower","ph":"X","pid":"2","tid":3,"ts":40,"dur":20},
  {"name":"raft.await_io","cat":"follower","ph":"X","pid":"2","tid":4,"ts":65,"dur":10},
  {"name":"raft.folly.fsync","cat":"follower","ph":"X","pid":"2","tid":5,"ts":55,"dur":25},
  {"name":"raft.fetch.receive","cat":"follower","ph":"X","pid":"2","tid":5,"ts":80,"dur":8}
]}`
	requests, err := AnalyzeFetchRequests(strings.NewReader(timeline))
	if err != nil || len(requests) != 1 {
		t.Fatalf("unexpected Fetch requests: %+v, %v", requests, err)
	}
	request := requests[0]
	if request.RPC.Time != 40 || request.RPCToReceiveMS != .02 || request.FirstOffset != 0 ||
		len(request.ClientBatches) != 0 || request.AwaitFsync || len(request.AwaitIO) != 0 || len(request.Fsync) != 1 {
		t.Fatalf("unexpected Fetch request in older Trace: %+v", request)
	}
}

func TestFetchWaitRankingAcrossWholeCapture(t *testing.T) {
	var events []Event
	for i := 1; i <= 12; i++ {
		start := float64(i * 100)
		gap := float64(20 - i)
		span := func(name string, ts, dur float64) Event {
			return Event{Name: name, Kind: "X", Cat: "follower", Node: "2", TID: 3, Time: ts, Dur: dur}
		}
		events = append(events, span("raft.fetch.rpc", start, 1), span("raft.fetch.receive", start+1+gap, 4),
			span("raft.log.append", start+1+gap, 2))
		if i == 12 {
			// Two overlapping waits cross both edges of the eight-microsecond return gap.
			events = append(events, span("raft.await_io", start, 5), span("raft.await_io", start+4, 6),
				span("raft.folly.fsync", start, 3), span("raft.folly.fsync", start+6, 8))
		}
	}
	raw, err := json.Marshal(map[string][]Event{"traceEvents": events})
	if err != nil {
		t.Fatal(err)
	}
	for _, order := range []string{"gap", "io", "overlap"} {
		rows, err := AnalyzeFetchWaits(strings.NewReader(string(raw)), "slowest", order)
		wantMatched := 12
		if order == "io" {
			wantMatched = 1
		}
		if err != nil || rows.Scope != "round" || rows.BatchAligned || rows.Matched != wantMatched || len(rows.Requests) != min(10, wantMatched) {
			t.Fatalf("unexpected %s scope or limit: %+v, %v", order, rows, err)
		}
		first := rows.Requests[0]
		if order == "gap" && (first.Number != 1 || rows.Requests[9].Number != 10) {
			t.Fatalf("incorrect gap ranking: %+v", rows.Requests)
		}
		if order != "gap" && (first.Number != 12 || first.IOMS != .008 || first.SyncOverlapMS != .005 ||
			len(first.SyncOverlap) != 2 || first.LeaderWAL != nil) {
			t.Fatalf("incorrect I/O ranking or clipped intervals: %+v", first)
		}
	}
}

func TestRoundFetchGapsUseSequenceAndIndependentIOWindows(t *testing.T) {
	const timeline = `{"traceEvents":[
  {"name":"raft.fetch.rpc","cat":"follower","pid":"2","tid":3,"ph":"X","ts":0,"dur":20,"args":{"arg1":8}},
  {"name":"raft.fetch.rpc","cat":"follower","pid":"2","tid":1,"ph":"X","ts":10,"dur":30,"args":{"arg1":9}},
  {"name":"raft.await_io","cat":"follower","pid":"2","tid":3,"ph":"X","ts":50,"dur":10},
  {"name":"raft.folly.fsync","cat":"follower","pid":"2","tid":7,"ph":"X","ts":90,"dur":10},
  {"name":"raft.fetch.receive","cat":"follower","pid":"2","tid":7,"ph":"X","ts":1020.001,"dur":20,"args":{"arg0":10,"arg1":8}},
  {"name":"raft.log.append","cat":"follower","pid":"2","tid":7,"ph":"X","ts":1022,"dur":10,"args":{"arg0":500}},
  {"name":"raft.log.pwrite","cat":"follower","pid":"2","tid":7,"ph":"X","ts":1023,"dur":2,"args":{"arg0":450}},
  {"name":"raft.fetch.appended","cat":"follower","pid":"2","tid":7,"ph":"X","ts":1041,"args":{"arg0":10,"arg1":12}}
]}`
	for _, tc := range []struct {
		receive string
		slow    int
	}{{"1020.001", 1}, {"1020.000", 0}} {
		raw := strings.Replace(timeline, "1020.001", tc.receive, 1)
		got, err := AnalyzeRoundFetchDelay(strings.NewReader(raw), 1)
		if err != nil || got.Fetches != 1 || got.Partial != 1 || got.PartialBlockedByFlush != tc.slow ||
			got.GapsOver1MS != tc.slow || len(got.Delays) != 1 {
			t.Fatalf("receive %s: %+v, %v", tc.receive, got, err)
		}
		ranked, err := AnalyzeFetchWaits(strings.NewReader(raw), "round", "gap")
		if err != nil || ranked.Matched != 1 || ranked.Round == nil || *ranked.Round != got.FetchCounts ||
			ranked.Requests[0].RPC.Args["arg1"] != 8 || ranked.Requests[0].PartialBlockedByFlush != (tc.slow == 1) {
			t.Fatalf("page and CLI disagree on sequenced Fetch: %+v, %v", ranked, err)
		}

		d := got.Delays[0]
		if d.RPCReturnNS != 20000 || d.AwaitFsync || got.AwaitFsyncOverlaps != 0 || d.PartialBlockedByFlush != (tc.slow == 1) {
			t.Fatalf("wrong sequence or gap evidence: %+v", d)
		}
	}
}
