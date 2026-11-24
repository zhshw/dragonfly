// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package workflow

import (
	"slices"
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

func TestCPUCallPaths(t *testing.T) {
	const folded = `a;worker;crc 2
b;worker;crc 3
root;other;crc 4
root;other;crc;inner;crc 6
root;other;crc;tail 1
root;unrelated 4
`
	paths, total, err := CPUCallPaths(folded, "crc", true, 1, 3)
	if err != nil || total != 20 || paths.Matched != 16 || len(paths.Paths) != 3 {
		t.Fatalf("paths = %+v, total = %d, err = %v", paths, total, err)
	}
	for i, frames := range [][]string{{"inner", "crc"}, {"other", "crc"}, {"worker", "crc"}} {
		want := []int64{6, 5, 5}[i]
		if path := paths.Paths[i]; !slices.Equal(path.Frames, frames) || path.Samples != want || path.Percent != float64(want*5) {
			t.Errorf("path %d = %+v, want %v with %d samples", i, path, frames, want)
		}
	}
	leaf, _, err := CPUCallPaths(folded, "crc", false, 1, 0)
	if err != nil || leaf.Matched != 15 || len(leaf.Paths) != 0 {
		t.Fatalf("leaf summary = %+v, err = %v", leaf, err)
	}
}
