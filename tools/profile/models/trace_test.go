// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package models

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"dragonflydb.io/profile/pkg/otel"
	"dragonflydb.io/profile/pkg/workflow"
)

func TestTraceFiltersBeforeSortingAndPagination(t *testing.T) {
	store := Store{Root: t.TempDir()}
	record := Record{Schema: Schema, Detail: Detail{
		Summary: Summary{Experiment: Label{ID: "v-test", State: "complete"}, Diagnostics: map[string]Diagnostic{"current": {}}},
		Runs: []Run{
			{ID: "benchmark-current-1", Kind: "benchmark", Variant: "current"},
			{ID: "diagnostic-current", Kind: "diagnostic", Variant: "current"},
		},
		Observations: []Observation{{Variant: "current", Stages: []otel.Stage{
			{Node: "12", Role: "follower", Name: "raft.fetch.small", Union: 4, P95: 1},
			{Node: "12", Role: "follower", Name: "raft.fetch.first", Union: 35, P95: 20},
			{Node: "12", Role: "follower", Name: "raft.fetch.second", Union: 40, P95: 10},
			{Node: "11", Role: "leader", Name: "raft.fetch.leader", Union: 300, P95: 160},
			{Node: "12", Role: "follower", Name: "metadata.fetch", Union: 200, P95: 100},
		}}},
	}}
	dir := filepath.Join(store.Root, "v-test")
	if err := os.Mkdir(dir, 0755); err != nil {
		t.Fatal(err)
	}
	if err := writeRecord(dir, record); err != nil {
		t.Fatal(err)
	}
	query := TraceQuery{Run: "diagnostic-current", Role: "follower", Stage: "raft.fetch", Sort: "p95", Order: "desc", MinUnionMS: 5, Offset: 1, Limit: 1}
	page, err := store.Trace("v-test", query)
	if err != nil || page.Total != 2 || len(page.Stages) != 1 || page.Stages[0].Name != "raft.fetch.second" {
		t.Fatalf("unexpected page %+v, %v", page, err)
	}
	query.Run, query.Limit, query.Offset = "", 0, 0
	page, err = store.Trace("v-test", query)
	if err != nil || page.Run != "diagnostic-current" || page.Total != 2 || len(page.Stages) != 0 {
		t.Fatalf("unexpected summary %+v, %v", page, err)
	}
	if _, err := store.Trace("v-test", TraceQuery{Run: "benchmark-current-1"}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("benchmark run selected as a diagnostic: %v", err)
	}
}

func TestFlamegraphHotspotsUseSelectedNodeAndView(t *testing.T) {
	path := filepath.Join(t.TempDir(), "follower.folded")
	if err := os.WriteFile(path, []byte("main;copy 4\nmain;poll 3\nmain;copy;clear 2\n"), 0644); err != nil {
		t.Fatal(err)
	}
	run := diagnosticRun{ID: "diagnostic-current", DiagnosticCapture: DiagnosticCapture{
		ExperimentID: "v-test",
		Profile: Diagnostic{Diagnostic: workflow.Diagnostic{Nodes: map[string]workflow.NodeDiagnostic{
			"follower": {CPU: 18},
		}}},
		Observation: Observation{Flamegraphs: map[string]string{"follower": "diagnostic-current-follower"}},
	}}
	reads := 0
	artifact := func(key string) (string, error) {
		reads++
		if key != "diagnostic-current-follower-folded" {
			t.Fatalf("unexpected artifact %q", key)
		}
		return path, nil
	}
	query := FlamegraphQuery{Role: "follower", View: "inclusive", Limit: 1}
	report, err := summarizeFlamegraph(run, query, artifact)
	if err != nil || report.Samples != 9 || len(report.Hotspots) != 1 || report.Hotspots[0].Name != "main" || report.Hotspots[0].Samples != 9 {
		t.Fatalf("unexpected CPU report %+v, %v", report, err)
	}
	query.Limit = 0
	report, err = summarizeFlamegraph(run, query, artifact)
	if err != nil || reads != 1 || report.Artifact != "diagnostic-current-follower" || len(report.Hotspots) != 0 {
		t.Fatalf("unexpected summary %+v, %d profile reads, %v", report, reads, err)
	}
}
