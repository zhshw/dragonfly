// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package models

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dragonflydb.io/profile/pkg/otel"
	"dragonflydb.io/profile/pkg/workflow"
)

func TestNativeWorkflowArchive(t *testing.T) {
	compare := testBenchmark(t, 'a', map[string]float64{"candidate": 200})
	run, report := t.TempDir(), t.TempDir()
	var rows []workflow.BenchRow
	if err := decodeFile(filepath.Join(compare, "summary.json"), &rows); err != nil {
		t.Fatal(err)
	}
	sha := strings.Repeat("f", 64)
	native := workflow.NativeDiagnostic{
		Run: run, Result: rows[0],
		Metadata: otel.Metadata{Mode: "raft.memory", BinarySHA: sha, HarnessSHA: strings.Repeat("a", 64),
			Frequency: 500, Nodes: map[string]otel.NodeInfo{"11": {Role: "leader"}, "12": {Role: "follower"}}},
		Capture: &workflow.CaptureReceipt{NativeCapture: otel.NativeCapture{
			ID: "capture", Experiment: "smoke", BinarySHA: sha, Release: sha, Workload: "raft.memory/c1/p64/256MiB", Streams: 2},
			Complete: true, Events: 10, Nodes: []string{"11", "12"}},
	}
	writeJSON := func(path string, value any) {
		t.Helper()
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, data, 0644); err != nil {
			t.Fatal(err)
		}
	}
	writeJSON(filepath.Join(run, "result.json"), rows[0])
	if err := os.WriteFile(filepath.Join(run, "test.log"), []byte("Ran 1 test\n\nOK\n"), 0644); err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{"leader-node-11", "follower-node-12"} {
		for suffix, data := range map[string]string{".folded": "root;work 10\n", ".svg": "<svg></svg>"} {
			if err := os.WriteFile(filepath.Join(report, prefix+suffix), []byte(data), 0644); err != nil {
				t.Fatal(err)
			}
		}
	}
	writeJSON(filepath.Join(report, "native.json"), native)
	store := Store{Root: t.TempDir()}
	config := ImportConfig{Experiment: Label{ID: "smoke", Title: "Workflow smoke"}, Compare: compare,
		Diagnostics: map[string]DiagnosticSource{"current": {Report: report, RequireTrace: true}}}
	summary, err := store.Import(config)
	if err != nil {
		t.Fatal(err)
	}
	d := summary.Diagnostics["current"]
	if !d.Native || d.CaptureID != "capture" || summary.Evidence.Flamegraphs != 2 || d.Nodes["leader"].CPU != 20 {
		t.Fatalf("native diagnostic not archived: %+v", summary)
	}
	record, err := store.Get("smoke")
	if err != nil {
		t.Fatal(err)
	}
	if len(record.Observations) != 1 || record.Observations[0].Timeline != "" || len(record.Observations[0].Stages) != 0 {
		t.Fatal("native archive regenerated trace files")
	}
	native.Metadata.Runner = "go"
	writeJSON(filepath.Join(report, "native.json"), native)
	if err := os.WriteFile(filepath.Join(run, "test.log"), []byte("Verified recovery and readback\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := (Store{Root: t.TempDir()}).Import(config); err != nil {
		t.Fatal(err)
	}
	var environment workflow.Environment
	if err := decodeFile(filepath.Join(compare, "environment.json"), &environment); err != nil {
		t.Fatal(err)
	}
	environment.Runner = "go"
	writeJSON(filepath.Join(compare, "environment.json"), environment)
	for _, row := range rows {
		folder := filepath.Join(compare, strings.TrimSuffix(row.LogName(), ".log"))
		if err := os.Mkdir(folder, 0755); err != nil {
			t.Fatal(err)
		}
		writeJSON(filepath.Join(folder, "result.json"), row)
	}
	if _, err := (Store{Root: t.TempDir()}).Import(config); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []func(*workflow.NativeDiagnostic){
		func(d *workflow.NativeDiagnostic) { d.Capture.Complete = false },
		func(d *workflow.NativeDiagnostic) { d.Capture.Experiment = "another" },
		func(d *workflow.NativeDiagnostic) { d.Capture.BinarySHA = strings.Repeat("b", 64) },
		func(d *workflow.NativeDiagnostic) { d.Capture.Nodes = []string{"11"} },
		func(d *workflow.NativeDiagnostic) { d.Result.Depth = 1 },
		func(d *workflow.NativeDiagnostic) { d.Result.Rate += 1 },
	} {
		copy := native
		capture := *native.Capture
		copy.Capture = &capture
		mutation(&copy)
		writeJSON(filepath.Join(report, "native.json"), copy)
		other := Store{Root: t.TempDir()}
		if _, err := other.Import(config); err == nil {
			t.Fatal("accepted incompatible native capture")
		}
	}
	if err := os.Remove(filepath.Join(compare, strings.TrimSuffix(rows[0].LogName(), ".log"), "result.json")); err != nil {
		t.Fatal(err)
	}
	writeJSON(filepath.Join(report, "native.json"), native)
	if _, err := (Store{Root: t.TempDir()}).Import(config); err == nil {
		t.Fatal("missing completed Go round accepted")
	}
}
