// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package models

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"dragonflydb.io/profile/pkg/workflow"
)

func TestSelectPreviousExperiment(t *testing.T) {
	base := Target{ID: "initial", Mode: "raft", Ack: "memory"}
	reference := Target{ID: "reference", Mode: "yes"}
	current := Target{
		ID: "v10:current", ExperimentID: "v10", Variant: "current", Mode: "raft", Ack: "memory",
		CumulativeID: base.ID, ReferenceID: reference.ID,
	}
	legacy := current
	legacy.ID, legacy.Variant = "v10:candidate", "candidate"
	targets := []Target{base, reference, legacy, current}
	for _, tc := range []struct {
		name, input, want string
		targets           []Target
	}{
		{"experiment ID", "v10", current.ID, targets},
		{"target ID", current.ID, current.ID, targets},
		{"legacy experiment ID", "v10", legacy.ID, targets[:3]},
		{"legacy target ID", legacy.ID, legacy.ID, targets},
	} {
		t.Run(tc.name, func(t *testing.T) {
			choices, err := selectTargets(ChoiceIDs{Previous: tc.input}, func() ([]Target, error) { return tc.targets, nil })
			if err != nil {
				t.Fatal(err)
			}
			if choices.Previous == nil || choices.Previous.Target.ID != tc.want {
				t.Fatalf("previous target = %+v, want %s", choices.Previous, tc.want)
			}
			if !choices.allows(ChoiceIDs{Previous: "v10", Cumulative: base.ID, Reference: reference.ID}) {
				t.Fatal("experiment ID and inherited comparisons should match the saved choices")
			}
		})
	}
	targets[0].Workload.Clients = 2
	if _, err := selectTargets(ChoiceIDs{Cumulative: base.ID, Reference: reference.ID},
		func() ([]Target, error) { return targets, nil }); !errors.Is(err, ErrInvalid) {
		t.Fatalf("different workloads should not define a new benchmark: %v", err)
	}
}

func TestArchiveUsesSavedTargetsAndSeparateRoundDelta(t *testing.T) {
	store := Store{Root: t.TempDir()}
	old := testBenchmark(t, 'a', map[string]float64{"memory": 100, "cluster": 400})
	current := testBenchmark(t, 'a', map[string]float64{"baseline": 150, "candidate": 200})
	otherHarness := testBenchmark(t, 'b', map[string]float64{"cluster": 400})
	for _, item := range []struct{ id, dir, variant string }{
		{"initial", old, "memory"}, {"reference", old, "cluster"}, {"incompatible", otherHarness, "cluster"},
	} {
		if _, err := store.ImportTarget(item.id, item.id, item.dir, item.variant); err != nil {
			t.Fatal(err)
		}
	}
	request := CreateRequest{ID: "historic", Title: "Historic references", Targets: ChoiceIDs{Cumulative: "initial", Reference: "incompatible"}}
	if _, err := store.Create(request); err != nil {
		t.Fatalf("historical harness should remain selectable: %v", err)
	}
	historical, err := store.Import(ImportConfig{Experiment: Label{ID: "historic"}, Compare: current})
	if err != nil || historical.Comparisons.Reference.Warning == "" || historical.Comparisons.Reference.Result.Reached != 50 {
		t.Fatalf("historical import lost comparison or harness warning: %+v %v", historical, err)
	}
	request.ID, request.Title = "next", "Next"
	request.Targets.Reference = "reference"
	draft, err := store.Create(request)
	if err != nil || draft.Experiment.State != "draft" || draft.Comparisons.Cumulative.Result != nil {
		t.Fatalf("unexpected draft: %+v, %v", draft, err)
	}
	// Once selected, target summaries remain usable independently of the catalogue.
	if err := os.Remove(filepath.Join(store.Root, ".targets", "initial.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Import(ImportConfig{Experiment: Label{ID: "next"}, Compare: current, Targets: ChoiceIDs{Cumulative: "other", Reference: "reference"}}); err == nil {
		t.Fatal("import replaced a saved comparison target")
	}
	checks := []string{
		filepath.Join(t.TempDir(), "build01-partition_log_test.log"),
		filepath.Join(t.TempDir(), "build01-integration.log"),
	}
	if err := os.WriteFile(checks[0], []byte("[  PASSED  ] 2 tests.\n"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(checks[1], []byte("Ran 1 test in 0.003s\n\nOK\n"), 0644); err != nil {
		t.Fatal(err)
	}
	got, err := store.Import(ImportConfig{Experiment: Label{ID: "next"}, Compare: current, ValidationLogs: checks})
	if err != nil {
		t.Fatal(err)
	}
	if got.Experiment.State != "complete" || got.Evidence.Confirmed != 6 ||
		got.Comparisons.Cumulative.Result.Throughput != 100 || got.Comparisons.Reference.Result.Reached != 50 ||
		math.Abs(got.Delta.Throughput-100.0/3) > 1e-8 {
		t.Fatalf("incorrect comparison: %+v", got)
	}
	record, err := store.Get("next")
	if err != nil || record.Summary.Experiment.State != "complete" || len(record.Runs) != 6 {
		t.Fatalf("completed draft is not readable: %v", err)
	}
	if len(record.Checks) != 2 || got.Validation["build01"] != (ValidationTotals{Unit: 2, Integration: 1}) {
		t.Fatalf("incorrect execution validation: %+v", got.Validation)
	}
	path, err := store.Artifact(record, record.Runs[0].Log)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}

func testBenchmark(t *testing.T, hash rune, rates map[string]float64) string {
	t.Helper()
	dir := t.TempDir()
	e := workflow.Environment{Clients: 1, Depth: 64, Distinct: true, Harness: strings.Repeat(string(hash), 64),
		Metric: "replication_mib_per_s", Builds: map[string]workflow.BuildInfo{}}
	var rows []workflow.BenchRow
	for name, rate := range rates {
		build := workflow.BuildInfo{SHA: strings.Repeat("f", 64), Mode: "raft", Ack: "memory"}
		if name == "cluster" {
			build.Mode, build.Ack = "yes", ""
		}
		e.Builds[name] = build
		for trial := 1; trial <= 3; trial++ {
			rows = append(rows, workflow.BenchRow{Order: len(rows) + 1, Trial: trial, Variant: name, Mode: build.Mode, Ack: build.Ack,
				Clients: 1, Depth: 64, Payload: 256, Keys: 1024, Warmup: 64, Settle: 5, Rate: rate, SetRate: rate,
				P95: 10, Confirmed: 1, Readback: 13, RecoveryMS: 500, Start: 1, WriteEnd: 2, End: 3})
			log := fmt.Sprintf("trial-%d-%s-clients-1-pipeline-64.log", trial, name)
			if err := os.WriteFile(filepath.Join(dir, log), []byte("Ran 1 test\n\nOK\n"), 0644); err != nil {
				t.Fatal(err)
			}
		}
	}
	for name, value := range map[string]any{"environment.json": e, "summary.json": rows} {
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), data, 0644); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}
