// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package models

import (
	"cmp"
	"context"
	"errors"
	"os"
	"reflect"
	"strconv"
	"testing"
	"time"
)

func comparisonFixture(t *testing.T, prefix string) (Store, string, ChoiceIDs) {
	t.Helper()
	store := Store{Root: t.TempDir()}
	for _, item := range []struct {
		suffix  string
		harness rune
		rates   map[string]float64
	}{
		{"base", 'a', map[string]float64{"candidate": 100, "cluster": 400}},
		{"previous", 'a', map[string]float64{"candidate": 150}},
		{"current", 'b', map[string]float64{"candidate": 200}},
	} {
		id := prefix + "-" + item.suffix
		_, err := store.Import(ImportConfig{Experiment: Label{ID: id, Title: id}, Compare: testBenchmark(t, item.harness, item.rates)})
		if err != nil {
			t.Fatal(err)
		}
	}
	return store, prefix + "-current", ChoiceIDs{Cumulative: prefix + "-base:current", Previous: prefix + "-previous", Reference: prefix + "-base:cluster"}
}

func checkComparisonUpdates(t *testing.T, repo Repository, id string, choices ChoiceIDs) {
	t.Helper()
	ctx := context.Background()
	before, err := repo.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	got, err := repo.SetComparisons(ctx, id, choices)
	if err != nil {
		t.Fatal(err)
	}
	if got.Comparisons.Cumulative.Result.Throughput != 100 || got.Comparisons.Reference.Result.Reached != 50 ||
		got.Comparisons.Previous.Target.ExperimentID != choices.Previous || got.Comparisons.Previous.Warning == "" {
		t.Fatalf("historical comparisons not retained: %+v", got.Comparisons)
	}
	after, err := repo.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	before.Summary.Comparisons = after.Summary.Comparisons
	if !reflect.DeepEqual(before, after) {
		t.Fatal("comparison update changed measured data or artifact references")
	}
	for _, bad := range []ChoiceIDs{{Cumulative: "missing"}, {Previous: id}, {Cumulative: choices.Reference}} {
		if _, err := repo.SetComparisons(ctx, id, bad); !errors.Is(err, ErrInvalid) {
			t.Fatalf("invalid choices %+v: %v", bad, err)
		}
		unchanged, err := repo.Get(id)
		if err != nil || !reflect.DeepEqual(unchanged, after) {
			t.Fatal("invalid update changed the saved comparisons")
		}
	}
	runs, err := CompareRuns(after, repo.Get)
	if err != nil || runs.Previous == nil || len(runs.Previous.Benchmarks) != 1 {
		t.Fatalf("previous runs unavailable: %+v %v", runs, err)
	}
	cleared, err := repo.SetComparisons(ctx, id, ChoiceIDs{})
	if err != nil || !reflect.DeepEqual(cleared.Comparisons, Comparisons{}) {
		t.Fatalf("clear comparisons: %+v %v", cleared.Comparisons, err)
	}
	initial, err := repo.SetComparisons(ctx, id, ChoiceIDs{Cumulative: id + ":current"})
	if err != nil || initial.Comparisons.Cumulative.Result.Throughput != 0 || initial.Comparisons.Cumulative.Warning != "" {
		t.Fatalf("an initial experiment can be its own baseline: %+v %v", initial, err)
	}
	if _, err := repo.SetComparisons(ctx, id, ChoiceIDs{}); err != nil {
		t.Fatal(err)
	}
}

func TestConfigureArchivedComparisons(t *testing.T) {
	store, id, choices := comparisonFixture(t, "comparison")
	checkComparisonUpdates(t, store, id, choices)
}

func TestPostgresConfigureComparisons(t *testing.T) {
	dsn := cmp.Or(os.Getenv("PROFILE_TEST_DSN_FILE"), os.Getenv("RAFT_WORKFLOW_TEST_DSN_FILE"))
	if dsn == "" {
		t.Skip("set PROFILE_TEST_DSN_FILE to run PostgreSQL integration tests")
	}
	store, id, choices := comparisonFixture(t, "comparison-"+strconv.FormatInt(time.Now().UnixNano(), 36))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	p, err := OpenPostgres(ctx, store.Root, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	if _, err := p.Sync(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		for _, remove := range []string{id, choices.Previous, choices.Cumulative[:len(choices.Cumulative)-len(":current")]} {
			if _, err := p.Delete(context.Background(), remove); err != nil {
				t.Error(err)
			}
		}
	}()
	checkComparisonUpdates(t, p, id, choices)
}
