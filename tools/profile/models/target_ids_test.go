// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package models

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"testing"
	"time"

	"dragonflydb.io/profile/pkg/workflow"
	"gorm.io/gorm"
)

func TestTargetIDsPreserveArchiveOwnership(t *testing.T) {
	store := Store{Root: t.TempDir()}
	for _, id := range []string{"v10", "v13"} {
		_, err := store.Import(ImportConfig{Experiment: Label{ID: id, Title: id},
			Compare: testBenchmark(t, 'a', map[string]float64{"candidate": 200, "cluster": 400})})
		if err != nil {
			t.Fatal(err)
		}
	}
	before, err := store.Get("v13")
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.SetComparisons(context.Background(), "v13", ChoiceIDs{
		Cumulative: "v10:current", Previous: "v10:candidate", Reference: "v10:cluster",
	})
	if err != nil {
		t.Fatal(err)
	}
	after, err := store.Get("v13")
	if err != nil {
		t.Fatal(err)
	}
	c := after.Summary.Comparisons
	if c.Cumulative.Target.ID != "v10" || c.Previous.Target.ID != "v10" ||
		c.Reference.Target.ID != "v0:cluster" || c.Reference.Target.Title != "v0:cluster" {
		t.Fatalf("noncanonical choices: %+v", c)
	}
	archive, variant, err := targetArchive(after, c.Reference.Target, store.Get)
	if err != nil || archive.Summary.Experiment.ID != "v10" || variant != "cluster" {
		t.Fatalf("renamed reference lost archive: %s %s %v", archive.Summary.Experiment.ID, variant, err)
	}
	for _, id := range []string{"v0:cluster", "v10:cluster"} {
		result, err := DiagnosticsForTarget(store, id)
		if err != nil || result.Target != c.Reference.Target {
			t.Fatalf("reference alias %s: %+v %v", id, result.Target, err)
		}
	}
	before.Summary.Comparisons = after.Summary.Comparisons
	if !reflect.DeepEqual(before, after) {
		t.Fatal("renaming changed measurement or artifact data")
	}

	// Old manifests remain byte-for-byte intact; reads expose the new names.
	c.Cumulative.Target.ID = "v10:current"
	c.Reference.Target.ID = "v10:cluster"
	raw, _ := json.Marshal(after)
	path := filepath.Join(after.dir, "experiment.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	loaded, err := store.Get("v13")
	if err != nil || loaded.Summary.Comparisons.Reference.Target.ID != "v0:cluster" {
		t.Fatalf("legacy archive read failed: %+v %v", loaded.Summary.Comparisons, err)
	}
	untouched, err := os.ReadFile(path)
	if err != nil || string(untouched) != string(raw) {
		t.Fatal("read rewrote the archived manifest")
	}
	legacy, err := store.readRecord("v13")
	if err != nil || legacy.Summary.Comparisons.Reference.Target.ID != "v10:cluster" {
		t.Fatal("sync cannot recover the original manifest identity")
	}
	if canonicalTargetID("v3:cluster") != "v3:cluster" || canonicalTargetID("v13:cluster") != "v13:cluster" {
		t.Fatal("distinct Cluster measurements were collapsed")
	}
}

func TestSharedClusterReference(t *testing.T) {
	store := Store{Root: t.TempDir()}
	for _, item := range []struct {
		id      string
		cluster float64
	}{{"v3", 800}, {"v10", 400}} {
		_, err := store.Import(ImportConfig{Experiment: Label{ID: item.id, Title: item.id},
			Compare: testBenchmark(t, 'a', map[string]float64{"candidate": 200, "cluster": item.cluster})})
		if err != nil {
			t.Fatal(err)
		}
	}
	targets, err := store.Targets()
	if err != nil {
		t.Fatal(err)
	}
	clusterCount := 0
	for _, target := range targets {
		if target.Mode == "yes" {
			clusterCount++
			if target.ID != clusterReferenceID || target.Metrics.Rate.Median != 400 || target.ExperimentID != "v10" {
				t.Fatalf("wrong shared Cluster measurement: %+v", target)
			}
		}
	}
	if clusterCount != 1 {
		t.Fatalf("Cluster targets: %d", clusterCount)
	}
	summary, err := store.SetComparisons(context.Background(), "v3", ChoiceIDs{Reference: "v3:cluster"})
	if err != nil || summary.Comparisons.Reference.Target.ID != clusterReferenceID || summary.Comparisons.Reference.Result.Reached != 50 {
		t.Fatalf("old reference did not resolve to the shared measurement: %+v %v", summary.Comparisons, err)
	}

	// Migrating a saved snapshot must recalculate its ratios, not rename its old metrics.
	old := &TargetComparison{Target: Target{ID: "v3:cluster", Mode: "yes",
		Metrics: workflow.Metrics{Rate: workflow.Distribution{Median: 800}, P95: workflow.Distribution{Median: 10}}}}
	summary.Comparisons.Reference = old
	old.setResult(summary.Workload, summary.Harness, summary.Variants["current"].Metrics)
	if old.Result.Reached != 25 {
		t.Fatal("invalid old-reference fixture")
	}
	reference, _ := clusterReference(targets)
	useClusterReference(&summary, reference)
	if summary.Comparisons.Reference.Target != reference || summary.Comparisons.Reference.Result.Reached != 50 {
		t.Fatal("reference metrics and computed result were not migrated together")
	}
	useClusterReference(&summary, reference)
	if summary.Comparisons.Reference.Result.Reached != 50 {
		t.Fatal("reference migration was not idempotent")
	}
}

func TestPostgresTargetIDMigration(t *testing.T) {
	dsn := cmp.Or(os.Getenv("PROFILE_TEST_DSN_FILE"), os.Getenv("RAFT_WORKFLOW_TEST_DSN_FILE"))
	if dsn == "" {
		t.Skip("set PROFILE_TEST_DSN_FILE to run PostgreSQL integration tests")
	}
	ctx := context.Background()
	p, err := OpenPostgres(ctx, t.TempDir(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	id := "target-ids-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	rollback := errors.New("rollback test fixtures")
	err = p.db.Transaction(func(tx *gorm.DB) error {
		legacy := Target{ID: id + ":current", Title: "Old title", ExperimentID: id,
			Variant: "current", CumulativeID: id + ":current", ReferenceID: "v10:cluster"}
		if err := tx.Create(&targetRow{ID: legacy.ID, Benchmark: legacy}).Error; err != nil {
			return err
		}
		if err := tx.Create(&experimentRow{ID: id, State: "draft", Summary: Summary{
			Experiment: Label{ID: id, State: "draft"}}, ManifestSHA256: "immutable-manifest"}).Error; err != nil {
			return err
		}
		comparison := TargetComparison{Target: legacy, Result: &RelativeChange{Throughput: 27.4}, Warning: "keep"}
		if err := tx.Create(&comparisonRow{ExperimentID: id, Role: "reference", TargetID: legacy.ID, Comparison: comparison}).Error; err != nil {
			return err
		}
		if err := tx.Exec(`UPDATE raft_workflow.comparison_targets
            SET benchmark=benchmark || '{"unknown":1000000000000000001}'::jsonb WHERE id=?`, legacy.ID).Error; err != nil {
			return err
		}
		if err := migrateTargetIDs(tx); err != nil {
			return err
		}
		if err := tx.Exec("SET CONSTRAINTS ALL IMMEDIATE").Error; err != nil {
			return err
		}
		var target targetRow
		if err := tx.First(&target, "id=?", id).Error; err != nil {
			return err
		}
		if target.Benchmark != canonicalTarget(legacy) {
			t.Errorf("unexpected target: %+v", target.Benchmark)
		}
		var saved comparisonRow
		if err := tx.First(&saved, "experiment_id=?", id).Error; err != nil {
			return err
		}
		comparison.Target = canonicalTarget(legacy)
		if saved.TargetID != id || !reflect.DeepEqual(saved.Comparison, comparison) {
			t.Errorf("comparison snapshot changed: %+v", saved)
		}
		var unknown, digest string
		tx.Raw("SELECT benchmark->>'unknown' FROM raft_workflow.comparison_targets WHERE id=?", id).Scan(&unknown)
		tx.Raw("SELECT manifest_sha256 FROM raft_workflow.experiments WHERE id=?", id).Scan(&digest)
		if unknown != "1000000000000000001" || digest != "immutable-manifest" {
			t.Error("migration changed archive identity or unrecognized metadata")
		}
		if err := migrateTargetIDs(tx); err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	if err := p.db.Transaction(func(tx *gorm.DB) error {
		for _, key := range []string{id, id + ":current"} {
			if err := tx.Create(&targetRow{ID: key, Benchmark: Target{ID: key}}).Error; err != nil {
				return err
			}
		}
		return migrateTargetIDs(tx)
	}); !errors.Is(dbError(err), ErrExists) {
		t.Fatalf("target collision did not abort migration: %v", err)
	}
	var count int64
	if err := p.db.Model(&targetRow{}).Where("id IN ?", []string{id, id + ":current"}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("collision left partial changes: %d %v", count, err)
	}
}

func TestPostgresSharedClusterReference(t *testing.T) {
	dsn := cmp.Or(os.Getenv("PROFILE_TEST_DSN_FILE"), os.Getenv("RAFT_WORKFLOW_TEST_DSN_FILE"))
	if dsn == "" {
		t.Skip("set PROFILE_TEST_DSN_FILE to run PostgreSQL integration tests")
	}
	p, err := OpenPostgres(context.Background(), t.TempDir(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	id := "shared-cluster-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	rollback := errors.New("rollback test fixtures")
	err = p.db.Transaction(func(tx *gorm.DB) error {
		var exists int64
		if err := tx.Model(&targetRow{}).Where("id=?", clusterReferenceID).Count(&exists).Error; err != nil {
			return err
		}
		if exists > 0 {
			t.Skip("shared reference fixture requires an isolated empty test database")
		}
		reference := Target{ID: clusterReferenceID, Title: clusterReferenceID, Mode: "yes", Variant: "cluster",
			Metrics: workflow.Metrics{Rate: workflow.Distribution{Median: 400}, P95: workflow.Distribution{Median: 10}}}
		old := reference
		old.ID, old.Title, old.Metrics.Rate.Median = id+":cluster", "old", 800
		for _, target := range []Target{reference, old, {ID: id, Mode: "raft", ReferenceID: old.ID}} {
			if err := tx.Create(&targetRow{ID: target.ID, Benchmark: target}).Error; err != nil {
				return err
			}
		}
		metrics := workflow.Metrics{Rate: workflow.Distribution{Median: 200}, P95: workflow.Distribution{Median: 10}}
		summary := Summary{Experiment: Label{ID: id, State: "complete"},
			Variants:    map[string]Variant{"current": {Metrics: metrics}},
			Comparisons: Comparisons{Reference: &TargetComparison{Target: old}}}
		summary.Comparisons.Reference.setResult(summary.Workload, summary.Harness, metrics)
		if err := p.writeRecord(tx, Record{Schema: Schema, Detail: Detail{Summary: summary}}); err != nil {
			return err
		}
		if err := consolidateClusterReference(tx); err != nil {
			return err
		}
		if err := tx.Exec("SET CONSTRAINTS ALL IMMEDIATE").Error; err != nil {
			return err
		}
		var saved comparisonRow
		if err := tx.First(&saved, "experiment_id=?", id).Error; err != nil {
			return err
		}
		if saved.TargetID != clusterReferenceID || saved.Comparison.Target != reference || saved.Comparison.Result.Reached != 50 {
			t.Errorf("invalid migrated reference: %+v", saved)
		}
		var count int64
		if err := tx.Model(&targetRow{}).Where("benchmark->>'mode'='yes'").Count(&count).Error; err != nil {
			return err
		}
		if count != 1 {
			t.Errorf("expected one Cluster target, got %d", count)
		}
		var current targetRow
		if err := tx.First(&current, "id=?", id).Error; err != nil {
			return err
		}
		if current.Benchmark.ReferenceID != clusterReferenceID {
			t.Error("future experiments would inherit a removed reference")
		}
		if err := consolidateClusterReference(tx); err != nil {
			return err
		}
		return rollback
	})
	if !errors.Is(err, rollback) {
		t.Fatal(err)
	}
}
