// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package models

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gorm.io/gorm"
)

// RefreshCurrent replaces one completed experiment's current measurement using a verified rerun.
func (p *Postgres) RefreshCurrent(ctx context.Context, id, output string) (Summary, error) {
	var empty Summary
	if !slug.MatchString(id) {
		return empty, fmt.Errorf("%w: invalid experiment ID", ErrInvalid)
	}
	config, err := WorkflowArchive(id, output)
	if err != nil {
		return empty, err
	}
	tx := p.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return empty, tx.Error
	}
	defer tx.Rollback()
	if err := lockCatalog(tx); err != nil {
		return empty, err
	}
	record, err := lockedExperiment(tx, id)
	if err != nil {
		return empty, err
	}
	if record.State != "complete" || !validAssetDir(id, record.AssetDir) {
		return empty, fmt.Errorf("%w: rerun requires a completed archive", ErrInvalid)
	}
	var dependent sql.NullString
	if err := tx.Model(&comparisonRow{}).Select("min(experiment_id)").Where("target_id IN ?", []string{id, id + ":current", id + ":candidate"}).
		Row().Scan(&dependent); err != nil {
		return empty, err
	}
	if dependent.Valid {
		return empty, fmt.Errorf("%s uses %s's current measurement as a comparison", dependent.String, id)
	}
	prior := Record{Schema: Schema, dir: filepath.Join(p.files.Root, record.AssetDir)}
	if err := scanJSON(tx.Raw(experimentDetail, id).Row(), &prior.Detail); err != nil {
		return empty, err
	}
	assets, err := os.MkdirTemp(filepath.Join(p.files.Root, id), "assets-")
	if err != nil {
		return empty, err
	}
	keepAssets := false
	defer func() {
		if !keepAssets {
			_ = os.RemoveAll(assets)
		}
	}()
	fresh, err := buildRecord(config, prior.Summary.Experiment, assets, func(Summary) (Comparisons, error) {
		return prior.Summary.Comparisons, nil
	})
	if err != nil {
		return empty, err
	}
	if len(fresh.Summary.Variants) != 1 || prior.Summary.Variants["current"].SHA != fresh.Summary.Variants["current"].SHA ||
		prior.Summary.Workload != fresh.Summary.Workload || prior.Summary.Harness != fresh.Summary.Harness {
		return empty, fmt.Errorf("%w: rerun must use the same binary and workload", ErrInvalid)
	}
	if err := p.preserveComparedAssets(tx, prior, &fresh, assets); err != nil {
		return empty, err
	}
	mergeRerun(&prior, fresh)
	prior.dir = assets
	relative, err := filepath.Rel(p.files.Root, assets)
	if err != nil {
		return empty, err
	}
	for _, table := range []any{&runRow{}, &benchmarkSetRow{}, &diagnosticRow{}, &artifactRow{}, &validationCheckRow{}, &comparisonRow{}} {
		if err := tx.Where("experiment_id = ?", id).Delete(table).Error; err != nil {
			return empty, err
		}
	}
	encoded, err := json.Marshal(prior)
	if err != nil {
		return empty, err
	}
	if err := updateExperiment(tx, storedExperiment(prior.Summary, relative, manifestHash(encoded))); err != nil {
		return empty, err
	}
	if err := p.writeChildren(tx, prior); err != nil {
		return empty, err
	}
	for _, target := range targetsFrom(prior.Summary) {
		if target.Variant == "current" {
			if err := tx.Model(&targetRow{}).Where("id = ?", target.ID).Select("benchmark").Updates(targetRow{Benchmark: target}).Error; err != nil {
				return empty, err
			}
		}
	}

	// Preserve the new asset directory if the commit result is lost with the SQL connection.
	keepAssets = true
	if err := tx.Commit().Error; err != nil {
		return empty, err
	}
	return prior.Summary, nil
}

func (p *Postgres) preserveComparedAssets(tx *gorm.DB, prior Record, fresh *Record, assets string) error {
	id := prior.Summary.Experiment.ID
	var rows []artifactRow
	if err := tx.Select("id", "filename").Where("experiment_id = ?", id).Find(&rows).Error; err != nil {
		return err
	}
	prior.Artifacts = map[string]string{}
	for _, row := range rows {
		key, filename := row.ID, row.Filename
		if _, replaced := fresh.Artifacts[key]; replaced || strings.HasPrefix(key, "validation-") ||
			strings.HasPrefix(key, "diagnostic-current-") || strings.HasPrefix(key, "benchmark-current-") {
			continue
		}
		prior.Artifacts[key] = filename
		source, err := p.files.Artifact(prior, key)
		if err != nil {
			return err
		}
		if err := fresh.copyArtifact(filepath.Join(assets, "artifacts"), key, source, filepath.Ext(filename)); err != nil {
			return err
		}
	}
	for _, run := range prior.Runs {
		if refreshed(run) {
			continue
		}
		var row runRow
		if err := tx.Select("measurement").Where("experiment_id = ? AND id = ?", id, run.ID).Take(&row).Error; err != nil {
			return err
		}
		fresh.Measurements[run.ID] = row.Measurement
	}
	return nil
}

func refreshed(run Run) bool {
	return run.Variant == "current" && (run.Set == "main" || run.Kind == "diagnostic")
}

func mergeRerun(prior *Record, fresh Record) {
	prior.Summary.Variants["current"] = fresh.Summary.Variants["current"]
	prior.Summary.Comparisons = fresh.Summary.Comparisons
	prior.Summary.Delta = fresh.Summary.Delta
	prior.Summary.Validation = fresh.Summary.Validation
	if prior.Summary.Diagnostics == nil {
		prior.Summary.Diagnostics = map[string]Diagnostic{}
	}
	prior.Summary.Diagnostics["current"] = fresh.Summary.Diagnostics["current"]
	for i, set := range prior.Benchmarks {
		if set.ID == "main" {
			prior.Benchmarks[i].Variants["current"] = fresh.Summary.Variants["current"]
		}
	}
	for _, run := range prior.Runs {
		if !refreshed(run) {
			fresh.Runs = append(fresh.Runs, run)
		}
	}
	for _, observation := range prior.Observations {
		if observation.Variant != "current" {
			fresh.Observations = append(fresh.Observations, observation)
		}
	}
	prior.Runs, prior.Observations, prior.Checks = fresh.Runs, fresh.Observations, fresh.Checks
	prior.Artifacts, prior.Measurements = fresh.Artifacts, fresh.Measurements
	var evidence Evidence
	for _, run := range prior.Runs {
		if run.Kind == "benchmark" && run.Set == "main" {
			evidence.Benchmark++
			evidence.Confirmed++
		}
	}
	for _, observation := range prior.Observations {
		evidence.Diagnostics++
		evidence.Flamegraphs += len(observation.Flamegraphs)
	}
	prior.Summary.Evidence = evidence
}
