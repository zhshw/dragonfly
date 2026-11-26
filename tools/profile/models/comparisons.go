// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package models

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"gorm.io/gorm"
)

// configuredComparisons changes references without changing the measured workload or archive.
func configuredComparisons(summary Summary, ids ChoiceIDs, targets []Target) (Comparisons, error) {
	var result Comparisons
	if summary.Experiment.State != "complete" {
		return result, fmt.Errorf("%w: 仅已完成实验可以调整对照", ErrInvalid)
	}
	current, ok := summary.Variants["current"]
	if !ok {
		current, ok = summary.Variants["candidate"]
	}
	if !ok {
		return result, fmt.Errorf("%w: 缺少本轮正式结果", ErrInvalid)
	}
	for _, selection := range []struct {
		id, role string
		dest     **TargetComparison
	}{
		{ids.Cumulative, "基准", &result.Cumulative},
		{ids.Previous, "上一轮", &result.Previous},
		{ids.Reference, "对比参照", &result.Reference},
	} {
		if selection.id == "" {
			continue
		}
		var target Target
		if selection.role == "上一轮" {
			target, _ = findPreviousTarget(selection.id, targets)
		} else {
			for _, candidate := range targets {
				if candidate.ID == resolveTargetID(selection.id, targets) {
					target = candidate
					break
				}
			}
		}
		if target.ID == "" {
			return result, fmt.Errorf("%w: %s记录不存在", ErrInvalid, selection.role)
		}
		if selection.role != "对比参照" && (target.Mode != "raft" || target.Ack != "memory") {
			return result, fmt.Errorf("%w: %s需要 Raft memory 正式结果", ErrInvalid, selection.role)
		}
		if selection.role == "上一轮" && (target.ExperimentID == "" || (target.Variant != "current" && target.Variant != "candidate")) {
			return result, fmt.Errorf("%w: 上一轮需要已完成实验的本轮结果", ErrInvalid)
		}
		if selection.role == "上一轮" && target.ExperimentID == summary.Experiment.ID {
			return result, fmt.Errorf("%w: 上一轮不能选择本轮实验", ErrInvalid)
		}
		choice := &TargetComparison{Target: target}
		choice.setResult(summary.Workload, summary.Harness, current.Metrics)
		*selection.dest = choice
	}
	return result, nil
}

func (s Store) SetComparisons(ctx context.Context, id string, ids ChoiceIDs) (Summary, error) {
	if err := ctx.Err(); err != nil {
		return Summary{}, err
	}
	record, err := s.Get(id)
	if err != nil {
		return Summary{}, err
	}
	targets, err := s.Targets()
	if err != nil {
		return Summary{}, err
	}
	record.Summary.Comparisons, err = configuredComparisons(record.Summary, ids, targets)
	if err != nil {
		return Summary{}, err
	}
	file, err := os.CreateTemp(record.dir, ".comparisons-")
	if err != nil {
		return Summary{}, err
	}
	defer os.Remove(file.Name())
	encoder := json.NewEncoder(file)
	encoder.SetIndent("", "  ")
	if err := errors.Join(encoder.Encode(record), file.Close()); err != nil {
		return Summary{}, err
	}
	if err := os.Rename(file.Name(), filepath.Join(record.dir, "experiment.json")); err != nil {
		return Summary{}, err
	}
	return record.Summary, nil
}

func (p *Postgres) SetComparisons(ctx context.Context, id string, ids ChoiceIDs) (Summary, error) {
	var summary Summary
	err := p.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockCatalog(tx); err != nil {
			return err
		}
		if _, err := lockedExperiment(tx, id); err != nil {
			return err
		}
		if err := scanJSON(tx.Raw("SELECT summary FROM raft_workflow.experiment_summaries WHERE id=?", id).Row(), &summary); err != nil {
			return err
		}
		var rows []targetRow
		if err := tx.Find(&rows).Error; err != nil {
			return err
		}
		targets := make([]Target, len(rows))
		for i, row := range rows {
			targets[i] = row.Benchmark
		}
		choices, err := configuredComparisons(summary, ids, targets)
		if err != nil {
			return err
		}
		if err := tx.Where("experiment_id = ?", id).Delete(&comparisonRow{}).Error; err != nil {
			return err
		}
		for role, choice := range choices.byRole() {
			if choice != nil {
				row := comparisonRow{ExperimentID: id, Role: role, TargetID: choice.Target.ID, Comparison: *choice}
				if err := tx.Create(&row).Error; err != nil {
					return err
				}
			}
		}
		summary.Comparisons = choices

		// Future experiments inherit these choices; existing snapshots keep their references.
		for _, target := range targetsFrom(summary) {
			if err := tx.Model(&targetRow{}).Where("id = ?", target.ID).Select("benchmark").Updates(targetRow{Benchmark: target}).Error; err != nil {
				return err
			}
		}
		return nil
	})
	return summary, dbError(err)
}
