// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package models

import (
	"encoding/json"
	"errors"
	"strings"

	"gorm.io/gorm"
)

const clusterReferenceID = "v0:cluster"

func resolveTargetID(id string, targets []Target) string {
	if _, ok := clusterReference(targets); ok && strings.HasSuffix(id, ":cluster") {
		return clusterReferenceID
	}
	return canonicalTargetID(id)
}

func clusterReference(targets []Target) (Target, bool) {
	for _, target := range targets {
		if target.ID == clusterReferenceID && target.Mode == "yes" {
			return target, true
		}
	}
	return Target{}, false
}

// singleClusterReference keeps the selected measurement, never relabeling another run's metrics.
func singleClusterReference(targets []Target) []Target {
	if _, ok := clusterReference(targets); !ok {
		return targets
	}
	result := make([]Target, 0, len(targets))
	for _, target := range targets {
		if target.Mode == "yes" && target.ID != clusterReferenceID {
			continue
		}
		if strings.HasSuffix(target.ReferenceID, ":cluster") {
			target.ReferenceID = clusterReferenceID
		}
		result = append(result, target)
	}
	return result
}

func useClusterReference(summary *Summary, reference Target) {
	for _, choice := range summary.Comparisons.byRole() {
		if choice == nil {
			continue
		}
		if choice.Target.Mode == "yes" && choice.Target.ID != reference.ID {
			choice.Target = reference
			if current, ok := summary.Variants["current"]; ok {
				choice.setResult(summary.Workload, summary.Harness, current.Metrics)
			} else if candidate, ok := summary.Variants["candidate"]; ok {
				choice.setResult(summary.Workload, summary.Harness, candidate.Metrics)
			} else {
				choice.Result, choice.Warning = nil, ""
			}
		}
		if strings.HasSuffix(choice.Target.ReferenceID, ":cluster") {
			choice.Target.ReferenceID = reference.ID
		}
	}
}

// consolidateClusterReference retires redundant catalog measurements and updates all snapshots
// in the same transaction. Raw experiment archives keep their original provenance.
func consolidateClusterReference(tx *gorm.DB) error {
	if err := lockCatalog(tx); err != nil {
		return err
	}
	var reference targetRow
	if err := tx.First(&reference, "id=? AND benchmark->>'mode'='yes'", clusterReferenceID).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return nil
		}
		return err
	}
	var summaries []experimentRow
	if err := tx.Table("raft_workflow.experiment_summaries").Select("id", "summary").Find(&summaries).Error; err != nil {
		return err
	}
	for _, row := range summaries {
		before, err := json.Marshal(row.Summary.Comparisons)
		if err != nil {
			return err
		}
		useClusterReference(&row.Summary, reference.Benchmark)
		after, err := json.Marshal(row.Summary.Comparisons)
		if err != nil {
			return err
		}
		if string(before) == string(after) {
			continue
		}
		for role, choice := range row.Summary.Comparisons.byRole() {
			if choice == nil {
				continue
			}
			update := comparisonRow{TargetID: choice.Target.ID, Comparison: *choice}
			if err := tx.Model(&comparisonRow{}).Where("experiment_id=? AND role=?", row.ID, role).
				Select("target_id", "comparison").Updates(update).Error; err != nil {
				return err
			}
		}
	}
	if err := tx.Exec(`UPDATE raft_workflow.comparison_targets
        SET benchmark=jsonb_set(benchmark, '{reference_id}', to_jsonb(?::text))
        WHERE benchmark->>'reference_id' LIKE '%:cluster'`, clusterReferenceID).Error; err != nil {
		return err
	}
	return tx.Where("benchmark->>'mode'='yes' AND id<>?", clusterReferenceID).Delete(&targetRow{}).Error
}
