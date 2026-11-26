// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package models

import (
	"encoding/json"
	"strings"

	"gorm.io/gorm"
)

// canonicalTargetID retains old IDs as input aliases without changing archive ownership.
func canonicalTargetID(id string) string {
	// v10's Cluster measurement is the selected v0 reference. Other Cluster runs are distinct.
	if id == "v10:cluster" {
		return "v0:cluster"
	}
	for _, suffix := range []string{":current", ":candidate"} {
		if strings.HasSuffix(id, suffix) {
			return strings.TrimSuffix(id, suffix)
		}
	}
	return id
}

func canonicalTarget(target Target) Target {
	id := canonicalTargetID(target.ID)
	if target.ExperimentID != "" || id != target.ID {
		target.Title = id
	}
	target.ID = id
	target.CumulativeID = canonicalTargetID(target.CumulativeID)
	target.ReferenceID = canonicalTargetID(target.ReferenceID)
	return target
}

func (c Comparisons) canonicalize() {
	for _, comparison := range c.byRole() {
		if comparison != nil {
			comparison.Target = canonicalTarget(comparison.Target)
		}
	}
}

// targetIDPatch changes only naming fields, preserving all stored measurements and metadata.
func targetIDPatch(target Target) ([]byte, error) {
	canonical := canonicalTarget(target)
	patch := map[string]string{}
	for key, values := range map[string][2]string{
		"id":                     {target.ID, canonical.ID},
		"title":                  {target.Title, canonical.Title},
		"cumulative_baseline_id": {target.CumulativeID, canonical.CumulativeID},
		"reference_id":           {target.ReferenceID, canonical.ReferenceID},
	} {
		if values[0] != values[1] {
			patch[key] = values[1]
		}
	}
	return json.Marshal(patch)
}

// migrateTargetIDs runs inside the schema transaction. Key collisions abort the entire migration.
func migrateTargetIDs(tx *gorm.DB) error {
	if err := lockCatalog(tx); err != nil {
		return err
	}
	var targets []targetRow
	if err := tx.Find(&targets).Error; err != nil {
		return err
	}
	for _, row := range targets {
		patch, err := targetIDPatch(row.Benchmark)
		if err != nil {
			return err
		}
		if err := tx.Exec(`UPDATE raft_workflow.comparison_targets
            SET id=?, benchmark=benchmark || ?::jsonb WHERE id=?`,
			canonicalTargetID(row.ID), string(patch), row.ID).Error; err != nil {
			return err
		}
	}
	var comparisons []comparisonRow
	if err := tx.Find(&comparisons).Error; err != nil {
		return err
	}
	for _, row := range comparisons {
		patch, err := targetIDPatch(row.Comparison.Target)
		if err != nil {
			return err
		}
		if err := tx.Exec(`UPDATE raft_workflow.experiment_comparisons
            SET target_id=?, comparison=jsonb_set(comparison, '{target}',
                (comparison->'target') || ?::jsonb)
            WHERE experiment_id=? AND role=?`,
			canonicalTargetID(row.TargetID), string(patch), row.ExperimentID, row.Role).Error; err != nil {
			return err
		}
	}
	return nil
}
