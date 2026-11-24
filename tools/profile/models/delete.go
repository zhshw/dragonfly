// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package models

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/jackc/pgx/v5/pgconn"
)

var ErrInUse = errors.New("experiment is in use")

type Deletion struct {
	Warning string `json:"warning,omitempty"`
}

// Delete removes an experiment, its catalog entries and its active archive directory.
func (p *Postgres) Delete(ctx context.Context, id string) (Deletion, error) {
	var result Deletion
	if !slug.MatchString(id) {
		return result, os.ErrNotExist
	}
	tx := p.db.WithContext(ctx).Begin()
	if tx.Error != nil {
		return result, tx.Error
	}
	defer tx.Rollback()
	if err := lockCatalog(tx); err != nil {
		return result, err
	}
	record, err := lockedExperiment(tx, id)
	if err != nil {
		return result, err
	}
	var active int64
	if err := tx.Model(&executionRow{}).Where("experiment_id = ? AND status = ?", id, "running").Count(&active).Error; err != nil {
		return result, err
	}
	if active != 0 {
		return result, fmt.Errorf("%w: 实验仍在运行", ErrInUse)
	}
	var dependent sql.NullString
	if err := tx.Raw(`SELECT min(experiment_id) FROM raft_workflow.experiment_comparisons
WHERE experiment_id<>? AND (comparison->'target'->>'experiment_id'=? OR target_id IN (
SELECT id FROM raft_workflow.comparison_targets WHERE benchmark->>'experiment_id'=?))`, id, id, id).Row().
		Scan(&dependent); err != nil {
		return result, err
	}
	if dependent.Valid {
		return result, fmt.Errorf("%w: %s 将此实验用作对照", ErrInUse, dependent.String)
	}
	if err := tx.Where("id = ?", id).Delete(&experimentRow{}).Error; err != nil {
		return result, err
	}
	if err := tx.Where("benchmark->>'experiment_id' = ?", id).Delete(&targetRow{}).Error; err != nil {
		return result, err
	}
	if err := tx.Commit().Error; err != nil {
		var conflict *pgconn.PgError
		if errors.As(err, &conflict) && conflict.ConstraintName == "experiment_comparison_target_fk" {
			return result, fmt.Errorf("%w: 此实验刚被选为对照", ErrInUse)
		}
		return result, err
	}
	if assets := record.AssetDir; assets != "" && validAssetDir(id, assets) {
		if err := os.RemoveAll(filepath.Join(p.files.Root, assets)); err != nil {
			log.Printf("remove experiment %s assets: %v", id, err)
			result.Warning = "实验已删除，归档文件清理失败"
		}
		_ = os.Remove(filepath.Join(p.files.Root, id))
	}
	return result, nil
}
