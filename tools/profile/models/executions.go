// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package models

import (
	"context"
	"database/sql/driver"
	"errors"
	"os"
	"time"

	"gorm.io/gorm"
)

type Execution struct {
	Attempt  int        `json:"attempt"`
	Status   string     `json:"status"`
	Started  time.Time  `json:"started_at"`
	Finished *time.Time `json:"finished_at,omitempty"`
	Error    string     `json:"error,omitempty"`
}

const reconcileExecutions = `UPDATE raft_workflow.executions x
SET status=CASE WHEN e.state='complete' THEN 'complete' ELSE 'failed' END,
    finished_at=now(), error=CASE WHEN e.state='complete' THEN '' ELSE '执行被中断，可继续运行' END
FROM raft_workflow.experiments e WHERE e.id=x.experiment_id AND x.status='running'`

// LockExecutor grants one platform process ownership of the execution records until shutdown.
func (p *Postgres) LockExecutor(ctx context.Context) (func(), error) {
	conn, err := p.pool.Conn(ctx)
	if err != nil {
		return nil, err
	}
	// The lock belongs to a PostgreSQL session, so use one pinned connection.
	lease := p.db.Session(&gorm.Session{NewDB: true, Context: ctx})
	lease.Statement.ConnPool = conn
	const key = "hashtext('dragonfly-raft-workflow-executor')"
	var locked bool
	if err := lease.Raw(`SELECT pg_try_advisory_lock(` + key + `)`).Row().Scan(&locked); err != nil || !locked {
		conn.Close()
		if err != nil {
			return nil, err
		}
		return nil, errors.New("another platform process owns the benchmark executor")
	}
	release := func() {
		ctx, cancel := queryContext()
		defer cancel()
		if err := lease.WithContext(ctx).Exec(`SELECT pg_advisory_unlock(` + key + `)`).Error; err != nil {
			_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		}
		_ = conn.Close()
	}
	// A previous process may have stopped after its import committed.
	if err := lease.Exec(reconcileExecutions).Error; err != nil {
		release()
		return nil, err
	}
	return release, nil
}

// ReconcileExecution records the durable result if the executor stopped before saving its status.
func (p *Postgres) ReconcileExecution(ctx context.Context, id string) error {
	return p.db.WithContext(ctx).Exec(reconcileExecutions+` AND x.experiment_id=?`, id).Error
}

func (p *Postgres) Execution(ctx context.Context, id string) (Execution, error) {
	var row executionRow
	err := p.db.WithContext(ctx).Where("experiment_id = ?", id).Order("attempt DESC").Take(&row).Error
	return Execution{Attempt: row.Attempt, Status: row.Status, Started: row.StartedAt, Finished: row.FinishedAt, Error: row.Error}, dbError(err)
}

func (p *Postgres) StartExecution(ctx context.Context, id string, attempt int) error {
	result := p.db.WithContext(ctx).Exec(`INSERT INTO raft_workflow.executions (experiment_id,attempt,status)
SELECT id,?,'running' FROM raft_workflow.experiments WHERE id=? AND state='draft'
ON CONFLICT (experiment_id,attempt) DO UPDATE SET status='running',started_at=now(),finished_at=NULL,error=''`, attempt, id)
	if result.Error != nil {
		return dbError(result.Error)
	}
	if result.RowsAffected == 0 {
		return os.ErrNotExist
	}
	return nil
}

func (p *Postgres) FinishExecution(ctx context.Context, id string, attempt int, failure string) error {
	status := "complete"
	if failure != "" {
		status = "failed"
	}
	return p.db.WithContext(ctx).Model(&executionRow{}).
		Where("experiment_id = ? AND attempt = ? AND status = ?", id, attempt, "running").
		Updates(map[string]any{"status": status, "finished_at": gorm.Expr("now()"), "error": failure}).Error
}
