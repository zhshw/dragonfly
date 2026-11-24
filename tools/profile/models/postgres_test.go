// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package models

import (
	"cmp"
	"context"
	"errors"
	"os"
	"strconv"
	"testing"
	"time"
)

func TestPostgresExecutionLifecycle(t *testing.T) {
	dsn := cmp.Or(os.Getenv("PROFILE_TEST_DSN_FILE"), os.Getenv("RAFT_WORKFLOW_TEST_DSN_FILE"))
	if dsn == "" {
		t.Skip("set PROFILE_TEST_DSN_FILE to run PostgreSQL integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	p, err := OpenPostgres(ctx, t.TempDir(), dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = p.Close() })
	id := "gorm-execution-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	label := Label{ID: id, Title: "PostgreSQL execution lifecycle", State: "draft"}
	record := Record{Schema: Schema, Detail: Detail{Summary: Summary{Experiment: label}}}
	if err := p.write(ctx, []Record{record}, nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = p.FinishExecution(cleanup, id, 1, "test cleanup")
		if _, err := p.Delete(cleanup, id); err != nil && !errors.Is(err, os.ErrNotExist) {
			t.Error(err)
		}
	})
	if err := p.StartExecution(ctx, id, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Delete(ctx, id); !errors.Is(err, ErrInUse) {
		t.Fatalf("delete running experiment returned %v", err)
	}
	if err := p.FinishExecution(ctx, id, 1, "retryable failure"); err != nil {
		t.Fatal(err)
	}
	failed, err := p.Execution(ctx, id)
	if err != nil || failed.Status != "failed" || failed.Finished == nil || failed.Error != "retryable failure" {
		t.Fatalf("unexpected failure record %+v, %v", failed, err)
	}
	if err := p.StartExecution(ctx, id, 1); err != nil {
		t.Fatal(err)
	}
	running, err := p.Execution(ctx, id)
	if err != nil || running.Attempt != 1 || running.Status != "running" || running.Started.IsZero() || running.Finished != nil || running.Error != "" {
		t.Fatalf("unexpected retry record %+v, %v", running, err)
	}
	if err := p.FinishExecution(ctx, id, 1, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Delete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Execution(ctx, id); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("execution remained after deletion: %v", err)
	}
	if err := p.StartExecution(ctx, id, 1); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("start deleted experiment returned %v", err)
	}
}
