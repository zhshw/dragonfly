// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package execution

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"dragonflydb.io/profile/models"
	"dragonflydb.io/profile/pkg/workflow"
)

var (
	ErrBusy     = errors.New("测试机正在运行其他实验")
	ErrComplete = errors.New("该实验已有正式结果")
)

type StepStatus struct {
	Name   string `json:"name"`
	Status string `json:"status"`
}

type Progress struct {
	Enabled bool              `json:"enabled"`
	Busy    string            `json:"busy,omitempty"`
	Run     *models.Execution `json:"execution,omitempty"`
	Steps   []StepStatus      `json:"steps,omitempty"`
	Log     bool              `json:"has_log"`
}

// Manager owns one benchmark job at a time and archives its outputs under the saved models.
type Manager struct {
	store   *models.Postgres
	config  workflow.Config
	steps   []string
	ctx     context.Context
	cancel  context.CancelFunc
	release func()

	mu     sync.Mutex
	active string
	jobs   sync.WaitGroup
}

func Open(ctx context.Context, store *models.Postgres, config workflow.Config) (*Manager, error) {
	if config.Baseline != "" || config.IncludeCluster {
		return nil, errors.New("the platform profile runs only the current build; select archived comparisons in the experiment")
	}
	if config.Profiler == "" || config.Flamegraph == "" {
		return nil, errors.New("the platform profile requires profiler and flamegraph_dir")
	}
	config.Diagnostics, config.Publish = true, true
	leaseCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	release, err := store.LockExecutor(leaseCtx)
	if err != nil {
		return nil, err
	}
	jobCtx, stop := context.WithCancel(ctx)
	return &Manager{store: store, config: config, steps: workflow.StepNames(config),
		ctx: jobCtx, cancel: stop, release: release}, nil
}

func (m *Manager) Close() {
	m.mu.Lock()
	m.cancel()
	m.mu.Unlock()
	m.jobs.Wait()
	m.release()
}

// Start schedules the draft independently of the HTTP request. A retry continues the checkpoint.
func (m *Manager) Start(ctx context.Context, draft models.Summary, restart bool) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if err := m.ctx.Err(); err != nil {
		return err
	}
	id := draft.Experiment.ID
	if m.active == id {
		return nil
	}
	if m.active != "" {
		return ErrBusy
	}
	if draft.Experiment.State != "draft" {
		return ErrComplete
	}
	config, err := m.config.ForExperiment(id)
	if err != nil {
		return fmt.Errorf("%w: %w", models.ErrInvalid, err)
	}
	if err := draft.CheckWorkflow(config.Repo); err != nil {
		return err
	}
	last, err := m.store.Execution(ctx, id)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	attempt := last.Attempt
	resume := false
	if attempt == 0 || restart {
		attempt++
	} else {
		output := m.output(id, attempt)
		if _, err := os.Stat(filepath.Join(output, "state.json")); err == nil {
			resume = true
		} else if !errors.Is(err, os.ErrNotExist) {
			return err
		} else if _, err := os.Stat(output); !errors.Is(err, os.ErrNotExist) {
			if err != nil {
				return err
			}
			attempt++
		}
	}
	config.Output = m.output(id, attempt)
	if err := os.MkdirAll(filepath.Dir(config.Output), 0750); err != nil {
		return err
	}
	if err := m.store.StartExecution(ctx, id, attempt); err != nil {
		return err
	}
	m.active = id
	m.jobs.Add(1)
	go m.run(id, attempt, config, resume)
	return nil
}

func (m *Manager) Status(ctx context.Context, id string) (Progress, error) {
	progress := Progress{Enabled: true}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active != id {
		progress.Busy = m.active
	}
	run, err := m.store.Execution(ctx, id)
	if errors.Is(err, os.ErrNotExist) {
		return progress, nil
	}
	if err != nil {
		return progress, err
	}
	if run.Status == "running" && m.active != id {
		if err := m.store.ReconcileExecution(ctx, id); err != nil {
			return progress, err
		}
		run, err = m.store.Execution(ctx, id)
		if err != nil {
			return progress, err
		}
	}
	progress.Run = &run
	output := m.output(id, run.Attempt)
	_, err = os.Stat(output + ".log")
	progress.Log = err == nil
	state, err := workflow.ReadState(output)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return progress, err
	}
	progress.Steps = make([]StepStatus, 0, len(m.steps)+1)
	for _, name := range m.steps {
		status := "pending"
		if record := state.Steps[name]; record != nil {
			status = record.Status
			if status == "running" && run.Status == "failed" {
				status = "failed"
			}
		}
		progress.Steps = append(progress.Steps, StepStatus{name, status})
	}
	archive := "pending"
	if state.Status == "done" {
		archive = run.Status
		if archive == "complete" {
			archive = "done"
		}
	}
	progress.Steps = append(progress.Steps, StepStatus{"archive", archive})
	return progress, nil
}

func (m *Manager) Log(ctx context.Context, id string) (string, error) {
	run, err := m.store.Execution(ctx, id)
	if err != nil {
		return "", err
	}
	return m.output(id, run.Attempt) + ".log", nil
}

func (m *Manager) Delete(ctx context.Context, id string) (models.Deletion, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	result, err := m.store.Delete(ctx, id)
	if err != nil {
		return result, err
	}
	if err := os.RemoveAll(filepath.Join(m.config.Output, id)); err != nil {
		log.Printf("remove experiment %s execution files: %v", id, err)
		result.Warning = "实验已删除，部分文件清理失败"
	}
	return result, nil
}

func (m *Manager) output(id string, attempt int) string {
	return filepath.Join(m.config.Output, id, fmt.Sprintf("%s-run-%03d", id, attempt))
}

func (m *Manager) run(id string, attempt int, config workflow.Config, resume bool) {
	defer m.jobs.Done()
	defer func() {
		m.mu.Lock()
		m.active = ""
		m.mu.Unlock()
	}()
	failure := ""
	file, err := os.OpenFile(config.Output+".log", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0640)
	if err == nil {
		defer file.Close()
		fmt.Fprintf(file, "[%s] %s\nSource: %s\n", time.Now().Format(time.RFC3339), id, config.Repo)
		err = m.collectAndArchive(id, config, resume, file)
	}
	if err != nil {
		failure, _, _ = strings.Cut(err.Error(), "\n")
		if file != nil {
			fmt.Fprintln(file, err)
		}
		log.Printf("experiment %s execution %d: %v", id, attempt, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := m.store.FinishExecution(ctx, id, attempt, failure); err != nil {
		log.Printf("save execution status for %s: %v", id, err)
	}
}

func (m *Manager) collectAndArchive(id string, config workflow.Config, resume bool, progress io.Writer) error {
	if err := workflow.Run(m.ctx, config, resume, progress); err != nil {
		return err
	}
	archive, err := models.WorkflowArchive(id, config.Output)
	if err != nil {
		return err
	}
	fmt.Fprintln(progress, "Archiving benchmark, trace and flamegraphs")
	result, err := m.store.Import(m.ctx, archive)
	if err != nil {
		return fmt.Errorf("archive benchmark: %w", err)
	}
	evidence := result.Evidence
	fmt.Fprintf(progress, "Archived %d benchmark runs, %d diagnostic run, %d flamegraphs\n",
		evidence.Benchmark, evidence.Diagnostics, evidence.Flamegraphs)
	return nil
}
