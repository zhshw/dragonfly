// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package models

import (
	"database/sql"
	"errors"
	"fmt"
	"os"

	"dragonflydb.io/profile/pkg/workflow"
	"gorm.io/gorm"
)

type FlamegraphQuery struct {
	Run, Role, View, Symbol string
	Limit, CallerDepth      int
}

type FlamegraphReport struct {
	ExperimentID string                   `json:"experiment_id"`
	Run          string                   `json:"run"`
	Role         string                   `json:"role"`
	View         string                   `json:"view"`
	Artifact     string                   `json:"artifact"`
	CPUMS        float64                  `json:"cpu_ms"`
	CPUPerMiB    float64                  `json:"cpu_ms_per_mib"`
	Samples      int64                    `json:"samples,omitempty"`
	Hotspots     []workflow.Hotspot       `json:"hotspots"`
	CallPaths    *workflow.CallPathReport `json:"call_paths,omitempty"`
}

func (q *FlamegraphQuery) validate() error {
	if q.Role == "" {
		q.Role = "leader"
	}
	if q.Role != "leader" && q.Role != "follower" {
		return fmt.Errorf("%w: 火焰图 role 请选择 leader 或 follower", ErrInvalid)
	}
	if q.View == "" {
		q.View = "leaf"
	}
	if q.View != "leaf" && q.View != "inclusive" {
		return fmt.Errorf("%w: view 请选择 leaf 或 inclusive", ErrInvalid)
	}
	if q.Limit < 0 || q.CallerDepth < 0 {
		return fmt.Errorf("%w: limit 和 caller_depth 需要为非负整数", ErrInvalid)
	}
	if q.CallerDepth > 0 && q.Symbol == "" {
		return fmt.Errorf("%w: caller_depth 需要指定 symbol", ErrInvalid)
	}
	return nil
}

func (s Store) Flamegraph(id string, q FlamegraphQuery) (FlamegraphReport, error) {
	if err := q.validate(); err != nil {
		return FlamegraphReport{}, err
	}
	record, err := s.Get(id)
	if err != nil {
		return FlamegraphReport{}, err
	}
	run, err := selectDiagnosticRun(record, q.Run)
	if err != nil {
		return FlamegraphReport{}, err
	}
	return summarizeFlamegraph(run, q, func(key string) (string, error) { return s.Artifact(record, key) })
}

func (p *Postgres) Flamegraph(id string, q FlamegraphQuery) (FlamegraphReport, error) {
	if err := q.validate(); err != nil {
		return FlamegraphReport{}, err
	}
	ctx, cancel := queryContext()
	defer cancel()
	var report FlamegraphReport
	err := p.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		run, err := loadDiagnosticRun(tx, id, q.Run)
		if err != nil {
			return err
		}
		report, err = summarizeFlamegraph(run, q, func(key string) (string, error) { return p.artifactFor(tx, id, key) })
		return err
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	return report, err
}

func summarizeFlamegraph(run diagnosticRun, q FlamegraphQuery, artifact func(string) (string, error)) (FlamegraphReport, error) {
	graph := run.Observation.Flamegraphs[q.Role]
	node, ok := run.Profile.Nodes[q.Role]
	if graph == "" || !ok {
		return FlamegraphReport{}, os.ErrNotExist
	}
	report := FlamegraphReport{
		ExperimentID: run.ExperimentID, Run: run.ID, Role: q.Role, View: q.View, Artifact: graph,
		CPUMS: node.CPU, CPUPerMiB: node.CPUPerMiB, Hotspots: []workflow.Hotspot{},
	}
	if q.Limit == 0 && q.Symbol == "" {
		return report, nil
	}
	path, err := artifact(graph + "-folded")
	var folded []byte
	if err == nil {
		folded, err = os.ReadFile(path)
	}
	if errors.Is(err, os.ErrNotExist) && q.View == "leaf" && q.Symbol == "" {
		report.Hotspots = append(report.Hotspots, node.Hotspots[:min(q.Limit, len(node.Hotspots))]...)
		return report, nil
	}
	if err != nil {
		return FlamegraphReport{}, err
	}
	if q.Symbol != "" {
		paths, samples, err := workflow.CPUCallPaths(string(folded), q.Symbol, q.View == "inclusive", q.CallerDepth, q.Limit)
		if err != nil {
			return FlamegraphReport{}, err
		}
		report.CallPaths, report.Samples = &paths, samples
		return report, nil
	}
	report.Hotspots, report.Samples, err = workflow.CPUHotspots(string(folded), q.View == "inclusive", q.Limit)
	return report, err
}
