// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package models

import (
	"encoding/json"
	"time"

	"dragonflydb.io/profile/pkg/otel"
)

type experimentRow struct {
	ID             string `gorm:"primaryKey"`
	State          string
	Summary        Summary `gorm:"serializer:json;type:jsonb"`
	AssetDir       string
	ManifestSHA256 string `gorm:"column:manifest_sha256"`
}

func (experimentRow) TableName() string { return "raft_workflow.experiments" }

type targetRow struct {
	ID        string `gorm:"primaryKey"`
	Benchmark Target `gorm:"serializer:json;type:jsonb"`
}

func (targetRow) TableName() string { return "raft_workflow.comparison_targets" }

type comparisonRow struct {
	ExperimentID string `gorm:"primaryKey"`
	Role         string `gorm:"primaryKey"`
	TargetID     string
	Comparison   TargetComparison `gorm:"serializer:json;type:jsonb"`
}

func (comparisonRow) TableName() string { return "raft_workflow.experiment_comparisons" }

type benchmarkSetRow struct {
	ExperimentID  string       `gorm:"primaryKey"`
	ID            string       `gorm:"primaryKey"`
	Configuration BenchmarkSet `gorm:"serializer:json;type:jsonb"`
}

func (benchmarkSetRow) TableName() string { return "raft_workflow.benchmark_sets" }

type benchmarkVariantRow struct {
	ExperimentID string  `gorm:"primaryKey"`
	SetID        string  `gorm:"primaryKey"`
	Variant      string  `gorm:"primaryKey"`
	Benchmark    Variant `gorm:"serializer:json;type:jsonb"`
}

func (benchmarkVariantRow) TableName() string { return "raft_workflow.benchmark_variants" }

type runRow struct {
	ExperimentID string `gorm:"primaryKey"`
	ID           string `gorm:"primaryKey"`
	Position     int
	Kind         string
	BenchmarkSet *string
	Variant      string
	Trial        *int
	Rate         float64         `gorm:"column:replication_mib_per_s"`
	P95          float64         `gorm:"column:batch_p95_ms"`
	Run          Run             `gorm:"serializer:json;type:jsonb"`
	Measurement  json.RawMessage `gorm:"serializer:json;type:jsonb"`
}

func (runRow) TableName() string { return "raft_workflow.runs" }

type diagnosticRow struct {
	ExperimentID string      `gorm:"primaryKey"`
	Variant      string      `gorm:"primaryKey"`
	Observation  Observation `gorm:"serializer:json;type:jsonb"`
	Profile      Diagnostic  `gorm:"serializer:json;type:jsonb"`
}

func (diagnosticRow) TableName() string { return "raft_workflow.diagnostics" }

type traceStageRow struct {
	ExperimentID string `gorm:"primaryKey"`
	Variant      string `gorm:"primaryKey"`
	Node         string `gorm:"primaryKey"`
	Stage        string `gorm:"primaryKey"`
	Count        int
	UnionMS      float64    `gorm:"column:union_ms"`
	Statistics   otel.Stage `gorm:"serializer:json;type:jsonb"`
}

func (traceStageRow) TableName() string { return "raft_workflow.trace_stages" }

type validationCheckRow struct {
	ExperimentID string `gorm:"primaryKey"`
	Build        string `gorm:"primaryKey"`
	Suite        string `gorm:"primaryKey"`
	Tests        int
	CheckResult  ValidationCheck `gorm:"serializer:json;type:jsonb"`
}

func (validationCheckRow) TableName() string { return "raft_workflow.validation_checks" }

type artifactRow struct {
	ExperimentID string `gorm:"primaryKey"`
	ID           string `gorm:"primaryKey"`
	Filename     string
	Bytes        int64
}

func (artifactRow) TableName() string { return "raft_workflow.artifacts" }

type executionRow struct {
	ExperimentID string `gorm:"primaryKey"`
	Attempt      int    `gorm:"primaryKey;autoIncrement:false"`
	Status       string
	StartedAt    time.Time
	FinishedAt   *time.Time
	Error        string
}

func (executionRow) TableName() string { return "raft_workflow.executions" }
