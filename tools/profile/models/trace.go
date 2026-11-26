// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package models

import (
	"database/sql"
	"errors"
	"fmt"
	"math"
	"os"
	"sort"
	"strings"

	"dragonflydb.io/profile/pkg/otel"
	"dragonflydb.io/profile/pkg/trace"
	"gorm.io/gorm"
)

const DefaultTraceLimit = 10
const ApplyToConfirmAnalysis = "apply-to-confirm"

type TraceQuery struct {
	Run, Role, Stage, Sort, Order, Analysis string
	Limit, Offset                           int
	MinUnionMS                              float64
}

type TracePage struct {
	ExperimentID   string                   `json:"experiment_id"`
	Run            string                   `json:"run"`
	Variant        string                   `json:"variant"`
	TraceURL       string                   `json:"trace_url,omitempty"`
	SpanCount      int                      `json:"span_count"`
	Verified       int                      `json:"verified_observations"`
	Sort           string                   `json:"sort"`
	Order          string                   `json:"order"`
	Total          int64                    `json:"total"`
	Limit          int                      `json:"limit"`
	Offset         int                      `json:"offset"`
	Stages         []otel.Stage             `json:"stages"`
	ApplyToConfirm *trace.ApplyConfirmation `json:"apply_to_confirm,omitempty"`
}

type diagnosticRun struct {
	ID string
	DiagnosticCapture
}

type traceMetric struct {
	column string
	value  func(otel.Stage) float64
}

var traceMetrics = map[string]traceMetric{
	"union": {"union_ms", func(s otel.Stage) float64 { return s.Union }},
	"sum":   {"COALESCE((statistics->>'sum_ms')::double precision, 0)", func(s otel.Stage) float64 { return s.Sum }},
	"p50":   {"COALESCE((statistics->>'p50_ms')::double precision, 0)", func(s otel.Stage) float64 { return s.P50 }},
	"p95":   {"COALESCE((statistics->>'p95_ms')::double precision, 0)", func(s otel.Stage) float64 { return s.P95 }},
	"max":   {"COALESCE((statistics->>'max_ms')::double precision, 0)", func(s otel.Stage) float64 { return s.Max }},
	"count": {"count", func(s otel.Stage) float64 { return float64(s.Count) }},
}

func (q *TraceQuery) validate() error {
	if q.Sort == "" {
		q.Sort = "union"
	}
	if _, ok := traceMetrics[q.Sort]; !ok {
		return fmt.Errorf("%w: sort 请选择 union、sum、p50、p95、max 或 count", ErrInvalid)
	}
	if q.Order == "" {
		q.Order = "desc"
	}
	if q.Order != "asc" && q.Order != "desc" {
		return fmt.Errorf("%w: order 请选择 asc 或 desc", ErrInvalid)
	}
	if q.Role != "" && q.Role != "leader" && q.Role != "follower" && q.Role != "client" {
		return fmt.Errorf("%w: role 请选择 leader、follower 或 client", ErrInvalid)
	}
	if q.Analysis != "" && (q.Analysis != ApplyToConfirmAnalysis || (q.Role != "" && q.Role != "leader")) {
		return fmt.Errorf("%w: analysis 支持 leader 的 %s", ErrInvalid, ApplyToConfirmAnalysis)
	}
	if q.Limit < 0 || q.Offset < 0 || q.MinUnionMS < 0 || math.IsNaN(q.MinUnionMS) || math.IsInf(q.MinUnionMS, 0) {
		return fmt.Errorf("%w: limit、offset 和 min_union_ms 需要为有限非负值", ErrInvalid)
	}
	return nil
}

func (q TraceQuery) page(run diagnosticRun) TracePage {
	return TracePage{
		ExperimentID: run.ExperimentID, Run: run.ID, Variant: run.Observation.Variant,
		TraceURL: run.Profile.TraceURL, SpanCount: run.Profile.SpanCount, Verified: run.Profile.Verified,
		Sort: q.Sort, Order: q.Order, Limit: q.Limit, Offset: q.Offset, Stages: []otel.Stage{},
	}
}

func (p *TracePage) analyzeApplyConfirmation(run diagnosticRun, artifact func(string) (string, error)) error {
	path, err := artifact(run.Observation.Timeline)
	if err != nil {
		return err
	}
	timeline, err := os.Open(path)
	if err != nil {
		return err
	}
	defer timeline.Close()
	analysis, err := trace.AnalyzeApplyConfirmation(timeline)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrInvalid, err)
	}
	p.ApplyToConfirm = &analysis
	return nil
}

func selectDiagnosticRun(record Record, runID string) (diagnosticRun, error) {
	if runID != "" {
		for _, run := range record.Runs {
			if run.ID != runID {
				continue
			}
			if run.Kind != "diagnostic" {
				return diagnosticRun{}, diagnosticRequired(runID)
			}
			if capture := diagnosticCapture(record, run.Variant); capture != nil {
				return diagnosticRun{ID: runID, DiagnosticCapture: *capture}, nil
			}
			break
		}
		return diagnosticRun{}, os.ErrNotExist
	}
	for _, variant := range []string{"current", "candidate"} {
		if capture := diagnosticCapture(record, variant); capture != nil {
			for _, run := range record.Runs {
				if run.Kind == "diagnostic" && run.Variant == variant {
					return diagnosticRun{ID: run.ID, DiagnosticCapture: *capture}, nil
				}
			}
			return diagnosticRun{ID: "diagnostic-" + variant, DiagnosticCapture: *capture}, nil
		}
	}
	return diagnosticRun{}, os.ErrNotExist
}

func diagnosticRequired(id string) error {
	return fmt.Errorf("%w: %s 是正式 benchmark 记录，Trace 和火焰图请选择独立诊断运行（例如 diagnostic-current）", ErrInvalid, id)
}

func (s Store) Trace(id string, q TraceQuery) (TracePage, error) {
	if err := q.validate(); err != nil {
		return TracePage{}, err
	}
	record, err := s.Get(id)
	if err != nil {
		return TracePage{}, err
	}
	run, err := selectDiagnosticRun(record, q.Run)
	if err != nil {
		return TracePage{}, err
	}
	page := q.page(run)
	if q.Analysis == ApplyToConfirmAnalysis {
		if err := page.analyzeApplyConfirmation(run, func(key string) (string, error) { return s.Artifact(record, key) }); err != nil {
			return TracePage{}, err
		}
	}
	stages := make([]otel.Stage, 0, len(run.Observation.Stages))
	for _, stage := range run.Observation.Stages {
		if (q.Role == "" || stage.Role == q.Role) && strings.HasPrefix(stage.Name, q.Stage) && stage.Union >= q.MinUnionMS {
			stages = append(stages, stage)
		}
	}
	value := traceMetrics[q.Sort].value
	sort.SliceStable(stages, func(i, j int) bool {
		a, b := stages[i], stages[j]
		if left, right := value(a), value(b); left != right {
			if q.Order == "asc" {
				return left < right
			}
			return left > right
		}
		if a.Node != b.Node {
			return a.Node < b.Node
		}
		return a.Name < b.Name
	})
	page.Total = int64(len(stages))
	from := min(q.Offset, len(stages))
	page.Stages = stages[from : from+min(q.Limit, len(stages)-from)]
	return page, nil
}

func loadDiagnosticRun(tx *gorm.DB, id, runID string) (diagnosticRun, error) {
	var row diagnosticRow
	capture := tx.Where("experiment_id = ?", id)
	if runID != "" {
		var run runRow
		if err := tx.Select("kind", "variant").Where("experiment_id = ? AND id = ?", id, runID).Take(&run).Error; err != nil {
			return diagnosticRun{}, dbError(err)
		}
		if run.Kind != "diagnostic" {
			return diagnosticRun{}, diagnosticRequired(runID)
		}
		capture = capture.Where("variant = ?", run.Variant)
	} else {
		capture = capture.Where("variant IN ?", []string{"current", "candidate"}).Order("CASE WHEN variant = 'current' THEN 0 ELSE 1 END")
	}
	if err := capture.Take(&row).Error; err != nil {
		return diagnosticRun{}, dbError(err)
	}
	if runID == "" {
		var run runRow
		err := tx.Select("id").Where("experiment_id = ? AND kind = ? AND variant = ?", id, "diagnostic", row.Variant).Order("position").Take(&run).Error
		if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
			return diagnosticRun{}, err
		}
		runID = run.ID
		if runID == "" {
			runID = "diagnostic-" + row.Variant
		}
	}
	return diagnosticRun{ID: runID, DiagnosticCapture: DiagnosticCapture{
		ExperimentID: id, Profile: row.Profile, Observation: row.Observation,
	}}, nil
}

func (p *Postgres) Trace(id string, q TraceQuery) (TracePage, error) {
	if err := q.validate(); err != nil {
		return TracePage{}, err
	}
	ctx, cancel := queryContext()
	defer cancel()
	var page TracePage
	err := p.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		run, err := loadDiagnosticRun(tx, id, q.Run)
		if err != nil {
			return err
		}
		page = q.page(run)
		if q.Analysis == ApplyToConfirmAnalysis {
			if err := page.analyzeApplyConfirmation(run, func(key string) (string, error) { return p.artifactFor(tx, id, key) }); err != nil {
				return err
			}
		}
		rows := tx.Model(&traceStageRow{}).Where("experiment_id = ? AND variant = ?", id, page.Variant)
		if q.Role != "" {
			rows = rows.Where("statistics->>'role' = ?", q.Role)
		}
		if q.Stage != "" {
			rows = rows.Where("strpos(stage, ?) = 1", q.Stage)
		}
		if q.MinUnionMS > 0 {
			rows = rows.Where("union_ms >= ?", q.MinUnionMS)
		}
		if err := rows.Session(&gorm.Session{}).Count(&page.Total).Error; err != nil || q.Limit == 0 {
			return err
		}
		var selected []traceStageRow
		order := traceMetrics[q.Sort].column + " " + q.Order + ", node, stage"
		if err := rows.Select("statistics").Order(order).Limit(q.Limit).Offset(q.Offset).Find(&selected).Error; err != nil {
			return err
		}
		for _, row := range selected {
			page.Stages = append(page.Stages, row.Statistics)
		}
		return nil
	}, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	return page, err
}
