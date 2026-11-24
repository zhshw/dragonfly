// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package models

import (
	"context"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

//go:embed schema.sql
var schemaSQL string

type Postgres struct {
	db    *gorm.DB
	pool  *sql.DB
	files Store
}

func OpenPostgres(ctx context.Context, root, dsnFile string) (*Postgres, error) {
	value, err := os.ReadFile(dsnFile)
	if err != nil {
		return nil, err
	}
	db, err := gorm.Open(postgres.Open(strings.TrimSpace(string(value))), &gorm.Config{
		DisableAutomaticPing:   true,
		SkipDefaultTransaction: true,
		Logger:                 logger.Discard,
	})
	if err != nil {
		return nil, errors.New("invalid experiment database connection file")
	}
	pool, err := db.DB()
	if err != nil {
		return nil, err
	}
	pool.SetMaxOpenConns(8)
	pool.SetMaxIdleConns(4)
	p := &Postgres{db: db, pool: pool, files: Store{Root: root}}
	if err := p.migrate(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("experiment database: %w", err)
	}
	return p, nil
}

func (p *Postgres) Close() error { return p.pool.Close() }

func (p *Postgres) migrate(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return p.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec(`SELECT pg_advisory_xact_lock(hashtext('dragonfly-raft-workflow-schema'))`).Error; err != nil {
			return err
		}
		return tx.Exec(schemaSQL).Error
	})
}

func queryContext() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), 5*time.Second)
}

func (p *Postgres) List() ([]Label, error) {
	ctx, cancel := queryContext()
	defer cancel()
	var rows []experimentRow
	if err := p.db.WithContext(ctx).Select("summary").Order("summary->'experiment'->>'date' DESC NULLS LAST, id DESC").Find(&rows).Error; err != nil {
		return nil, err
	}
	labels := make([]Label, len(rows))
	for i, row := range rows {
		labels[i] = row.Summary.Experiment
	}
	return labels, nil
}

func (p *Postgres) Summary(id string) (Summary, error) {
	var result experimentRow
	ctx, cancel := queryContext()
	defer cancel()
	err := p.db.WithContext(ctx).Table("raft_workflow.experiment_summaries").Select("summary").Where("id = ?", id).Take(&result).Error
	return result.Summary, dbError(err)
}

func (p *Postgres) Targets() ([]Target, error) {
	ctx, cancel := queryContext()
	defer cancel()
	var rows []targetRow
	err := p.db.WithContext(ctx).Select("benchmark").Where("benchmark->>'variant' <> ? OR benchmark->>'experiment_id' IS NULL", "baseline").Order("id").Find(&rows).Error
	if err != nil {
		return nil, err
	}
	targets := make([]Target, len(rows))
	for i, row := range rows {
		targets[i] = row.Benchmark
	}
	return targets, nil
}

func (p *Postgres) Create(in CreateRequest) (Summary, error) {
	record, err := draftRecord(in, p.Targets)
	if err != nil {
		return Summary{}, err
	}
	ctx, cancel := queryContext()
	defer cancel()
	if err := p.write(ctx, []Record{record}, nil); err != nil {
		return Summary{}, err
	}
	return record.Summary, nil
}

const experimentDetail = `SELECT jsonb_build_object(
  'summary', e.summary,
  'runs', COALESCE((SELECT jsonb_agg(run ORDER BY position) FROM raft_workflow.runs WHERE experiment_id=e.id), '[]'),
  'observations', COALESCE((SELECT jsonb_agg(d.observation || jsonb_build_object('stages',
      COALESCE((SELECT jsonb_agg(statistics ORDER BY s.node,s.stage) FROM raft_workflow.trace_stages s
                WHERE s.experiment_id=e.id AND s.variant=d.variant), '[]')) ORDER BY d.variant)
      FROM raft_workflow.diagnostics d WHERE d.experiment_id=e.id), '[]'),
  'benchmark_sets', COALESCE((SELECT jsonb_agg(b.configuration || jsonb_build_object('variants',
      (SELECT jsonb_object_agg(v.variant,v.benchmark) FROM raft_workflow.benchmark_variants v
        WHERE v.experiment_id=e.id AND v.set_id=b.id)) ORDER BY (b.id <> 'main'), b.id)
      FROM raft_workflow.benchmark_sets b WHERE b.experiment_id=e.id), '[]'),
  'validation_checks', COALESCE((SELECT jsonb_agg(check_result ORDER BY build,suite)
      FROM raft_workflow.validation_checks WHERE experiment_id=e.id), '[]')
) FROM raft_workflow.experiment_summaries e WHERE id=?`

func (p *Postgres) Get(id string) (Record, error) {
	result := Record{Schema: Schema}
	ctx, cancel := queryContext()
	defer cancel()
	err := scanJSON(p.db.WithContext(ctx).Raw(experimentDetail, id).Row(), &result.Detail)
	return result, err
}

func (p *Postgres) Measurement(id, run string) (json.RawMessage, error) {
	var result runRow
	ctx, cancel := queryContext()
	defer cancel()
	err := p.db.WithContext(ctx).Select("measurement").Where("experiment_id = ? AND id = ? AND measurement IS NOT NULL", id, run).Take(&result).Error
	return result.Measurement, dbError(err)
}

func (p *Postgres) writeChildren(tx *gorm.DB, r Record) error {
	id := r.Summary.Experiment.ID
	for role, choice := range r.Summary.Comparisons.byRole() {
		if choice != nil {
			row := comparisonRow{ExperimentID: id, Role: role, TargetID: choice.Target.ID, Comparison: *choice}
			if err := tx.Create(&row).Error; err != nil {
				return err
			}
		}
	}
	sets := r.Benchmarks
	if len(sets) == 0 && len(r.Summary.Variants) > 0 {
		// Allow manifests created before additional benchmark sets were supported.
		sets = []BenchmarkSet{{ID: "main", Title: "主对照", Workload: r.Summary.Workload, Harness: r.Summary.Harness, Variants: r.Summary.Variants}}
	}
	for _, set := range sets {
		variants := set.Variants
		set.Variants = nil
		row := benchmarkSetRow{ExperimentID: id, ID: set.ID, Configuration: set}
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
		for name, variant := range variants {
			row := benchmarkVariantRow{ExperimentID: id, SetID: set.ID, Variant: name, Benchmark: variant}
			if err := tx.Create(&row).Error; err != nil {
				return err
			}
		}
	}
	for i, run := range r.Runs {
		if run.Kind == "benchmark" && run.Set == "" {
			run.Set = "main"
		}
		row := runRow{ExperimentID: id, ID: run.ID, Position: i, Kind: run.Kind, Variant: run.Variant,
			Rate: run.Rate, P95: run.P95, Run: run, Measurement: r.Measurements[run.ID]}
		if run.Set != "" {
			row.BenchmarkSet = &run.Set
		}
		if run.Trial != 0 {
			row.Trial = &run.Trial
		}
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
	}
	for _, observation := range r.Observations {
		stages := observation.Stages
		observation.Stages = nil
		row := diagnosticRow{ExperimentID: id, Variant: observation.Variant, Observation: observation,
			Profile: r.Summary.Diagnostics[observation.Variant]}
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
		for _, stage := range stages {
			row := traceStageRow{ExperimentID: id, Variant: observation.Variant, Node: stage.Node,
				Stage: stage.Name, Count: stage.Count, UnionMS: stage.Union, Statistics: stage}
			if err := tx.Create(&row).Error; err != nil {
				return err
			}
		}
	}
	for _, check := range r.Checks {
		row := validationCheckRow{ExperimentID: id, Build: check.Build, Suite: check.Suite, Tests: check.Tests, CheckResult: check}
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
	}
	for key, name := range r.Artifacts {
		path, err := p.files.Artifact(r, key)
		if err != nil {
			return err
		}
		info, err := os.Stat(path)
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() || info.Size() == 0 {
			return fmt.Errorf("missing artifact contents: %s", key)
		}
		row := artifactRow{ExperimentID: id, ID: key, Filename: name, Bytes: info.Size()}
		if err := tx.Create(&row).Error; err != nil {
			return err
		}
	}
	return nil
}

func (p *Postgres) ArtifactFor(id, artifact string) (string, error) {
	ctx, cancel := queryContext()
	defer cancel()
	return p.artifactFor(p.db.WithContext(ctx), id, artifact)
}

func (p *Postgres) artifactFor(db *gorm.DB, id, artifact string) (string, error) {
	var found struct{ AssetDir, Filename string }
	err := db.Table("raft_workflow.artifacts AS a").Select("e.asset_dir, a.filename").
		Joins("JOIN raft_workflow.experiments AS e ON e.id = a.experiment_id").Where("a.experiment_id = ? AND a.id = ?", id, artifact).Take(&found).Error
	if err != nil {
		return "", dbError(err)
	}
	dir, name := found.AssetDir, found.Filename
	if !slug.MatchString(id) || filepath.Base(name) != name || !validAssetDir(id, dir) {
		return "", os.ErrNotExist
	}
	return filepath.Join(p.files.Root, dir, "artifacts", name), nil
}

func validAssetDir(id, dir string) bool {
	return dir == id || dir == filepath.Join(id, "result") ||
		(filepath.Dir(dir) == id && strings.HasPrefix(filepath.Base(dir), "assets-"))
}

func scanJSON(row *sql.Row, value any) error {
	var data []byte
	if err := row.Scan(&data); err != nil {
		return dbError(err)
	}
	return json.Unmarshal(data, value)
}

func dbError(err error) error {
	if errors.Is(err, sql.ErrNoRows) || errors.Is(err, gorm.ErrRecordNotFound) {
		return os.ErrNotExist
	}
	var e *pgconn.PgError
	if errors.As(err, &e) && e.Code == "23505" {
		return ErrExists
	}
	return err
}

// Sync inserts archived experiments and all associated rows in one transaction.
func (p *Postgres) Sync(ctx context.Context) (int, error) {
	records, err := p.files.records()
	if err != nil {
		return 0, err
	}
	targets, err := p.files.Targets()
	if err != nil {
		return 0, err
	}
	return len(records), p.write(ctx, records, targets)
}

func (p *Postgres) Import(ctx context.Context, config ImportConfig) (Summary, error) {
	var empty Summary
	label := config.Experiment
	if !slug.MatchString(label.ID) || config.Compare == "" {
		return empty, fmt.Errorf("%w: experiment ID and compare archive are required", ErrInvalid)
	}
	draft, err := p.Summary(label.ID)
	if err == nil {
		if draft.Experiment.State != "draft" {
			return empty, ErrExists
		}
		if !draft.Comparisons.allows(config.Targets) {
			return empty, errors.New("results must use the draft's saved comparison choices")
		}
		decision := label.Decision
		label = draft.Experiment
		if decision != "" {
			label.Decision = decision
		}
	} else if !os.IsNotExist(err) {
		return empty, err
	}
	dir := filepath.Join(p.files.Root, label.ID)
	if err := os.MkdirAll(dir, 0750); err != nil {
		return empty, err
	}
	assets, err := os.MkdirTemp(dir, "assets-")
	if err != nil {
		return empty, err
	}
	record, err := buildRecord(config, label, assets, func(measured Summary) (Comparisons, error) {
		if draft.Experiment.State == "draft" {
			return draft.Comparisons, nil
		}
		if config.Targets == (ChoiceIDs{}) {
			return Comparisons{}, nil
		}
		return selectTargets(config.Targets, func() ([]Target, error) {
			catalog, err := p.Targets()
			return append(catalog, targetsFrom(measured)...), err
		})
	})
	if err != nil {
		os.RemoveAll(assets)
		return empty, err
	}
	record.dir = assets
	record.fromDraft = draft.Experiment.State == "draft"
	if err := p.write(ctx, []Record{record}, targetsFrom(record.Summary)); err != nil {
		return empty, err
	}
	return record.Summary, nil
}

func (p *Postgres) write(ctx context.Context, records []Record, targets []Target) error {
	return dbError(p.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := lockCatalog(tx); err != nil {
			return err
		}
		for _, target := range targets {
			row := targetRow{ID: target.ID, Benchmark: target}
			if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&row).Error; err != nil {
				return err
			}
		}
		for _, record := range records {
			if err := p.writeRecord(tx, record); err != nil {
				return fmt.Errorf("%s: %w", record.Summary.Experiment.ID, err)
			}
		}
		return nil
	}))
}

func lockCatalog(tx *gorm.DB) error {
	return tx.Exec(`SELECT pg_advisory_xact_lock(hashtext('dragonfly-raft-workflow-catalog'))`).Error
}

func lockedExperiment(tx *gorm.DB, id string) (experimentRow, error) {
	var row experimentRow
	err := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Select("state", "asset_dir", "manifest_sha256").Where("id = ?", id).Take(&row).Error
	return row, dbError(err)
}

func (p *Postgres) writeRecord(tx *gorm.DB, r Record) error {
	encoded, err := json.Marshal(r)
	if err != nil {
		return err
	}
	digest := manifestHash(encoded)
	id, state := r.Summary.Experiment.ID, r.Summary.Experiment.State
	var dir string
	if r.dir != "" {
		dir, err = filepath.Rel(p.files.Root, r.dir)
		if err != nil || !validAssetDir(id, dir) {
			return errors.New("invalid experiment asset directory")
		}
	}
	previous, err := lockedExperiment(tx, id)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	if r.fromDraft && errors.Is(err, os.ErrNotExist) {
		return os.ErrNotExist
	}
	if err == nil {
		if previous.State == "complete" && previous.ManifestSHA256 == digest {
			return nil
		}
		if previous.State != "draft" || state != "complete" {
			if state == "draft" && previous.State == "draft" && previous.ManifestSHA256 == digest {
				return nil
			}
			return ErrExists
		}
		for role, choice := range r.Summary.Comparisons.byRole() {
			if choice == nil {
				continue
			}
			var saved comparisonRow
			if err := tx.Select("comparison").Where("experiment_id = ? AND role = ?", id, role).Take(&saved).Error; err != nil {
				return dbError(err)
			}
			if choice.Target != saved.Comparison.Target {
				return errors.New("archive has different comparison choices from the saved draft")
			}
		}
		if err := tx.Where("experiment_id = ?", id).Delete(&comparisonRow{}).Error; err != nil {
			return err
		}
	}
	if state == "draft" {
		for _, choice := range r.Summary.Comparisons.byRole() {
			if choice == nil {
				continue
			}
			var target targetRow
			err := tx.Select("benchmark").Where("id = ?", choice.Target.ID).Take(&target).Error
			if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
				return err
			}
			if target.Benchmark != choice.Target {
				return fmt.Errorf("%w: 对照 %s 已更新，请重新选择", ErrInvalid, choice.Target.ID)
			}
		}
	}
	row := storedExperiment(r.Summary, dir, digest)
	if previous.State == "draft" {
		if err := updateExperiment(tx, row); err != nil {
			return err
		}
	} else if err := tx.Create(&row).Error; err != nil {
		return err
	}
	return p.writeChildren(tx, r)
}

func storedExperiment(summary Summary, directory, digest string) experimentRow {
	summary.Variants, summary.Diagnostics, summary.Comparisons = nil, nil, Comparisons{}
	return experimentRow{ID: summary.Experiment.ID, State: summary.Experiment.State,
		Summary: summary, AssetDir: directory, ManifestSHA256: digest}
}

func updateExperiment(tx *gorm.DB, row experimentRow) error {
	return tx.Model(&experimentRow{}).Where("id = ?", row.ID).
		Select("state", "summary", "asset_dir", "manifest_sha256").Updates(row).Error
}

func manifestHash(encoded []byte) string {
	return fmt.Sprintf("%x", sha256.Sum256(encoded))
}
