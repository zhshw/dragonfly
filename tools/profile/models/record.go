// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package models

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"

	"dragonflydb.io/profile/pkg/langfuse"
	"dragonflydb.io/profile/pkg/workflow"
)

const Schema = 1

const DefaultRoot = "docs/cluster/benchmarks/experiments"

type Label struct {
	ID           string `json:"id"`
	Title        string `json:"title"`
	Optimization string `json:"optimization,omitempty"`
	Date         string `json:"date,omitempty"`
	Decision     string `json:"decision"`
	Description  string `json:"description,omitempty"`
	State        string `json:"state"`
}

type Workload struct {
	AckMode  string  `json:"ack_mode"`
	Clients  int     `json:"clients"`
	Pipeline int     `json:"pipeline_depth"`
	Payload  int     `json:"payload_mib"`
	Value    int     `json:"value_kib"`
	Warmup   int     `json:"warmup_writes"`
	Settle   float64 `json:"settle_seconds"`
	Rounds   int     `json:"rounds_per_variant"`
}

type Variant struct {
	SHA     string           `json:"sha256"`
	Metrics workflow.Metrics `json:"metrics"`
	Mode    string           `json:"mode,omitempty"`
	Ack     string           `json:"ack_mode,omitempty"`
}

type BenchmarkSet struct {
	ID       string             `json:"id"`
	Title    string             `json:"title"`
	Workload Workload           `json:"workload"`
	Harness  string             `json:"harness_sha256"`
	Mount    string             `json:"mount,omitempty"`
	Variants map[string]Variant `json:"variants"`
}

type Delta struct {
	Throughput float64   `json:"throughput_pct"`
	P95        float64   `json:"p95_pct"`
	Paired     []float64 `json:"paired_throughput_pct"`
	PairedP95  []float64 `json:"paired_p95_pct,omitempty"`
}

type Evidence struct {
	Confirmed   int `json:"confirmed_runs"`
	Benchmark   int `json:"benchmark_runs"`
	Diagnostics int `json:"diagnostics"`
	Flamegraphs int `json:"flamegraphs"`
}

type ValidationTotals struct {
	Unit        int `json:"unit_tests"`
	Integration int `json:"integration_tests"`
}

type ValidationCheck struct {
	Build string `json:"build"`
	Suite string `json:"suite"`
	Tests int    `json:"tests"`
	Log   string `json:"log"`
}

type Diagnostic struct {
	workflow.Diagnostic
	TraceURL string `json:"trace_url,omitempty"`
	Verified int    `json:"verified_observations,omitempty"`
}

type Summary struct {
	Experiment  Label                       `json:"experiment"`
	Workload    Workload                    `json:"workload"`
	Harness     string                      `json:"harness_sha256"`
	Variants    map[string]Variant          `json:"variants"`
	Delta       Delta                       `json:"delta"`
	Evidence    Evidence                    `json:"evidence"`
	Comparisons Comparisons                 `json:"comparisons"`
	Diagnostics map[string]Diagnostic       `json:"diagnostics,omitempty"`
	Validation  map[string]ValidationTotals `json:"validation,omitempty"`
}

type Run struct {
	ID         string  `json:"id"`
	Kind       string  `json:"kind"`
	Variant    string  `json:"variant"`
	Order      int     `json:"order,omitempty"`
	Trial      int     `json:"trial,omitempty"`
	Status     string  `json:"status"`
	Rate       float64 `json:"replication_mib_per_s"`
	SetRate    float64 `json:"set_mib_per_s"`
	Tail       float64 `json:"wait_tail_ms"`
	P95        float64 `json:"batch_p95_ms"`
	RecoveryMS float64 `json:"recovery_ms"`
	Log        string  `json:"log,omitempty"`
	Set        string  `json:"benchmark_set,omitempty"`
}

type Observation struct {
	Variant     string            `json:"variant"`
	Flamegraphs map[string]string `json:"flamegraphs"`
	Timeline    string            `json:"timeline"`
	Stages      []langfuse.Stage  `json:"stages"`
}

type Detail struct {
	Summary      Summary           `json:"summary"`
	Runs         []Run             `json:"runs"`
	Observations []Observation     `json:"observations"`
	Checks       []ValidationCheck `json:"validation_checks,omitempty"`
	Benchmarks   []BenchmarkSet    `json:"benchmark_sets,omitempty"`
}

type Record struct {
	Schema int `json:"schema"`
	Detail
	Artifacts    map[string]string          `json:"artifacts"`
	Measurements map[string]json.RawMessage `json:"measurements,omitempty"`
	dir          string
	fromDraft    bool
}

type Store struct{ Root string }

type Repository interface {
	List() ([]Label, error)
	Summary(string) (Summary, error)
	Get(string) (Record, error)
	Targets() ([]Target, error)
	Create(CreateRequest) (Summary, error)
	ArtifactFor(string, string) (string, error)
	Measurement(string, string) (json.RawMessage, error)
	Trace(string, TraceQuery) (TracePage, error)
	Flamegraph(string, FlamegraphQuery) (FlamegraphReport, error)
}

var slug = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,63}$`)

// Get loads the saved experiment without reading the raw measurements.
func (s Store) Get(id string) (Record, error) {
	var rec Record
	if !slug.MatchString(id) {
		return rec, os.ErrNotExist
	}
	dir := filepath.Join(s.Root, id, "result")
	err := decodeFile(filepath.Join(dir, "experiment.json"), &rec)
	if os.IsNotExist(err) {
		dir = filepath.Join(s.Root, id)
		err = decodeFile(filepath.Join(dir, "experiment.json"), &rec)
	}
	if err != nil {
		return rec, err
	}
	if rec.Schema != Schema || rec.Summary.Experiment.ID != id {
		return rec, fmt.Errorf("invalid experiment identity or schema: %s", id)
	}
	rec.dir = dir
	if rec.Summary.Experiment.State == "" {
		rec.Summary.Experiment.State = "complete"
	}
	return rec, nil
}

func (s Store) List() ([]Label, error) {
	labels := []Label{}
	records, err := s.records()
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		labels = append(labels, record.Summary.Experiment)
	}
	sort.Slice(labels, func(i, j int) bool {
		if labels[i].Date != labels[j].Date {
			return labels[i].Date > labels[j].Date
		}
		return labels[i].ID < labels[j].ID
	})
	return labels, nil
}

func (s Store) records() ([]Record, error) {
	var records []Record
	entries, err := os.ReadDir(s.Root)
	if os.IsNotExist(err) {
		return records, nil
	}
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if !entry.IsDir() || !slug.MatchString(entry.Name()) {
			continue
		}
		record, err := s.Get(entry.Name())
		if os.IsNotExist(err) {
			continue
		}
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

func (s Store) Artifact(rec Record, id string) (string, error) {
	name, ok := rec.Artifacts[id]
	if !ok || filepath.Base(name) != name || !slug.MatchString(id) {
		return "", os.ErrNotExist
	}
	return filepath.Join(rec.dir, "artifacts", name), nil
}

func (s Store) Summary(id string) (Summary, error) {
	record, err := s.Get(id)
	return record.Summary, err
}

func (s Store) ArtifactFor(id, artifact string) (string, error) {
	record, err := s.Get(id)
	if err != nil {
		return "", err
	}
	return s.Artifact(record, artifact)
}

func (s Store) Measurement(id, run string) (json.RawMessage, error) {
	record, err := s.Get(id)
	if err != nil {
		return nil, err
	}
	if raw := record.Measurements[run]; len(raw) > 0 {
		return raw, nil
	}
	return nil, os.ErrNotExist
}
