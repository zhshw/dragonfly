// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package models

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"dragonflydb.io/profile/pkg/workflow"
)

var (
	ErrInvalid = errors.New("invalid experiment input")
	ErrExists  = errors.New("experiment or comparison target already exists")
)

type Target struct {
	ID           string           `json:"id"`
	Title        string           `json:"title"`
	ExperimentID string           `json:"experiment_id,omitempty"`
	Archive      string           `json:"benchmark_archive,omitempty"`
	Variant      string           `json:"variant"`
	Mode         string           `json:"mode"`
	Ack          string           `json:"ack_mode,omitempty"`
	SHA          string           `json:"sha256"`
	Harness      string           `json:"harness_sha256"`
	Workload     Workload         `json:"workload"`
	Metrics      workflow.Metrics `json:"metrics"`
	CumulativeID string           `json:"cumulative_baseline_id,omitempty"`
	ReferenceID  string           `json:"reference_id,omitempty"`
}

type ChoiceIDs struct {
	Cumulative string `json:"cumulative_baseline"`
	Reference  string `json:"reference"`
	Previous   string `json:"previous,omitempty"`
}

type Comparisons struct {
	Cumulative *TargetComparison `json:"cumulative_baseline,omitempty"`
	Reference  *TargetComparison `json:"reference,omitempty"`
	Previous   *TargetComparison `json:"previous,omitempty"`
}

func (c Comparisons) byRole() map[string]*TargetComparison {
	return map[string]*TargetComparison{"cumulative_baseline": c.Cumulative, "reference": c.Reference, "previous": c.Previous}
}

func (c Comparisons) allows(ids ChoiceIDs) bool {
	matches := func(id string, choice *TargetComparison) bool {
		return id == "" || (choice != nil && id == choice.Target.ID)
	}
	previous := matches(ids.Previous, c.Previous) || (c.Previous != nil && ids.Previous == c.Previous.Target.ExperimentID)
	return matches(ids.Cumulative, c.Cumulative) && matches(ids.Reference, c.Reference) && previous
}

type TargetComparison struct {
	Target Target          `json:"target"`
	Result *RelativeChange `json:"result,omitempty"`
}

type RelativeChange struct {
	Throughput float64 `json:"throughput_pct"`
	P95        float64 `json:"p95_pct"`
	Reached    float64 `json:"target_throughput_pct"`
}

type CreateRequest struct {
	ID           string    `json:"id"`
	Title        string    `json:"title"`
	Optimization string    `json:"optimization,omitempty"`
	Description  string    `json:"description,omitempty"`
	Targets      ChoiceIDs `json:"targets"`
}

func workloadFromRow(row workflow.BenchRow) Workload {
	return Workload{
		AckMode: row.Ack, Clients: row.Clients, Pipeline: row.Depth, Payload: row.Payload,
		Value: row.Payload * 1024 / row.Keys, Warmup: row.Warmup, Settle: row.Settle, Rounds: 3,
	}
}

func (w Workload) matches(other Workload) bool {
	// The legacy Cluster reference has no Raft acknowledgement mode.
	w.AckMode, other.AckMode = "", ""
	return w == other
}

// CheckWorkflow verifies the saved comparison workload before starting a build.
func (s Summary) CheckWorkflow(repo string) error {
	if s.Workload != workloadFromRow(workflow.BenchmarkWorkload()) {
		return fmt.Errorf("%w: 所选对照与执行机的 Raft memory benchmark 负载不一致", ErrInvalid)
	}
	harness, err := workflow.HarnessSHA(repo)
	if err != nil {
		return err
	}
	if s.Harness != harness {
		return fmt.Errorf("%w: 所选对照与执行机的 benchmark 脚本版本不一致", ErrInvalid)
	}
	return nil
}

func (s Store) Targets() ([]Target, error) {
	targets := []Target{}
	catalog := filepath.Join(s.Root, ".targets")
	entries, err := os.ReadDir(catalog)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}
	for _, entry := range entries {
		if !strings.HasSuffix(entry.Name(), ".json") || entry.IsDir() {
			continue
		}
		var target Target
		if err := decodeFile(filepath.Join(catalog, entry.Name()), &target); err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	records, err := s.records()
	if err != nil {
		return nil, err
	}
	for _, record := range records {
		targets = append(targets, targetsFrom(record.Summary)...)
	}
	sort.Slice(targets, func(i, j int) bool { return targets[i].ID < targets[j].ID })
	return targets, nil
}

func targetsFrom(summary Summary) []Target {
	if summary.Experiment.State != "complete" {
		return nil
	}
	var targets []Target
	for name, variant := range summary.Variants {
		label, mode, ack := name, variant.Mode, variant.Ack
		switch name {
		case "baseline":
			continue
		case "candidate", "current":
			label = "current"
		case "cluster":
			label = "原 Cluster"
		}
		if mode == "" {
			mode, ack = "raft", summary.Workload.AckMode
			if name == "cluster" {
				mode, ack = "yes", ""
			}
		}
		workload := summary.Workload
		workload.AckMode = ack
		target := Target{
			ID: summary.Experiment.ID + ":" + name, Title: summary.Experiment.ID + " · " + label,
			ExperimentID: summary.Experiment.ID, Variant: name, Mode: mode, Ack: ack,
			SHA: variant.SHA, Harness: summary.Harness, Workload: workload, Metrics: variant.Metrics,
		}
		if summary.Comparisons.Cumulative != nil {
			target.CumulativeID = summary.Comparisons.Cumulative.Target.ID
		}
		if summary.Comparisons.Reference != nil {
			target.ReferenceID = summary.Comparisons.Reference.Target.ID
		}
		targets = append(targets, target)
	}
	return targets
}

// ImportTarget records one measured variant from a formal benchmark archive.
func (s Store) ImportTarget(id, title, archive, variant string) (Target, error) {
	if !slug.MatchString(id) || strings.TrimSpace(title) == "" || archive == "" || variant == "" {
		return Target{}, fmt.Errorf("%w: target ID, title, compare directory and variant are required", ErrInvalid)
	}
	e, row, metrics, err := workflow.ReadBenchmarkVariant(archive, variant)
	if err != nil {
		return Target{}, err
	}
	build := e.Builds[variant]
	target := Target{
		ID: id, Title: strings.TrimSpace(title), Archive: filepath.Base(archive), Variant: variant,
		Mode: build.Mode, Ack: build.Ack, SHA: build.SHA, Harness: e.Harness, Workload: workloadFromRow(row), Metrics: metrics,
	}
	catalog := filepath.Join(s.Root, ".targets")
	if err := os.MkdirAll(catalog, 0755); err != nil {
		return Target{}, err
	}
	f, err := os.CreateTemp(catalog, ".target-")
	if err != nil {
		return Target{}, err
	}
	defer os.Remove(f.Name())
	if err := errors.Join(json.NewEncoder(f).Encode(target), f.Close()); err != nil {
		return Target{}, err
	}
	if err := os.Link(f.Name(), filepath.Join(catalog, id+".json")); err != nil {
		if os.IsExist(err) {
			return Target{}, ErrExists
		}
		return Target{}, err
	}
	return target, nil
}

func findPreviousTarget(id string, targets []Target) (Target, bool) {
	var candidate Target
	for _, target := range targets {
		if target.ID == id && target.ExperimentID != "" {
			return target, true
		}
		if target.ExperimentID == id {
			switch target.Variant {
			case "current":
				return target, true
			case "candidate":
				candidate = target
			}
		}
	}
	return candidate, candidate.ID != ""
}

func selectTargets(ids ChoiceIDs, list func() ([]Target, error)) (Comparisons, error) {
	var choices Comparisons
	targets, err := list()
	if err != nil {
		return choices, err
	}
	if ids.Previous != "" {
		target, ok := findPreviousTarget(ids.Previous, targets)
		if !ok {
			return choices, fmt.Errorf("%w: 上一轮实验不存在", ErrInvalid)
		}
		if (target.Variant != "current" && target.Variant != "candidate") || target.Mode != "raft" || target.Ack != "memory" {
			return choices, fmt.Errorf("%w: 上一轮需要选择已完成实验的 Raft memory 结果", ErrInvalid)
		}
		choices.Previous = &TargetComparison{Target: target}
		if ids.Cumulative == "" {
			ids.Cumulative = target.CumulativeID
		}
		if ids.Reference == "" {
			ids.Reference = target.ReferenceID
		}
	}
	for _, target := range targets {
		if target.ID == ids.Cumulative {
			choices.Cumulative = &TargetComparison{Target: target}
		}
		if target.ID == ids.Reference {
			choices.Reference = &TargetComparison{Target: target}
		}
	}
	if choices.Cumulative == nil || choices.Reference == nil {
		return choices, fmt.Errorf("%w: 选择的 benchmark 记录不存在", ErrInvalid)
	}
	base, ref := choices.Cumulative.Target, choices.Reference.Target
	if base.Mode != "raft" || base.Ack != "memory" {
		return choices, fmt.Errorf("%w: 基线需要使用 Raft memory 的正式结果", ErrInvalid)
	}
	if base.Harness != ref.Harness || !base.Workload.matches(ref.Workload) {
		return choices, fmt.Errorf("%w: 基线与对比参照的负载或测试口径不同", ErrInvalid)
	}
	if prev := choices.Previous; prev != nil && (prev.Target.Harness != base.Harness || !prev.Target.Workload.matches(base.Workload)) {
		return choices, fmt.Errorf("%w: 上一轮基线与基线的负载或测试口径不同", ErrInvalid)
	}
	return choices, nil
}

func (c *Comparisons) measure(workload Workload, harness string, current workflow.Metrics) error {
	for _, comparison := range []*TargetComparison{c.Cumulative, c.Reference, c.Previous} {
		if comparison == nil {
			continue
		}
		target := comparison.Target
		if target.Harness != harness || !workload.matches(target.Workload) {
			return fmt.Errorf("%w: 本轮与 %s 的负载或测试口径不同", ErrInvalid, target.Title)
		}
		rate := current.Rate.Median / target.Metrics.Rate.Median
		comparison.Result = &RelativeChange{100 * (rate - 1), 100 * (current.P95.Median/target.Metrics.P95.Median - 1), 100 * rate}
	}
	return nil
}

func draftRecord(in CreateRequest, list func() ([]Target, error)) (Record, error) {
	if !slug.MatchString(in.ID) || strings.TrimSpace(in.Title) == "" || len([]rune(in.Title)) > 120 {
		return Record{}, fmt.Errorf("%w: ID 应由小写字母、数字、下划线或连字符组成，标题必填", ErrInvalid)
	}
	choices, err := selectTargets(in.Targets, list)
	if err != nil {
		return Record{}, err
	}
	label := Label{ID: in.ID, Title: strings.TrimSpace(in.Title), Optimization: in.Optimization, Description: in.Description,
		Date: time.Now().Format(time.DateOnly), Decision: "pending", State: "draft"}
	summary := Summary{Experiment: label, Workload: choices.Cumulative.Target.Workload, Harness: choices.Cumulative.Target.Harness,
		Comparisons: choices, Variants: map[string]Variant{}}
	return Record{Schema: Schema, Detail: Detail{Summary: summary, Runs: []Run{}, Observations: []Observation{}}}, nil
}

func (s Store) Create(in CreateRequest) (Summary, error) {
	record, err := draftRecord(in, s.Targets)
	if err != nil {
		return Summary{}, err
	}
	if err := s.writeDraft(record); err != nil {
		return Summary{}, err
	}
	return record.Summary, nil
}

func (s Store) writeDraft(record Record) error {
	final := filepath.Join(s.Root, record.Summary.Experiment.ID)
	if err := os.MkdirAll(s.Root, 0755); err != nil {
		return err
	}
	temp, err := os.MkdirTemp(s.Root, ".create-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temp)
	if err := writeRecord(temp, record); err != nil {
		return err
	}
	if err := os.Rename(temp, final); err != nil {
		if _, statErr := os.Stat(final); statErr == nil {
			return ErrExists
		}
		return err
	}
	return nil
}

func writeRecord(dir string, record Record) error {
	f, err := os.Create(filepath.Join(dir, "experiment.json"))
	if err != nil {
		return err
	}
	encode := json.NewEncoder(f)
	encode.SetIndent("", "  ")
	return errors.Join(encode.Encode(record), f.Close())
}
