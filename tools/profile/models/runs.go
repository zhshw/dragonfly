// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package models

type BenchmarkRuns struct {
	ID       string   `json:"id"`
	Title    string   `json:"title"`
	Workload Workload `json:"workload"`
	Runs     []Run    `json:"runs"`
}

type RunSource struct {
	ExperimentID string                      `json:"experiment_id,omitempty"`
	SourceTitle  string                      `json:"source_title,omitempty"`
	SHA          string                      `json:"sha256,omitempty"`
	Benchmarks   []BenchmarkRuns             `json:"benchmarks,omitempty"`
	Diagnostics  []Run                       `json:"diagnostics,omitempty"`
	Validation   map[string]ValidationTotals `json:"validation,omitempty"`
	Checks       []ValidationCheck           `json:"validation_checks,omitempty"`
}

type RunComparison struct {
	Baseline *RunSource `json:"baseline,omitempty"`
	Previous *RunSource `json:"previous,omitempty"`
	Current  *RunSource `json:"current,omitempty"`
}

// CompareRuns uses the saved baseline and previous selections and keeps artifact ownership intact.
func CompareRuns(record Record, load func(string) (Record, error)) (RunComparison, error) {
	current := "current"
	if record.Summary.Variants[current].SHA == "" {
		current = "candidate"
	}
	result := RunComparison{Current: runsFor(record, current, true)}
	selectSource := func(choice *TargetComparison) (*RunSource, error) {
		if choice == nil {
			return nil, nil
		}
		target := choice.Target
		archive, variant, err := targetArchive(record, target, load)
		if err != nil {
			return nil, err
		}
		source := runsFor(archive, variant, false)
		if source == nil {
			source = &RunSource{ExperimentID: target.ExperimentID, SHA: target.SHA}
		}
		source.SourceTitle = target.Title
		return source, nil
	}
	var err error
	result.Baseline, err = selectSource(record.Summary.Comparisons.Cumulative)
	if err != nil {
		return result, err
	}
	result.Previous, err = selectSource(record.Summary.Comparisons.Previous)
	return result, err
}

func runsFor(record Record, variant string, includeAdditional bool) *RunSource {
	build, ok := record.Summary.Variants[variant]
	if !ok || build.SHA == "" {
		return nil
	}
	result := &RunSource{ExperimentID: record.Summary.Experiment.ID, SHA: build.SHA}
	sets := record.Benchmarks
	if len(sets) == 0 {
		sets = []BenchmarkSet{{ID: "main", Title: "主对照", Workload: record.Summary.Workload}}
	}
	for _, set := range sets {
		if set.ID != "main" && (!includeAdditional || set.Variants[variant].SHA != build.SHA) {
			continue
		}
		group := BenchmarkRuns{ID: set.ID, Title: set.Title, Workload: set.Workload}
		for _, run := range record.Runs {
			setID := run.Set
			if setID == "" {
				setID = "main"
			}
			if run.Variant == variant && run.Kind == "benchmark" && setID == set.ID {
				group.Runs = append(group.Runs, run)
			}
		}
		if len(group.Runs) > 0 {
			result.Benchmarks = append(result.Benchmarks, group)
		}
	}
	for _, run := range record.Runs {
		if run.Variant == variant && run.Kind == "diagnostic" {
			result.Diagnostics = append(result.Diagnostics, run)
		}
	}
	if variant == "current" || variant == "candidate" {
		result.Validation, result.Checks = record.Summary.Validation, record.Checks
	}
	return result
}
