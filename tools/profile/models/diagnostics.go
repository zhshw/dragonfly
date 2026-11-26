// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package models

import (
	"errors"
	"fmt"
	"os"
)

type DiagnosticCapture struct {
	ExperimentID string      `json:"experiment_id"`
	Profile      Diagnostic  `json:"profile"`
	Observation  Observation `json:"observation"`
}

type DiagnosticComparison struct {
	Current    *DiagnosticCapture `json:"current,omitempty"`
	Previous   *DiagnosticCapture `json:"previous,omitempty"`
	PreviousID string             `json:"previous_id,omitempty"`
}

// CompareDiagnostics resolves the same previous experiment selected for benchmark comparison.
func CompareDiagnostics(record Record, load func(string) (Record, error)) (DiagnosticComparison, error) {
	comparison := DiagnosticComparison{Current: diagnosticCapture(record, "current")}
	if comparison.Current == nil {
		comparison.Current = diagnosticCapture(record, "candidate")
	}
	previous := record.Summary.Comparisons.Previous
	if previous == nil {
		return comparison, nil
	}

	target := previous.Target
	comparison.PreviousID = target.ExperimentID
	if target.ExperimentID == "" || target.ExperimentID == record.Summary.Experiment.ID {
		return comparison, nil
	}
	prior, variant, err := targetArchive(record, target, load)
	if err != nil {
		return comparison, err
	}
	if variant != "" {
		comparison.Previous = diagnosticCapture(prior, variant)
	}
	return comparison, nil
}

type ReferenceDiagnostics struct {
	Target  Target             `json:"target"`
	Capture *DiagnosticCapture `json:"capture,omitempty"`
}

func DiagnosticsForTarget(store Repository, id string) (ReferenceDiagnostics, error) {
	targets, err := store.Targets()
	if err != nil {
		return ReferenceDiagnostics{}, err
	}
	for _, target := range targets {
		if target.ID != resolveTargetID(id, targets) {
			continue
		}
		result := ReferenceDiagnostics{Target: target}
		record, variant, err := targetArchive(Record{}, target, store.Get)
		if err == nil && variant != "" {
			result.Capture = diagnosticCapture(record, variant)
		}
		return result, err
	}
	return ReferenceDiagnostics{}, os.ErrNotExist
}

func targetArchive(record Record, target Target, load func(string) (Record, error)) (Record, string, error) {
	if target.ExperimentID == "" || target.SHA == "" {
		return Record{}, "", nil
	}
	if target.ExperimentID != record.Summary.Experiment.ID {
		var err error
		record, err = load(target.ExperimentID)
		if errors.Is(err, os.ErrNotExist) {
			return Record{}, "", nil
		}
		if err != nil {
			return Record{}, "", fmt.Errorf("load archive for %s: %w", target.ExperimentID, err)
		}
	}
	variant := target.Variant
	if variant == "current" && record.Summary.Variants[variant].SHA == "" {
		variant = "candidate"
	} else if variant == "candidate" && record.Summary.Variants[variant].SHA == "" {
		variant = "current"
	}
	if record.Summary.Variants[variant].SHA != target.SHA {
		return Record{}, "", nil
	}
	return record, variant, nil
}

func diagnosticCapture(record Record, variant string) *DiagnosticCapture {
	profile, ok := record.Summary.Diagnostics[variant]
	if !ok {
		return nil
	}
	for _, observation := range record.Observations {
		if observation.Variant == variant {
			return &DiagnosticCapture{ExperimentID: record.Summary.Experiment.ID, Profile: profile, Observation: observation}
		}
	}
	return nil
}
