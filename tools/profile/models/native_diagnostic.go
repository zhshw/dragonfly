// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package models

import (
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"

	"dragonflydb.io/profile/pkg/workflow"
)

func (r *Record) importNativeDiagnostic(dest, variant, dir string, report workflow.NativeDiagnostic, env workflow.Environment) error {
	d, _, row, err := workflow.ReadDiagnostic(dir, env.Builds[variant], env.Harness)
	if err != nil {
		return err
	}
	if report.Capture != nil && report.Capture.Experiment != r.Summary.Experiment.ID {
		return errors.New("native capture belongs to another experiment")
	}
	log := filepath.Join(report.Run, "test.log")
	if report.Metadata.Runner == "go" {
		var completed workflow.BenchRow
		if err := decodeFile(filepath.Join(report.Run, "result.json"), &completed); err != nil {
			return err
		}
		if !reflect.DeepEqual(completed, row) {
			return errors.New("diagnostic result differs from the completed Go benchmark")
		}
	} else {
		if report.Metadata.Runner != "" {
			return errors.New("unknown diagnostic runner")
		}
		if err := checkDiagnosticLog(log); err != nil {
			return err
		}
	}
	id := "diagnostic-" + variant
	if err := r.copyArtifact(dest, id+"-log", log, ".log"); err != nil {
		return err
	}
	if err := r.copyArtifact(dest, id+"-report", filepath.Join(dir, "native.json"), ".json"); err != nil {
		return err
	}
	graphs := map[string]string{}
	for role, node := range d.Nodes {
		key := id + "-" + role
		prefix := fmt.Sprintf("%s-node-%s", role, node.Node)
		if err := r.copyArtifact(dest, key, filepath.Join(dir, prefix+".svg"), ".svg"); err != nil {
			return err
		}
		if err := r.copyArtifact(dest, key+"-folded", filepath.Join(dir, prefix+".folded"), ".txt"); err != nil {
			return err
		}
		graphs[role] = key
	}
	r.Runs = append(r.Runs, newRun(id, "diagnostic", variant, id+"-log", row))
	var result json.RawMessage
	if err := decodeFile(filepath.Join(report.Run, "result.json"), &result); err != nil {
		return err
	}
	r.Measurements[id] = result
	r.Observations = append(r.Observations, Observation{Variant: variant, Flamegraphs: graphs})
	r.Summary.Diagnostics[variant] = Diagnostic{Diagnostic: d, Verified: d.SpanCount}
	r.Summary.Evidence.Flamegraphs += len(graphs)
	r.Summary.Evidence.Diagnostics++
	return nil
}
