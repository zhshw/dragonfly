// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package workflow

import (
	"errors"
	"fmt"
	"path/filepath"
)

// StepNames returns workflow steps in execution order for progress displays.
func StepNames(config Config) []string {
	steps := plan(config)
	names := make([]string, len(steps))
	for i, step := range steps {
		names[i] = step.Name
	}
	return names
}

// Artifacts locates the completed benchmark and validation inputs for archive import.
type Artifacts struct {
	Compare        string
	Report         string
	ValidationLogs []string
}

func CurrentArtifacts(output string) (Artifacts, error) {
	var files Artifacts
	state, err := ReadState(output)
	if err != nil {
		return files, err
	}
	if state.Status != "done" {
		return files, errors.New("workflow is incomplete")
	}
	for _, name := range []string{"compare", "observe-after", "report-after"} {
		step := state.Steps[name]
		if step == nil || step.Status != "done" || step.Dir == "" {
			return files, fmt.Errorf("workflow step %s is incomplete", name)
		}
	}
	files.Compare = state.Steps["compare"].Dir
	files.Report = state.Steps["report-after"].Dir
	for i := range state.Config.Validation {
		name := fmt.Sprintf("validate-%02d", i+1)
		step := state.Steps[name]
		if step == nil || step.Status != "done" {
			return files, fmt.Errorf("workflow step %s is incomplete", name)
		}
		logs, err := filepath.Glob(filepath.Join(step.Dir, "*.log"))
		if err != nil || len(logs) == 0 {
			return files, fmt.Errorf("no test logs in %s", step.Dir)
		}
		files.ValidationLogs = append(files.ValidationLogs, logs...)
	}
	return files, nil
}
