// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package workflow

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"dragonflydb.io/profile/pkg/langfuse"
)

const observe = "tools/raft-observability/"

func plan(c Config) []Step {
	var steps []Step
	if c.Baseline != "" {
		steps = append(steps, Step{"baseline", func(r *Runner, rec *Record) ([]string, error) {
			return copyFiles(c.Baseline, rec.Dir, "dragonfly", "raft_storage")
		}})
	}
	if c.Diagnostics && c.Baseline != "" {
		steps = append(steps, Step{"baseline-probe", func(r *Runner, rec *Record) ([]string, error) {
			if err := verifyProbe(c.BaselineProbe, r.dir("baseline")); err != nil {
				return nil, err
			}
			files, err := copyFiles(c.BaselineProbe, rec.Dir, "dragonfly", "sources.json")
			if err != nil {
				return nil, err
			}
			storage := filepath.Join(rec.Dir, "raft_storage")
			return append(files, storage), copyFile(filepath.Join(r.dir("baseline"), "raft_storage"), storage)
		}})
	}
	for i, v := range c.Validation {
		steps = append(steps, Step{fmt.Sprintf("validate-%02d", i+1), func(r *Runner, rec *Record) ([]string, error) {
			args := []string{"ninja", "-C", v.Build, fmt.Sprintf("-j%d", c.Jobs), "dragonfly", "raft_storage"}
			if err := r.command(nil, append(args, v.Unit...)...); err != nil {
				return nil, err
			}
			env := []string{"DRAGONFLY_PATH=" + filepath.Join(v.Build, "dragonfly")}
			if v.Sanitize {
				env = append(env, "ASAN_OPTIONS=detect_leaks=1:abort_on_error=1", "UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1")
			}
			if err := os.Mkdir(rec.Dir, 0755); err != nil {
				return nil, err
			}
			build := fmt.Sprintf("build%02d", i+1)
			for _, test := range v.Unit {
				log := filepath.Join(rec.Dir, build+"-"+test+".log")
				if err := r.validationLog(log, env, filepath.Join(v.Build, test)); err != nil {
					return nil, err
				}
			}
			if len(v.Integration) > 0 {
				log := filepath.Join(rec.Dir, build+"-integration.log")
				if err := r.validationLog(log, env, append([]string{"python3", "-m", "unittest", "-v"}, v.Integration...)...); err != nil {
					return nil, err
				}
			}
			if err := atomicJSON(filepath.Join(rec.Dir, "passed.json"), v); err != nil {
				return nil, err
			}
			files, err := artifactFiles(rec.Dir)
			return append(files, rec.Log), err
		}})
	}
	steps = append(steps,
		Step{"candidate", func(r *Runner, rec *Record) ([]string, error) {
			if err := r.command(nil, "ninja", "-C", c.Release, fmt.Sprintf("-j%d", c.Jobs), "dragonfly", "raft_storage"); err != nil {
				return nil, err
			}
			return copyFiles(c.Release, rec.Dir, "dragonfly", "raft_storage")
		}},
		Step{"compare", func(r *Runner, rec *Record) ([]string, error) {
			workload := BenchmarkWorkload()
			args := []string{"--build-dir", r.dir("candidate"),
				"--ack-mode", workload.Ack, "--clients", strconv.Itoa(workload.Clients),
				"--pipeline-depth", strconv.Itoa(workload.Depth), "--output-dir", rec.Dir}
			if c.Baseline != "" {
				args = append(args, "--baseline-build", r.dir("baseline"))
			}
			if c.IncludeCluster {
				args = append(args, "--include-cluster")
			}
			if err := r.python("tests/integration/raft_commit_benchmark/dragonfly/pipeline_compare.py", args...); err != nil {
				return nil, err
			}
			if _, err := ReadComparison(rec.Dir); err != nil {
				return nil, err
			}
			return artifactFiles(rec.Dir)
		}})
	if c.Diagnostics {
		steps = append(steps, Step{"candidate-probe", func(r *Runner, rec *Record) ([]string, error) {
			if err := r.python(observe+"prepare.py", "--build-dir", c.Release, "--skip-build", "--output", rec.Dir); err != nil {
				return nil, err
			}
			if err := verifyProbe(rec.Dir, r.dir("candidate")); err != nil {
				return nil, err
			}
			storage := filepath.Join(rec.Dir, "raft_storage")
			if err := os.Remove(storage); err != nil {
				return nil, err
			}
			if err := copyFile(filepath.Join(r.dir("candidate"), "raft_storage"), storage); err != nil {
				return nil, err
			}
			return artifactFiles(rec.Dir)
		}})
		for _, variant := range observedVariants(c) {
			steps = append(steps, Step{"observe-" + variant, func(r *Runner, rec *Record) ([]string, error) {
				probe, mode := "candidate-probe", "memory"
				if variant == "before" {
					probe = "baseline-probe"
				} else if variant == "cluster" {
					mode = "cluster"
				}
				run := observationDir(c, rec, variant)
				if err := r.python(observe+"collect.py", "--binary", filepath.Join(r.dir(probe), "dragonfly"),
					"--output", run, "--mode", mode, "--profiler", c.Profiler); err != nil {
					return nil, err
				}
				return artifactFiles(run)
			}})
			steps = append(steps, Step{"report-" + variant, func(r *Runner, rec *Record) ([]string, error) {
				run := observationDir(c, r.State.Steps["observe-"+variant], variant)
				ctx, cancel := context.WithTimeout(r.Ctx, time.Duration(c.Timeout)*time.Minute)
				defer cancel()
				out := filepath.Join(run, "report")
				if err := generateReport(ctx, run, out, ReportOptions{Go: c.Go, Flamegraph: c.Flamegraph}, r.Log); err != nil {
					return nil, err
				}
				return artifactFiles(out)
			}})
		}
	}
	steps = append(steps, Step{"summarize", func(r *Runner, rec *Record) ([]string, error) {
		reports := map[string]string{}
		if c.Diagnostics {
			for _, variant := range observedVariants(c) {
				reports[variant] = filepath.Join(observationDir(c, r.State.Steps["observe-"+variant], variant), "report")
			}
		}
		if err := summarize(r.dir("compare"), reports["before"], reports["after"], reports["cluster"], rec.Dir, r.Log); err != nil {
			return nil, err
		}
		return artifactFiles(rec.Dir)
	}})
	if c.Publish {
		for _, variant := range observedVariants(c) {
			steps = append(steps, Step{"publish-" + variant, func(r *Runner, rec *Record) ([]string, error) {
				run := observationDir(c, r.State.Steps["observe-"+variant], variant)
				ctx, cancel := context.WithTimeout(r.Ctx, time.Duration(c.Timeout)*time.Minute)
				defer cancel()
				var config langfuse.Config
				if c.Langfuse != nil {
					config = *c.Langfuse
				}
				receipt, err := langfuse.Import(ctx, run, config)
				if err != nil {
					return nil, err
				}
				path := filepath.Join(run, "report", "langfuse.json")
				if err := atomicJSON(path, receipt); err != nil {
					return nil, err
				}
				if err := os.Chmod(path, 0644); err != nil {
					return nil, err
				}
				fmt.Fprintf(r.Log, "Langfuse verified %d observations, %s\n", receipt.Verified, receipt.URL)
				if err := r.python(observe+"publish.py", "--run", run); err != nil {
					return nil, err
				}
				return []string{path, rec.Log}, nil
			}})
		}
	}
	return steps
}

func observedVariants(c Config) []string {
	var variants []string
	if c.Baseline != "" {
		variants = append(variants, "before")
	}
	variants = append(variants, "after")
	if c.IncludeCluster {
		variants = append(variants, "cluster")
	}
	return variants
}

func verifyProbe(probe, baseline string) error {
	var sources map[string]string
	if err := readJSON(filepath.Join(probe, "sources.json"), &sources); err != nil {
		return err
	}
	for key, path := range map[string]string{"baseline": filepath.Join(baseline, "dragonfly"), "probe": filepath.Join(probe, "dragonfly")} {
		hash, err := hashFile(path)
		if err != nil {
			return err
		}
		if sources[key] != hash {
			return fmt.Errorf("probe %s hash mismatch: %s", key, probe)
		}
	}
	return nil
}

// The publisher uses the directory basename as its public ID. Include experiment and variant.
func observationDir(c Config, rec *Record, variant string) string {
	hash := sha256.Sum256([]byte(c.Output))
	return filepath.Join(rec.Dir, fmt.Sprintf("%s-%s-%x-a%d", filepath.Base(c.Output), variant, hash[:4], rec.Attempt))
}

// Preserve large raw evidence on disk; hash files used by subsequent stages, never test datasets.
func artifactFiles(dir string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if d.Name() == "testdata" || d.Name() == "bin" || d.Name() == "__pycache__" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".o" {
			files = append(files, path)
		}
		return nil
	})
	if err == nil && len(files) == 0 {
		err = errors.New("no artifacts produced: " + dir)
	}
	return files, err
}
