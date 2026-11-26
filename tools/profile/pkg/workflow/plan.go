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
	"time"
)

func plan(c Config) []Step {
	var steps []Step
	if c.Baseline != "" {
		steps = append(steps, Step{"baseline", func(r *Runner, rec *Record) ([]string, error) {
			if c.Diagnostics {
				if err := requireNativeBuild(c.Baseline); err != nil {
					return nil, err
				}
			}
			return copyFiles(c.Baseline, rec.Dir, "dragonfly", "raft_storage")
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
			if c.Diagnostics {
				if err := requireNativeBuild(c.Release); err != nil {
					return nil, err
				}
			}
			return copyFiles(c.Release, rec.Dir, "dragonfly", "raft_storage")
		}},
		Step{"compare", func(r *Runner, rec *Record) ([]string, error) {
			if err := requireBenchmarkSource(c.Repo); err != nil {
				return nil, err
			}
			workload := BenchmarkWorkload()
			options := benchmarkOptions{build: r.dir("candidate"), ack: workload.Ack,
				clients: workload.Clients, depth: workload.Depth, output: rec.Dir, cluster: c.IncludeCluster}
			if c.Baseline != "" {
				options.baseline = r.dir("baseline")
			}
			if err := runBenchmark(r.Ctx, options, r.Log); err != nil {
				return nil, err
			}
			if _, err := ReadComparison(rec.Dir); err != nil {
				return nil, err
			}
			return artifactFiles(rec.Dir)
		}})
	if c.Diagnostics {
		for _, variant := range observedVariants(c) {
			steps = append(steps, Step{"observe-" + variant, func(r *Runner, rec *Record) ([]string, error) {
				return r.observe(rec, variant)
			}})
			steps = append(steps, Step{"report-" + variant, func(r *Runner, rec *Record) ([]string, error) {
				run := observationDir(c, r.State.Steps["observe-"+variant], variant)
				ctx, cancel := context.WithTimeout(r.Ctx, time.Duration(c.Timeout)*time.Minute)
				defer cancel()
				if err := nativeReport(ctx, c, run, rec.Dir, r.Log); err != nil {
					return nil, err
				}
				return artifactFiles(rec.Dir)
			}})
		}
	}

	steps = append(steps, Step{"summarize", func(r *Runner, rec *Record) ([]string, error) {
		reports := map[string]string{}
		if c.Diagnostics {
			for _, variant := range observedVariants(c) {
				reports[variant] = r.dir("report-" + variant)
			}
		}
		if err := summarize(r.dir("compare"), reports["before"], reports["after"], reports["cluster"], rec.Dir, r.Log); err != nil {
			return nil, err
		}
		return artifactFiles(rec.Dir)
	}})
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

// Each workload attempt owns a distinct capture; report retries keep the completed capture.
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
