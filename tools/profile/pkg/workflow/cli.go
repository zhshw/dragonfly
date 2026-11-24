// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"dragonflydb.io/profile/pkg/langfuse"
)

type Validation struct {
	Build       string   `json:"build"`
	Unit        []string `json:"unit_tests"`
	Integration []string `json:"integration_tests"`
	Sanitize    bool     `json:"sanitize,omitempty"`
}

type Config struct {
	Repo           string           `json:"repo"`
	Output         string           `json:"output"`
	Baseline       string           `json:"baseline_build,omitempty"`
	Release        string           `json:"release_build"`
	Validation     []Validation     `json:"validation"`
	Diagnostics    bool             `json:"diagnostics"`
	IncludeCluster bool             `json:"include_cluster,omitempty"`
	BaselineProbe  string           `json:"baseline_probe,omitempty"`
	Profiler       string           `json:"profiler,omitempty"`
	Go             string           `json:"go,omitempty"`
	Flamegraph     string           `json:"flamegraph_dir,omitempty"`
	Publish        bool             `json:"publish,omitempty"`
	Langfuse       *langfuse.Config `json:"langfuse,omitempty"`
	ExtraInputs    []string         `json:"extra_inputs,omitempty"`
	Jobs           int              `json:"jobs,omitempty"`
	Timeout        int              `json:"timeout_minutes,omitempty"`
}

func readJSON(path string, value any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}

// LoadConfig resolves paths in a workflow profile against its source checkout.
func LoadConfig(path string) (Config, error) {
	c := Config{Release: "build-opt", Go: "go", Jobs: 4, Timeout: 30}
	data, err := os.ReadFile(path)
	if err != nil {
		return c, err
	}
	d := json.NewDecoder(strings.NewReader(string(data)))
	d.DisallowUnknownFields()
	if err = d.Decode(&c); err != nil {
		return c, err
	}
	if err = d.Decode(new(any)); err != io.EOF {
		return c, errors.New("config must contain one JSON object")
	}
	if c.Repo == "" || c.Output == "" || len(c.Validation) == 0 {
		return c, errors.New("repo, output and validation are required")
	}
	if strings.HasPrefix(c.Repo, "~/") {
		home, _ := os.UserHomeDir()
		c.Repo = filepath.Join(home, c.Repo[2:])
	}
	c.Repo, err = filepath.Abs(c.Repo)
	if err != nil {
		return c, err
	}
	resolve := func(p string) string {
		if strings.HasPrefix(p, "~/") {
			home, _ := os.UserHomeDir()
			p = filepath.Join(home, p[2:])
		}
		if p != "" && !filepath.IsAbs(p) {
			p = filepath.Join(c.Repo, p)
		}
		return p
	}
	c.Output, c.Baseline, c.Release = resolve(c.Output), resolve(c.Baseline), resolve(c.Release)
	c.BaselineProbe, c.Profiler, c.Flamegraph = resolve(c.BaselineProbe), resolve(c.Profiler), resolve(c.Flamegraph)
	if c.Langfuse != nil {
		c.Langfuse.EnvFile = resolve(c.Langfuse.EnvFile)
	}
	if strings.ContainsRune(c.Go, '/') {
		c.Go = resolve(c.Go)
	}
	if c.Jobs < 1 || c.Timeout < 1 {
		return c, errors.New("jobs and timeout_minutes must be positive")
	}
	for i := range c.Validation {
		v := &c.Validation[i]
		v.Build = resolve(v.Build)
		if v.Build == "" || len(v.Unit) == 0 {
			return c, errors.New("each validation needs a build and unit_tests")
		}
		for _, target := range v.Unit {
			if !regexp.MustCompile(`^[A-Za-z0-9_]+$`).MatchString(target) {
				return c, fmt.Errorf("invalid unit test target %q", target)
			}
		}
		for _, name := range v.Integration {
			if !regexp.MustCompile(`^tests\.integration\.[A-Za-z0-9_.]+$`).MatchString(name) {
				return c, fmt.Errorf("expected a unittest module/class/method, got %q", name)
			}
		}
	}
	if c.Release == "" || c.Go == "" {
		return c, errors.New("release_build and go must not be empty")
	}
	for _, build := range append([]Validation{{Build: c.Release}}, c.Validation...) {
		baseline, bErr := filepath.EvalSymlinks(c.Baseline)
		candidate, cErr := filepath.EvalSymlinks(build.Build)
		if c.Baseline == build.Build || (bErr == nil && cErr == nil && baseline == candidate) {
			return c, errors.New("baseline_build must be a frozen directory, separate from all build directories")
		}
	}
	for i, p := range c.ExtraInputs {
		c.ExtraInputs[i] = resolve(p)
	}
	if c.Diagnostics && (c.Profiler == "" || c.Flamegraph == "" || (c.Baseline != "" && c.BaselineProbe == "")) {
		return c, errors.New("diagnostics requires profiler and flamegraph_dir, with baseline_probe for a baseline_build")
	}
	if c.Publish && !c.Diagnostics {
		return c, errors.New("publish requires diagnostics")
	}
	return c, nil
}

func CLI(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("usage: profile plan|run -config experiment.json [-resume]; status -run DIR; summarize -compare DIR [-before REPORT -after REPORT -cluster REPORT] -out DIR")
	}
	f := flag.NewFlagSet(args[0], flag.ContinueOnError)
	switch args[0] {
	case "plan", "run":
		file := f.String("config", "", "experiment JSON; relative paths use repo")
		resume := f.Bool("resume", false, "verify inputs and skip completed steps")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		if f.NArg() != 0 || *file == "" {
			return errors.New("expected -config FILE without positional arguments")
		}
		c, err := LoadConfig(*file)
		if err != nil {
			return err
		}
		steps := plan(c)
		if args[0] == "plan" {
			fmt.Println("memory; clients=1; pipeline=64; 256 MiB; current x 3")
			if c.Baseline != "" {
				fmt.Println("explicit baseline build x 3")
			}
			if c.IncludeCluster {
				fmt.Println("original Cluster control x 3")
			}
			for i, step := range steps {
				fmt.Printf("%02d %s\n", i+1, step.Name)
			}
			return nil
		}
		return runWorkflow(ctx, c, steps, *resume, os.Stdout)
	case "status":
		dir := f.String("run", "", "workflow output directory")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		if f.NArg() != 0 || *dir == "" {
			return errors.New("status requires -run DIR without positional arguments")
		}
		var state State
		if err := readJSON(filepath.Join(*dir, "state.json"), &state); err != nil {
			return err
		}
		fmt.Printf("%s: %d/%d steps complete; updated %s\n", state.Status, completed(state), len(state.Steps), state.Updated)
		for name, step := range state.Steps {
			if step.Status == "running" || step.Status == "failed" {
				fmt.Printf("%s: %s; log=%s\n", name, step.Status, step.Log)
			}
		}
		return nil
	case "summarize":
		compare := f.String("compare", "", "completed pipeline_compare output")
		before := f.String("before", "", "baseline report directory")
		after := f.String("after", "", "candidate report directory")
		cluster := f.String("cluster", "", "original cluster report directory")
		out := f.String("out", "", "new summary directory; does not run commands")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		if f.NArg() != 0 || *compare == "" || *out == "" {
			return errors.New("summarize requires -compare and -out")
		}
		return summarize(*compare, *before, *after, *cluster, *out, os.Stdout)
	case "report":
		run := f.String("run", "", "raw diagnostic collection directory")
		out := f.String("out", "", "new report directory; defaults to RUN/report")
		goBin := f.String("go", "go", "Go executable for pprof")
		flamegraph := f.String("flamegraph-dir", "", "directory containing flamegraph.pl")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		if f.NArg() != 0 || *run == "" {
			return errors.New("report requires -run DIR")
		}
		if *out == "" {
			*out = filepath.Join(*run, "report")
		}
		if err := generateReport(ctx, *run, *out, ReportOptions{Go: *goBin, Flamegraph: *flamegraph}, os.Stderr); err != nil {
			return err
		}
		fmt.Println(*out)
		return nil
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
}
