// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package models

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"dragonflydb.io/profile/pkg/langfuse"
	"dragonflydb.io/profile/pkg/workflow"
)

type DiagnosticSource struct {
	Report       string `json:"report"`
	Sources      string `json:"probe_sources"`
	RequireTrace bool   `json:"require_langfuse,omitempty"`
}

type ImportConfig struct {
	Experiment     Label                       `json:"experiment"`
	Compare        string                      `json:"compare"`
	Diagnostics    map[string]DiagnosticSource `json:"diagnostics"`
	Targets        ChoiceIDs                   `json:"targets,omitempty"`
	ValidationDir  string                      `json:"validation_dir,omitempty"`
	ValidationLogs []string                    `json:"validation_logs,omitempty"`
	Aliases        map[string]string           `json:"aliases,omitempty"`
	Only           []string                    `json:"only_variants,omitempty"`
	Additional     []BenchmarkSource           `json:"additional_benchmarks,omitempty"`
}

// WorkflowArchive collects the validated outputs of a completed platform measurement.
func WorkflowArchive(id, output string) (ImportConfig, error) {
	files, err := workflow.CurrentArtifacts(output)
	if err != nil {
		return ImportConfig{}, err
	}
	return ImportConfig{
		Experiment: Label{ID: id}, Compare: files.Compare, ValidationLogs: files.ValidationLogs,
		Diagnostics: map[string]DiagnosticSource{
			"current": {Report: files.Report, Sources: files.ProbeSources, RequireTrace: true},
		},
	}, nil
}

func ReadImportConfig(path string) (ImportConfig, error) {
	var c ImportConfig
	f, err := os.Open(path)
	if err != nil {
		return c, err
	}
	defer f.Close()
	d := json.NewDecoder(f)
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return c, err
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return c, errors.New("import config must contain one JSON object")
	}
	return c, nil
}

// Import validates version and measurement identities before making the record visible.
func (s Store) Import(c ImportConfig) (Summary, error) {
	var empty Summary
	label := c.Experiment
	if !slug.MatchString(label.ID) || c.Compare == "" {
		return empty, errors.New("experiment slug and compare directory are required")
	}
	dest := filepath.Join(s.Root, label.ID)
	tempRoot := s.Root
	var choices Comparisons
	draft, err := s.Get(label.ID)
	if err == nil {
		if draft.Summary.Experiment.State != "draft" {
			return empty, ErrExists
		}
		decision := label.Decision
		label = draft.Summary.Experiment
		if decision != "" {
			label.Decision = decision
		}
		choices = draft.Summary.Comparisons
		if choices.Cumulative == nil || choices.Reference == nil {
			return empty, errors.New("draft is missing its comparison choices")
		}
		if !choices.allows(c.Targets) {
			return empty, errors.New("results must use the draft's saved comparison choices")
		}
		tempRoot = dest
		dest = filepath.Join(dest, "result")
	} else if !os.IsNotExist(err) {
		return empty, err
	} else if c.Targets != (ChoiceIDs{}) {
		choices, err = selectTargets(c.Targets, s.Targets)
		if err != nil {
			return empty, err
		}
	}
	if _, err := os.Lstat(dest); !os.IsNotExist(err) {
		return empty, fmt.Errorf("experiment result already exists or path is unavailable: %s", dest)
	}
	if err := os.MkdirAll(s.Root, 0755); err != nil {
		return empty, err
	}
	temp, err := os.MkdirTemp(tempRoot, ".import-")
	if err != nil {
		return empty, err
	}
	defer os.RemoveAll(temp)
	record, err := buildRecord(c, label, temp, func(_ Summary) (Comparisons, error) { return choices, nil })
	if err != nil {
		return empty, err
	}
	if err := writeRecord(temp, record); err != nil {
		return empty, err
	}
	return record.Summary, os.Rename(temp, dest)
}
func buildRecord(c ImportConfig, label Label, temp string, selectComparisons func(Summary) (Comparisons, error)) (Record, error) {
	var empty Record
	if strings.TrimSpace(label.Title) == "" {
		return empty, errors.New("title is required for an experiment without a saved draft")
	}
	if label.Decision == "" {
		label.Decision = "pending"
	}
	if label.Decision != "pending" && label.Decision != "retained" && label.Decision != "reverted" {
		return empty, errors.New("decision must be pending, retained or reverted")
	}
	if label.Date != "" {
		if _, err := time.Parse(time.DateOnly, label.Date); err != nil {
			return empty, errors.New("date must be YYYY-MM-DD")
		}
	}
	compare, primary, env, err := openBenchmark(BenchmarkSource{ID: "main", Title: "主对照", Dir: c.Compare, Aliases: c.Aliases, Only: c.Only})
	if err != nil {
		return empty, err
	}
	current, ok := primary.Variants["current"]
	if !ok || current.Mode != "raft" || current.Ack != "memory" {
		return empty, errors.New("main benchmark needs a Raft memory current")
	}
	artifactDir := filepath.Join(temp, "artifacts")
	if err := os.Mkdir(artifactDir, 0755); err != nil {
		return empty, err
	}
	label.State = "complete"
	primary.Workload.AckMode = current.Ack
	record := Record{
		Schema: Schema,
		Detail: Detail{
			Summary: Summary{
				Experiment: label, Harness: primary.Harness,
				Workload:    primary.Workload,
				Variants:    primary.Variants,
				Delta:       measureRound(compare.Rows, primary.Variants),
				Evidence:    Evidence{Confirmed: len(compare.Rows), Benchmark: len(compare.Rows)},
				Diagnostics: map[string]Diagnostic{},
			},
			Observations: []Observation{},
		},
		Artifacts: map[string]string{}, Measurements: map[string]json.RawMessage{},
	}
	choices, err := selectComparisons(record.Summary)
	if err != nil {
		return empty, err
	}
	if err := choices.measure(primary.Workload, primary.Harness, current.Metrics); err != nil {
		return empty, err
	}
	record.Summary.Comparisons = choices
	if err := record.addBenchmark(artifactDir, primary, compare); err != nil {
		return empty, err
	}
	sets := map[string]bool{"main": true}
	for _, source := range c.Additional {
		if sets[source.ID] {
			return empty, fmt.Errorf("duplicate benchmark set %q", source.ID)
		}
		sets[source.ID] = true
		archived, set, _, err := openBenchmark(source)
		if err != nil {
			return empty, err
		}
		if err := record.addBenchmark(artifactDir, set, archived); err != nil {
			return empty, err
		}
	}
	var diagnosticNames []string
	for name := range c.Diagnostics {
		if name == "candidate" {
			name = "current"
		}
		if _, ok := env.Builds[name]; !ok {
			return empty, fmt.Errorf("unknown diagnostic variant %q", name)
		}
		diagnosticNames = append(diagnosticNames, name)
	}
	sort.Strings(diagnosticNames)
	for _, name := range diagnosticNames {
		source, ok := c.Diagnostics[name]
		if name == "current" && !ok {
			source = c.Diagnostics["candidate"]
		}
		if err := record.importDiagnostic(artifactDir, name, source, env); err != nil {
			return empty, fmt.Errorf("%s diagnostic: %w", name, err)
		}
	}
	if c.ValidationDir != "" || len(c.ValidationLogs) != 0 {
		if err := record.importValidation(artifactDir, c.ValidationDir, c.ValidationLogs); err != nil {
			return empty, err
		}
	}
	return record, nil
}

type BenchmarkSource struct {
	ID      string            `json:"id"`
	Title   string            `json:"title"`
	Dir     string            `json:"compare"`
	Aliases map[string]string `json:"aliases,omitempty"`
	Only    []string          `json:"only_variants,omitempty"`
}

type loadedBenchmark struct {
	Rows   []workflow.BenchRow
	Origin []string
	Raw    []json.RawMessage
}

func openBenchmark(source BenchmarkSource) (loadedBenchmark, BenchmarkSet, workflow.Environment, error) {
	var rows loadedBenchmark
	var set BenchmarkSet
	var env workflow.Environment
	if !slug.MatchString(source.ID) || source.Dir == "" || strings.TrimSpace(source.Title) == "" {
		return rows, set, env, errors.New("benchmark set needs an ID, title and archive directory")
	}
	archive, err := workflow.ReadBenchmark(source.Dir, source.Only...)
	if err != nil {
		return rows, set, env, err
	}
	for from, to := range source.Aliases {
		if _, ok := archive.Environment.Builds[from]; !ok || !slug.MatchString(to) {
			return rows, set, env, fmt.Errorf("invalid benchmark alias %q to %q", from, to)
		}
	}
	alias := func(name string) string {
		if mapped := source.Aliases[name]; mapped != "" {
			return mapped
		}
		if name == "candidate" {
			return "current"
		}
		return name
	}
	env = archive.Environment
	env.Builds = map[string]workflow.BuildInfo{}
	set = BenchmarkSet{ID: source.ID, Title: source.Title, Workload: workloadFromRow(archive.Rows[0]), Harness: env.Harness, Mount: env.Mount, Variants: map[string]Variant{}}
	for sourceName, build := range archive.Environment.Builds {
		name := alias(sourceName)
		if _, exists := env.Builds[name]; exists || !slug.MatchString(name) {
			return rows, set, env, fmt.Errorf("duplicate or invalid benchmark variant %q", name)
		}
		env.Builds[name] = build
		set.Variants[name] = Variant{SHA: build.SHA, Metrics: archive.Metrics[sourceName], Mode: build.Mode, Ack: build.Ack}
	}
	if current, ok := set.Variants["current"]; ok {
		set.Workload.AckMode = current.Ack
	}
	var raw []json.RawMessage
	if err := decodeFile(filepath.Join(source.Dir, "summary.json"), &raw); err != nil {
		return rows, set, env, err
	}
	for _, row := range archive.Rows {
		if row.Order < 1 || row.Order > len(raw) {
			return rows, set, env, errors.New("benchmark archive changed during import")
		}
		rows.Raw = append(rows.Raw, raw[row.Order-1])
		rows.Origin = append(rows.Origin, filepath.Join(source.Dir, row.LogName()))
		row.Variant = alias(row.Variant)
		rows.Rows = append(rows.Rows, row)
	}
	return rows, set, env, nil
}

func (r *Record) addBenchmark(dest string, set BenchmarkSet, data loadedBenchmark) error {
	r.Benchmarks = append(r.Benchmarks, set)
	for i, row := range data.Rows {
		id := fmt.Sprintf("benchmark-%s-%d", row.Variant, row.Trial)
		if set.ID != "main" {
			id = fmt.Sprintf("benchmark-%s-%s-%d", set.ID, row.Variant, row.Trial)
		}
		if err := r.copyArtifact(dest, id+"-log", data.Origin[i], ".log"); err != nil {
			return err
		}
		run := newRun(id, "benchmark", row.Variant, id+"-log", row)
		run.Set = set.ID
		r.Runs = append(r.Runs, run)
		r.Measurements[id] = data.Raw[i]
	}
	return nil
}

func measureRound(rows []workflow.BenchRow, variants map[string]Variant) Delta {
	before, hasBefore := variants["baseline"]
	if !hasBefore {
		return Delta{}
	}
	current := variants["current"]
	relative := func(a, b float64) float64 { return (b/a - 1) * 100 }
	delta := Delta{Throughput: relative(before.Metrics.Rate.Median, current.Metrics.Rate.Median), P95: relative(before.Metrics.P95.Median, current.Metrics.P95.Median)}
	a, b := map[int]workflow.BenchRow{}, map[int]workflow.BenchRow{}
	for _, row := range rows {
		switch row.Variant {
		case "baseline":
			a[row.Trial] = row
		case "current":
			b[row.Trial] = row
		}
	}
	for trial := 1; trial <= 3; trial++ {
		delta.Paired = append(delta.Paired, relative(a[trial].Rate, b[trial].Rate))
		delta.PairedP95 = append(delta.PairedP95, relative(a[trial].P95, b[trial].P95))
	}
	return delta
}

var (
	validationLog = regexp.MustCompile(`^([a-z0-9]+)-(integration|[a-z0-9_]+_test)\.log$`)
	gtestPassed   = regexp.MustCompile(`(?m)^\[  PASSED  \] ([0-9]+) tests?\.$`)
	unittestRan   = regexp.MustCompile(`(?m)^Ran ([0-9]+) tests? in [\d.]+s$`)
)

func (r *Record) importValidation(dest, dir string, paths []string) error {
	if dir != "" {
		if len(paths) != 0 {
			return errors.New("specify either validation_dir or validation_logs")
		}
		entries, err := os.ReadDir(dir)
		if err != nil {
			return err
		}
		for _, entry := range entries {
			if !entry.IsDir() {
				paths = append(paths, filepath.Join(dir, entry.Name()))
			}
		}
	}
	r.Summary.Validation = map[string]ValidationTotals{}
	for _, path := range paths {
		build, suite := validationIdentity(filepath.Base(path))
		if build == "" {
			if dir == "" {
				return fmt.Errorf("invalid validation log name: %s", path)
			}
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(data)
		integration := suite == "integration" || suite == "migration"
		var counts [][]string
		if integration {
			if strings.HasSuffix(strings.TrimSpace(text), "\nOK") {
				counts = unittestRan.FindAllStringSubmatch(text, -1)
			}
		} else {
			counts = gtestPassed.FindAllStringSubmatch(text, -1)
		}
		if len(counts) == 0 || strings.Contains(text, "[  FAILED  ]") {
			return fmt.Errorf("missing passed validation result: %s", path)
		}
		n := 0
		for _, count := range counts {
			value, err := strconv.Atoi(count[1])
			if err != nil || value < 1 {
				return fmt.Errorf("invalid validation count: %s", path)
			}
			n += value
		}
		id := "validation-" + build + "-" + suite
		if err := r.copyArtifact(dest, id, path, ".log"); err != nil {
			return err
		}
		totals := r.Summary.Validation[build]
		if integration {
			totals.Integration += n
		} else {
			totals.Unit += n
		}
		r.Summary.Validation[build] = totals
		r.Checks = append(r.Checks, ValidationCheck{Build: build, Suite: suite, Tests: n, Log: id})
	}
	if len(r.Checks) == 0 {
		return errors.New("no matching validation logs")
	}
	return nil
}

func validationIdentity(name string) (build, suite string) {
	if parts := validationLog.FindStringSubmatch(name); parts != nil {
		return parts[1], parts[2]
	}
	switch name {
	case "integration.log", "dragonfly-apply-integration.log":
		return "archive", "integration"
	case "encoded-integration.log":
		return "encoded", "integration"
	case "dragonfly-apply-migration.log":
		return "archive", "migration"
	case "dragonfly-apply-final-unit.log":
		return "archive", "unit"
	case "dragonfly-apply-unit.log":
		return "screening", "unit"
	case "dragonfly-apply-sanitize-test.log", "sanitize-tests.log":
		return "sanitize", "unit"
	}
	return "", ""
}

func newRun(id, kind, variant, log string, row workflow.BenchRow) Run {
	return Run{
		ID: id, Kind: kind, Variant: variant, Order: row.Order, Trial: row.Trial, Status: "passed",
		Rate: row.Rate, SetRate: row.SetRate, Tail: row.Tail, P95: row.P95, RecoveryMS: row.RecoveryMS, Log: log,
	}
}

func (r *Record) importDiagnostic(dest, variant string, source DiagnosticSource, env workflow.Environment) error {
	if source.Report == "" || source.Sources == "" {
		return errors.New("report and probe_sources are required for a diagnostic")
	}
	d, stages, row, err := workflow.ReadDiagnostic(source.Report, env.Builds[variant], env.Harness)
	if err != nil {
		return err
	}
	var hashes map[string]string
	if err := decodeFile(source.Sources, &hashes); err != nil {
		return err
	}
	if hashes["baseline"] != env.Builds[variant].SHA || hashes["probe"] != d.ProbeSHA {
		return errors.New("diagnostic probe does not match the formal benchmark binary")
	}
	log := filepath.Join(filepath.Dir(source.Report), "test.log")
	if err := checkDiagnosticLog(log); err != nil {
		return err
	}
	id := "diagnostic-" + variant
	if err := r.copyArtifact(dest, id+"-log", log, ".log"); err != nil {
		return err
	}
	timeline := id + "-timeline"
	if err := r.copyArtifact(dest, timeline, filepath.Join(source.Report, "timeline.json"), ".json"); err != nil {
		return err
	}
	graphs := map[string]string{}
	for role, node := range d.Nodes {
		key := id + "-" + role
		name := fmt.Sprintf("%s-node-%s.svg", role, node.Node)
		if err := r.copyArtifact(dest, key, filepath.Join(source.Report, name), ".svg"); err != nil {
			return err
		}
		graphs[role] = key
		folded := fmt.Sprintf("%s-node-%s.folded", role, node.Node)
		if err := r.copyArtifact(dest, key+"-folded", filepath.Join(source.Report, folded), ".txt"); err != nil {
			return err
		}
	}
	trace := Diagnostic{Diagnostic: d}
	var receipt langfuse.Receipt
	err = decodeFile(filepath.Join(source.Report, "langfuse.json"), &receipt)
	if err == nil {
		link, e := url.Parse(receipt.URL)
		if e != nil || (link.Scheme != "http" && link.Scheme != "https") || link.Host == "" || link.User != nil || receipt.FullSpans != d.SpanCount || receipt.Verified < 1 {
			return errors.New("invalid Langfuse verification receipt")
		}
		trace.TraceURL, trace.Verified = receipt.URL, receipt.Verified
	} else if source.RequireTrace || !os.IsNotExist(err) {
		return err
	}
	for artifact, path := range map[string]string{"report": filepath.Join(source.Report, "summary.json"), "probe": source.Sources} {
		if err := r.copyArtifact(dest, id+"-"+artifact, path, ".json"); err != nil {
			return err
		}
	}
	r.Runs = append(r.Runs, newRun(id, "diagnostic", variant, id+"-log", row))
	var rowResult json.RawMessage
	if err := decodeFile(filepath.Join(filepath.Dir(source.Report), "result.json"), &rowResult); err != nil {
		return err
	}
	r.Measurements[id] = rowResult
	r.Observations = append(r.Observations, Observation{variant, graphs, timeline, stages})
	r.Summary.Diagnostics[variant] = trace
	r.Summary.Evidence.Flamegraphs += len(graphs)
	r.Summary.Evidence.Diagnostics++
	return nil
}

func decodeFile(path string, value any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}

func checkDiagnosticLog(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		if s.Text() == "OK" {
			return nil
		}
	}
	return fmt.Errorf("missing successful diagnostic unittest result: %s", path)
}

func (r *Record) copyArtifact(dest, id, path, ext string) error {
	in, err := os.Open(path)
	if err != nil {
		return err
	}
	defer in.Close()
	if st, err := in.Stat(); err != nil || !st.Mode().IsRegular() || st.Size() == 0 {
		return fmt.Errorf("invalid artifact: %s", path)
	}
	name := id + ext
	out, err := os.Create(filepath.Join(dest, name))
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	if err := errors.Join(err, out.Close()); err != nil {
		return err
	}
	r.Artifacts[id] = name
	return nil
}
