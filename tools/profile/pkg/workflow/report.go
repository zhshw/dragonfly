// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package workflow

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"html/template"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"dragonflydb.io/profile/pkg/langfuse"
)

type ReportOptions struct {
	Go         string
	Flamegraph string
	CPUFrom    string
}

type timelineEvent struct {
	Name string           `json:"name"`
	Cat  string           `json:"cat"`
	Kind string           `json:"ph"`
	Node string           `json:"pid"`
	TID  int64            `json:"tid"`
	Time float64          `json:"ts"`
	Dur  float64          `json:"dur"`
	Args map[string]int64 `json:"args"`
}

func decodeReport(report langfuse.Report) (langfuse.Metadata, BenchRow, error) {
	var metadata langfuse.Metadata
	var row BenchRow
	if err := errors.Join(json.Unmarshal(report.Metadata, &metadata), json.Unmarshal(report.Result, &row)); err != nil {
		return metadata, row, err
	}
	return metadata, row, validWorkload(row)
}

func generateReport(ctx context.Context, run, out string, options ReportOptions, log io.Writer) error {
	var m langfuse.Metadata
	var result json.RawMessage
	var metadata map[string]json.RawMessage
	if err := readJSON(filepath.Join(run, "metadata.json"), &metadata); err != nil {
		return err
	}
	if err := readJSON(filepath.Join(run, "result.json"), &result); err != nil {
		return err
	}
	if string(metadata["mode"]) == `"memory"` {
		metadata["mode"] = json.RawMessage(`"raft.memory"`)
	}
	encoded, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	dropped := 0
	report := langfuse.Report{Metadata: encoded, Result: result, Dropped: &dropped}
	m, b, err := decodeReport(report)
	if err != nil {
		return err
	}
	if len(m.Nodes) != 2 {
		return errors.New("expected leader and follower in the trace metadata")
	}
	for node, info := range m.Nodes {
		if n, e := strconv.Atoi(node); e != nil || n < 1 || strconv.Itoa(n) != node || (info.Role != "leader" && info.Role != "follower") {
			return fmt.Errorf("invalid profile node %q", node)
		}
	}
	if _, err := os.Lstat(out); !os.IsNotExist(err) {
		return fmt.Errorf("report output exists or is unavailable: %s", out)
	}
	events, err := langfuse.LoadSpans(run, m, langfuse.Window{Start: b.Start, WriteEnd: b.WriteEnd, End: b.End})
	if err != nil {
		return err
	}
	report.Count, report.Stages = len(events), langfuse.Stages(events)
	if err := os.MkdirAll(filepath.Dir(out), 0755); err != nil {
		return err
	}
	temp, err := os.MkdirTemp(filepath.Dir(out), ".report-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temp)
	timeline := make([]timelineEvent, 0, len(events))
	for _, span := range events {
		timeline = append(timeline, timelineEvent{span.Stage, span.Role, "X", span.Node, span.TID,
			float64(span.Start-b.Start) / 1000, float64(span.End-span.Start) / 1000,
			map[string]int64{"arg0": span.Arg0, "arg1": span.Arg1}})
	}
	for name, value := range map[string]any{"spans.json": events, "summary.json": report, "timeline.json": map[string]any{"traceEvents": timeline}} {
		if err := writeReportFile(filepath.Join(temp, name), func(w io.Writer) error { return json.NewEncoder(w).Encode(value) }); err != nil {
			return err
		}
	}
	if err := writeReportFile(filepath.Join(temp, "stages.csv"), func(w io.Writer) error {
		rows := [][]string{{"node", "role", "stage", "count", "sum_ms", "union_ms", "p50_ms", "p95_ms", "max_ms"}}
		for _, stage := range report.Stages {
			row := []string{stage.Node, stage.Role, stage.Name, strconv.Itoa(stage.Count)}
			for _, n := range []float64{stage.Sum, stage.Union, stage.P50, stage.P95, stage.Max} {
				row = append(row, strconv.FormatFloat(n, 'f', 3, 64))
			}
			rows = append(rows, row)
		}
		return csv.NewWriter(w).WriteAll(rows)
	}); err != nil {
		return err
	}
	var profiles []string
	if options.Flamegraph != "" {
		if m.Frequency <= 0 {
			return errors.New("no CPU sample rate in trace metadata")
		}
		if options.CPUFrom == "" {
			if sha, err := hashFile(m.Binary); err != nil || sha != m.BinarySHA {
				return errors.New("profile binary is unavailable or differs from trace metadata")
			}
		}
		profiles, err = cpuReports(ctx, run, temp, m, options, log)
		if err != nil {
			return err
		}
	}
	rows := append([]langfuse.Stage(nil), report.Stages...)
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Role != rows[j].Role {
			return rows[i].Role < rows[j].Role
		}
		return rows[i].Union > rows[j].Union
	})
	page := struct {
		Name, Mode string
		Throughput float64
		Rows       []langfuse.Stage
		Profiles   []string
	}{filepath.Base(run), m.Mode, b.Rate, rows, profiles}
	if err := writeReportFile(filepath.Join(temp, "index.html"), func(w io.Writer) error { return reportPage.Execute(w, page) }); err != nil {
		return err
	}
	return os.Rename(temp, out)
}

func writeReportFile(path string, write func(io.Writer) error) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	return errors.Join(write(f), f.Close())
}

var reportPage = template.Must(template.New("report").Parse(`<!doctype html>
<html lang="en"><head><meta charset="utf-8"><title>{{.Name}} · {{.Mode}}</title>
<style>body{font:15px system-ui;margin:30px auto;padding:0 18px;max-width:1280px;color:#1a2234}
table{border-collapse:collapse;width:100%}td,th{padding:.45rem;border-bottom:1px solid #e3e7ef;text-align:left}
object{width:100%;height:620px;border:1px solid #e3e7ef}</style></head><body>
<h1>{{.Name}} · {{.Mode}}</h1><p>Diagnostic SET + WAIT 1 throughput {{.Throughput}} MiB/s</p>
<p><a href="timeline.json">Perfetto timeline</a> · <a href="stages.csv">Stage CSV</a> · <a href="summary.json">Metadata</a></p>
{{range .Profiles}}<h2>{{.}}</h2><p><a href="{{.}}.svg">Flamegraph</a> · <a href="{{.}}.top.txt">CPU top</a></p>
<object data="{{.}}.svg" type="image/svg+xml"></object>{{end}}
<h2>Trace stages</h2><p>Union merges overlapping intervals within each row. Stage durations can overlap.</p>
<table><thead><tr><th>Role</th><th>Stage</th><th>Count</th><th>Union ms</th><th>Sum ms</th><th>P95 ms</th></tr></thead><tbody>
{{range .Rows}}<tr><td>{{.Role}}</td><td>{{.Name}}</td><td>{{.Count}}</td><td>{{.Union}}</td><td>{{.Sum}}</td><td>{{.P95}}</td></tr>{{end}}
</tbody></table></body></html>`))
