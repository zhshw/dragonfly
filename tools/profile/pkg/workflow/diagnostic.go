// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package workflow

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"

	"dragonflydb.io/profile/pkg/otel"
)

type NodeDiagnostic struct {
	Node              string    `json:"node"`
	CPU               float64   `json:"cpu_ms"`
	CPUPerMiB         float64   `json:"cpu_ms_per_mib"`
	PeakRSS           float64   `json:"peak_rss_mib,omitempty"`
	FsyncCount        int       `json:"fsync_count"`
	FsyncAwait        float64   `json:"fsync_await_ms"`
	WorkerQueue       float64   `json:"worker_queue_ms"`
	StartsDuringFsync int       `json:"starts_during_fsync"`
	Hotspots          []Hotspot `json:"hotspots"`
}

type Diagnostic struct {
	Native     bool                      `json:"native,omitempty"`
	CaptureID  string                    `json:"capture_id,omitempty"`
	ProbeSHA   string                    `json:"probe_sha256"`
	Frequency  float64                   `json:"frequency_hz"`
	SpanCount  int                       `json:"span_count"`
	FsyncUnion float64                   `json:"fsync_union_ms"`
	Nodes      map[string]NodeDiagnostic `json:"nodes"`
}

// ReadDiagnostic reads the full trace once and returns compact statistics with the run result.
func ReadDiagnostic(dir string, build BuildInfo, harness string) (Diagnostic, []otel.Stage, BenchRow, error) {
	if report, err := ReadNativeDiagnostic(dir); err == nil {
		d, row, err := readNativeDiagnostic(dir, report, build, harness)
		return d, nil, row, err
	} else if !os.IsNotExist(err) {
		return Diagnostic{}, nil, BenchRow{}, err
	}
	var d Diagnostic
	var report otel.Report
	var empty BenchRow
	if err := readJSON(filepath.Join(dir, "summary.json"), &report); err != nil {
		return d, nil, empty, err
	}
	m, row, err := decodeReport(report)
	if err != nil {
		return d, nil, row, err
	}
	mode := "raft.memory"
	if build.Mode == "yes" {
		mode = "cluster"
	}
	if m.Mode != mode || row.Mode != build.Mode || m.HarnessSHA != harness || !validSHA(m.BinarySHA) {
		return d, nil, row, errors.New("incompatible diagnostic build mode or benchmark harness")
	}
	if report.Dropped == nil || *report.Dropped != 0 || m.Frequency <= 0 || len(m.Nodes) != 2 {
		return d, nil, row, errors.New("need complete trace and two profiled nodes")
	}
	d = Diagnostic{ProbeSHA: m.BinarySHA, Frequency: m.Frequency, SpanCount: report.Count, Nodes: map[string]NodeDiagnostic{}}
	var events []otel.Span
	if err := readJSON(filepath.Join(dir, "spans.json"), &events); err != nil {
		return d, nil, row, err
	}
	if len(events) != report.Count || len(events) == 0 {
		return d, nil, row, errors.New("trace event count differs from report")
	}
	byNode := map[string]map[string][]otel.Interval{}
	for _, span := range events {
		role := m.Nodes[span.Node].Role
		if span.Node == "client" {
			role = "client"
		}
		if role == "" || span.Role != role || span.Stage == "" || span.Start < row.Start || span.End > row.End || span.End < span.Start {
			return d, nil, row, errors.New("trace event is outside the measurement window or has an invalid node")
		}
		if byNode[span.Node] == nil {
			byNode[span.Node] = map[string][]otel.Interval{}
		}
		byNode[span.Node][span.Stage] = append(byNode[span.Node][span.Stage], otel.Interval{Begin: span.Start, End: span.End})
	}
	var allFsync []otel.Interval
	for node, info := range m.Nodes {
		if n, err := strconv.Atoi(node); err != nil || n < 1 || strconv.Itoa(n) != node {
			return d, nil, row, fmt.Errorf("invalid node id %q", node)
		}
		if _, dup := d.Nodes[info.Role]; dup || (info.Role != "leader" && info.Role != "follower") {
			return d, nil, row, errors.New("invalid node profile metadata")
		}
		data, err := os.ReadFile(filepath.Join(dir, info.Role+"-node-"+node+".folded"))
		if err != nil {
			return d, nil, row, err
		}
		top, samples, err := CPUHotspots(string(data), false, 5)
		if err != nil {
			return d, nil, row, err
		}
		readings := byNode[node]
		fsync := readings["raft.folly.fsync"]
		allFsync = append(allFsync, fsync...)
		work := "raft.set.encode"
		if info.Role == "follower" {
			work = "raft.log.append"
		}
		active := otel.Merged(fsync)
		starts := 0
		for _, v := range readings[work] {
			i := sort.Search(len(active), func(i int) bool { return active[i].End > v.Begin })
			if i < len(active) && active[i].Begin <= v.Begin {
				starts++
			}
		}
		cpu := float64(samples) * 1000 / m.Frequency
		d.Nodes[info.Role] = NodeDiagnostic{
			Node: node, CPU: cpu, CPUPerMiB: cpu / float64(row.Payload), PeakRSS: info.After.Peak / 1024,
			FsyncCount: len(fsync), FsyncAwait: otel.IntersectionMS(fsync, readings["raft.await_io"]),
			WorkerQueue: otel.UnionMS(readings["raft.worker.queue"]), StartsDuringFsync: starts, Hotspots: top,
		}
	}
	d.FsyncUnion = otel.UnionMS(allFsync)
	return d, otel.Stages(events), row, nil
}
