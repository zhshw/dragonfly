// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package langfuse

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type Span struct {
	Node  string `json:"node"`
	Role  string `json:"role"`
	TID   int64  `json:"tid"`
	Stage string `json:"stage"`
	Start int64  `json:"start_ns"`
	End   int64  `json:"end_ns"`
	Arg0  int64  `json:"arg0"`
	Arg1  int64  `json:"arg1"`
}

type Memory struct {
	RSS  float64 `json:"rss_kib"`
	Peak float64 `json:"peak_rss_kib"`
	Anon float64 `json:"anon_rss_kib"`
}

type NodeInfo struct {
	PID    int    `json:"pid"`
	Role   string `json:"role"`
	Before Memory `json:"memory_before"`
	After  Memory `json:"memory_after"`
}

type Metadata struct {
	Mode       string              `json:"mode"`
	Binary     string              `json:"binary"`
	BinarySHA  string              `json:"binary_sha256"`
	HarnessSHA string              `json:"harness_sha256"`
	Frequency  float64             `json:"frequency_hz"`
	WallOffset int64               `json:"wall_offset_ns"`
	Nodes      map[string]NodeInfo `json:"nodes"`
}

type Stage struct {
	Node  string  `json:"node"`
	Role  string  `json:"role"`
	Name  string  `json:"stage"`
	Count int     `json:"count"`
	Sum   float64 `json:"sum_ms"`
	Union float64 `json:"union_ms"`
	P50   float64 `json:"p50_ms"`
	P95   float64 `json:"p95_ms"`
	Max   float64 `json:"max_ms"`
}

// Keep the original metadata and workload result intact for existing report consumers.
type Report struct {
	Metadata json.RawMessage `json:"metadata"`
	Result   json.RawMessage `json:"result"`
	Count    int             `json:"span_count"`
	Dropped  *int            `json:"dropped"`
	Stages   []Stage         `json:"stages"`
}

type Window struct {
	Start    int64 `json:"measure_start_ns"`
	WriteEnd int64 `json:"measure_end_ns"`
	End      int64 `json:"replication_end_ns"`
}

func readJSON(path string, value any) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, value)
}

type Interval struct{ Begin, End int64 }

func Merged(in []Interval) []Interval {
	values := append([]Interval(nil), in...)
	sort.Slice(values, func(i, j int) bool { return values[i].Begin < values[j].Begin })
	var out []Interval
	for _, v := range values {
		if v.End <= v.Begin {
			continue
		}
		if len(out) == 0 || v.Begin > out[len(out)-1].End {
			out = append(out, v)
		} else if v.End > out[len(out)-1].End {
			out[len(out)-1].End = v.End
		}
	}
	return out
}

func UnionMS(in []Interval) float64 {
	var total int64
	for _, v := range Merged(in) {
		total += v.End - v.Begin
	}
	return float64(total) / 1e6
}

func IntersectionMS(a, b []Interval) float64 {
	a, b = Merged(a), Merged(b)
	var total int64
	for i, j := 0, 0; i < len(a) && j < len(b); {
		lo, hi := max(a[i].Begin, b[j].Begin), min(a[i].End, b[j].End)
		if hi > lo {
			total += hi - lo
		}
		if a[i].End < b[j].End {
			i++
		} else {
			j++
		}
	}
	return float64(total) / 1e6
}

func LoadSpans(run string, m Metadata, b Window) ([]Span, error) {
	var events []Span
	for _, node := range sortedKeys(m.Nodes) {
		info := m.Nodes[node]
		paths, err := filepath.Glob(filepath.Join(run, "spans", "node-"+node, fmt.Sprintf("%d-*.csv", info.PID)))
		if err != nil || len(paths) == 0 {
			return nil, fmt.Errorf("no trace CSV for node %s", node)
		}
		for _, path := range paths {
			batch, err := readSpanCSV(path, node, info.Role, b.Start, b.End)
			if err != nil {
				return nil, fmt.Errorf("%s: %w", path, err)
			}
			events = append(events, batch...)
		}
	}
	if len(events) == 0 {
		return nil, errors.New("no server spans in measured window")
	}
	var client []Span
	if err := readJSON(filepath.Join(run, "client.json"), &client); err != nil {
		return nil, err
	}
	for _, span := range client {
		if span.Start < b.Start || span.End > b.End || span.End < span.Start {
			return nil, errors.New("client span outside measurement window")
		}
		span.Node, span.Role, span.TID = "client", "client", 0
		events = append(events, span)
	}
	events = append(events, Span{Node: "client", Role: "client", Stage: "client.final_wait", Start: b.WriteEnd, End: b.End, Arg0: 1})
	sort.SliceStable(events, func(i, j int) bool { return events[i].Start < events[j].Start })
	return events, nil
}

func readSpanCSV(path, node, role string, begin, end int64) ([]Span, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	header, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	key, value, _ := strings.Cut(strings.TrimSpace(header), "=")
	dropped, err := strconv.Atoi(value)
	if err != nil || key != "# dropped" || dropped != 0 {
		return nil, errors.New("trace has missing/invalid dropped count or lost events")
	}
	stem := strings.TrimSuffix(filepath.Base(path), ".csv")
	_, tidText, _ := strings.Cut(stem, "-")
	tid, err := strconv.ParseInt(tidText, 10, 64)
	if err != nil {
		return nil, err
	}
	csvReader := csv.NewReader(r)
	cols, err := csvReader.Read()
	if err != nil || strings.Join(cols, ",") != "start_ns,end_ns,stage,arg0,arg1" {
		return nil, errors.New("unexpected trace CSV header")
	}
	var events []Span
	for {
		row, err := csvReader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		n := [4]int64{}
		for i := range n {
			col := []int{0, 1, 3, 4}[i]
			n[i], err = strconv.ParseInt(row[col], 10, 64)
			if err != nil {
				return nil, err
			}
		}
		if n[1] < n[0] || row[2] == "" {
			return nil, errors.New("invalid span interval/stage")
		}
		start, stop := max(n[0], begin), min(n[1], end)
		if stop <= start {
			continue
		}
		stage := row[2]
		if stage == "raft.peer.backoff" {
			stage += ".idle"
			if n[3] != 0 {
				stage = "raft.peer.backoff.after_pull"
			}
		}
		events = append(events, Span{node, role, tid, stage, start, stop, n[2], n[3]})
	}
	return events, nil
}

func Stages(events []Span) []Stage {
	groups := map[string][]Span{}
	for _, e := range events {
		key := e.Node + "\x00" + e.Role + "\x00" + e.Stage
		groups[key] = append(groups[key], e)
	}
	var rows []Stage
	round := func(v float64) float64 { return math.Round(v*1000) / 1000 }
	for _, key := range sortedKeys(groups) {
		group := groups[key]
		var durations []float64
		var spans []Interval
		var total int64
		for _, e := range group {
			durations = append(durations, float64(e.End-e.Start)/1e6)
			spans = append(spans, Interval{e.Start, e.End})
			total += e.End - e.Start
		}
		sort.Float64s(durations)
		n, first := len(group), group[0]
		rows = append(rows, Stage{first.Node, first.Role, first.Stage, n, round(float64(total) / 1e6), round(UnionMS(spans)), round(durations[n/2]), round(durations[int(math.Ceil(float64(n)*.95))-1]), round(durations[n-1])})
	}
	return rows
}

func sortedKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}
