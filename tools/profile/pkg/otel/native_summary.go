// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package otel

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// NativeSummary counts recorded events, including zero-duration points, excluding manifests.
// Stages retains the archive response shape for comparisons with older captures.
type NativeSummary struct {
	Capture    string   `json:"capture_id"`
	Complete   bool     `json:"complete"`
	Issues     []string `json:"issues"`
	Total      int      `json:"total"`
	EventNames []string `json:"event_names"`
	Stages     []Stage  `json:"stages"`
	Scope      string   `json:"scope"`
}

// Summary uses the same validated capture and cache as Slow Path and Fetch delay queries.
// It aggregates recorded span boundaries without deriving business wait attributes.
func (r *NativeReader) Summary(ctx context.Context, experiment, capture string) (NativeSummary, error) {
	return r.SummaryWindow(ctx, experiment, capture, Window{})
}

// SummaryWindow clips intersecting spans to the same measurement bounds as archived traces.
func (r *NativeReader) SummaryWindow(ctx context.Context, experiment, capture string, window Window) (NativeSummary, error) {
	data, err := r.load(ctx, experiment, capture)
	if err != nil {
		return NativeSummary{}, err
	}
	return data.eventSummary(window)
}

func (data *nativeCaptureData) eventSummary(windows ...Window) (NativeSummary, error) {
	r := NativeSummary{Capture: data.summary.Capture, Complete: data.summary.Complete,
		Issues: data.summary.Issues, Stages: []Stage{}, Scope: "capture"}
	var window Window
	if len(windows) > 0 {
		window = windows[0]
	}
	if window.Start != 0 || window.End != 0 {
		if window.Start < 0 || window.End <= window.Start {
			return r, fmt.Errorf("invalid measurement window")
		}
		r.Scope = "measurement"
	}
	names := map[string]bool{}
	type group struct {
		stage     Stage
		nodes     map[string]bool
		clocks    map[string][]Interval
		durations []int64
	}
	groups := map[string]*group{}
	for _, e := range data.events {
		if e.Name == "raft.capture" {
			continue
		}
		start, startErr := strconv.ParseInt(e.Start, 10, 64)
		end, endErr := strconv.ParseInt(e.End, 10, 64)
		if startErr != nil || endErr != nil || start < 0 || end < start {
			return r, fmt.Errorf("invalid event boundaries for %s", e.Name)
		}
		if r.Scope == "measurement" {
			if end < window.Start || start > window.End || (end > start && (end == window.Start || start == window.End)) {
				continue
			}
			start, end = max(start, window.Start), min(end, window.End)
		}
		names[e.Name] = true
		role := strings.ToLower(strings.Join(e.NodeRoles, " / "))
		if role == "" {
			role = "unknown"
		}
		key := e.Fields["node_id"] + "\x00" + role + "\x00" + e.Name
		g := groups[key]
		if g == nil {
			g = &group{stage: Stage{Role: role, Name: e.Name}, nodes: map[string]bool{}, clocks: map[string][]Interval{}}
			groups[key] = g
		}
		g.nodes[e.Fields["node_id"]] = true

		// Streams from one process share a clock. Never merge intervals across processes.
		clock := e.Fields["process_instance"]
		if clock == "" {
			clock = e.TraceID
		}
		clock = e.Fields["node_id"] + "\x00" + clock
		g.clocks[clock] = append(g.clocks[clock], Interval{start, end})
		g.durations = append(g.durations, end-start)
		g.stage.Sum += float64(end-start) / 1e6
		r.Total++
	}
	for _, key := range sortedKeys(groups) {
		g := groups[key]
		sort.Slice(g.durations, func(i, j int) bool { return g.durations[i] < g.durations[j] })
		s := g.stage
		s.Count, s.Node = len(g.durations), strings.Join(sortedKeys(g.nodes), ", ")
		s.P50 = float64(g.durations[s.Count/2]) / 1e6
		s.P95 = float64(g.durations[int(math.Ceil(float64(s.Count)*.95))-1]) / 1e6
		s.Max = float64(g.durations[s.Count-1]) / 1e6
		for _, clock := range sortedKeys(g.clocks) {
			s.Union += UnionMS(g.clocks[clock])
		}
		r.Stages = append(r.Stages, s)
	}
	r.EventNames = sortedKeys(names)
	return r, nil
}
