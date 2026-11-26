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
	RoleStages []Stage  `json:"role_stages"`
	Scope      string   `json:"scope"`
}

// Summary uses the same validated capture and cache as Slow Path and Fetch delay queries.
// It aggregates recorded span boundaries without deriving business wait attributes.
func (r *NativeReader) Summary(ctx context.Context, experiment, capture string) (NativeSummary, error) {
	return r.SummaryWindow(ctx, experiment, capture, Window{}, nil)
}

// SummaryWindow clips intersecting spans to the same measurement bounds as archived traces.
func (r *NativeReader) SummaryWindow(ctx context.Context, experiment, capture string, window Window, nodeRoles map[string]string) (NativeSummary, error) {
	data, err := r.load(ctx, experiment, capture)
	if err != nil {
		return NativeSummary{}, err
	}
	return data.eventSummary(window, nodeRoles)
}

func (data *nativeCaptureData) eventSummary(window Window, nodeRoles map[string]string) (NativeSummary, error) {
	r := NativeSummary{Capture: data.summary.Capture, Complete: data.summary.Complete,
		Issues: data.summary.Issues, Scope: "capture"}
	if window.Start != 0 || window.End != 0 {
		if window.Start < 0 || window.End <= window.Start {
			return r, fmt.Errorf("invalid measurement window")
		}
		r.Scope = "measurement"
	}
	names := map[string]bool{}
	type group struct {
		stage     Stage
		clocks    map[string][]Interval
		durations []int64
	}
	groups := map[string]*group{}
	roleGroups := map[string]*group{}
	add := func(groups map[string]*group, key string, stage Stage, clock string, interval Interval) {
		g := groups[key]
		if g == nil {
			g = &group{stage: stage, clocks: map[string][]Interval{}}
			groups[key] = g
		}
		g.clocks[clock] = append(g.clocks[clock], interval)
		duration := interval.End - interval.Begin
		g.durations = append(g.durations, duration)
		g.stage.Sum += float64(duration) / 1e6
	}
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
		node := e.Fields["node_id"]

		// Streams from one process share a clock. Never merge intervals across processes.
		clock := e.Fields["process_instance"]
		if clock == "" {
			clock = e.TraceID
		}
		clock = node + "\x00" + clock
		interval := Interval{start, end}
		add(groups, node+"\x00"+role+"\x00"+e.Name, Stage{Node: node, Role: role, Name: e.Name}, clock, interval)

		// Match the benchmark data-group role across rounds, even if a node changes ID.
		comparisonRole := nodeRoles[node]
		if comparisonRole == "" {
			comparisonRole = role
		}
		add(roleGroups, comparisonRole+"\x00"+e.Name, Stage{Role: comparisonRole, Name: e.Name}, clock, interval)
		r.Total++
	}
	summarize := func(groups map[string]*group) []Stage {
		stages := make([]Stage, 0, len(groups))
		for _, key := range sortedKeys(groups) {
			g := groups[key]
			sort.Slice(g.durations, func(i, j int) bool { return g.durations[i] < g.durations[j] })
			s := g.stage
			s.Count = len(g.durations)
			s.P50 = float64(g.durations[s.Count/2]) / 1e6
			s.P95 = float64(g.durations[int(math.Ceil(float64(s.Count)*.95))-1]) / 1e6
			s.Max = float64(g.durations[s.Count-1]) / 1e6
			for _, clock := range sortedKeys(g.clocks) {
				s.Union += UnionMS(g.clocks[clock])
			}
			stages = append(stages, s)
		}
		return stages
	}
	r.Stages = summarize(groups)
	r.RoleStages = summarize(roleGroups)
	r.EventNames = sortedKeys(names)
	return r, nil
}
