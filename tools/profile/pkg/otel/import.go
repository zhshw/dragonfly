// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package otel

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const perStage = 50

type otlpAttribute struct {
	Key   string `json:"key"`
	Value struct {
		Text string `json:"stringValue"`
	} `json:"value"`
}

type otlpSpan struct {
	Trace  string          `json:"traceId"`
	ID     string          `json:"spanId"`
	Parent string          `json:"parentSpanId,omitempty"`
	Name   string          `json:"name"`
	Kind   int             `json:"kind"`
	Start  string          `json:"startTimeUnixNano"`
	End    string          `json:"endTimeUnixNano"`
	Attrs  []otlpAttribute `json:"attributes"`
	Status map[string]int  `json:"status"`
}

func attributes(values map[string]any) []otlpAttribute {
	attrs := make([]otlpAttribute, 0, len(values))
	for _, key := range sortedKeys(values) {
		value := values[key]
		text, ok := value.(string)
		if !ok {
			encoded, _ := json.Marshal(value)
			text = string(encoded)
		}
		attr := otlpAttribute{Key: key}
		attr.Value.Text = text
		attrs = append(attrs, attr)
	}
	return attrs
}

// Import sends missing OTLP observations and returns a receipt after every ID is readable.
func Import(ctx context.Context, run string, config LangfuseConfig) (Receipt, error) {
	return checkRun(ctx, run, config, true)
}

// Verify checks the observations for a captured run without submitting spans.
func Verify(ctx context.Context, run string, config LangfuseConfig) (Receipt, error) {
	return checkRun(ctx, run, config, false)
}

func checkRun(ctx context.Context, run string, config LangfuseConfig, upload bool) (Receipt, error) {
	var receipt Receipt
	if config.URL == "" {
		config.URL = "http://172.31.23.2:3007"
	}
	if config.EnvFile == "" {
		config.EnvFile = "/data/dragonfly-observability/config/langfuse.env"
	}
	if config.ArtifactsURL == "" {
		config.ArtifactsURL = "http://172.31.23.2:3137"
	}
	if config.Project == "" {
		config.Project = "dragonfly-performance"
	}
	c, err := openLangfuse(config)
	if err != nil {
		return receipt, err
	}
	run, err = filepath.Abs(run)
	if err != nil {
		return receipt, err
	}
	run, err = filepath.EvalSymlinks(run)
	if err != nil {
		return receipt, err
	}
	var report Report
	if err := readJSON(filepath.Join(run, "report", "summary.json"), &report); err != nil {
		return receipt, err
	}
	var events []Span
	if err := readJSON(filepath.Join(run, "report", "spans.json"), &events); err != nil {
		return receipt, err
	}
	var meta Metadata
	var measurement struct {
		Window
		Clients int `json:"clients"`
		Depth   int `json:"pipeline_depth"`
		Payload int `json:"payload_mib"`
		Keys    int `json:"keys"`
	}
	if err := errors.Join(json.Unmarshal(report.Metadata, &meta), json.Unmarshal(report.Result, &measurement)); err != nil {
		return receipt, err
	}
	if report.Dropped == nil || *report.Dropped != 0 || report.Count != len(events) || len(events) == 0 ||
		measurement.Start < 1 || measurement.End <= measurement.Start || measurement.Keys < 1 {
		return receipt, errors.New("incomplete Langfuse report inputs")
	}
	trace := uuid5([16]byte{0x6b, 0xa7, 0xb8, 0x11, 0x9d, 0xad, 0x11, 0xd1, 0x80, 0xb4, 0, 0xc0, 0x4f, 0xd4, 0x30, 0xc8},
		fmt.Sprintf("%s:%d", run, measurement.Start))
	traceID := hex.EncodeToString(trace[:])
	input := map[string]any{"clients": measurement.Clients, "pipeline": measurement.Depth,
		"payload_mib": measurement.Payload, "value_kib": measurement.Payload * 1024 / measurement.Keys}
	reportURL := strings.TrimRight(config.ArtifactsURL, "/") + "/" + filepath.Base(run) + "/"
	spans, err := selectSpans(trace, meta, measurement.Window, report, events, filepath.Base(run), reportURL, input)
	if err != nil {
		return receipt, err
	}
	begin, end := measurement.Start+meta.WallOffset, measurement.End+meta.WallOffset
	existing, err := c.observed(ctx, traceID, begin, end)
	if err != nil {
		return receipt, err
	}
	var missing []otlpSpan
	for _, span := range spans {
		if !existing[span.ID] {
			missing = append(missing, span)
		}
	}
	if len(missing) != 0 && !upload {
		return receipt, fmt.Errorf("%d Langfuse observations are not readable", len(missing))
	}
	for at := 0; at < len(missing); at += 250 {
		if err := c.send(ctx, missing[at:min(at+250, len(missing))]); err != nil {
			return receipt, err
		}
	}
	unchecked := len(missing)
	for attempt := 0; unchecked != 0 && attempt < 20; attempt++ {
		verified, err := c.observed(ctx, traceID, begin, end)
		if err != nil {
			return receipt, err
		}
		unchecked = 0
		for _, span := range spans {
			if !verified[span.ID] {
				unchecked++
			}
		}
		if unchecked != 0 && attempt+1 < 20 {
			select {
			case <-ctx.Done():
				return receipt, ctx.Err()
			case <-time.After(3 * time.Second):
			}
		}
	}
	if unchecked != 0 {
		return receipt, fmt.Errorf("%d Langfuse observations are not readable yet", unchecked)
	}
	return Receipt{TraceID: traceID, Verified: len(spans), FullSpans: len(events),
		URL: c.base + "/project/" + config.Project + "/traces/" + traceID}, nil
}

func selectSpans(trace [16]byte, meta Metadata, window Window, report Report, events []Span, name, reportURL string, input map[string]any) ([]otlpSpan, error) {
	traceID := hex.EncodeToString(trace[:])
	name = "replication/" + meta.Mode + "/" + name
	common := attributes(map[string]any{
		"langfuse.trace.name": name, "langfuse.session.id": "dragonfly-replication-observability",
		"langfuse.environment": "performance", "langfuse.observation.type": "span",
		"langfuse.trace.metadata.mode": meta.Mode, "langfuse.trace.metadata.binary_sha256": meta.BinarySHA,
		"langfuse.trace.metadata.flamegraphs": reportURL + "index.html",
	})
	spanID := func(identity string) string { id := uuid5(trace, identity); return hex.EncodeToString(id[:8]) }
	var spans []otlpSpan
	emit := func(id, parent, spanName string, start, end int64, extra map[string]any) {
		attrs := append(append([]otlpAttribute(nil), common...), attributes(extra)...)
		spans = append(spans, otlpSpan{Trace: traceID, ID: id, Parent: parent, Name: spanName, Kind: 1,
			Start: strconv.FormatInt(start+meta.WallOffset, 10), End: strconv.FormatInt(end+meta.WallOffset, 10),
			Attrs: attrs, Status: map[string]int{"code": 1}})
	}
	const metadata = "langfuse.observation.metadata."
	root := spanID("root")
	emit(root, "", name, window.Start, window.End, map[string]any{
		metadata + "full_span_count": len(events), metadata + "selection": fmt.Sprintf("Longest %d spans per node/stage", perStage),
		metadata + "full_timeline": reportURL + "timeline.json", metadata + "warning": "Durations include waits; overlapping spans are not additive",
		"langfuse.observation.input": input, "langfuse.observation.output": report.Result,
	})
	parents := map[string]string{}
	for _, node := range append(sortedKeys(meta.Nodes), "client") {
		id := spanID("node-" + node)
		parents[node] = id
		role := "client"
		if source, ok := meta.Nodes[node]; ok {
			role = source.Role
		}
		stages := []Stage{}
		for _, stage := range report.Stages {
			if stage.Node == node {
				stages = append(stages, stage)
			}
		}
		emit(id, root, role+" / node "+node, window.Start, window.End, map[string]any{metadata + "container": true, metadata + "stages": stages})
	}
	groups := map[string][]Span{}
	for _, event := range events {
		if parents[event.Node] == "" || event.Start < window.Start || event.End > window.End || event.End < event.Start {
			return nil, errors.New("span has an invalid source or time window")
		}
		key := event.Node + "\x00" + event.Stage
		groups[key] = append(groups[key], event)
	}
	for _, key := range sortedKeys(groups) {
		group := groups[key]
		sort.SliceStable(group, func(i, j int) bool { return group[i].End-group[i].Start > group[j].End-group[j].Start })
		selected := min(perStage, len(group))
		for i, event := range group[:selected] {
			id := spanID(fmt.Sprintf("%s/%s/%d", event.Node, event.Stage, i))
			emit(id, parents[event.Node], event.Stage, event.Start, event.End, map[string]any{
				metadata + "tid": event.TID, metadata + "arg0": event.Arg0, metadata + "arg1": event.Arg1,
				metadata + "stage_total": len(group), metadata + "stage_selected": selected,
			})
		}
	}
	return spans, nil
}

// The RFC 4122 version-5 identities match the existing exporter and allow retries by ID.
func uuid5(namespace [16]byte, name string) [16]byte {
	sum := sha1.Sum(append(namespace[:], name...))
	sum[6] = sum[6]&0x0f | 0x50
	sum[8] = sum[8]&0x3f | 0x80
	return [16]byte(sum[:16])
}
