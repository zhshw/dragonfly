// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package otel

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// RecordedEvent is an operation observed by its execution owner, before any export.
type RecordedEvent struct {
	Name       string
	Start, End int64
	Fields     map[string]string
}

type NativeStream struct {
	Capture                    NativeCapture
	Node, Role, Group, Process string
	WallOffset                 int64
	ExpectedNodes              []string
}

// ExportNativeStream publishes in-memory observations directly as OTEL spans. The final
// manifest makes partial delivery detectable by the same reader used for C++ captures.
func ExportNativeStream(ctx context.Context, config LangfuseConfig, stream NativeStream, events []RecordedEvent) error {
	if len(events) == 0 || stream.Capture.ID == "" || stream.Process == "" || stream.Node == "" || stream.WallOffset <= 0 {
		return errors.New("invalid native stream")
	}
	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		return err
	}
	traceID := hex.EncodeToString(nonce[:])
	const rootID = "ffffffffffffffff"
	begin, end := events[0].Start, events[0].End
	spans := make([]otlpSpan, 0, len(events))
	encode := func(event RecordedEvent, id uint64, parent string) otlpSpan {
		fields := map[string]string{
			"schema": "3", "stream_id": traceID, "capture_id": stream.Capture.ID,
			"event_id": strconv.FormatUint(id, 10), "request_id": "0",
			"start_ns": strconv.FormatInt(event.Start, 10), "end_ns": strconv.FormatInt(event.End, 10),
			"node_id": stream.Node, "node_role": stream.Role, "group_id": stream.Group,
			"process_instance": stream.Process, "outcome": "Observed",
			"experiment_id": stream.Capture.Experiment, "binary_sha": stream.Capture.BinarySHA,
			"release": stream.Capture.Release, "workload": stream.Capture.Workload,
		}
		for key, value := range event.Fields {
			fields[key] = value
		}
		attrs := map[string]any{"langfuse.environment": "raft-diagnostic", "langfuse.trace.name": stream.Capture.ID, "langfuse.release": stream.Capture.Release}
		for key, value := range fields {
			attrs["langfuse.observation.metadata."+key] = value
		}
		return otlpSpan{Trace: traceID, ID: fmt.Sprintf("%016x", id), Parent: parent, Name: event.Name, Kind: 1,
			Start: strconv.FormatInt(event.Start+stream.WallOffset, 10), End: strconv.FormatInt(event.End+stream.WallOffset, 10),
			Attrs: attributes(attrs), Status: map[string]int{"code": 0}}
	}
	for i, event := range events {
		if event.Name == "" || event.Start < 0 || event.End < event.Start {
			return errors.New("invalid recorded event")
		}
		begin, end = min(begin, event.Start), max(end, event.End)
		spans = append(spans, encode(event, uint64(i+1), rootID))
	}
	c, err := openLangfuse(config)
	if err != nil {
		return err
	}
	for at := 0; at < len(spans); at += 256 {
		if err := c.sendTrace(ctx, spans[at:min(at+256, len(spans))], "dragonfly.benchmark", "dragonfly.benchmark", "3"); err != nil {
			return err
		}
	}
	manifest := encode(RecordedEvent{Name: "raft.capture", Start: begin, End: end, Fields: map[string]string{
		"event_count": strconv.Itoa(len(events)), "expected_streams": "1", "expected_nodes": strings.Join(stream.ExpectedNodes, ","),
		"dropped": "0", "dropped_streams": "0", "outcome": "Complete",
	}}, ^uint64(0), "")
	return c.sendTrace(ctx, []otlpSpan{manifest}, "dragonfly.benchmark", "dragonfly.benchmark", "3")
}
