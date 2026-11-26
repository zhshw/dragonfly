// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package otel

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestNativeClientExportKeepsBoundsAndPublishesManifestLast(t *testing.T) {
	var received []NativeEvent
	var batches [][]otlpSpan
	reject := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload struct {
			ResourceSpans []struct {
				ScopeSpans []struct{ Spans []otlpSpan }
			}
		}
		user, password, _ := r.BasicAuth()
		if r.URL.Path != "/api/public/otel/v1/traces" || user != "public" || password != "secret" {
			t.Error("wrong OTEL endpoint or authentication")
		}
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
			w.WriteHeader(400)
			return
		}
		batch := payload.ResourceSpans[0].ScopeSpans[0].Spans
		batches = append(batches, batch)
		for _, span := range batch {
			fields := map[string]any{}
			for _, attr := range span.Attrs {
				if key, ok := strings.CutPrefix(attr.Key, "langfuse.observation.metadata."); ok {
					fields[key] = attr.Value.Text
				}
			}
			e, err := decodeNative(nativeObservation{ID: span.ID, TraceID: span.Trace, Name: span.Name, Metadata: fields})
			if err != nil {
				t.Error(err)
			}
			start, _ := strconv.ParseInt(e.Start, 10, 64)
			if span.Start != strconv.FormatInt(start+1700000000000000000, 10) {
				t.Error("wall clock conversion changed original boundaries")
			}
			received = append(received, e)
		}
		if reject {
			w.Write([]byte(`{"partialSuccess":{"rejectedSpans":"1"}}`))
		} else {
			w.Write([]byte(`{}`))
		}
	}))
	defer server.Close()
	file := filepath.Join(t.TempDir(), "credentials")
	if err := os.WriteFile(file, []byte("LANGFUSE_PUBLIC_KEY=public\nLANGFUSE_SECRET_KEY=secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	config := LangfuseConfig{URL: server.URL, EnvFile: file}
	stream := NativeStream{Capture: NativeCapture{ID: "capture", Experiment: "v16", Client: true}, Node: "client",
		Role: "Client", Group: "client", Process: "client-process", WallOffset: 1700000000000000000, ExpectedNodes: []string{"client"}}
	events := []RecordedEvent{
		{Name: "client.pipeline", Start: 1000000, End: 11000000, Fields: map[string]string{"writes": "64"}},
		{Name: "client.final_wait", Start: 11000000, End: 14000000},
	}
	if err := ExportNativeStream(context.Background(), config, stream, events); err != nil {
		t.Fatal(err)
	}
	if len(batches) != 2 || len(batches[0]) != 2 || len(batches[1]) != 1 || batches[1][0].Name != "raft.capture" || batches[1][0].Parent != "" {
		t.Fatalf("manifest order: %+v", batches)
	}
	report, err := RankNative("capture", received, NativeQuery{Limit: 5})
	if err != nil || !report.Complete || report.Total != 2 || report.Rows[0].Duration != "10.000000" || report.Rows[0].Fields["writes"] != "64" || report.Rows[1].Duration != "3.000000" {
		t.Fatalf("client query differs from observation: %+v %v", report, err)
	}
	missing, err := RankNative("capture", received[:2], NativeQuery{Limit: 5})
	if err != nil || missing.Complete {
		t.Fatalf("partial publication accepted: %+v %v", missing, err)
	}
	reject, batches = true, nil
	if err := ExportNativeStream(context.Background(), config, stream, events); err == nil || len(batches) != 1 {
		t.Fatalf("rejected events must not publish a manifest: %v %d", err, len(batches))
	}
}
