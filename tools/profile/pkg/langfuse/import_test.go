// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package langfuse

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestImportVerifiesEveryObservationAndSkipsExistingIDs(t *testing.T) {
	run := t.TempDir()
	reportDir := filepath.Join(run, "report")
	if err := os.Mkdir(reportDir, 0700); err != nil {
		t.Fatal(err)
	}
	write := func(name string, data any) {
		t.Helper()
		encoded, err := json.Marshal(data)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(reportDir, name), encoded, 0600); err != nil {
			t.Fatal(err)
		}
	}
	dropped := 0
	write("summary.json", Report{
		Metadata: json.RawMessage(`{"mode":"raft.memory","wall_offset_ns":1700000000000000000,"nodes":{"11":{"role":"leader"}}}`),
		Result:   json.RawMessage(`{"measure_start_ns":1,"replication_end_ns":30,"keys":4,"clients":1,"pipeline_depth":64,"payload_mib":1}`),
		Count:    2, Dropped: &dropped,
	})
	write("spans.json", []Span{
		{Node: "11", Role: "leader", Stage: "raft.set", Start: 2, End: 20},
		{Node: "client", Role: "client", Stage: "client.final_wait", Start: 21, End: 30},
	})
	env := filepath.Join(run, "keys.env")
	if err := os.WriteFile(env, []byte("LANGFUSE_PUBLIC_KEY=test-public\nLANGFUSE_SECRET_KEY=test-secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	var uploaded []otlpSpan
	var calls int
	var mu sync.Mutex
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user, key, ok := r.BasicAuth()
		if !ok || user != "test-public" || key != "test-secret" || r.Header.Get("x-langfuse-ingestion-version") != "4" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/api/public/otel/v1/traces":
			var request struct {
				ResourceSpans []struct {
					ScopeSpans []struct{ Spans []otlpSpan }
				}
			}
			if err := json.NewDecoder(r.Body).Decode(&request); err != nil || len(request.ResourceSpans) != 1 || len(request.ResourceSpans[0].ScopeSpans) != 1 {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			calls++
			uploaded = append(uploaded, request.ResourceSpans[0].ScopeSpans[0].Spans...)
			_, _ = w.Write([]byte(`{"partialSuccess":{"rejectedSpans":"0"}}`))
		case "/api/public/v2/observations":
			data := []map[string]string{}
			start, end, cursor := 0, len(uploaded), ""
			if end > 0 && r.URL.Query().Get("cursor") == "" {
				end, cursor = 1, "next"
			} else if r.URL.Query().Get("cursor") == "next" {
				start = 1
			}
			for _, span := range uploaded[start:end] {
				if span.Trace == r.URL.Query().Get("traceId") {
					data = append(data, map[string]string{"id": span.ID})
				}
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"data": data, "meta": map[string]string{"cursor": cursor}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	for i := 0; i < 2; i++ {
		receipt, err := Import(context.Background(), run, Config{URL: srv.URL, EnvFile: env})
		if err != nil {
			t.Fatal(err)
		}
		if receipt.Verified != 5 || receipt.FullSpans != 2 || len(receipt.TraceID) != 32 {
			t.Fatalf("unexpected receipt: %+v", receipt)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("existing observations were uploaded again (%d requests)", calls)
	}
}
