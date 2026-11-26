// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"dragonflydb.io/profile/models"
	"dragonflydb.io/profile/pkg/otel"
	"dragonflydb.io/profile/pkg/workflow"
)

type nativeSummaryStore struct{ models.Repository }

func (nativeSummaryStore) Get(id string) (models.Record, error) {
	if id != "v16" {
		return models.Record{}, os.ErrNotExist
	}
	return models.Record{Detail: models.Detail{Summary: models.Summary{Diagnostics: map[string]models.Diagnostic{
		"current": {Diagnostic: workflow.Diagnostic{CaptureID: "capture", Native: true,
			Nodes: map[string]workflow.NodeDiagnostic{"leader": {Node: "11"}}}},
	}}}}, nil
}

func (nativeSummaryStore) Measurement(id, run string) (json.RawMessage, error) {
	return json.RawMessage(`{"measure_start_ns":1000000,"replication_end_ns":6000000}`), nil
}

func TestEventSummaryUsesNativeCapture(t *testing.T) {
	row := func(id, name string, end int64) map[string]any {
		return map[string]any{
			"id": id, "traceId": "stream", "name": name,
			"startTime": "2026-11-26T00:00:00Z", "endTime": "2026-11-26T00:00:01Z",
			"metadata": map[string]any{
				"schema": 3, "stream_id": "stream", "capture_id": "capture", "experiment_id": "v16",
				"start_ns": 1000000, "end_ns": end, "node_id": "11", "node_role": "Follower", "group_id": "10",
				"process_instance": "process", "expected_streams": 1, "expected_nodes": "11",
				"event_count": 2, "outcome": "Complete", "dropped": 0, "dropped_streams": 0,
			},
		}
	}
	manifest := row("manifest", "raft.capture", 11000000)
	langfuse := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rows := []any{manifest}
		if r.URL.Query().Get("name") != "raft.capture" {
			rows = append(rows, row("one", "custom.event", 11000000), row("two", "checkpoint", 1000000))
		}
		json.NewEncoder(w).Encode(map[string]any{"data": rows})
	}))
	defer langfuse.Close()
	env := filepath.Join(t.TempDir(), "langfuse.env")
	if err := os.WriteFile(env, []byte("LANGFUSE_PUBLIC_KEY=test\nLANGFUSE_SECRET_KEY=test\n"), 0600); err != nil {
		t.Fatal(err)
	}
	api := http.NewServeMux()
	nativeTraceRoutes(api, nativeSummaryStore{}, otel.NativeConfig{Langfuse: otel.LangfuseConfig{URL: langfuse.URL, EnvFile: env}})
	for _, tc := range []struct {
		path string
		code int
	}{
		{"/api/v1/experiments/v16/event-summary?capture=capture", 200},
		{"/api/v1/experiments/v16/event-summary?capture=unrelated", 422},
		{"/api/v1/experiments/v16/event-summary", 400},
		{"/api/v1/experiments/absent/event-summary?capture=capture", 404},
	} {
		response := httptest.NewRecorder()
		api.ServeHTTP(response, httptest.NewRequest(http.MethodGet, tc.path, nil))
		if response.Code != tc.code {
			t.Fatalf("%s: %d %s", tc.path, response.Code, response.Body)
		}
		if tc.code == 200 {
			var report otel.NativeSummary
			if err := json.Unmarshal(response.Body.Bytes(), &report); err != nil || !report.Complete || report.Total != 2 || len(report.Stages) != 2 || len(report.EventNames) != 2 || report.Scope != "measurement" {
				t.Fatalf("summary: %+v %v", report, err)
			}
			for _, stage := range report.Stages {
				if stage.Count != 1 || (stage.Name == "custom.event" && (stage.Union != 5 || stage.P95 != 5)) {
					t.Fatalf("incorrect event statistics: %+v", stage)
				}
			}
			if len(report.RoleStages) != 2 || report.RoleStages[1].Role != "leader" || report.Stages[1].Role != "follower" {
				t.Fatalf("expected role totals using the benchmark role: %+v", report)
			}
		}
	}
	const ranked = "/api/v1/experiments/v16/events?capture=capture&limit=1"
	page := httptest.NewRecorder()
	api.ServeHTTP(page, httptest.NewRequest(http.MethodGet, ranked+"&offset=1", nil))
	var report otel.NativeReport
	if err := json.Unmarshal(page.Body.Bytes(), &report); err != nil ||
		page.Code != http.StatusOK || report.Total != 2 || report.Offset != 1 ||
		len(report.Rows) != 1 || report.Rows[0].Name != "checkpoint" {
		t.Fatalf("expected second page: %d %+v %v", page.Code, report, err)
	}
	invalid := httptest.NewRecorder()
	api.ServeHTTP(invalid, httptest.NewRequest(http.MethodGet, ranked+"&offset=-1", nil))
	if invalid.Code != http.StatusBadRequest {
		t.Fatalf("negative offset should fail: %d %s", invalid.Code, invalid.Body)
	}
}
