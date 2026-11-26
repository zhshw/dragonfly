// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package otel

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
)

func TestNativeReaderClickHouse(t *testing.T) {
	for _, tc := range []struct {
		name          string
		apiCalls      int32
		databaseCalls int32
	}{
		{name: "complete", apiCalls: 1, databaseCalls: 1},
		{name: "incomplete", apiCalls: 2, databaseCalls: 1},
		{name: "unavailable", apiCalls: 2, databaseCalls: 1},
		{name: "mixed projects", apiCalls: 2, databaseCalls: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			root := nativeManifest(1)
			e := nativeEvent("1", "raft.fetch", "1", 9007199254740993)
			tickets := "1234,5678"
			if tc.name == "complete" {
				tickets = strings.Repeat("1234,", 50) + "5678"
			}
			e.Fields["ticket_ids"] = tickets
			e.Fields["experiment_id"], root.Fields["experiment_id"] = "v15", "v15"
			e.Values["raft.fetch.return_wait"] = "17.909001"
			e.Values["raft.fetch.rpc"] = "2.751000"
			manifest, row := nativeTestObservation(root), nativeTestObservation(e)
			bounds := map[string]any{
				"id": root.SpanID, "traceId": root.TraceID, "projectId": "p-from-langfuse",
				"startTime": manifest["startTime"], "endTime": manifest["endTime"],
			}
			var apiCalls, databaseCalls atomic.Int32
			api := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				apiCalls.Add(1)
				rows := []any{manifest, row}
				if r.URL.Query().Get("name") == "raft.capture" {
					rows = []any{bounds}
					if tc.name == "mixed projects" {
						rows = append(rows, map[string]any{
							"id": "other", "traceId": "other", "projectId": "other-project",
							"startTime": bounds["startTime"], "endTime": bounds["endTime"],
						})
					}
				}
				json.NewEncoder(w).Encode(map[string]any{"data": rows})
			}))
			db := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				databaseCalls.Add(1)
				q := r.URL.Query()
				user, password, ok := r.BasicAuth()
				query, err := io.ReadAll(r.Body)
				if err != nil || r.Method != http.MethodPost || string(query) != nativeClickHouseQuery ||
					q.Get("param_project") != "p-from-langfuse" || q.Get("param_experiment") != "v15" ||
					q.Get("param_capture") != "capture" || q.Get("readonly") != "1" ||
					q.Get("param_from") != "2026-11-25T18:48:53.941Z" || q.Get("param_to") != "2026-11-25T18:49:05.933Z" ||
					!ok || user != "profile" || password != "test-password" {
					t.Error("ClickHouse query did not use bound parameters, read-only mode and its own credentials")
				}
				if tc.name == "unavailable" {
					w.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				json.NewEncoder(w).Encode(manifest)
				if tc.name != "incomplete" {
					json.NewEncoder(w).Encode(row)
				}
			}))
			defer db.Close()
			credentials := filepath.Join(t.TempDir(), "clickhouse.env")
			if err := os.WriteFile(credentials, []byte("PROFILE_CLICKHOUSE_PASSWORD=test-password\n"), 0600); err != nil {
				t.Fatal(err)
			}
			reader := NewNativeReader(NativeConfig{
				Langfuse: nativeTestConfig(t, api), ClickHouse: ClickHouseConfig{URL: db.URL, User: "profile", EnvFile: credentials},
			})
			for i := 0; i < 2; i++ {
				report, err := reader.Query(context.Background(), "v15", "capture", NativeQuery{Limit: 5})
				if err != nil || !report.Complete || report.Total != 1 ||
					report.Rows[0].Values["raft.fetch.return_wait"] != "17.909001" ||
					report.Rows[0].Start != "9007199254740993" {
					t.Fatalf("wrong native capture report: %+v %v", report, err)
				}
				if tc.name == "complete" && (report.Rows[0].Fields["ticket_ids"] != tickets || report.Rows[0].Values["raft.fetch.rpc"] != "2.751") {
					t.Fatal("ClickHouse lost long metadata or changed the value")
				}
			}
			if apiCalls.Load() != tc.apiCalls || databaseCalls.Load() != tc.databaseCalls {
				t.Fatalf("unexpected query counts after two report requests: API=%d ClickHouse=%d", apiCalls.Load(), databaseCalls.Load())
			}
		})
	}
}
