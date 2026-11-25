package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"testing/fstest"

	"dragonflydb.io/profile/models"
	"dragonflydb.io/profile/pkg/httpapi"
	"dragonflydb.io/profile/pkg/langfuse"
)

func TestNativeCLIAndWebUseTheSameStoredTraceQuery(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, "v15"), 0700); err != nil {
		t.Fatal(err)
	}
	record := models.Record{Schema: models.Schema, Detail: models.Detail{Summary: models.Summary{Experiment: models.Label{ID: "v15"}}}}
	raw, _ := json.Marshal(record)
	if err := os.WriteFile(filepath.Join(root, "v15", "experiment.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	credentials := filepath.Join(root, "env")
	if err := os.WriteFile(credentials, []byte("LANGFUSE_PUBLIC_KEY=test\nLANGFUSE_SECRET_KEY=test\n"), 0600); err != nil {
		t.Fatal(err)
	}
	observation := func(id, name string, metadata map[string]any) map[string]any {
		for k, v := range map[string]any{"schema": 3, "stream_id": "stream", "capture_id": "capture", "event_id": id, "request_id": id, "start_ns": "9007199254740993", "end_ns": "9007199272650000", "process_instance": "process", "node_id": 11, "group_id": 10, "experiment_id": "v15"} {
			metadata[k] = v
		}
		return map[string]any{"id": id, "traceId": "stream", "name": name, "metadata": metadata}
	}
	request := observation("1", "raft.fetch", map[string]any{"outcome": "Data", "value.raft.fetch.return_wait": "17.909001", "state.raft.fetch.return_wait": "Observed", "value.raft.await_io": "2.3", "state.raft.await_io": "Observed", "value.raft.fetch.appended_bytes": "4096", "state.raft.fetch.appended_bytes": "Observed"})
	manifest := observation("root", "raft.capture", map[string]any{"outcome": "Complete", "event_count": 2, "dropped": 0, "expected_streams": 1, "dropped_streams": 0, "expected_nodes": "11", "unit.raft.fetch.return_wait": "ms", "unit.raft.await_io": "ms", "unit.raft.fetch.appended_bytes": "bytes"})
	other := observation("2", "custom.event", map[string]any{"outcome": "Observed"})
	other["metadata"].(map[string]any)["end_ns"] = "9007199274740993"
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.HasPrefix(r.URL.Path, "/api/public/v2/observations") {
			t.Errorf("unexpected upstream path %s", r.URL.Path)
		}
		json.NewEncoder(w).Encode(map[string]any{"data": []any{manifest, request, other}, "meta": map[string]any{}})
	}))
	defer upstream.Close()
	platform := httptest.NewServer(httpapi.Handler(models.Store{Root: root}, fstest.MapFS{}, nil, langfuse.Config{URL: upstream.URL, EnvFile: credentials}))
	defer platform.Close()
	for _, tc := range []struct {
		command, path, query, name, duration, value string
		args                                        []string
	}{
		{"fetch-delay", "requests", "min=17.909001", "raft.fetch", "17.909007", "17.909 ms", []string{"-min-return-wait-ms", "17.909001"}},
		{"fetch-delay", "requests", "attribute=raft.await_io&min=2.3", "raft.fetch", "17.909007", "2.300 ms", []string{"-min-io-wait-ms", "2.3"}},
		{"fetch-delay", "requests", "attribute=raft.fetch.appended_bytes&min=4096", "raft.fetch", "17.909007", "4096 bytes", []string{"-min-appended-bytes", "4096"}},
		{"slow-path", "events", "", "custom.event", "20.000000", "", nil},
		{"slow-path", "events", "min=20", "custom.event", "20.000000", "", []string{"-min-duration-ms", "20"}},
		{"slow-path", "events", "name=custom.event", "custom.event", "20.000000", "", []string{"-name", "custom.event"}},
	} {
		t.Run(tc.command+tc.query, func(t *testing.T) {
			saved := os.Stdout
			read, write, err := os.Pipe()
			if err != nil {
				t.Fatal(err)
			}
			os.Stdout = write
			args := append([]string{tc.command, "-url", platform.URL, "-id", "v15", "-capture", "capture", "-json"}, tc.args...)
			cliErr := cli(context.Background(), args)
			os.Stdout = saved
			write.Close()
			printed, _ := io.ReadAll(read)
			read.Close()
			if cliErr != nil {
				t.Fatal(cliErr)
			}
			response, err := http.Get(platform.URL + "/api/v1/experiments/v15/" + tc.path + "?capture=capture&" + tc.query)
			if err != nil {
				t.Fatal(err)
			}
			defer response.Body.Close()
			var fromCLI, fromWeb langfuse.NativeReport
			if err = json.Unmarshal(printed, &fromCLI); err != nil {
				t.Fatal(err)
			}
			if err = json.NewDecoder(response.Body).Decode(&fromWeb); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(fromCLI, fromWeb) || !fromWeb.Complete || len(fromWeb.Rows) == 0 || fromWeb.Rows[0].Name != tc.name || fromWeb.Rows[0].Duration != tc.duration || fromWeb.Rows[0].Start != "9007199254740993" {
				t.Fatalf("CLI/Web diverged: cli=%s web=%+v", printed, fromWeb)
			}
			if tc.command == "fetch-delay" {
				if text := nativeValueText(fromWeb.Rows[0], fromWeb.Attribute, fromWeb.Units); text != tc.value {
					t.Fatalf("incorrect display unit: %s", text)
				}
			} else if fromWeb.Sort != "duration" || fromWeb.Attribute != "" {
				t.Fatalf("event query requires an attribute: %+v", fromWeb)
			}
		})
	}
	for _, args := range [][]string{
		{"fetch-delay", "-min", "10"},
		{"slow-path", "-min-return-wait-ms", "10"},
		{"fetch-delay", "-min-return-wait-ms", "10", "-attribute", "raft.fetch.appended_bytes"},
		{"fetch-delay", "-min-return-wait-ms", "10", "-min-io-wait-ms", "5"},
	} {
		if err := cli(context.Background(), append(args, "-id", "v15")); err == nil {
			t.Fatalf("ambiguous threshold accepted: %v", args)
		}
	}
	for _, query := range []string{"capture=capture&partial=bad", "capture=capture&limit=0", "capture=capture&min=-1", "capture=capture&min=NaN", "capture=capture&min=1/2", "capture=capture&min_ns=1000", "capture=capture&start_ns=2&end_ns=1", ""} {
		r, err := http.Get(platform.URL + "/api/v1/experiments/v15/requests?" + query)
		if err != nil {
			t.Fatal(err)
		}
		r.Body.Close()
		if r.StatusCode != http.StatusBadRequest {
			t.Fatalf("invalid query accepted: %s status=%d", query, r.StatusCode)
		}
	}

	response, err := http.Get(platform.URL + "/api/v1/experiments/v15/events?capture=capture&attribute=raft.set.admission.wait")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusBadRequest {
		t.Fatal("event query accepted request attribute ranking")
	}
}
