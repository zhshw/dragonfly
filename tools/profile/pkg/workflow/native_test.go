// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package workflow

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"dragonflydb.io/profile/pkg/otel"
)

func TestNativePlan(t *testing.T) {
	config := Config{Baseline: "baseline", Diagnostics: true, IncludeCluster: true}
	want := []string{"baseline", "candidate", "compare", "observe-before", "report-before",
		"observe-after", "report-after", "observe-cluster", "report-cluster", "summarize"}
	if got := StepNames(config); !reflect.DeepEqual(got, want) {
		t.Fatalf("steps: %v", got)
	}
}

func TestCaptureUsesArchivableExperimentID(t *testing.T) {
	for _, tc := range []struct {
		id, variant string
		valid       bool
	}{
		{"smoke", "after", true}, {"smoke", "before", true},
		{"Smoke", "after", false}, {strings.Repeat("a", 64), "before", false},
	} {
		_, err := traceExperiment(Config{Experiment: tc.id}, tc.variant)
		if (err == nil) != tc.valid {
			t.Fatalf("%+v: %v", tc, err)
		}
	}
}

func TestExecuteIsolatesDiagnosticEnvironment(t *testing.T) {
	for _, key := range []string{"RAFT_TRACE_CAPTURE_ID", "OTEL_EXPORTER_OTLP_TRACES_HEADERS",
		"CPUPROFILE", "PROFILE_DIAGNOSTIC_DIR", "PROFILE_CPU_PROFILER", "LD_PRELOAD", "RAFT_OBSERVE_DIR"} {
		t.Setenv(key, "parent-value")
	}
	var output bytes.Buffer
	check := []string{"sh", "-c", "env"}
	if err := execute(context.Background(), t.TempDir(), nil, check, &output, io.Discard); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(output.String(), "parent-value") {
		t.Fatal("formal run inherited diagnostic environment")
	}
	output.Reset()
	if err := execute(context.Background(), t.TempDir(), []string{"RAFT_TRACE_CAPTURE_ID=explicit"}, check, &output, io.Discard); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "RAFT_TRACE_CAPTURE_ID=explicit") {
		t.Fatal("diagnostic run lost its capture")
	}
}

func nativeServer(t *testing.T, incomplete bool) (otel.LangfuseConfig, otel.NativeCapture, *atomic.Int32) {
	t.Helper()
	want := otel.NativeCapture{ID: "capture", Experiment: "smoke", Release: strings.Repeat("a", 64), BinarySHA: strings.Repeat("a", 64), Workload: nativeWorkload}
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != "GET" {
			t.Error("verification must not upload traces")
		}
		if user, pass, _ := r.BasicAuth(); user != "public" || pass != "secret" {
			t.Error("missing API credentials")
		}
		n := calls.Add(1)
		var rows []any
		for _, node := range []string{"11", "12"} {
			if node == "12" && (incomplete || n == 1) {
				continue
			}
			for _, name := range []string{"raft.capture", "raft.await_io"} {
				fields := map[string]string{
					"schema": "3", "stream_id": "stream-" + node, "capture_id": want.ID, "node_id": node,
					"group_id": "10", "process_instance": "process-" + node, "start_ns": "100", "end_ns": "200",
					"binary_sha": want.BinarySHA, "release": want.Release, "workload": want.Workload, "experiment_id": want.Experiment,
					"event_count": "1", "expected_streams": "1", "expected_nodes": "11,12",
					"outcome": "Complete", "dropped": "0", "dropped_streams": "0",
				}
				if filter := r.URL.Query().Get("name"); filter != "" && filter != name {
					continue
				}
				rows = append(rows, map[string]any{"id": name + "-" + node, "traceId": "stream-" + node, "name": name, "metadata": fields})
			}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": rows})
	}))
	t.Cleanup(server.Close)
	file := filepath.Join(t.TempDir(), "credentials")
	if err := os.WriteFile(file, []byte("LANGFUSE_PUBLIC_KEY=public\nLANGFUSE_SECRET_KEY=secret\n"), 0600); err != nil {
		t.Fatal(err)
	}
	return otel.LangfuseConfig{URL: server.URL, EnvFile: file}, want, &calls
}

func TestVerifyNativeCapture(t *testing.T) {
	config, want, calls := nativeServer(t, false)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := verifyNative(ctx, config, want, time.Millisecond)
	if err != nil || !got.Complete || got.Events != 2 || got.Streams != 2 || calls.Load() < 3 {
		t.Fatalf("got %+v: %v", got, err)
	}
	want.Client = true
	clientContext, clientCancel := context.WithTimeout(ctx, 30*time.Millisecond)
	defer clientCancel()
	if _, err := verifyNative(clientContext, config, want, time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("missing required client stream accepted")
	}
	want.Client = false
	want.BinarySHA = strings.Repeat("b", 64)
	if _, err := verifyNative(ctx, config, want, time.Millisecond); err == nil || !strings.Contains(err.Error(), "identity mismatch") {
		t.Fatalf("accepted another binary: %v", err)
	}
}

func TestIncompleteCaptureCannotPass(t *testing.T) {
	config, want, _ := nativeServer(t, true)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if _, err := verifyNative(ctx, config, want, time.Millisecond); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("missing node accepted: %v", err)
	}
}

func TestResumeVerificationDoesNotRepeatWorkload(t *testing.T) {
	repo := t.TempDir()
	for _, path := range []string{"src", "build"} {
		if err := os.Mkdir(filepath.Join(repo, path), 0755); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{"CMakeLists.txt", "build/CMakeCache.txt"} {
		if err := os.WriteFile(filepath.Join(repo, path), []byte("fixture"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	c := Config{Repo: repo, Release: filepath.Join(repo, "build"), Output: filepath.Join(repo, "run")}
	var workloads, verifications int
	steps := []Step{
		{"observe-after", func(r *Runner, rec *Record) ([]string, error) {
			workloads++
			return []string{rec.Log}, nil
		}},
		{"report-after", func(r *Runner, rec *Record) ([]string, error) {
			verifications++
			if verifications == 1 {
				return nil, errors.New("ingestion not ready")
			}
			return []string{rec.Log}, nil
		}},
	}
	if err := runWorkflow(context.Background(), c, steps, false, io.Discard); err == nil {
		t.Fatal("expected pending verification")
	}
	if err := runWorkflow(context.Background(), c, steps, true, io.Discard); err != nil {
		t.Fatal(err)
	}
	if workloads != 1 || verifications != 2 {
		t.Fatalf("reran workload: %d / %d", workloads, verifications)
	}
	state, err := ReadState(c.Output)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(state.Steps["observe-after"].Log, []byte("changed"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := runWorkflow(context.Background(), c, steps, true, io.Discard); err == nil {
		t.Fatal("accepted altered completed output")
	}
}

func TestNativeBuildMustBeEnabled(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "CMakeCache.txt")
	for _, enabled := range []bool{false, true} {
		value := "OFF"
		if enabled {
			value = "ON"
		}
		if err := os.WriteFile(path, []byte("WITH_RAFT_OTEL:BOOL="+value+"\n"), 0644); err != nil {
			t.Fatal(err)
		}
		if err := requireNativeBuild(dir); (err == nil) != enabled {
			t.Fatalf("enabled=%v, err=%v", enabled, err)
		}
	}
}

// This opt-in Linux test exercises the real Go workload, C++ shutdown export and CPU report.
func TestGoNativeWorkloadIntegration(t *testing.T) {
	binary := os.Getenv("PROFILE_TEST_NATIVE_BINARY")
	if binary == "" {
		t.Skip("set PROFILE_TEST_NATIVE_BINARY to run native diagnostic integration")
	}
	output, profiler := os.Getenv("PROFILE_TEST_NATIVE_OUTPUT"), os.Getenv("PROFILE_TEST_PROFILER")
	flamegraph := os.Getenv("PROFILE_TEST_FLAMEGRAPH")
	if output == "" || profiler == "" || flamegraph == "" {
		t.Fatal("native test requires output, profiler and flamegraph paths")
	}
	if err := os.Mkdir(output, 0755); err != nil {
		t.Fatal(err)
	}
	repo, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	config := Config{Repo: repo, Output: output, Experiment: "go-workload-test", Profiler: profiler,
		Go: "go", Flamegraph: flamegraph, Timeout: 5}
	log, err := os.Create(filepath.Join(output, "command.log"))
	if err != nil {
		t.Fatal(err)
	}
	defer log.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	runner := Runner{Config: config, Ctx: ctx, Out: os.Stdout, Log: log,
		State: State{Steps: map[string]*Record{"candidate": {Dir: filepath.Dir(binary)}}}}
	record := &Record{Dir: output, Attempt: 1}
	if _, err := runner.observe(record, "after"); err != nil {
		t.Fatal(err)
	}
	run := observationDir(config, record, "after")
	report := filepath.Join(output, "report")
	if err := nativeReport(ctx, config, run, report, os.Stdout); err != nil {
		t.Fatal(err)
	}
	result, err := ReadNativeDiagnostic(report)
	if err != nil {
		t.Fatal(err)
	}
	if result.Metadata.Runner != "go" || result.Capture == nil || !result.Capture.Complete || result.Capture.Events == 0 {
		t.Fatalf("incomplete native report: %+v", result.Capture)
	}
}
