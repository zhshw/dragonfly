// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package workflow

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"time"

	"dragonflydb.io/profile/pkg/benchmark"
	"dragonflydb.io/profile/pkg/otel"
)

const nativeWorkload = "raft.memory/c1/p64/256MiB"

// CaptureReceipt is a verified reference, not an exported copy of trace events.
type CaptureReceipt struct {
	otel.NativeCapture
	Complete bool     `json:"complete"`
	Events   int      `json:"events"`
	Nodes    []string `json:"nodes"`
}

type NativeDiagnostic struct {
	Run      string          `json:"run"`
	Metadata otel.Metadata   `json:"metadata"`
	Result   BenchRow        `json:"result"`
	Capture  *CaptureReceipt `json:"capture,omitempty"`
}

func traceConfig(c Config) otel.LangfuseConfig {
	config := otel.LangfuseConfig{URL: "http://127.0.0.1:3007", EnvFile: "/data/dragonfly-observability/config/langfuse.env"}
	if c.Langfuse != nil {
		if c.Langfuse.URL != "" {
			config.URL = c.Langfuse.URL
		}
		if c.Langfuse.EnvFile != "" {
			config.EnvFile = c.Langfuse.EnvFile
		}
	}
	return config
}

func requireNativeBuild(build string) error {
	cache, err := os.ReadFile(filepath.Join(build, "CMakeCache.txt"))
	if err != nil {
		return err
	}
	if !strings.Contains("\n"+string(cache), "\nWITH_RAFT_OTEL:BOOL=ON\n") {
		return fmt.Errorf("%s requires -DWITH_RAFT_OTEL=ON; baseline builds must retain CMakeCache.txt", build)
	}
	return nil
}

func (r *Runner) observe(rec *Record, variant string) ([]string, error) {
	if err := requireBenchmarkSource(r.Config.Repo); err != nil {
		return nil, err
	}
	build, mode := "candidate", "raft"
	if variant == "before" {
		build = "baseline"
	}
	if variant == "cluster" {
		mode = "yes"
	}
	binary := filepath.Join(r.dir(build), "dragonfly")
	sha, err := hashFile(binary)
	if err != nil {
		return nil, err
	}
	run := observationDir(r.Config, rec, variant)
	for _, name := range []string{"cpu", "logs", "testdata"} {
		if err := os.MkdirAll(filepath.Join(run, name), 0755); err != nil {
			return nil, err
		}
	}
	var env []string
	var capture otel.NativeCapture
	if variant != "cluster" {
		auth, err := otel.TraceEnvironment(traceConfig(r.Config))
		if err != nil {
			return nil, err
		}
		experiment, err := traceExperiment(r.Config, variant)
		if err != nil {
			return nil, err
		}
		var nonce [16]byte
		if _, err := rand.Read(nonce[:]); err != nil {
			return nil, err
		}
		capture = otel.NativeCapture{
			ID: fmt.Sprintf("%s-%x", filepath.Base(run), nonce), Experiment: experiment, Release: sha, BinarySHA: sha, Workload: nativeWorkload,
			Client: true,
		}
		if err := atomicJSON(filepath.Join(run, "capture.json"), capture); err != nil {
			return nil, err
		}
		env = append(env, auth...)
		env = append(env, "RAFT_TRACE_CAPTURE_ID="+capture.ID, "RAFT_TRACE_EXPERIMENT="+capture.Experiment,
			"RAFT_TRACE_RELEASE="+capture.Release, "RAFT_TRACE_BINARY_SHA256="+sha,
			"RAFT_TRACE_WORKLOAD="+capture.Workload, "RAFT_TRACE_NODES=11,12")
	}
	file, err := os.OpenFile(filepath.Join(run, "test.log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	config := workloadConfig(binary, run, mode, "memory", io.MultiWriter(file, r.Log))
	config.Profiler, config.TraceEnvironment = r.Config.Profiler, env
	ctx, cancel := context.WithTimeout(r.Ctx, time.Duration(r.Config.Timeout)*time.Minute)
	defer cancel()
	if capture.Client {
		process := fmt.Sprintf("client:%d:%d", os.Getpid(), time.Now().UnixNano())
		config.Observe = func(events []benchmark.ClientEvent, wallOffset int64) error {
			recorded := make([]otel.RecordedEvent, 0, len(events))
			for _, event := range events {
				recorded = append(recorded, otel.RecordedEvent{Name: event.Name, Start: event.Start, End: event.End,
					Fields: map[string]string{"client_id": strconv.Itoa(event.Client), "writes": strconv.Itoa(event.Writes), "payload_bytes": strconv.Itoa(event.Bytes)}})
			}
			return otel.ExportNativeStream(ctx, traceConfig(r.Config), otel.NativeStream{Capture: capture,
				Node: "client", Role: "Client", Group: "client", Process: process, WallOffset: wallOffset,
				ExpectedNodes: []string{"11", "12", "client"}}, recorded)
		}
	}
	row, err := benchmark.Run(ctx, config)
	err = errors.Join(err, file.Close())
	if err != nil {
		return nil, err
	}
	if err = validWorkload(BenchRow(row)); err != nil {
		return nil, err
	}
	if err = atomicJSON(filepath.Join(run, "result.json"), row); err != nil {
		return nil, err
	}
	return artifactFiles(run)
}

// Poll only the completed capture. A report retry never starts another workload.
func verifyNative(ctx context.Context, config otel.LangfuseConfig, want otel.NativeCapture, interval time.Duration) (CaptureReceipt, error) {
	var last error
	for {
		report, err := otel.QueryNative(ctx, config, want.Experiment, want.ID, otel.NativeQuery{Limit: 1})
		if err == nil && report.Complete {
			captures, e := otel.NativeCaptures(ctx, config, want.Experiment)
			if e != nil {
				return CaptureReceipt{}, e
			}
			for _, got := range captures {
				if got.ID != want.ID {
					continue
				}
				if got.Experiment != want.Experiment || got.BinarySHA != want.BinarySHA || got.Release != want.Release || got.Workload != want.Workload {
					return CaptureReceipt{}, errors.New("native capture build/workload identity mismatch")
				}
				var nodes []string
				for _, n := range report.Nodes {
					nodes = append(nodes, n.ID)
				}
				if !reflect.DeepEqual(nodes, captureNodes(want)) || got.Client != want.Client || report.Total == 0 {
					err = fmt.Errorf("native capture needs events from %v", captureNodes(want))
					break
				}
				return CaptureReceipt{NativeCapture: got, Complete: true, Events: report.Total, Nodes: nodes}, nil
			}
			if err == nil {
				err = errors.New("capture manifest not yet visible")
			}
		} else if err == nil {
			err = fmt.Errorf("incomplete native capture: %s", strings.Join(report.Issues, "; "))
		}
		last = err
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return CaptureReceipt{}, fmt.Errorf("verify capture %s: %w (%v)", want.ID, ctx.Err(), last)
		case <-timer.C:
		}
	}
}

func captureNodes(c otel.NativeCapture) []string {
	nodes := []string{"11", "12"}
	if c.Client {
		nodes = append(nodes, "client")
	}
	return nodes
}

func nativeReport(ctx context.Context, c Config, run, out string, log io.Writer) error {
	report := NativeDiagnostic{Run: run}
	if err := readJSON(filepath.Join(run, "metadata.json"), &report.Metadata); err != nil {
		return err
	}
	if err := readJSON(filepath.Join(run, "result.json"), &report.Result); err != nil {
		return err
	}
	if err := validWorkload(report.Result); err != nil {
		return err
	}
	if report.Result.Mode == "raft" {
		var capture otel.NativeCapture
		if err := readJSON(filepath.Join(run, "capture.json"), &capture); err != nil {
			return err
		}
		receipt, err := verifyNative(ctx, traceConfig(c), capture, 2*time.Second)
		if err != nil {
			return err
		}
		report.Capture = &receipt
		fmt.Fprintf(log, "Verified native capture %s\n", capture.ID)
	}
	if sha, err := hashFile(report.Metadata.Binary); err != nil || sha != report.Metadata.BinarySHA {
		return errors.New("diagnostic binary changed")
	}
	if err := os.Mkdir(out, 0755); err != nil {
		return err
	}
	if _, err := cpuReports(ctx, run, out, report.Metadata, ReportOptions{Go: c.Go, Flamegraph: c.Flamegraph}, log); err != nil {
		return err
	}
	return atomicJSON(filepath.Join(out, "native.json"), report)
}

func ReadNativeDiagnostic(dir string) (NativeDiagnostic, error) {
	var report NativeDiagnostic
	err := readJSON(filepath.Join(dir, "native.json"), &report)
	return report, err
}

// VerifyCapture checks the stored reference against Langfuse using the CLI/Web query package.
func VerifyCapture(ctx context.Context, output string) (CaptureReceipt, error) {
	files, err := CurrentArtifacts(output)
	if err != nil {
		return CaptureReceipt{}, err
	}
	state, err := ReadState(output)
	if err != nil {
		return CaptureReceipt{}, err
	}
	report, err := ReadNativeDiagnostic(files.Report)
	if err != nil {
		return CaptureReceipt{}, err
	}
	if report.Capture == nil {
		return CaptureReceipt{}, errors.New("workflow has no native Raft capture")
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	return verifyNative(ctx, traceConfig(state.Config), report.Capture.NativeCapture, 2*time.Second)
}

func readNativeDiagnostic(dir string, report NativeDiagnostic, build BuildInfo, harness string) (Diagnostic, BenchRow, error) {
	m, row := report.Metadata, report.Result
	d := Diagnostic{ProbeSHA: m.BinarySHA, Frequency: m.Frequency, Nodes: map[string]NodeDiagnostic{}, Native: true}
	if err := validWorkload(row); err != nil {
		return d, row, err
	}
	if m.BinarySHA != build.SHA || !validSHA(m.BinarySHA) || m.HarnessSHA != harness ||
		row.Mode != build.Mode || row.Clients != 1 || row.Depth != 64 || row.Payload != 256 ||
		row.Keys != 1024 || row.Warmup != 64 || row.Settle != 5 || m.Frequency != 500 || len(m.Nodes) != 2 {
		return d, row, errors.New("incompatible native diagnostic build or workload")
	}
	if build.Mode == "raft" {
		c := report.Capture
		if build.Ack != "memory" || m.Mode != "raft.memory" || c == nil || !c.Complete || c.Events < 1 || c.Streams < 2 ||
			c.ID == "" || c.Experiment == "" || c.BinarySHA != build.SHA || c.Release != build.SHA ||
			c.Workload != nativeWorkload || !reflect.DeepEqual(c.Nodes, captureNodes(c.NativeCapture)) {
			return d, row, errors.New("missing or incompatible native capture receipt")
		}
		d.CaptureID, d.SpanCount = c.ID, c.Events
	} else if build.Mode != "yes" || m.Mode != "cluster" || report.Capture != nil {
		return d, row, errors.New("invalid Cluster diagnostic")
	}
	for node, info := range m.Nodes {
		if (node != "11" && node != "12") || (info.Role != "leader" && info.Role != "follower") || d.Nodes[info.Role].Node != "" {
			return d, row, errors.New("invalid diagnostic node roles")
		}
		data, err := os.ReadFile(filepath.Join(dir, info.Role+"-node-"+node+".folded"))
		if err != nil {
			return d, row, err
		}
		top, samples, err := CPUHotspots(string(data), false, 5)
		if err != nil {
			return d, row, err
		}
		cpu := float64(samples) * 1000 / m.Frequency
		d.Nodes[info.Role] = NodeDiagnostic{Node: node, CPU: cpu, CPUPerMiB: cpu / float64(row.Payload), PeakRSS: info.After.Peak / 1024, Hotspots: top}
	}
	return d, row, nil
}
