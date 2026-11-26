// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package benchmark

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPythonPayloadEquivalence(t *testing.T) {
	r := newPayloadRandom()
	// Golden SHA256 values from Python random.Random(42).randbytes(256 << 10).
	for _, want := range []string{"91ebaad5f5abd4c2f092b80c37bf16dfcf3e68d1bce9d61d219a56a54f3b2831", "466af3051d66d13a7049b154bd0edbafd5efa83aeb5d5d6ba6d2b595fa5d41e7"} {
		if got := fmt.Sprintf("%x", sha256.Sum256(r.value())); got != want {
			t.Fatalf("payload changed: %s", got)
		}
	}
}

func TestPipelineAndWaitShareSocket(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	c := &connection{client, bufio.NewReader(client), context.Background()}
	done := make(chan error, 1)
	go func() {
		r := bufio.NewReader(server)
		// Both SET commands must arrive before their responses. WAIT follows on this socket.
		for _, key := range []string{"key0", "key1"} {
			v, e := readRESP(r, 0)
			if e != nil {
				done <- e
				return
			}
			a, ok := v.([]any)
			if !ok || len(a) != 3 || stringValue(a[0]) != "SET" || stringValue(a[1]) != key {
				done <- fmt.Errorf("unexpected command: %v", v)
				return
			}
		}
		if _, e := io.WriteString(server, "+OK\r\n+OK\r\n"); e != nil {
			done <- e
			return
		}
		v, e := readRESP(r, 0)
		if e != nil {
			done <- e
			return
		}
		a, ok := v.([]any)
		if !ok || len(a) != 3 || stringValue(a[0]) != "WAIT" || stringValue(a[1]) != "1" {
			done <- fmt.Errorf("unexpected WAIT: %v", v)
			return
		}
		_, e = io.WriteString(server, ":1\r\n")
		done <- e
	}()
	data := []record{{[]byte("key0"), []byte{0, 1, 255}}, {[]byte("key1"), []byte("value")}}
	samples, e := writeBatch([]*connection{c}, data, 2, true)
	if e != nil {
		t.Fatal(e)
	}
	if len(samples) != 1 || len(samples[0].latencies) != 1 {
		t.Fatalf("wrong batch count: %+v", samples)
	}
	if len(samples[0].events) != 1 {
		t.Fatalf("pipeline event missing: %+v", samples)
	}
	event := samples[0].events[0]
	if event.Name != "client.pipeline" || event.Start < samples[0].start || event.End != samples[0].end ||
		float64(event.End-event.Start)/1e6 != samples[0].latencies[0] || event.Writes != 2 || event.Bytes != 8 {
		t.Fatalf("pipeline trace differs from measured batch: %+v", event)
	}
	if e = confirm([]*connection{c}, 1, 5000); e != nil {
		t.Fatal(e)
	}
	if e = <-done; e != nil {
		t.Fatal(e)
	}
}

func TestIncompletePipelineFails(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	c := &connection{client, bufio.NewReader(client), context.Background()}
	go func() {
		defer server.Close()
		r := bufio.NewReader(server)
		_, _ = readRESP(r, 0)
		_, _ = readRESP(r, 0)
		_, _ = io.WriteString(server, "+OK\r\n")
	}()
	_, e := writeBatch([]*connection{c}, []record{{[]byte("a"), nil}, {[]byte("b"), nil}}, 2)
	if e == nil {
		t.Fatal("missing acknowledgement accepted")
	}
}

func TestMeasurementIncludesIdleTailAndAllWriters(t *testing.T) {
	c := Config{Mode: "raft", Ack: "memory", Clients: 2, Depth: 64, Payload: 256, Warmup: 64, Settle: 5 * time.Second}
	r := summarize(c, []sample{{start: 1e9, end: 1100e6, latencies: []float64{20, 80}}, {start: 1010e6, end: 1120e6, latencies: []float64{30, 70}}}, 1200e6)
	if r.Start != 1e9 || r.WriteMS != 120 || r.ReplicationMS != 200 || r.Tail != 80 || r.Rate != 1280 || r.P95 != 80 || r.P50 != 30 || r.SetP95 != nil {
		t.Fatalf("timing/percentile changed: %+v", r)
	}
}

func TestRESPRejectsMalformedReplies(t *testing.T) {
	for _, wire := range []string{"$2\r\naax\n", "$-2\r\n", "*99999999\r\n", "+OK\n", "-ERR fail\r\n", "$3\r\na"} {
		if _, err := readRESP(bufio.NewReader(strings.NewReader(wire)), 0); err == nil {
			t.Fatalf("accepted %q", wire)
		}
	}
	if err := expect([]any{}, nil, "OK"); err == nil {
		t.Fatal("unexpected array accepted")
	}
}

func TestFormalEnvironmentAndSourceIdentity(t *testing.T) {
	for _, name := range []string{"RAFT_TRACE_CAPTURE_ID", "CPUPROFILE", "LD_PRELOAD", "OTEL_EXPORTER_OTLP_TRACES_HEADERS"} {
		t.Setenv(name, "inherited-secret")
	}
	if strings.Contains(strings.Join(ProcessEnvironment(nil), "\n"), "inherited-secret") {
		t.Fatal("formal run inherited diagnostics")
	}
	if !strings.Contains(strings.Join(ProcessEnvironment([]string{"RAFT_TRACE_CAPTURE_ID=explicit"}), "\n"), "RAFT_TRACE_CAPTURE_ID=explicit") {
		t.Fatal("explicit capture missing")
	}
	embedded, e := HarnessSHA("")
	if e != nil {
		t.Fatal(e)
	}
	source, e := HarnessSHA("../../../..")
	if e != nil || source != embedded {
		t.Fatalf("source mismatch: %s %s %v", source, embedded, e)
	}
	var b bytes.Buffer
	if e = writeCommands(&b, [][][]byte{{[]byte("WAIT"), []byte("1"), []byte("5000")}}); e != nil {
		t.Fatal(e)
	}
	if b.String() != "*3\r\n$4\r\nWAIT\r\n$1\r\n1\r\n$4\r\n5000\r\n" {
		t.Fatal("wire encoding changed")
	}
}

func TestOnlySelectedNodesLoadCPUProfiler(t *testing.T) {
	f := fixture{config: Config{Profiler: "/lib/profiler.so", Output: "/run/diagnostic", TraceEnvironment: []string{"RAFT_TRACE_CAPTURE_ID=capture"}}}
	for _, node := range []int{1, 11, 12, 13, 21, 22} {
		env := strings.Join(f.processEnvironment(node), "\n")
		if !strings.Contains(env, "RAFT_TRACE_CAPTURE_ID=capture") {
			t.Fatal("capture identity missing")
		}
		if got, want := strings.Contains(env, "LD_PRELOAD=/lib/profiler.so"), node == 11 || node == 12; got != want {
			t.Fatalf("CPU selection for node %d: %v", node, got)
		}
	}
}

func TestClusterIntegration(t *testing.T) {
	binary := os.Getenv("PROFILE_TEST_BINARY")
	if binary == "" {
		t.Skip("set PROFILE_TEST_BINARY to a Linux Release dragonfly binary")
	}
	root := os.Getenv("PROFILE_TEST_OUTPUT")
	if root == "" {
		t.Fatal("PROFILE_TEST_OUTPUT must be a fresh directory on a real disk")
	}
	for _, mode := range []string{"raft", "yes"} {
		t.Run(mode, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
			defer cancel()
			row, e := Run(ctx, Config{Binary: binary, Output: filepath.Join(root, mode), Mode: mode, Ack: "memory", Clients: 2, Depth: 2, Payload: 1, Warmup: 4, Log: os.Stdout})
			if e != nil {
				t.Fatal(e)
			}
			if row.Confirmed != 1 || row.Readback != 13 || row.RecoveryMS <= 0 || row.Rate <= 0 {
				t.Fatalf("incomplete run: %+v", row)
			}
		})
	}
}
