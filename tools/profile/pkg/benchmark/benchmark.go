// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package benchmark

import (
	"bytes"
	"context"
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

//go:embed benchmark.go fixture.go payload.go quorum.go resp.go
var sources embed.FS

// HarnessSHA fingerprints the workload implementation, not the server under test.
func HarnessSHA(repo string) (string, error) {
	h := sha256.New()
	entries, err := sources.ReadDir(".")
	if err != nil {
		return "", err
	}
	for _, entry := range entries {
		var b []byte
		if repo == "" {
			b, err = sources.ReadFile(entry.Name())
		} else {
			b, err = os.ReadFile(filepath.Join(repo, "tools/profile/pkg/benchmark", entry.Name()))
		}
		if err != nil {
			return "", err
		}
		fmt.Fprintf(h, "%s\x00%d\x00", entry.Name(), len(b))
		h.Write(b)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

type Config struct {
	Binary, Output, Mode, Ack       string
	Clients, Depth, Payload, Warmup int
	Settle                          time.Duration
	Profiler                        string
	TraceEnvironment                []string
	Log                             io.Writer
}

// Result keeps the archived measurement schema used by the CLI and Web.
type Result struct {
	Order         int      `json:"run_order,omitempty"`
	Trial         int      `json:"trial,omitempty"`
	Variant       string   `json:"variant,omitempty"`
	Mode          string   `json:"mode"`
	Ack           string   `json:"ack_mode,omitempty"`
	Clients       int      `json:"clients"`
	Depth         int      `json:"pipeline_depth"`
	Payload       int      `json:"payload_mib"`
	Keys          int      `json:"keys"`
	Warmup        int      `json:"warmup_writes"`
	Settle        float64  `json:"settle_seconds"`
	Rate          float64  `json:"replication_mib_per_s"`
	SetRate       float64  `json:"mib_per_s"`
	Tail          float64  `json:"replication_tail_ms"`
	P95           float64  `json:"pipeline_batch_p95_ms"`
	P50           float64  `json:"pipeline_batch_p50_ms"`
	Confirmed     int      `json:"confirmed_replicas"`
	RecoveryMS    float64  `json:"restart_to_wait_two_ms"`
	Readback      int      `json:"readback_node"`
	Start         int64    `json:"measure_start_ns"`
	WriteEnd      int64    `json:"measure_end_ns"`
	End           int64    `json:"replication_end_ns"`
	WriteMS       float64  `json:"write_ms"`
	ReplicationMS float64  `json:"replication_ms"`
	FirstSetP50   *float64 `json:"first_set_p50_ms"`
	SetP50        *float64 `json:"set_p50_ms"`
	SetP95        *float64 `json:"set_p95_ms"`
	SetMax        *float64 `json:"set_max_ms"`
}

type record struct{ key, value []byte }
type sample struct {
	start, end int64
	latencies  []float64
	err        error
}

func mono() int64 {
	var t unix.Timespec
	// CLOCK_MONOTONIC is shared with C++ trace timestamps on the Linux execution host.
	if err := unix.ClockGettime(unix.CLOCK_MONOTONIC, &t); err != nil {
		panic(err)
	}
	return t.Nano()
}
func rounded(v float64) float64 { return math.RoundToEven(v*100) / 100 }
func percentile(v []float64, fraction float64) float64 {
	v = append([]float64{}, v...)
	sort.Float64s(v)
	return v[int(math.Ceil(float64(len(v))*fraction))-1]
}

func writeSlice(c *connection, records []record, depth int, start <-chan struct{}) sample {
	s := sample{}
	<-start
	s.start = mono()
	for i := 0; i < len(records); i += depth {
		sent := mono()
		chunk := records[i:min(i+depth, len(records))]
		commands := make([][][]byte, len(chunk))
		for j, r := range chunk {
			commands[j] = [][]byte{[]byte("SET"), r.key, r.value}
		}
		if s.err = c.deadline(); s.err != nil {
			return s
		}
		if s.err = writeCommands(c.Conn, commands); s.err != nil {
			return s
		}
		for range chunk {
			v, e := readRESP(c.reader, 0)
			if s.err = expect(v, e, "OK"); s.err != nil {
				return s
			}
		}
		s.end = mono()
		s.latencies = append(s.latencies, float64(s.end-sent)/1e6)
	}
	return s
}

func writeBatch(writers []*connection, data []record, depth int) ([]sample, error) {
	active := min(len(writers), len(data))
	samples := make([]sample, active)
	start := make(chan struct{})
	var ready, done sync.WaitGroup
	ready.Add(active)
	done.Add(active)
	for i := 0; i < active; i++ {
		go func(i int) {
			defer done.Done()
			var records []record
			for j := i; j < len(data); j += len(writers) {
				records = append(records, data[j])
			}
			ready.Done()
			samples[i] = writeSlice(writers[i], records, depth, start)
		}(i)
	}
	ready.Wait()
	close(start)
	done.Wait()
	for _, s := range samples {
		if s.err != nil {
			return nil, s.err
		}
	}
	return samples, nil
}

func confirm(writers []*connection, replicas, timeout int) error {
	errorsByClient := make([]error, len(writers))
	var done sync.WaitGroup
	done.Add(len(writers))
	for i, w := range writers {
		go func(i int, c *connection) { defer done.Done(); errorsByClient[i] = c.wait(replicas, timeout) }(i, w)
	}
	done.Wait()
	return errors.Join(errorsByClient...)
}

// Run executes one round and returns success only after recovery, readback and clean shutdown.
func Run(ctx context.Context, c Config) (result Result, err error) {
	if runtime.GOOS != "linux" {
		return result, errors.New("replication benchmark requires Linux io_uring")
	}
	if (c.Mode != "raft" && c.Mode != "yes") || (c.Mode == "raft" && c.Ack != "memory" && c.Ack != "flush") || c.Payload < 1 || c.Depth < 1 || c.Clients < 1 || c.Clients > min(1024, c.Payload*4) || c.Warmup < 0 || c.Settle < 0 {
		return result, errors.New("invalid benchmark configuration")
	}
	if c.Profiler == "" && len(c.TraceEnvironment) != 0 {
		return result, errors.New("trace environment requires an independent diagnostic run")
	}
	if c.Log == nil {
		c.Log = io.Discard
	}
	if c.Mode == "yes" {
		c.Ack = ""
	}
	c.Binary, err = filepath.Abs(c.Binary)
	if err != nil {
		return result, err
	}
	c.Output, err = filepath.Abs(c.Output)
	if err != nil {
		return result, err
	}
	for _, name := range []string{"logs", "cpu", "testdata"} {
		if err = os.MkdirAll(filepath.Join(c.Output, name), 0755); err != nil {
			return result, err
		}
	}
	// Existing node logs identify an already attempted run; never merge two captures.
	if files, _ := filepath.Glob(filepath.Join(c.Output, "logs", "*.log")); len(files) != 0 {
		return result, errors.New("benchmark output already contains node logs")
	}
	f := &fixture{ctx: ctx, config: c}
	defer func() {
		err = errors.Join(err, f.close())
		if err == nil {
			err = os.RemoveAll(f.root)
		}
	}()
	if err = f.prepare(); err != nil {
		return result, err
	}
	rng := newPayloadRandom()
	tags := []string{"{12561}", "{104856}"}
	makeRecords := func(count int, suffix string) []record {
		data := make([]record, count)
		for i := range data {
			data[i] = record{[]byte(fmt.Sprintf("%s:%s%d", tags[i%2], suffix, i)), rng.value()}
		}
		return data
	}
	data := makeRecords(c.Payload*4, "")
	warmup := makeRecords(c.Warmup, "warmup:")
	var writers []*connection
	defer func() {
		for _, w := range writers {
			w.Close()
		}
	}()
	for i := 0; i < c.Clients; i++ {
		w, e := connect(ctx, f.nodes[f.leader].resp)
		if e != nil {
			return result, e
		}
		writers = append(writers, w)
		v, e := w.command("INFO", "server")
		if e != nil {
			return result, e
		}
		if !strings.Contains(stringValue(v), "thread_count:2\r\n") {
			return result, errors.New("expected two data threads")
		}
	}
	other, e := connect(ctx, f.nodes[f.other].resp)
	if e != nil {
		return result, e
	}
	defer other.Close()
	confirmValue := func(w *connection, key, value string, replicas int) error {
		if e := w.ok("SET", key, value); e != nil {
			return e
		}
		if e := w.wait(replicas, 5000); e != nil {
			return e
		}
		v, e := w.command("GET", key)
		return expect(v, e, value)
	}
	if err = confirmValue(writers[0], "{104856}:confirmed", "ready", 2); err != nil {
		return result, err
	}
	if err = confirmValue(other, "k0", "ready", 1); err != nil {
		return result, err
	}
	if err = f.stop(13, true); err != nil {
		return result, err
	}
	if err = confirmValue(writers[0], "{12561}:blocking", "after stop", 1); err != nil {
		return result, err
	}
	if err = confirmValue(other, "k0", "after stop", 1); err != nil {
		return result, err
	}
	select {
	case <-ctx.Done():
		return result, ctx.Err()
	case <-time.After(c.Settle):
	}
	if _, err = writeBatch(writers, warmup, c.Depth); err != nil {
		return result, err
	}
	if err = confirm(writers[:min(c.Clients, c.Warmup)], 1, 5000); err != nil {
		return result, err
	}
	if c.Profiler != "" {
		if err = f.profile(true); err != nil {
			return result, err
		}
	}
	samples, err := writeBatch(writers, data, c.Depth)
	if err != nil {
		return result, err
	}
	if err = confirm(writers, 1, 5000); err != nil {
		return result, err
	}
	end := mono()
	if c.Profiler != "" {
		if err = f.profile(false); err != nil {
			return result, err
		}
	}
	result = summarize(c, samples, end)
	recovery := mono()
	if err = f.start(13); err != nil {
		return result, err
	}
	if err = f.poll(time.Duration(60+c.Payload)*time.Second, func() (bool, error) {
		for _, w := range writers {
			if e := w.wait(2, 500); e != nil {
				return false, e
			}
		}
		return true, nil
	}); err != nil {
		return result, err
	}
	result.RecoveryMS = rounded(float64(mono()-recovery) / 1e6)
	if err = f.prepareReadback(); err != nil {
		return result, err
	}
	readback, e := connect(ctx, f.nodes[13].resp)
	if e != nil {
		return result, e
	}
	defer readback.Close()
	if err = f.poll(35*time.Second, func() (bool, error) {
		v, e := readback.command("GET", "{104856}:confirmed")
		return stringValue(v) == "ready", e
	}); err != nil {
		return result, err
	}
	for _, batch := range [][]record{warmup, data} {
		for _, r := range batch {
			v, e := readback.bytesCommand([]byte("GET"), r.key)
			if e != nil {
				return result, e
			}
			value, ok := v.([]byte)
			if !ok || !bytes.Equal(value, r.value) {
				return result, fmt.Errorf("readback mismatch: %s", r.key)
			}
		}
	}
	result.Confirmed, result.Readback = 1, 13
	fmt.Fprintf(c.Log, "Verified WAIT 1, recovery WAIT 2 and %d values on node 13\n", len(data)+len(warmup))
	return result, nil
}

func summarize(c Config, samples []sample, end int64) Result {
	start, writeEnd := samples[0].start, samples[0].end
	var latencies, first []float64
	for _, s := range samples {
		start = min(start, s.start)
		writeEnd = max(writeEnd, s.end)
		latencies = append(latencies, s.latencies...)
		first = append(first, s.latencies[0])
	}
	writeMS := rounded(float64(writeEnd-start) / 1e6)
	replicationMS := float64(end-start) / 1e6
	r := Result{Mode: c.Mode, Ack: c.Ack, Clients: c.Clients, Depth: c.Depth, Payload: c.Payload, Keys: c.Payload * 4, Warmup: c.Warmup, Settle: c.Settle.Seconds(),
		Start: start, WriteEnd: writeEnd, End: end, WriteMS: writeMS, ReplicationMS: rounded(replicationMS), Tail: rounded(float64(end-writeEnd) / 1e6),
		Rate: rounded(float64(c.Payload) * 1000 / replicationMS), SetRate: rounded(float64(c.Payload) * 1000 / writeMS), P50: rounded(percentile(latencies, .5)), P95: rounded(percentile(latencies, .95))}
	if c.Depth == 1 {
		firstP50, p50, p95, maxMS := rounded(percentile(first, .5)), r.P50, r.P95, rounded(percentile(latencies, 1))
		r.FirstSetP50, r.SetP50, r.SetP95, r.SetMax = &firstP50, &p50, &p95, &maxMS
	}
	return r
}

func (f *fixture) profile(start bool) error {
	if f.leader != 11 && f.leader != 12 {
		return errors.New("leader outside profiled nodes 11 and 12")
	}
	path := filepath.Join(f.config.Output, "metadata.json")
	metadata := map[string]any{}
	nodes := map[string]any{}
	if start {
		binary, e := os.Open(f.config.Binary)
		if e != nil {
			return e
		}
		h := sha256.New()
		_, e = io.Copy(h, binary)
		binary.Close()
		if e != nil {
			return e
		}
		harness, e := HarnessSHA("")
		if e != nil {
			return e
		}
		mode := "raft." + f.config.Ack
		if f.config.Mode == "yes" {
			mode = "cluster"
		}
		metadata = map[string]any{"runner": "go", "mode": mode, "binary": f.config.Binary, "binary_sha256": hex.EncodeToString(h.Sum(nil)), "harness_sha256": harness, "frequency_hz": 500, "nodes": nodes}
	} else {
		b, e := os.ReadFile(path)
		if e != nil {
			return e
		}
		if e = json.Unmarshal(b, &metadata); e != nil {
			return e
		}
		nodes = metadata["nodes"].(map[string]any)
	}
	for _, id := range []int{11, 12} {
		n := f.nodes[id]
		memory, e := processMemory(n.cmd.Process.Pid)
		if e != nil {
			return e
		}
		key := strconv.Itoa(id)
		if start {
			role := "follower"
			if id == f.leader {
				role = "leader"
			}
			nodes[key] = map[string]any{"pid": n.cmd.Process.Pid, "role": role, "memory_before": memory}
		} else {
			nodes[key].(map[string]any)["memory_after"] = memory
		}
		if e = n.cmd.Process.Signal(syscall.SIGUSR2); e != nil {
			return e
		}
	}
	if start {
		if e := f.pollEvery(3*time.Second, 10*time.Millisecond, func() (bool, error) {
			for _, id := range []int{11, 12} {
				if _, e := os.Stat(filepath.Join(f.config.Output, "cpu", fmt.Sprintf("node-%d.prof.0", id))); e != nil {
					return false, e
				}
			}
			return true, nil
		}); e != nil {
			return e
		}
	}
	b, e := json.MarshalIndent(metadata, "", "  ")
	if e != nil {
		return e
	}
	return os.WriteFile(path, append(b, '\n'), 0644)
}

func processMemory(pid int) (map[string]int64, error) {
	b, e := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if e != nil {
		return nil, e
	}
	result := map[string]int64{}
	names := map[string]string{"VmRSS:": "rss_kib", "VmHWM:": "peak_rss_kib", "RssAnon:": "anon_rss_kib"}
	for _, line := range strings.Split(string(b), "\n") {
		fields := strings.Fields(line)
		if len(fields) > 1 && names[fields[0]] != "" {
			value, e := strconv.ParseInt(fields[1], 10, 64)
			if e != nil {
				return nil, e
			}
			result[names[fields[0]]] = value
		}
	}
	if len(result) != 3 {
		return nil, errors.New("process memory readings missing")
	}
	return result, nil
}
