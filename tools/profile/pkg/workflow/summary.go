// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package workflow

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"dragonflydb.io/profile/pkg/benchmark"
)

type BenchRow benchmark.Result

type BuildInfo struct {
	SHA  string `json:"sha256"`
	Mode string `json:"mode"`
	Ack  string `json:"ack_mode"`
}

type Environment struct {
	Runner   string               `json:"runner,omitempty"`
	Builds   map[string]BuildInfo `json:"builds"`
	Harness  string               `json:"harness_sha256"`
	Metric   string               `json:"primary_metric"`
	Clients  int                  `json:"clients"`
	Depth    int                  `json:"pipeline_depth"`
	Distinct bool                 `json:"distinct_keys"`
	Mount    string               `json:"mount"`
}

type Distribution struct {
	Median float64 `json:"median"`
	Min    float64 `json:"min"`
	Max    float64 `json:"max"`
}

type Metrics struct {
	Rate    Distribution `json:"replication_mib_per_s"`
	SetRate Distribution `json:"set_mib_per_s"`
	Tail    Distribution `json:"wait_tail_ms"`
	P95     Distribution `json:"batch_p95_ms"`
}

type Comparison struct {
	Path        string                `json:"source"`
	Environment Environment           `json:"environment"`
	Rows        []BenchRow            `json:"rounds"`
	Metrics     map[string]Metrics    `json:"metrics"`
	RateChange  float64               `json:"median_rate_change_percent"`
	P95Change   float64               `json:"median_p95_change_percent"`
	PairedRate  []float64             `json:"paired_rate_change_percent"`
	PairedP95   []float64             `json:"paired_p95_change_percent"`
	Diagnostics map[string]Diagnostic `json:"diagnostics,omitempty"`
}

func validSHA(s string) bool {
	_, err := hex.DecodeString(s)
	return len(s) == 64 && err == nil
}

func validWorkload(r BenchRow) error {
	if r.Clients < 1 || r.Depth < 1 || r.Payload < 1 || r.Keys < 1 || r.Warmup < 0 || r.Settle < 0 {
		return errors.New("invalid benchmark workload")
	}
	if r.Confirmed != 1 || r.Readback != 13 || r.RecoveryMS <= 0 {
		return errors.New("missing replica confirmation/recovery/readback result")
	}
	for _, n := range []float64{r.Rate, r.SetRate, r.P95, r.RecoveryMS} {
		if math.IsNaN(n) || math.IsInf(n, 0) || n <= 0 {
			return errors.New("invalid benchmark metric")
		}
	}
	if r.Tail < 0 || r.Start <= 0 || r.WriteEnd < r.Start || r.End < r.WriteEnd {
		return errors.New("invalid measurement window")
	}
	return nil
}

func distribution(values []float64) Distribution {
	sort.Float64s(values)
	return Distribution{values[len(values)/2], values[0], values[len(values)-1]}
}

func change(before, after float64) float64 { return 100 * (after/before - 1) }

func readBenchmark(dir string) (*Comparison, error) {
	c := &Comparison{Path: dir, Metrics: map[string]Metrics{}}
	if err := readJSON(filepath.Join(dir, "environment.json"), &c.Environment); err != nil {
		return nil, err
	}
	if err := readJSON(filepath.Join(dir, "summary.json"), &c.Rows); err != nil {
		return nil, err
	}
	e := c.Environment
	if e.Metric != "replication_mib_per_s" || e.Clients < 1 || e.Depth < 1 || !e.Distinct || !validSHA(e.Harness) {
		return nil, errors.New("incompatible benchmark metadata")
	}
	if len(c.Rows) == 0 {
		return nil, errors.New("no completed benchmark rows")
	}
	first := c.Rows[0]
	for i, row := range c.Rows {
		if row.Order != i+1 {
			return nil, errors.New("invalid run order")
		}
		if _, ok := e.Builds[row.Variant]; !ok {
			return nil, errors.New("unknown benchmark variant")
		}
		if row.Clients != e.Clients || row.Depth != e.Depth || row.Payload != first.Payload || row.Keys != first.Keys ||
			row.Warmup != first.Warmup || row.Settle != first.Settle {
			return nil, errors.New("benchmark rows have different workloads")
		}
	}
	return c, nil
}

func (c *Comparison) readVariant(name string) (map[int]BenchRow, error) {
	// Variant names also occur in log filenames.
	if name == "" || strings.ContainsAny(name, `/\`) {
		return nil, errors.New("invalid benchmark variant")
	}
	build := c.Environment.Builds[name]
	modeOK := (build.Mode == "raft" && (build.Ack == "memory" || build.Ack == "flush")) || (build.Mode == "yes" && build.Ack == "")
	if !validSHA(build.SHA) || !modeOK {
		return nil, fmt.Errorf("invalid build identity/mode: %s", name)
	}
	trials := map[int]BenchRow{}
	for _, row := range c.Rows {
		if row.Variant != name {
			continue
		}
		if row.Trial < 1 || row.Trial > 3 {
			return nil, errors.New("invalid trial number")
		}
		if _, duplicate := trials[row.Trial]; duplicate {
			return nil, errors.New("duplicate variant/trial")
		}
		if err := validWorkload(row); err != nil {
			return nil, fmt.Errorf("%s trial %d: %w", row.Variant, row.Trial, err)
		}
		if row.Ack != build.Ack || row.Mode != build.Mode {
			return nil, errors.New("mixed acknowledgement/cluster mode")
		}
		// The Go runner writes its receipt only after recovery, readback and shutdown succeed.
		log := filepath.Join(c.Path, row.LogName())
		data, err := os.ReadFile(log)
		if c.Environment.Runner == "go" {
			var receipt BenchRow
			path := filepath.Join(c.Path, strings.TrimSuffix(row.LogName(), ".log"), "result.json")
			if err != nil || readJSON(path, &receipt) != nil || !reflect.DeepEqual(receipt, row) {
				return nil, fmt.Errorf("missing or inconsistent completed benchmark receipt: %s", path)
			}
		} else if c.Environment.Runner != "" || err != nil || !strings.HasSuffix(strings.TrimSpace(string(data)), "\nOK") {
			return nil, fmt.Errorf("missing successful legacy recovery/readback log: %s", log)
		}
		trials[row.Trial] = row
	}
	if len(trials) != 3 {
		return nil, fmt.Errorf("incomplete trials for %s", name)
	}
	var rate, set, tail, p95 []float64
	for _, r := range trials {
		rate, set, tail, p95 = append(rate, r.Rate), append(set, r.SetRate), append(tail, r.Tail), append(p95, r.P95)
	}
	c.Metrics[name] = Metrics{distribution(rate), distribution(set), distribution(tail), distribution(p95)}
	return trials, nil
}

func (row BenchRow) LogName() string {
	return fmt.Sprintf("trial-%d-%s-clients-%d-pipeline-%d.log", row.Trial, row.Variant, row.Clients, row.Depth)
}

// ReadBenchmark accepts all measured variants in one archive, including screening rounds.
func ReadBenchmark(dir string, include ...string) (*Comparison, error) {
	c, err := readBenchmark(dir)
	if err != nil {
		return nil, err
	}
	selected := map[string]bool{}
	for _, name := range include {
		selected[name] = true
	}
	if len(selected) == 0 {
		for name := range c.Environment.Builds {
			selected[name] = true
		}
	}
	for name := range selected {
		if _, err := c.readVariant(name); err != nil {
			return nil, err
		}
	}
	for name := range c.Environment.Builds {
		if !selected[name] {
			delete(c.Environment.Builds, name)
		}
	}
	rows := c.Rows[:0]
	for _, row := range c.Rows {
		if selected[row.Variant] {
			rows = append(rows, row)
		}
	}
	c.Rows = rows
	return c, nil
}

// ReadBenchmarkVariant imports the three rounds for one named historical benchmark variant.
func ReadBenchmarkVariant(dir, name string) (Environment, BenchRow, Metrics, error) {
	c, err := readBenchmark(dir)
	if err != nil {
		return Environment{}, BenchRow{}, Metrics{}, err
	}
	t, err := c.readVariant(name)
	return c.Environment, t[1], c.Metrics[name], err
}

// BenchmarkWorkload is the workload shared by the formal benchmark and its diagnostic run.
func BenchmarkWorkload() BenchRow {
	return BenchRow{Mode: "raft", Ack: "memory", Clients: 1, Depth: 64, Payload: 256, Keys: 1024, Warmup: 64, Settle: 5}
}

func HarnessSHA(repo string) (string, error) {
	return benchmark.HarnessSHA(repo)
}

// ReadComparison validates current and any explicitly included comparison builds.
func ReadComparison(dir string) (*Comparison, error) {
	c, err := readBenchmark(dir)
	if err != nil {
		return nil, err
	}
	e := c.Environment
	first, want := c.Rows[0], BenchmarkWorkload()
	if first.Clients != want.Clients || first.Depth != want.Depth || first.Payload != want.Payload ||
		first.Keys != want.Keys || first.Warmup != want.Warmup || first.Settle != want.Settle {
		return nil, errors.New("workflow comparison expects c1/p64, 256 MiB, 1024 keys, 64 warmup, 5s settle")
	}
	names := []string{"candidate"}
	if _, ok := e.Builds["baseline"]; ok {
		names = append(names, "baseline")
	}
	if cluster, ok := e.Builds["cluster"]; ok {
		if cluster.Mode != "yes" || cluster.SHA != e.Builds["candidate"].SHA {
			return nil, errors.New("cluster control must use the candidate binary in cluster mode")
		}
		names = append(names, "cluster")
	}
	if len(c.Rows) != 3*len(names) || len(e.Builds) != len(names) {
		return nil, errors.New("expected three rounds for current and each included comparison build")
	}
	byVariant := map[string]map[int]BenchRow{}
	for _, name := range names {
		build := e.Builds[name]
		if name != "cluster" && (build.Mode != "raft" || build.Ack != "memory") {
			return nil, errors.New("before/current must both be Raft memory")
		}
		byVariant[name], err = c.readVariant(name)
		if err != nil {
			return nil, err
		}
	}
	if _, ok := byVariant["baseline"]; !ok {
		return c, nil
	}
	for trial := 1; trial <= 3; trial++ {
		b, a := byVariant["baseline"][trial], byVariant["candidate"][trial]
		c.PairedRate = append(c.PairedRate, change(b.Rate, a.Rate))
		c.PairedP95 = append(c.PairedP95, change(b.P95, a.P95))
	}
	b, a := c.Metrics["baseline"], c.Metrics["candidate"]
	c.RateChange, c.P95Change = change(b.Rate.Median, a.Rate.Median), change(b.P95.Median, a.P95.Median)
	return c, nil
}

func summarize(compare, before, after, cluster, out string, output io.Writer) error {
	c, err := ReadComparison(compare)
	if err != nil {
		return err
	}
	if before != "" || after != "" || cluster != "" {
		c.Diagnostics = map[string]Diagnostic{}
		for name, dir := range map[string]string{"baseline": before, "candidate": after, "cluster": cluster} {
			if dir == "" {
				continue
			}
			if _, ok := c.Environment.Builds[name]; !ok {
				return fmt.Errorf("%s diagnostic has no corresponding formal benchmark", name)
			}
			d, _, _, err := ReadDiagnostic(dir, c.Environment.Builds[name], c.Environment.Harness)
			if err != nil {
				return fmt.Errorf("%s diagnostic: %w", name, err)
			}
			c.Diagnostics[name] = d
		}
	}
	var md strings.Builder
	fmt.Fprintln(&md, "memory · c1/p64 · 256 MiB · SET + WAIT 1 · 每版本三轮")
	fmt.Fprintln(&md)
	fmt.Fprintln(&md, "| 版本 | 确认 MiB/s 中位数（范围） | SET MiB/s | WAIT ms | 批次 P95 ms |")
	fmt.Fprintln(&md, "| --- | ---: | ---: | ---: | ---: |")
	for _, name := range []string{"baseline", "candidate", "cluster"} {
		m, ok := c.Metrics[name]
		if !ok {
			continue
		}
		fmt.Fprintf(&md, "| %s | %.2f（%.2f–%.2f） | %.2f | %.2f | %.2f |\n", name, m.Rate.Median, m.Rate.Min, m.Rate.Max, m.SetRate.Median, m.Tail.Median, m.P95.Median)
	}
	if len(c.PairedRate) == 3 {
		fmt.Fprintf(&md, "\n吞吐 %+.1f%%；P95 %+.1f%%。配对吞吐 %+.1f%% / %+.1f%% / %+.1f%%。\n", c.RateChange, c.P95Change, c.PairedRate[0], c.PairedRate[1], c.PairedRate[2])
	}
	if c.Diagnostics != nil {
		fmt.Fprintln(&md, "\n| 独立诊断（leader / follower） | baseline | candidate | cluster |")
		fmt.Fprintln(&md, "| --- | ---: | ---: | ---: |")
		rows := []struct {
			label string
			value func(Diagnostic) string
		}{
			{"CPU ms", func(d Diagnostic) string {
				return fmt.Sprintf("%.0f / %.0f", d.Nodes["leader"].CPU, d.Nodes["follower"].CPU)
			}},
			{"CPU ms/MiB", func(d Diagnostic) string {
				return fmt.Sprintf("%.2f / %.2f", d.Nodes["leader"].CPUPerMiB, d.Nodes["follower"].CPUPerMiB)
			}},
			{"峰值 RSS MiB", func(d Diagnostic) string {
				return fmt.Sprintf("%.2f / %.2f", d.Nodes["leader"].PeakRSS, d.Nodes["follower"].PeakRSS)
			}},
			{"fsync 区间并集 ms（两节点）", func(d Diagnostic) string { return fmt.Sprintf("%.2f", d.FsyncUnion) }},
			{"follower fsync ∩ await_io ms", func(d Diagnostic) string { return fmt.Sprintf("%.2f", d.Nodes["follower"].FsyncAwait) }},
			{"leader worker 排队并集 ms", func(d Diagnostic) string { return fmt.Sprintf("%.2f", d.Nodes["leader"].WorkerQueue) }},
			{"fsync 中启动编码 / 追加次数", func(d Diagnostic) string {
				return fmt.Sprintf("%d / %d", d.Nodes["leader"].StartsDuringFsync, d.Nodes["follower"].StartsDuringFsync)
			}},
		}
		native := false
		for _, d := range c.Diagnostics {
			native = native || d.Native
		}
		if native {
			rows = rows[:3]
		}
		for _, row := range rows {
			fmt.Fprintf(&md, "| %s |", row.label)
			for _, name := range []string{"baseline", "candidate", "cluster"} {
				value := "—"
				if d, ok := c.Diagnostics[name]; ok {
					value = row.value(d)
				}
				fmt.Fprintf(&md, " %s |", value)
			}
			fmt.Fprintln(&md)
		}
		fmt.Fprintln(&md, "\nCPU 来自独立诊断采样；单次诊断不能证明吞吐收益。")
		for _, name := range []string{"baseline", "candidate"} {
			if id := c.Diagnostics[name].CaptureID; id != "" {
				fmt.Fprintf(&md, "\n%s Trace capture: %s\n", name, id)
			}
		}
	}
	fmt.Fprintln(&md, "\n完整指标、哈希与各角色 CPU top 在 summary.json；保留/回退仍需结合正确性与实验波动判断。")
	if err := os.MkdirAll(filepath.Dir(out), 0755); err != nil {
		return err
	}
	if err := os.Mkdir(out, 0755); err != nil {
		return fmt.Errorf("summary output must be new: %w", err)
	}
	if err := atomicJSON(filepath.Join(out, "summary.json"), c); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(out, "summary.md"), []byte(md.String()), 0644); err != nil {
		return err
	}
	_, err = io.WriteString(output, md.String())
	return err
}
