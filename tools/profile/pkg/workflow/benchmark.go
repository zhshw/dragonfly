// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package workflow

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"dragonflydb.io/profile/pkg/benchmark"
)

type benchmarkOptions struct {
	build, baseline, output, ack string
	clients, depth               int
	cluster                      bool
	ackModes                     []string
}

func benchmarkCLI(ctx context.Context, f *flag.FlagSet, args []string) error {
	o := benchmarkOptions{}
	f.StringVar(&o.build, "build-dir", "", "Release directory containing dragonfly and raft_storage")
	f.StringVar(&o.baseline, "baseline-build", "", "optional frozen baseline build")
	f.StringVar(&o.output, "output-dir", "", "new benchmark output directory")
	f.StringVar(&o.ack, "ack-mode", "memory", "Raft acknowledgement mode: memory or flush")
	modes := f.String("ack-modes", "", "compare comma-separated acknowledgement modes using one binary")
	f.IntVar(&o.clients, "clients", 1, "reserved writing connections")
	f.IntVar(&o.depth, "pipeline-depth", 64, "SET commands per pipeline batch")
	f.BoolVar(&o.cluster, "include-cluster", false, "also measure the original Cluster")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || o.build == "" || o.output == "" {
		return errors.New("benchmark requires -build-dir and -output-dir")
	}
	if *modes != "" {
		o.ackModes = strings.Split(*modes, ",")
	}
	return runBenchmark(ctx, o, os.Stdout)
}

func workloadConfig(binary, output, mode, ack string, log io.Writer) benchmark.Config {
	w := BenchmarkWorkload()
	return benchmark.Config{Binary: binary, Output: output, Mode: mode, Ack: ack, Clients: w.Clients, Depth: w.Depth,
		Payload: w.Payload, Warmup: w.Warmup, Settle: time.Duration(w.Settle * float64(time.Second)), Log: log}
}

func runBenchmark(ctx context.Context, o benchmarkOptions, log io.Writer) error {
	if o.clients < 1 || o.clients > 1024 || o.depth < 1 || (o.ack != "memory" && o.ack != "flush") {
		return errors.New("invalid benchmark clients, pipeline depth or acknowledgement mode")
	}
	if len(o.ackModes) > 0 && o.baseline != "" {
		return errors.New("ack-modes requires one binary without baseline-build")
	}
	paths := map[string]string{"candidate": o.build}
	order := []string{"candidate"}
	acks := map[string]string{"candidate": o.ack}
	if len(o.ackModes) > 0 {
		paths, acks, order = map[string]string{}, map[string]string{}, nil
		for _, mode := range o.ackModes {
			if (mode != "memory" && mode != "flush") || paths[mode] != "" {
				return errors.New("ack-modes must contain distinct memory/flush values")
			}
			paths[mode] = o.build
			acks[mode] = mode
			order = append(order, mode)
		}
	}
	if o.baseline != "" {
		paths["baseline"] = o.baseline
		acks["baseline"] = o.ack
		order = append([]string{"baseline"}, order...)
	}
	if o.cluster {
		paths["cluster"] = o.build
		order = append(order, "cluster")
	}
	harness, err := benchmark.HarnessSHA("")
	if err != nil {
		return err
	}
	env := Environment{Runner: "go", Builds: map[string]BuildInfo{}, Harness: harness, Metric: "replication_mib_per_s", Clients: o.clients, Depth: o.depth, Distinct: true}
	for label, dir := range paths {
		path, e := filepath.Abs(filepath.Join(dir, "dragonfly"))
		if e != nil {
			return e
		}
		paths[label] = path
		sha, e := hashFile(path)
		if e != nil {
			return e
		}
		mode := "raft"
		if label == "cluster" {
			mode = "yes"
		}
		env.Builds[label] = BuildInfo{SHA: sha, Mode: mode, Ack: acks[label]}
	}
	if o.output, err = filepath.Abs(o.output); err != nil {
		return err
	}
	if err = os.Mkdir(o.output, 0755); err != nil {
		return err
	}
	mount, err := capture(ctx, "", log, "findmnt", "-no", "SOURCE,FSTYPE,OPTIONS", "-T", o.output)
	if err != nil {
		return err
	}
	env.Mount = strings.TrimSpace(mount)
	if fields := strings.Fields(env.Mount); len(fields) < 2 || fields[1] == "tmpfs" || fields[1] == "ramfs" {
		return errors.New("benchmark output must use a real disk filesystem")
	}
	if err = atomicJSON(filepath.Join(o.output, "environment.json"), env); err != nil {
		return err
	}
	var rows []BenchRow
	for trial := 0; trial < 3; trial++ {
		shift := (trial + o.clients + o.depth) % len(order)
		for i := range order {
			label := order[(i+shift)%len(order)]
			build := env.Builds[label]
			name := fmt.Sprintf("trial-%d-%s-clients-%d-pipeline-%d", trial+1, label, o.clients, o.depth)
			file, e := os.OpenFile(filepath.Join(o.output, name+".log"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
			if e != nil {
				return e
			}
			fmt.Fprintf(log, "%s\n", name)
			c := workloadConfig(paths[label], filepath.Join(o.output, name), build.Mode, build.Ack, file)
			c.Clients, c.Depth = o.clients, o.depth
			roundCtx, cancel := context.WithTimeout(ctx, 4*time.Minute)
			row, e := benchmark.Run(roundCtx, c)
			cancel()
			if e != nil {
				fmt.Fprintln(file, e)
				file.Close()
				return fmt.Errorf("%s: %w", name, e)
			}
			row.Order, row.Trial, row.Variant = len(rows)+1, trial+1, label
			if e = json.NewEncoder(file).Encode(row); e != nil {
				file.Close()
				return e
			}
			if e = file.Close(); e != nil {
				return e
			}
			if e = atomicJSON(filepath.Join(c.Output, "result.json"), row); e != nil {
				return e
			}
			rows = append(rows, BenchRow(row))
			if e = json.NewEncoder(log).Encode(row); e != nil {
				return e
			}
			if e = atomicJSON(filepath.Join(o.output, "summary.json"), rows); e != nil {
				return e
			}
			if e = writeRoundsCSV(filepath.Join(o.output, "rounds.csv"), rows); e != nil {
				return e
			}
		}
	}
	return nil
}

func writeRoundsCSV(path string, rows []BenchRow) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	w := csv.NewWriter(file)
	var header []string
	shape := reflect.TypeOf(BenchRow{})
	for i := 0; i < shape.NumField(); i++ {
		header = append(header, strings.Split(shape.Field(i).Tag.Get("json"), ",")[0])
	}
	if err = w.Write(header); err == nil {
		for _, row := range rows {
			data, e := json.Marshal(row)
			if e != nil {
				err = e
				break
			}
			var values map[string]json.RawMessage
			if e = json.Unmarshal(data, &values); e != nil {
				err = e
				break
			}
			cells := make([]string, len(header))
			for i, name := range header {
				value := values[name]
				if len(value) == 0 || string(value) == "null" {
					continue
				}
				if value[0] == '"' {
					_ = json.Unmarshal(value, &cells[i])
				} else {
					cells[i] = string(value)
				}
			}
			if err = w.Write(cells); err != nil {
				break
			}
		}
	}
	w.Flush()
	return errors.Join(err, w.Error(), file.Close())
}

func requireBenchmarkSource(repo string) error {
	actual, err := benchmark.HarnessSHA(repo)
	if err != nil {
		return err
	}
	embedded, err := benchmark.HarnessSHA("")
	if err != nil {
		return err
	}
	if actual != embedded {
		return errors.New("benchmark source changed; rebuild profile before running")
	}
	return nil
}
