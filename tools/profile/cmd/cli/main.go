// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"dragonflydb.io/profile/models"
	"dragonflydb.io/profile/pkg/workflow"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := cli(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "profile:", err)
		os.Exit(1)
	}
}

func cli(ctx context.Context, args []string) error {
	if len(args) == 0 {
		return errors.New("commands: create, run, show, captures, fetch-delay, slow-path, trace, flamegraph, delete, refresh, import, import-target, backfill, sync, plan, status, report, summarize, executor")
	}
	f := flag.NewFlagSet(args[0], flag.ContinueOnError)
	dsnFile := cmp.Or(os.Getenv("PROFILE_DSN_FILE"), os.Getenv("RAFT_WORKFLOW_DSN_FILE"))
	switch args[0] {
	case "captures", "fetch-delay", "slow-path":
		return nativeTraceCLI(ctx, args[0], f, args[1:])
	case "executor":
		return executorCLI(ctx, f, args[1:], dsnFile)
	case "create", "run", "show", "delete":
		return experimentCLI(ctx, args[0], f, args[1:])
	case "trace", "flamegraph":
		return diagnosticCLI(ctx, args[0], f, args[1:])
	case "refresh":
		id := f.String("id", "", "completed experiment ID")
		run := f.String("run", "", "completed workflow output directory")
		data := f.String("data", models.DefaultRoot, "directory of experiment artifacts")
		dsn := f.String("dsn-file", dsnFile, "PostgreSQL connection file")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		if f.NArg() != 0 || *id == "" || *run == "" || *dsn == "" {
			return errors.New("refresh requires -id ID, -run DIR and -dsn-file FILE")
		}
		pg, err := models.OpenPostgres(ctx, *data, *dsn)
		if err != nil {
			return err
		}
		defer pg.Close()
		summary, err := pg.RefreshCurrent(ctx, *id, *run)
		if err != nil {
			return err
		}
		printExperiment(summary)
		return nil
	case "import":
		data := f.String("data", models.DefaultRoot, "destination directory of experiment records")
		dsn := f.String("dsn-file", dsnFile, "PostgreSQL connection file")
		config := f.String("config", "", "JSON with experiment identity and benchmark/report paths")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		if f.NArg() != 0 || *config == "" {
			return errors.New("import requires -config FILE")
		}
		c, err := models.ReadImportConfig(*config)
		if err != nil {
			return err
		}
		var summary models.Summary
		if *dsn != "" {
			pg, openErr := models.OpenPostgres(ctx, *data, *dsn)
			if openErr != nil {
				return openErr
			}
			defer pg.Close()
			summary, err = pg.Import(ctx, c)
		} else {
			summary, err = (models.Store{Root: *data}).Import(c)
		}
		if err != nil {
			return err
		}
		e := summary.Evidence
		fmt.Printf("%s: %d benchmark runs, %d diagnostics, %d flamegraphs\n", summary.Experiment.ID, e.Benchmark, e.Diagnostics, e.Flamegraphs)
		return nil
	case "import-target":
		data := f.String("data", models.DefaultRoot, "destination directory of experiment records")
		id := f.String("id", "", "comparison target ID")
		title := f.String("title", "", "comparison target title")
		archive := f.String("compare", "", "completed benchmark directory")
		variant := f.String("variant", "", "variant in the benchmark (e.g. memory or cluster)")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		if f.NArg() != 0 {
			return errors.New("import-target expects flags only")
		}
		target, err := (models.Store{Root: *data}).ImportTarget(*id, *title, *archive, *variant)
		if err != nil {
			return err
		}
		fmt.Printf("%s: %.2f MiB/s from %s/%s\n", target.ID, target.Metrics.Rate.Median, target.Archive, target.Variant)
		return nil
	case "backfill", "sync":
		data := f.String("data", models.DefaultRoot, "directory of experiment records and artifacts")
		dsn := f.String("dsn-file", dsnFile, "PostgreSQL connection file")
		archive := f.String("archive", "docs/cluster/benchmarks", "root of benchmark archives")
		if err := f.Parse(args[1:]); err != nil {
			return err
		}
		if f.NArg() != 0 || *dsn == "" {
			return errors.New("backfill and sync require -dsn-file FILE")
		}
		pg, err := models.OpenPostgres(ctx, *data, *dsn)
		if err != nil {
			return err
		}
		defer pg.Close()
		if args[0] == "backfill" {
			return pg.Backfill(ctx, *archive, os.Stdout)
		}
		n, err := pg.Sync(ctx)
		if err != nil {
			return err
		}
		fmt.Printf("PostgreSQL: %d experiments synchronized\n", n)
		return nil
	default:
		return workflow.CLI(ctx, args)
	}
}
