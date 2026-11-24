// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package main

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strconv"

	"dragonflydb.io/profile/models"
	"dragonflydb.io/profile/pkg/langfuse"
	"dragonflydb.io/profile/pkg/workflow"
)

func diagnosticCLI(ctx context.Context, command string, f *flag.FlagSet, args []string) error {
	base := f.String("url", cmp.Or(os.Getenv("PROFILE_URL"), os.Getenv("RAFT_WORKFLOW_URL")), "platform URL (default http://127.0.0.1:8787)")
	id := f.String("id", "", "experiment ID")
	run := f.String("run", "", "archived diagnostic run ID (default current diagnostic)")
	limit := f.Int("limit", models.DefaultTraceLimit, "maximum rows (0 shows only the summary)")
	asJSON := f.Bool("json", false, "write only the selected results as JSON")
	var role, stage, order, metric, view, verifyRun string
	var minUnion float64
	var offset int
	if command == "trace" {
		f.StringVar(&role, "role", "", "filter by leader, follower or client")
		f.StringVar(&stage, "stage", "", "literal stage prefix, e.g. raft.fetch")
		f.StringVar(&metric, "sort", "union", "sort by union, sum, p50, p95, max or count")
		f.StringVar(&order, "order", "desc", "sort direction (asc or desc)")
		f.IntVar(&offset, "offset", 0, "skip this many matching stages")
		f.Float64Var(&minUnion, "min-union-ms", 0, "minimum union time in milliseconds")
		f.StringVar(&verifyRun, "verify-run", "", "check a completed workflow's observations directly in Langfuse")
	} else {
		f.StringVar(&role, "role", "leader", "profiled node (leader or follower)")
		f.StringVar(&view, "view", "leaf", "CPU hotspot view (leaf or inclusive)")
	}
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("expected flags only")
	}
	if verifyRun != "" {
		if *id != "" || *run != "" {
			return errors.New("choose trace -id ID or trace -verify-run DIR")
		}
		return verifyTrace(ctx, verifyRun, *asJSON)
	}
	if *id == "" {
		return errors.New("-id ID is required")
	}
	client, err := newPlatformClient(*base)
	if err != nil {
		return err
	}
	path := experimentsAPI + "/" + url.PathEscape(*id)
	params := url.Values{"run": {*run}, "role": {role}, "limit": {strconv.Itoa(*limit)}}
	if command == "flamegraph" {
		params.Set("view", view)
		var report models.FlamegraphReport
		if err := client.request(ctx, http.MethodGet, path+"/flamegraph?"+params.Encode(), nil, &report); err != nil {
			return err
		}
		if *asJSON {
			return json.NewEncoder(os.Stdout).Encode(report)
		}
		fmt.Printf("%s/%s  %s  CPU %.2f ms  %.2f ms/MiB\n", report.ExperimentID, report.Run, report.Role, report.CPUMS, report.CPUPerMiB)
		fmt.Println(client.base + path + "/artifacts/" + url.PathEscape(report.Artifact))
		if report.Samples > 0 {
			fmt.Printf("%d samples, %s hotspots\n", report.Samples, report.View)
		}
		for _, spot := range report.Hotspots {
			fmt.Printf("%6.2f%%  %6d  %s\n", spot.Percent, spot.Samples, spot.Name)
		}
		return nil
	}
	params.Set("stage", stage)
	params.Set("sort", metric)
	params.Set("order", order)
	params.Set("offset", strconv.Itoa(offset))
	params.Set("min_union_ms", strconv.FormatFloat(minUnion, 'g', -1, 64))
	var page models.TracePage
	if err := client.request(ctx, http.MethodGet, path+"/trace?"+params.Encode(), nil, &page); err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(os.Stdout).Encode(page)
	}
	fmt.Printf("%s/%s  %d events  %d verified observations\n", page.ExperimentID, page.Run, page.SpanCount, page.Verified)
	if page.TraceURL != "" {
		fmt.Println(page.TraceURL)
	}
	fmt.Printf("%d/%d stages, offset %d, sort %s %s\n", len(page.Stages), page.Total, page.Offset, page.Sort, page.Order)
	for _, stage := range page.Stages {
		fmt.Printf("%-8s %-34s  union %.2f ms  p95 %.2f ms  %d spans", stage.Role, stage.Name, stage.Union, stage.P95, stage.Count)
		switch page.Sort {
		case "sum":
			fmt.Printf("  sum %.2f ms", stage.Sum)
		case "p50":
			fmt.Printf("  p50 %.2f ms", stage.P50)
		case "max":
			fmt.Printf("  max %.2f ms", stage.Max)
		}
		fmt.Println()
	}
	return nil
}

func verifyTrace(ctx context.Context, run string, asJSON bool) error {
	files, err := workflow.CurrentArtifacts(run)
	if err != nil {
		return err
	}
	state, err := workflow.ReadState(run)
	if err != nil {
		return err
	}
	var config langfuse.Config
	if state.Config.Langfuse != nil {
		config = *state.Config.Langfuse
	}
	receipt, err := langfuse.Verify(ctx, filepath.Dir(files.Report), config)
	if err != nil {
		return err
	}
	if asJSON {
		return json.NewEncoder(os.Stdout).Encode(receipt)
	}
	fmt.Printf("%d events, %d Langfuse observations verified\n%s\n", receipt.FullSpans, receipt.Verified, receipt.URL)
	return nil
}
