// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package main

import (
	"bytes"
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
	"strings"

	"dragonflydb.io/profile/models"
	"dragonflydb.io/profile/pkg/langfuse"
	"dragonflydb.io/profile/pkg/trace"
	"dragonflydb.io/profile/pkg/workflow"
)

func diagnosticCLI(ctx context.Context, command string, f *flag.FlagSet, args []string) error {
	base := f.String("url", cmp.Or(os.Getenv("PROFILE_URL"), os.Getenv("RAFT_WORKFLOW_URL")), "platform URL (default http://127.0.0.1:8787)")
	id := f.String("id", "", "experiment ID")
	run := f.String("run", "", "archived diagnostic run ID (default current diagnostic)")
	limit := f.Int("limit", models.DefaultTraceLimit, "maximum rows (0 shows only the summary)")
	asJSON := f.Bool("json", false, "write only the selected results as JSON")
	var role, stage, order, metric, view, verifyRun, analysis, symbol, timeline, scope string
	var minUnion float64
	var offset, callerDepth, batch, fullWaveTickets int
	if command == "trace" {
		f.StringVar(&role, "role", "", "filter by leader, follower or client")
		f.StringVar(&stage, "stage", "", "literal stage prefix, e.g. raft.fetch")
		f.StringVar(&metric, "sort", "union", "sort by union, sum, p50, p95, max or count")
		f.StringVar(&order, "order", "desc", "sort direction (asc or desc)")
		f.IntVar(&offset, "offset", 0, "skip this many matching stages")
		f.Float64Var(&minUnion, "min-union-ms", 0, "minimum union time in milliseconds")
		f.StringVar(&verifyRun, "verify-run", "", "check a completed workflow's observations directly in Langfuse")
		f.StringVar(&analysis, "analysis", "", "apply-to-confirm, request-phases, slow-path, admission-wait or fetch-delay")
		f.StringVar(&scope, "scope", "", "fetch-delay: batch (default) or round. admission-wait: thread (default) or leader")
		f.StringVar(&timeline, "timeline", "", "read a local timeline.json or - for stdin (request analyses)")
		f.IntVar(&batch, "batch", 0, "batch number (default slowest, or all for admission-wait)")
		f.IntVar(&fullWaveTickets, "tickets", 0, "number of tickets in a full admission wave")
	} else {
		f.StringVar(&role, "role", "leader", "profiled node (leader or follower)")
		f.StringVar(&view, "view", "leaf", "CPU hotspot view (leaf or inclusive)")
		f.StringVar(&symbol, "symbol", "", "match a substring and group samples by call path")
		f.IntVar(&callerDepth, "caller-depth", 0, "number of callers to group by (0 uses the full path)")
	}
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("expected flags only")
	}
	if scope != "" {
		switch analysis {
		case "fetch-delay":
			if (scope != "batch" && scope != "round") || (scope == "round" && batch != 0) {
				return errors.New("fetch-delay requires batch or round scope, and round excludes -batch")
			}
		case "admission-wait":
			if scope != "thread" && scope != "leader" {
				return errors.New("admission-wait scope must be thread or leader")
			}
		default:
			return errors.New("-scope requires fetch-delay or admission-wait")
		}
	}
	switch analysis {
	case "request-phases", "slow-path", "admission-wait", "fetch-delay":
		if verifyRun != "" || (timeline != "" && (*id != "" || *run != "")) {
			return errors.New("choose -id ID or -timeline FILE for request analysis")
		}
		if *limit < 0 || batch < 0 {
			return errors.New("limit and batch must be nonnegative")
		}
		if (analysis != "slow-path" && (role != "" || stage != "")) || (analysis == "request-phases" && batch != 0) {
			return errors.New("-role and -stage select slow-path spans; -batch requires a batch analysis")
		}
		if analysis == "admission-wait" && fullWaveTickets <= 0 {
			return errors.New("admission-wait requires -tickets N")
		}
		if analysis != "admission-wait" && fullWaveTickets != 0 {
			return errors.New("-tickets requires admission-wait")
		}
		if metric != "union" && metric != "count" {
			return errors.New("request analysis supports -sort count or the default time-share order")
		}
		if role != "" && role != "leader" && role != "follower" && role != "client" {
			return errors.New("role must be leader, follower or client")
		}
		query := trace.RequestTraceQuery{
			FetchPhases: true, Slow: analysis == "slow-path", ByWrites: metric == "count", Batch: batch, Limit: *limit, Role: role, Stage: stage,
		}
		return requestTraceCLI(ctx, *base, *id, *run, timeline, analysis, scope, query, fullWaveTickets, *asJSON)
	}
	if timeline != "" || batch != 0 || fullWaveTickets != 0 {
		return errors.New("choose a request-phases, slow-path, admission-wait or fetch-delay analysis")
	}
	if analysis != "" && (analysis != models.ApplyToConfirmAnalysis || (role != "" && role != "leader")) {
		return fmt.Errorf("analysis %q is unavailable for role %q", analysis, role)
	}
	if verifyRun != "" {
		if *id != "" || *run != "" || analysis != "" {
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
		params.Set("symbol", symbol)
		params.Set("caller_depth", strconv.Itoa(callerDepth))
		var report models.FlamegraphReport
		if err := client.request(ctx, http.MethodGet, path+"/flamegraph?"+params.Encode(), nil, &report); err != nil {
			return err
		}
		if symbol != "" && report.CallPaths == nil {
			return errors.New("profile server does not support flamegraph call paths")
		}
		if *asJSON {
			return json.NewEncoder(os.Stdout).Encode(report)
		}
		fmt.Printf("%s/%s  %s  CPU %.2f ms  %.2f ms/MiB\n", report.ExperimentID, report.Run, report.Role, report.CPUMS, report.CPUPerMiB)
		fmt.Println(client.base + path + "/artifacts/" + url.PathEscape(report.Artifact))
		if calls := report.CallPaths; calls != nil {
			fmt.Printf("%d/%d samples match %q (%s)\n", calls.Matched, report.Samples, calls.Symbol, report.View)
			for _, call := range calls.Paths {
				fmt.Printf("%6.2f%%  %6d  %s\n", call.Percent, call.Samples, strings.Join(call.Frames, " → "))
			}
			return nil
		}
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
	if analysis == models.ApplyToConfirmAnalysis {
		data, err := platformTimelineForPage(ctx, client, path, page)
		if err != nil {
			return err
		}
		report, err := trace.AnalyzeApplyConfirmation(bytes.NewReader(data))
		if err != nil {
			return err
		}
		page.ApplyToConfirm = &report
	}
	if *asJSON {
		return json.NewEncoder(os.Stdout).Encode(page)
	}
	fmt.Printf("%s/%s  %d events  %d verified observations\n", page.ExperimentID, page.Run, page.SpanCount, page.Verified)
	if page.TraceURL != "" {
		fmt.Println(page.TraceURL)
	}
	if result := page.ApplyToConfirm; result != nil {
		fmt.Printf("Matched %d/%d confirmations across %d apply batches (%d–%d writes per batch)\n",
			result.Matched, result.Confirmations, result.ApplyBatches, result.MinBatchWrites, result.MaxBatchWrites)
		delay := result.PostApply
		fmt.Printf("After apply  p50 %.3f ms  p95 %.3f ms  max %.3f ms  union %.3f ms  %d over 1 ms\n",
			delay.P50MS, delay.P95MS, delay.MaxMS, delay.UnionMS, delay.Over1MS)
		fmt.Printf("Pipeline tails %d/%d  sum %.1f µs  max %.1f µs\n",
			result.PipelineTails, result.PipelineWindows, result.PipelineTailSumUS, result.PipelineTailMaxUS)
		fmt.Printf("Worker queue overlapping apply %.3f ms  tickets %d  peak in flight %d\n",
			result.WorkerQueueInApply, result.Tickets, result.PeakInFlight)
		return nil
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
