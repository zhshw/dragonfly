// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package main

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"

	"dragonflydb.io/profile/models"
	"dragonflydb.io/profile/pkg/trace"
)

func openTrace(ctx context.Context, base, id, run, timeline string) (io.ReadCloser, error) {
	switch timeline {
	case "-":
		return io.NopCloser(os.Stdin), nil
	case "":
		if id == "" {
			return nil, errors.New("trace analysis requires -id ID or -timeline FILE")
		}
		client, err := newPlatformClient(base)
		if err != nil {
			return nil, err
		}
		path, err := platformTimelinePath(ctx, client, id, run)
		if err != nil {
			return nil, err
		}
		var data json.RawMessage
		err = client.request(ctx, http.MethodGet, path, nil, &data)
		if err != nil {
			return nil, err
		}
		return io.NopCloser(bytes.NewReader(data)), nil
	default:
		return os.Open(timeline)
	}
}

func requestTraceCLI(ctx context.Context, base, id, run, timeline, analysis, scope string, query trace.RequestTraceQuery,
	tickets int, asJSON bool) error {
	if analysis == "fetch-delay" {
		report, err := fetchDelayQuery(ctx, base, id, run, timeline, scope, query)
		if err != nil {
			return err
		}
		return printFetchDelay(report, asJSON)
	}
	source, err := openTrace(ctx, base, id, run, timeline)
	if err != nil {
		return err
	}
	defer source.Close()
	switch analysis {
	case "admission-wait":
		report, err := trace.AnalyzeAdmissionTraceWithScope(source, query.Batch, tickets, query.Limit, cmp.Or(scope, "thread"))
		if err != nil {
			return err
		}
		if asJSON {
			return json.NewEncoder(os.Stdout).Encode(report)
		}
		fmt.Printf("%d/%d waits retired %d tickets (%s scope); %.3f ms without a WAL completion after the first retirement (%s)\n",
			report.FullWaveWaits, report.Waits, report.FullWaveTickets, report.TicketScope, report.FullWaveWALIdleMS, report.WALStage)
		for _, wait := range report.Longest {
			fmt.Printf("Batch %d  wait %.3f ms  %d retired  %.3f ms until admission resumed after retirement/WAL completion\n",
				wait.Batch, float64(wait.EndNS-wait.BeginNS)/1e6, wait.Retired, wait.WALIdleMS)
		}
		return nil

	}
	report, err := trace.AnalyzeRequestTrace(source, query)
	if err != nil {
		return err
	}
	if asJSON {
		return json.NewEncoder(os.Stdout).Encode(report)
	}
	fmt.Printf("SET %.3f ms", report.SetMS)
	if report.WaitMS != nil {
		fmt.Printf("  WAIT %.3f ms", *report.WaitMS)
	}
	fmt.Printf("  selected %d batches, %d writes\n", report.Batches, report.Writes)
	fmt.Println("Time share      Time        Phase                      Writes affected")
	for _, phase := range report.Phases {
		fmt.Printf("%6.2f%%  %9.3f ms  %-25s  %d (%.2f%%)\n",
			phase.Percent, phase.MS, phase.Name, phase.Writes, phase.WritePercent)
	}
	if slow := report.Slow; slow != nil {
		fmt.Printf("\nBatch %d  %.3f ms\n", slow.Batch, float64(slow.EndNS-slow.BeginNS)/1e6)
		for _, gap := range slow.ReplyGaps {
			fmt.Printf("Offset %d  %.3f ms", gap.Offset, float64(gap.EndNS-gap.BeginNS)/1e6)
			if f := gap.Fetch; f != nil {
				fmt.Printf("  Fetch %d–%d RPC-to-receive %.3f ms", f.FirstOffset, f.LastOffset, float64(f.ReceiveStartNS-f.RPCReturnNS)/1e6)
			}
			for _, phase := range gap.Phases {
				fmt.Printf("  %s %.3f ms", phase.Name, phase.MS)
			}
			fmt.Println()
		}
		if query.Role != "" || query.Stage != "" {
			fmt.Printf("\n%d/%d matching spans (times relative to the trace start)\n", len(slow.Spans), slow.SpanMatches)
			for _, span := range slow.Spans {
				fmt.Printf("%10.3f ms  %9.3f ms  %-8s %-34s  node %s tid %d  arg0 %d arg1 %d\n",
					span.Time/1000, span.Dur/1000, span.Cat, span.Name, span.Node, span.TID, span.Args["arg0"], span.Args["arg1"])
			}
		}
	}
	return nil
}

func fetchDelayQuery(ctx context.Context, base, id, run, file, scope string, query trace.RequestTraceQuery) (trace.FetchDelayReport, error) {
	var report trace.FetchDelayReport
	if file != "" {
		source, err := openTrace(ctx, base, id, run, file)
		if err != nil {
			return report, err
		}
		defer source.Close()
		if scope == "round" {
			return trace.AnalyzeRoundFetchDelay(source, query.Limit)
		}
		return trace.AnalyzeFetchDelay(source, query.Batch, query.Limit)
	}
	if id == "" {
		return report, errors.New("fetch-delay requires -id ID or -timeline FILE")
	}
	client, err := newPlatformClient(base)
	if err != nil {
		return report, err
	}
	path, err := platformTimelinePath(ctx, client, id, run)
	if err != nil {
		return report, err
	}
	params := url.Values{"scope": {scope}, "batch": {strconv.Itoa(query.Batch)}, "limit": {strconv.Itoa(query.Limit)}}
	err = client.request(ctx, http.MethodGet, path+"/fetch-delay?"+params.Encode(), nil, &report)
	return report, err
}

func printFetchDelay(report trace.FetchDelayReport, asJSON bool) error {
	if asJSON {
		return json.NewEncoder(os.Stdout).Encode(report)
	}
	label := "Round"
	if report.Batch != 0 {
		label = fmt.Sprintf("Batch %d", report.Batch)
	}
	fmt.Printf("%s  %d data Fetches, %d partial, %d RPC-to-receive gaps > 1 ms, %d partial blocked by flush\n",
		label, report.Fetches, report.Partial, report.GapsOver1MS, report.PartialBlockedByFlush)
	for _, delay := range report.Delays {
		fmt.Printf("Offsets %d–%d  node %s  %.3f ms  %d/%d bytes appended  await_io overlaps fsync: %t  partial blocked by flush: %t\n",
			delay.FirstOffset, delay.LastOffset, delay.Node, delay.RPCToReceiveMS,
			delay.AppendedBytes, delay.PayloadBytes, delay.AwaitFsync, delay.PartialBlockedByFlush)
	}
	return nil
}

func platformTimelinePath(ctx context.Context, client platformClient, id, run string) (string, error) {
	path := experimentsAPI + "/" + url.PathEscape(id)
	params := url.Values{"run": {run}, "limit": {"0"}}
	var page models.TracePage
	if err := client.request(ctx, http.MethodGet, path+"/trace?"+params.Encode(), nil, &page); err != nil {
		return "", err
	}
	return timelinePathForPage(ctx, client, path, page)
}

func platformTimelineForPage(ctx context.Context, client platformClient, path string, page models.TracePage) (json.RawMessage, error) {
	path, err := timelinePathForPage(ctx, client, path, page)
	if err != nil {
		return nil, err
	}
	var data json.RawMessage
	err = client.request(ctx, http.MethodGet, path, nil, &data)
	return data, err
}

func timelinePathForPage(ctx context.Context, client platformClient, path string, page models.TracePage) (string, error) {
	var detail models.Detail
	if err := client.request(ctx, http.MethodGet, path, nil, &detail); err != nil {
		return "", err
	}
	for _, observation := range detail.Observations {
		if observation.Variant == page.Variant && observation.Timeline != "" {
			return path + "/artifacts/" + url.PathEscape(observation.Timeline), nil
		}
	}
	return "", fmt.Errorf("no full timeline for %s/%s", page.ExperimentID, page.Run)
}
