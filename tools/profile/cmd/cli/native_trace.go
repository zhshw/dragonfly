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
	"strconv"
	"time"

	"dragonflydb.io/profile/pkg/langfuse"
)

func nativeTraceCLI(ctx context.Context, command string, flags *flag.FlagSet, args []string) error {
	base := flags.String("url", cmp.Or(os.Getenv("PROFILE_URL"), os.Getenv("RAFT_WORKFLOW_URL")), "profile server URL")
	id := flags.String("id", "", "experiment ID")
	capture := flags.String("capture", "", "native capture ID (default latest)")
	attribute := flags.String("attribute", "", "Fetch request attribute to sort by (fetch-delay only)")
	name := flags.String("name", "", "exact span name")
	node := flags.String("node", "", "node ID")
	group := flags.String("group", "", "group ID")
	outcome := flags.String("outcome", "", "outcome filter")
	partial := flags.String("partial", "", "partial response filter: 0 or 1")
	start := flags.Int64("start-ns", 0, "minimum original request start time")
	end := flags.Int64("end-ns", 0, "maximum original request start time")
	minimum := "0"
	thresholds := map[string]string{}
	if command == "slow-path" {
		thresholds["min-duration-ms"] = ""
		flags.String("min-duration-ms", "0", "minimum event duration in ms")
	} else if command == "fetch-delay" {
		for _, field := range []struct{ flag, attribute, unit string }{
			{"min-return-wait-ms", "raft.fetch.return_wait", "ms"},
			{"min-duration-ms", "raft.fetch", "ms"},
			{"min-rpc-ms", "raft.fetch.rpc", "ms"},
			{"min-receive-ms", "raft.fetch.receive", "ms"},
			{"min-gate-wait-ms", "raft.fetch.await_flush", "ms"},
			{"min-io-wait-ms", "raft.await_io", "ms"},
			{"min-fsync-overlap-ms", "raft.fetch.fsync_overlap", "ms"},
			{"min-io-fsync-overlap-ms", "raft.fetch.io_fsync_overlap", "ms"},
			{"min-payload-bytes", "raft.fetch.payload_bytes", "bytes"},
			{"min-appended-bytes", "raft.fetch.appended_bytes", "bytes"},
			{"min-appended-records", "raft.fetch.appended_records", "records"},
			{"min-gate-rejections", "raft.fetch.flush_gate.rejections", "count"},
		} {
			thresholds[field.flag] = field.attribute
			flags.String(field.flag, "0", "minimum "+field.attribute+" in "+field.unit+"; rank by this field")
		}
	}
	limit := flags.Int("limit", 10, "maximum rows (1-100)")
	selected := flags.String("selected", "", "trace_id:span_id for evidence detail")
	asJSON := flags.Bool("json", false, "print response JSON")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 || *id == "" {
		return errors.New("-id is required")
	}
	if command == "slow-path" && (*attribute != "" || *partial != "") {
		return errors.New("slow-path ranks event duration; use -name to filter events, or fetch-delay for request attributes")
	}
	var threshold string
	var thresholdErr error
	flags.Visit(func(f *flag.Flag) {
		field, ok := thresholds[f.Name]
		if !ok {
			return
		}
		if threshold != "" {
			thresholdErr = errors.New("choose one minimum field; it also determines the ranking")
		} else if *attribute != "" && *attribute != field {
			thresholdErr = fmt.Errorf("-%s filters %s; -attribute must use the same field", f.Name, field)
		} else {
			*attribute = field
			minimum = f.Value.String()
		}
		threshold = f.Name
	})
	if thresholdErr != nil {
		return thresholdErr
	}
	client, err := newPlatformClient(*base)
	if err != nil {
		return err
	}
	client.http.Timeout = 90 * time.Second
	path := experimentsAPI + "/" + url.PathEscape(*id)
	if command == "captures" || *capture == "" {
		var result struct {
			Captures []langfuse.NativeCapture `json:"captures"`
		}
		if err := client.request(ctx, http.MethodGet, path+"/captures", nil, &result); err != nil {
			return err
		}
		if command == "captures" {
			if *asJSON {
				return json.NewEncoder(os.Stdout).Encode(result)
			}
			for _, c := range result.Captures {
				fmt.Printf("%s  %s  %d streams  %s\n", c.ID, c.Started, c.Streams, c.BinarySHA)
			}
			return nil
		}
		if len(result.Captures) == 0 {
			return errors.New("no native captures; legacy timeline files are not used")
		}
		*capture = result.Captures[0].ID
	}
	params := url.Values{"start_ns": {strconv.FormatInt(*start, 10)}, "end_ns": {strconv.FormatInt(*end, 10)}, "capture": {*capture}, "attribute": {*attribute}, "name": {*name}, "node": {*node}, "group": {*group}, "outcome": {*outcome}, "partial": {*partial}, "min": {minimum}, "limit": {strconv.Itoa(*limit)}, "selected": {*selected}}
	endpoint := "/events"
	if command == "fetch-delay" {
		endpoint = "/requests"
	}
	var report langfuse.NativeReport
	if err := client.request(ctx, http.MethodGet, path+endpoint+"?"+params.Encode(), nil, &report); err != nil {
		return err
	}
	if *asJSON {
		return json.NewEncoder(os.Stdout).Encode(report)
	}
	fmt.Printf("%s  complete=%t  %d matching  %d missing %s\n", report.Capture, report.Complete, report.Total, report.Missing, cmp.Or(report.Attribute, "duration"))
	for _, issue := range report.Issues {
		fmt.Println("Incomplete:", issue)
	}
	for _, row := range report.Rows {
		label, value := report.Attribute, nativeValueText(row, report.Attribute, report.Units)
		if report.Sort == "duration" {
			label = "duration"
			number, err := langfuse.NativeNumber(row.Duration)
			if err != nil {
				return err
			}
			value = number.FloatString(3) + " ms"
		}
		fmt.Printf("%s:%s  %s node=%s group=%s  %s=%s", row.TraceID, row.SpanID, row.Name, row.Fields["node_id"], row.Fields["group_id"], label, value)
		if command == "fetch-delay" {
			fmt.Printf("  offset=%s..%s rpc=%s return_wait=%s gate=%s io=%s fsync=%s appended=%s/%s", row.Fields["first_offset"], row.Fields["last_offset"], nativeValueText(row, "raft.fetch.rpc", report.Units), nativeValueText(row, "raft.fetch.return_wait", report.Units), nativeValueText(row, "raft.fetch.await_flush", report.Units), nativeValueText(row, "raft.await_io", report.Units), nativeValueText(row, "raft.fetch.fsync_overlap", report.Units), row.Values["raft.fetch.appended_bytes"], row.Values["raft.fetch.payload_bytes"])
		}
		fmt.Println()
	}
	for _, event := range report.Events {
		start, _ := strconv.ParseInt(event.Start, 10, 64)
		end, _ := strconv.ParseInt(event.End, 10, 64)
		fmt.Printf("  %-36s %.3f ms  %s  reason=%s io=%s flush=%s\n", event.Name, float64(end-start)/1e6, event.Fields["outcome"], event.Fields["reason"], event.Fields["io_id"], event.Fields["flush_id"])
	}
	return nil
}

func nativeValueText(event langfuse.NativeEvent, name string, units map[string]string) string {
	value, ok := event.Values[name]
	if !ok || event.States[name] != "Observed" {
		return cmp.Or(event.States[name], "Missing")
	}
	if units[name] == "ms" {
		number, err := langfuse.NativeNumber(value)
		if err != nil {
			return "Invalid"
		}
		return number.FloatString(3) + " ms"
	}
	return value + " " + units[name]
}
