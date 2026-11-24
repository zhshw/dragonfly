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
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"dragonflydb.io/profile/models"
	"dragonflydb.io/profile/pkg/execution"
	"dragonflydb.io/profile/pkg/workflow"
)

const experimentsAPI = "/api/v1/experiments"

func experimentCLI(ctx context.Context, command string, f *flag.FlagSet, args []string) error {
	var create models.CreateRequest
	var config string
	var restart, resume, asJSON bool
	base := f.String("url", cmp.Or(os.Getenv("PROFILE_URL"), os.Getenv("RAFT_WORKFLOW_URL")), "platform URL (default http://127.0.0.1:8787)")
	id := f.String("id", "", "experiment ID")
	switch command {
	case "create":
		f.StringVar(&create.Title, "title", "", "experiment title")
		f.StringVar(&create.Description, "description", "", "experiment description")
		f.StringVar(&create.Optimization, "optimization", "", "optimization ID")
		f.StringVar(&create.Targets.Previous, "previous", "", "previous experiment ID, e.g. v10")
		f.StringVar(&create.Targets.Cumulative, "baseline", "", "cumulative baseline target ID")
		f.StringVar(&create.Targets.Reference, "reference", "", "reference target ID")
	case "run":
		f.StringVar(&config, "config", "", "standalone workflow config file")
		f.BoolVar(&resume, "resume", false, "resume a standalone checkpoint")
		f.BoolVar(&restart, "restart", false, "start the platform experiment from the beginning")
	case "show":
		f.BoolVar(&asJSON, "json", false, "write experiment detail JSON")
	}
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 {
		return errors.New("expected flags only")
	}
	if config != "" {
		if *id != "" || restart {
			return errors.New("choose run -config FILE or run -id ID")
		}
		loaded, err := workflow.LoadConfig(config)
		if err != nil {
			return err
		}
		return workflow.Run(ctx, loaded, resume, os.Stdout)
	}
	if *id == "" {
		return errors.New("-id ID is required")
	}
	if resume {
		return errors.New("run -id resumes automatically; -resume applies to run -config")
	}
	client, err := newPlatformClient(*base)
	if err != nil {
		return err
	}
	path := experimentsAPI + "/" + url.PathEscape(*id)
	switch command {
	case "create":
		create.ID = *id
		var summary models.Summary
		if err := client.request(ctx, http.MethodPost, experimentsAPI, create, &summary); err != nil {
			return err
		}
		fmt.Printf("Created %s\n", summary.Experiment.ID)
	case "run":
		return client.run(ctx, path, restart)
	case "delete":
		var result models.Deletion
		if err := client.request(ctx, http.MethodDelete, path, nil, &result); err != nil {
			return err
		}
		fmt.Printf("Deleted %s\n", *id)
		if result.Warning != "" {
			fmt.Fprintln(os.Stderr, result.Warning)
		}
	case "show":
		var detail models.Detail
		if err := client.request(ctx, http.MethodGet, path, nil, &detail); err != nil {
			return err
		}
		if asJSON {
			return json.NewEncoder(os.Stdout).Encode(detail)
		}
		printExperiment(detail.Summary)
		for _, run := range detail.Runs {
			if run.Kind == "benchmark" && run.Set == "main" && run.Variant == "current" {
				fmt.Printf("Round %d  %.2f MiB/s  P95 %.2f ms\n", run.Trial, run.Rate, run.P95)
			}
		}
	}
	return nil
}

type platformClient struct {
	base string
	http *http.Client
}

func newPlatformClient(base string) (platformClient, error) {
	if base == "" {
		base = "http://127.0.0.1:8787"
	}
	u, err := url.Parse(base)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery != "" || u.Fragment != "" {
		return platformClient{}, errors.New("invalid platform URL")
	}
	return platformClient{strings.TrimRight(base, "/"), &http.Client{Timeout: 15 * time.Second}}, nil
}

func (c platformClient) request(ctx context.Context, method, path string, in, out any) error {
	var body io.Reader
	if in != nil {
		b, err := json.Marshal(in)
		if err != nil {
			return err
		}
		body = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, body)
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "application/json")
	if in != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode >= 300 {
		var response struct {
			Error string `json:"error"`
		}
		_ = json.NewDecoder(io.LimitReader(res.Body, 4096)).Decode(&response)
		return fmt.Errorf("HTTP %d %s", res.StatusCode, response.Error)
	}
	if out == nil || res.StatusCode == http.StatusNoContent {
		return nil
	}
	return json.NewDecoder(res.Body).Decode(out)
}

func (c platformClient) run(ctx context.Context, path string, restart bool) error {
	execute := path + "/execution"
	if err := c.request(ctx, http.MethodPost, execute, map[string]bool{"restart": restart}, nil); err != nil {
		return err
	}
	last := ""
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		var status execution.Progress
		if err := c.request(ctx, http.MethodGet, execute, nil, &status); err != nil {
			return err
		}
		if status.Run == nil {
			return errors.New("execution disappeared")
		}
		switch status.Run.Status {
		case "complete":
			var summary models.Summary
			if err := c.request(ctx, http.MethodGet, path+"/summary", nil, &summary); err != nil {
				return err
			}
			printExperiment(summary)
			return nil
		case "failed":
			return fmt.Errorf("%s\nLog: %s%s/log", status.Run.Error, c.base, execute)
		}
		done, active := 0, "preparing"
		for _, step := range status.Steps {
			if step.Status == "done" {
				done++
			}
			if step.Status == "running" {
				active = step.Name
			}
		}
		line := fmt.Sprintf("[%d/%d] %s", done, len(status.Steps), active)
		if line != last {
			fmt.Println(line)
			last = line
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func printExperiment(summary models.Summary) {
	label := summary.Experiment
	if current, ok := summary.Variants["current"]; ok {
		rate, p95 := current.Metrics.Rate, current.Metrics.P95
		fmt.Printf("%s  %.2f MiB/s (%.2f–%.2f)  P95 %.2f ms (%.2f–%.2f)\n",
			label.ID, rate.Median, rate.Min, rate.Max, p95.Median, p95.Min, p95.Max)
		fmt.Printf("%d benchmark runs, %d diagnostic runs, %d flamegraphs\n",
			summary.Evidence.Benchmark, summary.Evidence.Diagnostics, summary.Evidence.Flamegraphs)
	} else {
		fmt.Printf("%s  %s\n", label.ID, label.State)
	}
}
