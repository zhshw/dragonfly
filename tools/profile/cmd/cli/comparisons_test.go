// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"dragonflydb.io/profile/models"
	"dragonflydb.io/profile/pkg/httpapi"
	"dragonflydb.io/profile/pkg/workflow"
)

func TestCLIConfiguresComparisonsThroughWebAPI(t *testing.T) {
	store := models.Store{Root: t.TempDir()}
	for _, id := range []string{"v15", "v16"} {
		record := models.Record{Schema: models.Schema, Detail: models.Detail{Summary: models.Summary{
			Experiment: models.Label{ID: id, Title: id, State: "complete"}, Harness: id,
			Workload: models.Workload{AckMode: "memory"}, Variants: map[string]models.Variant{
				"current": {Mode: "raft", Ack: "memory", Metrics: workflow.Metrics{
					Rate: workflow.Distribution{Median: 100}, P95: workflow.Distribution{Median: 20},
				}},
			},
		}}}
		if err := os.Mkdir(filepath.Join(store.Root, id), 0700); err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(record)
		if err := os.WriteFile(filepath.Join(store.Root, id, "experiment.json"), raw, 0600); err != nil {
			t.Fatal(err)
		}
	}
	server := httptest.NewServer(httpapi.Handler(store, fstest.MapFS{}, nil))
	defer server.Close()
	args := []string{"comparisons", "-url", server.URL, "-id", "v16"}
	if err := cli(context.Background(), append(args, "-baseline", "v15:current", "-previous", "v15", "-reference", "v15:current")); err != nil {
		t.Fatal(err)
	}
	summary, err := store.Summary("v16")
	if err != nil || summary.Comparisons.Previous.Target.ID != "v15:current" || summary.Comparisons.Previous.Warning == "" {
		t.Fatalf("CLI did not save historical choices: %+v %v", summary, err)
	}
	if err := cli(context.Background(), append(args, "-previous", "")); err != nil {
		t.Fatal(err)
	}
	summary, err = store.Summary("v16")
	if err != nil || summary.Comparisons.Previous != nil || summary.Comparisons.Cumulative == nil || summary.Comparisons.Reference == nil {
		t.Fatalf("clearing previous overwrote other choices: %+v %v", summary, err)
	}
	for _, body := range []string{"{", "{\"previous\":\"v15\",\"unknown\":true}", "{} {}"} {
		request, _ := http.NewRequest(http.MethodPut, server.URL+"/api/v1/experiments/v16/comparisons", strings.NewReader(body))
		request.Header.Set("Content-Type", "application/json")
		response, err := http.DefaultClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		response.Body.Close()
		if response.StatusCode != http.StatusBadRequest {
			t.Fatalf("invalid configuration returned %d", response.StatusCode)
		}
	}
}
