// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package httpapi

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"

	"dragonflydb.io/profile/models"
	"dragonflydb.io/profile/pkg/trace"
)

func diagnosticRoutes(api *http.ServeMux, store models.Repository) {
	api.HandleFunc("GET /api/v1/experiments/{id}/artifacts/{artifact}/request-path", func(w http.ResponseWriter, r *http.Request) {
		batch, err := diagnosticInt(r.URL.Query(), "batch", 0)
		if err != nil {
			respondError(w, err)
			return
		}
		if batch == 0 {
			respondError(w, fmt.Errorf("%w: batch 需要为正整数", models.ErrInvalid))
			return
		}
		serveTimelineAnalysis(w, r, store, func(timeline io.Reader) (trace.RequestPath, error) {
			return trace.AnalyzeRequestPath(timeline, batch)
		})
	})
	api.HandleFunc("GET /api/v1/experiments/{id}/artifacts/{artifact}/reply-gap-stats", func(w http.ResponseWriter, r *http.Request) {
		serveTimelineAnalysis(w, r, store, trace.AnalyzeReplyGapStats)
	})
	api.HandleFunc("GET /api/v1/experiments/{id}/artifacts/{artifact}/fetch-requests", func(w http.ResponseWriter, r *http.Request) {
		serveTimelineAnalysis(w, r, store, trace.AnalyzeFetchRequests)
	})
	api.HandleFunc("GET /api/v1/experiments/{id}/artifacts/{artifact}/fetch-delay", func(w http.ResponseWriter, r *http.Request) {
		params := r.URL.Query()
		batch, err := diagnosticInt(params, "batch", 0)
		if err != nil {
			respondError(w, err)
			return
		}
		limit, err := diagnosticInt(params, "limit", models.DefaultTraceLimit)
		if err != nil {
			respondError(w, err)
			return
		}
		scope := params.Get("scope")
		if (scope != "" && scope != "batch" && scope != "round") || (scope == "round" && batch != 0) {
			respondError(w, fmt.Errorf("%w: scope 应为 batch 或 round，整轮不能指定批次", models.ErrInvalid))
			return
		}
		serveTimelineAnalysis(w, r, store, func(timeline io.Reader) (trace.FetchDelayReport, error) {
			if scope == "round" {
				return trace.AnalyzeRoundFetchDelay(timeline, limit)
			}
			return trace.AnalyzeFetchDelay(timeline, batch, limit)
		})
	})
	api.HandleFunc("GET /api/v1/experiments/{id}/artifacts/{artifact}/fetch-waits", func(w http.ResponseWriter, r *http.Request) {
		params := r.URL.Query()
		scope, order := params.Get("scope"), params.Get("sort")
		if scope == "" {
			scope = "slowest"
		}
		if order == "" {
			order = "gap"
		}
		serveTimelineAnalysis(w, r, store, func(timeline io.Reader) (trace.FetchWaitReport, error) {
			return trace.AnalyzeFetchWaits(timeline, scope, order)
		})
	})
	api.HandleFunc("GET /api/v1/experiments/{id}/trace", func(w http.ResponseWriter, r *http.Request) {
		params := r.URL.Query()
		limit, err := diagnosticInt(params, "limit", models.DefaultTraceLimit)
		if err != nil {
			respondError(w, err)
			return
		}
		offset, err := diagnosticInt(params, "offset", 0)
		if err != nil {
			respondError(w, err)
			return
		}
		query := models.TraceQuery{
			Run: params.Get("run"), Role: params.Get("role"), Stage: params.Get("stage"),
			Sort: params.Get("sort"), Order: params.Get("order"), Limit: limit, Offset: offset,
			Analysis: params.Get("analysis"),
		}
		if raw := params.Get("min_union_ms"); raw != "" {
			query.MinUnionMS, err = strconv.ParseFloat(raw, 64)
			if err != nil {
				respondError(w, fmt.Errorf("%w: min_union_ms 需要为数值", models.ErrInvalid))
				return
			}
		}
		page, err := store.Trace(r.PathValue("id"), query)
		if err != nil {
			respondError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, page)
	})
	api.HandleFunc("GET /api/v1/experiments/{id}/flamegraph", func(w http.ResponseWriter, r *http.Request) {
		params := r.URL.Query()
		limit, err := diagnosticInt(params, "limit", models.DefaultTraceLimit)
		if err != nil {
			respondError(w, err)
			return
		}
		depth, err := diagnosticInt(params, "caller_depth", 0)
		if err != nil {
			respondError(w, err)
			return
		}
		report, err := store.Flamegraph(r.PathValue("id"), models.FlamegraphQuery{
			Run: params.Get("run"), Role: params.Get("role"), View: params.Get("view"), Limit: limit,
			Symbol: params.Get("symbol"), CallerDepth: depth,
		})
		if err != nil {
			respondError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, report)
	})
}

func serveTimelineAnalysis[T any](w http.ResponseWriter, r *http.Request, store models.Repository, analyze func(io.Reader) (T, error)) {
	path, err := store.ArtifactFor(r.PathValue("id"), r.PathValue("artifact"))
	if err != nil {
		respondError(w, err)
		return
	}
	timeline, err := os.Open(path)
	if err != nil {
		respondError(w, err)
		return
	}
	defer timeline.Close()
	report, err := analyze(timeline)
	if err != nil {
		respondError(w, fmt.Errorf("%w: %v", models.ErrInvalid, err))
		return
	}
	writeJSON(w, http.StatusOK, report)
}

func diagnosticInt(params url.Values, name string, defaultValue int) (int, error) {
	raw := params.Get(name)
	if raw == "" {
		return defaultValue, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("%w: %s 需要为非负整数", models.ErrInvalid, name)
	}
	return value, nil
}
