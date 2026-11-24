// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package httpapi

import (
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"dragonflydb.io/profile/models"
)

func diagnosticRoutes(api *http.ServeMux, store models.Repository) {
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
		report, err := store.Flamegraph(r.PathValue("id"), models.FlamegraphQuery{
			Run: params.Get("run"), Role: params.Get("role"), View: params.Get("view"), Limit: limit,
		})
		if err != nil {
			respondError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, report)
	})
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
