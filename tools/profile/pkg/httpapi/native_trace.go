// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package httpapi

import (
	"net/http"
	"strconv"
	"strings"

	"dragonflydb.io/profile/models"
	"dragonflydb.io/profile/pkg/otel"
)

func nativeTraceRoutes(api *http.ServeMux, store models.Repository, config otel.NativeConfig) {
	reader := otel.NewNativeReader(config)
	ready := func(w http.ResponseWriter, r *http.Request) bool {
		if config.Langfuse.URL == "" || config.Langfuse.EnvFile == "" {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "Native trace query is not configured"})
			return false
		}
		if _, err := store.Get(r.PathValue("id")); err != nil {
			respondError(w, err)
			return false
		}
		return true
	}
	api.HandleFunc("GET /api/v1/experiments/{id}/captures", func(w http.ResponseWriter, r *http.Request) {
		if !ready(w, r) {
			return
		}
		captures, err := otel.NativeCaptures(r.Context(), config.Langfuse, r.PathValue("id"))
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"captures": captures})
	})
	query := func(w http.ResponseWriter, r *http.Request) {
		if !ready(w, r) {
			return
		}
		p := r.URL.Query()
		limit := 10
		var err error
		if p.Has("limit") {
			limit, err = strconv.Atoi(p.Get("limit"))
		}
		minimum := p.Get("min")
		if minimum == "" {
			minimum = "0"
		}
		value, valueErr := otel.NativeNumber(minimum)
		if err != nil || limit < 1 || limit > 100 || valueErr != nil || value.Sign() < 0 || p.Has("min_ns") {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid limit or min; use the attribute unit (ms for durations)"})
			return
		}
		start, startErr := strconv.ParseInt("0"+p.Get("start_ns"), 10, 64)
		end, endErr := strconv.ParseInt("0"+p.Get("end_ns"), 10, 64)
		if startErr != nil || endErr != nil || (end != 0 && start > end) || (p.Get("partial") != "" && p.Get("partial") != "0" && p.Get("partial") != "1") || p.Get("capture") == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid capture, partial or time range"})
			return
		}
		name, attribute := p.Get("name"), p.Get("attribute")
		if strings.HasSuffix(r.URL.Path, "/requests") {
			name = "raft.fetch"
			if attribute == "" {
				attribute = "raft.fetch.return_wait"
			}
		} else if attribute != "" || p.Get("partial") != "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Event Slow Path ranks event duration; use name to filter events, or /requests for request attributes"})
			return
		}
		report, err := reader.Query(r.Context(), r.PathValue("id"), p.Get("capture"), otel.NativeQuery{Name: name, Attribute: attribute, Node: p.Get("node"), Group: p.Get("group"), Outcome: p.Get("outcome"), Partial: p.Get("partial"), Minimum: minimum, Limit: limit, Selected: p.Get("selected"), Start: start, End: end})
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, report)
	}
	api.HandleFunc("GET /api/v1/experiments/{id}/requests", query)
	api.HandleFunc("GET /api/v1/experiments/{id}/events", query)
}
