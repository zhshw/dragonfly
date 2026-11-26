// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package httpapi

import (
	"encoding/json"
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
	api.HandleFunc("GET /api/v1/experiments/{id}/event-summary", func(w http.ResponseWriter, r *http.Request) {
		if !ready(w, r) {
			return
		}
		capture := r.URL.Query().Get("capture")
		if capture == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "capture is required"})
			return
		}
		var window otel.Window
		scope := r.URL.Query().Get("scope")
		if scope != "" && scope != "measurement" && scope != "capture" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid scope"})
			return
		}
		if scope != "capture" {
			record, err := store.Get(r.PathValue("id"))
			if err != nil {
				respondError(w, err)
				return
			}
			for variant, diagnostic := range record.Summary.Diagnostics {
				if diagnostic.CaptureID != capture {
					continue
				}
				data, err := store.Measurement(r.PathValue("id"), "diagnostic-"+variant)
				if err != nil || json.Unmarshal(data, &window) != nil || window.Start <= 0 || window.End <= window.Start {
					writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "Capture has no valid measurement window"})
					return
				}
				break
			}
			if window.Start == 0 {
				writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": "Capture is not linked to a diagnostic measurement; use scope=capture for the full capture"})
				return
			}
		}
		report, err := reader.SummaryWindow(r.Context(), r.PathValue("id"), capture, window)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, report)
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
