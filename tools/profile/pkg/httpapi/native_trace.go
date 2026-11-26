// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package httpapi

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"dragonflydb.io/profile/models"
	"dragonflydb.io/profile/pkg/otel"
)

func nativeTraceRoutes(api *http.ServeMux, store models.Repository, config otel.NativeConfig) {
	reader := otel.NewNativeReader(config)
	configured := func(w http.ResponseWriter) bool {
		if config.Langfuse.URL == "" || config.Langfuse.EnvFile == "" {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "Native trace query is not configured"})
			return false
		}
		return true
	}
	readRecord := func(w http.ResponseWriter, r *http.Request) (models.Record, bool) {
		if !configured(w) {
			return models.Record{}, false
		}
		record, err := store.Get(r.PathValue("id"))
		if err != nil {
			respondError(w, err)
			return models.Record{}, false
		}
		return record, true
	}
	api.HandleFunc("GET /api/v1/experiments/{id}/captures", func(w http.ResponseWriter, r *http.Request) {
		if _, ok := readRecord(w, r); !ok {
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
		record, ok := readRecord(w, r)
		if !ok {
			return
		}
		params := r.URL.Query()
		capture := params.Get("capture")
		if capture == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "capture is required"})
			return
		}
		scope := params.Get("scope")
		if scope != "" && scope != "measurement" && scope != "capture" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid scope"})
			return
		}
		nodeRoles := map[string]string{}
		variant, diagnostic := nativeDiagnosticFor(record, capture)
		for role, info := range diagnostic.Nodes {
			nodeRoles[info.Node] = role
		}
		include := params.Get("include_protocol_waits")
		if include != "" && include != "0" && include != "1" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid include_protocol_waits"})
			return
		}
		id := r.PathValue("id")
		window, err := nativeWindow(store, id, variant, scope)
		if err != nil {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
			return
		}
		exclude := include == "0" || (include == "" && scope != "capture")
		report, err := reader.SummaryWindow(r.Context(), id, capture, window, nodeRoles, exclude)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, report)
	})
	query := func(w http.ResponseWriter, r *http.Request) {
		record, ok := readRecord(w, r)
		if !ok {
			return
		}
		p := r.URL.Query()
		limit := 10
		var limitErr error
		if p.Has("limit") {
			limit, limitErr = strconv.Atoi(p.Get("limit"))
		}
		offset, err := diagnosticInt(p, "offset", 0)
		if err != nil {
			respondError(w, err)
			return
		}
		minimum := p.Get("min")
		if minimum == "" {
			minimum = "0"
		}
		value, valueErr := otel.NativeNumber(minimum)
		if limitErr != nil || limit < 1 || limit > 100 || valueErr != nil || value.Sign() < 0 || p.Has("min_ns") {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid limit or min; use the attribute unit (ms for durations)"})
			return
		}
		start, startErr := strconv.ParseInt("0"+p.Get("start_ns"), 10, 64)
		end, endErr := strconv.ParseInt("0"+p.Get("end_ns"), 10, 64)
		if startErr != nil || endErr != nil || (end != 0 && start > end) || (p.Get("partial") != "" && p.Get("partial") != "0" && p.Get("partial") != "1") || p.Get("capture") == "" {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid capture, partial or time range"})
			return
		}
		scope, include := p.Get("scope"), p.Get("include_protocol_waits")
		if (scope != "" && scope != "auto" && scope != "capture" && scope != "measurement") ||
			(include != "" && include != "0" && include != "1") {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid scope or include_protocol_waits"})
			return
		}
		if scope == "" {
			scope = "auto"
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
		id, capture := r.PathValue("id"), p.Get("capture")
		variant, _ := nativeDiagnosticFor(record, capture)
		window, err := nativeWindow(store, id, variant, scope)
		if err != nil {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"error": err.Error()})
			return
		}
		report, err := reader.Query(r.Context(), id, capture, otel.NativeQuery{
			Name: name, Attribute: attribute, Node: p.Get("node"), Group: p.Get("group"),
			Outcome: p.Get("outcome"), Partial: p.Get("partial"), Minimum: minimum, Limit: limit, Offset: offset,
			Selected: p.Get("selected"), Start: start, End: end, Window: window, ExcludeProtocolWaits: include != "1",
		})
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]string{"error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, report)
	}
	api.HandleFunc("GET /api/v1/experiments/{id}/requests", query)
	api.HandleFunc("GET /api/v1/experiments/{id}/events", query)
}

func nativeDiagnosticFor(record models.Record, capture string) (string, models.Diagnostic) {
	for variant, diagnostic := range record.Summary.Diagnostics {
		if diagnostic.CaptureID == capture {
			return variant, diagnostic
		}
	}
	return "", models.Diagnostic{}
}

func nativeWindow(store models.Repository, id, variant, scope string) (otel.Window, error) {
	var window otel.Window
	if scope == "capture" || (scope == "auto" && variant == "") {
		return window, nil
	}
	if variant == "" {
		return window, errors.New("capture is not linked to a diagnostic measurement")
	}
	data, err := store.Measurement(id, "diagnostic-"+variant)
	if err != nil || json.Unmarshal(data, &window) != nil || window.Start <= 0 || window.End <= window.Start {
		return window, errors.New("capture has no valid measurement window")
	}
	return window, nil
}
