// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"
	"time"

	"dragonflydb.io/profile/models"
	"dragonflydb.io/profile/pkg/execution"
)

func executionRoutes(api *http.ServeMux, store models.Repository, runner *execution.Manager) {
	const route = "/api/v1/experiments/{id}/execution"
	api.HandleFunc("GET "+route, func(w http.ResponseWriter, r *http.Request) {
		if _, err := store.Summary(r.PathValue("id")); err != nil {
			respondError(w, err)
			return
		}
		if runner == nil {
			writeJSON(w, http.StatusOK, execution.Progress{})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		status, err := runner.Status(ctx, r.PathValue("id"))
		if err != nil {
			respondError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, status)
	})
	api.HandleFunc("POST "+route, func(w http.ResponseWriter, r *http.Request) {
		if runner == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "执行机尚未配置"})
			return
		}
		media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || media != "application/json" {
			writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "请提交 JSON"})
			return
		}
		var request *struct {
			Restart bool `json:"restart"`
		}
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1024))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil || request == nil || decoder.Decode(new(any)) != io.EOF {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "运行参数不是有效的 JSON"})
			return
		}
		draft, err := store.Summary(r.PathValue("id"))
		if err != nil {
			respondError(w, err)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		if err := runner.Start(ctx, draft, request.Restart); err != nil {
			if errors.Is(err, execution.ErrBusy) || errors.Is(err, execution.ErrComplete) {
				writeJSON(w, http.StatusConflict, map[string]string{"error": err.Error()})
				return
			}
			respondError(w, err)
			return
		}
		w.Header().Set("Location", r.URL.Path)
		writeJSON(w, http.StatusAccepted, map[string]string{"status": "running"})
	})
	api.HandleFunc("GET "+route+"/log", func(w http.ResponseWriter, r *http.Request) {
		if runner == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "执行机尚未配置"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()
		path, err := runner.Log(ctx, r.PathValue("id"))
		if err != nil {
			respondError(w, err)
			return
		}
		w.Header().Set("Cache-Control", "no-cache")
		serveArtifact(w, r, path)
	})
}
