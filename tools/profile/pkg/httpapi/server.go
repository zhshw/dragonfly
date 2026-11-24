// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"io/fs"
	"log"
	"mime"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"dragonflydb.io/profile/models"
	"dragonflydb.io/profile/pkg/execution"
)

func Handler(store models.Repository, web fs.FS, runner *execution.Manager) http.Handler {
	api := http.NewServeMux()
	var deleteExperiment func(context.Context, string) (models.Deletion, error)
	if runner != nil {
		deleteExperiment = runner.Delete
	} else if pg, ok := store.(*models.Postgres); ok {
		deleteExperiment = pg.Delete
	}
	api.HandleFunc("GET /api/v1/experiments", func(w http.ResponseWriter, r *http.Request) {
		labels, err := store.List()
		if err != nil {
			respondError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"experiments": labels, "can_delete": deleteExperiment != nil})
	})
	api.HandleFunc("DELETE /api/v1/experiments/{id}", func(w http.ResponseWriter, r *http.Request) {
		if deleteExperiment == nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "删除实验需要 PostgreSQL"})
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		result, err := deleteExperiment(ctx, r.PathValue("id"))
		if err != nil {
			respondError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	})
	api.HandleFunc("GET /api/v1/comparison-targets", func(w http.ResponseWriter, _ *http.Request) {
		targets, err := store.Targets()
		if err != nil {
			respondError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"targets": targets})
	})
	api.HandleFunc("GET /api/v1/comparison-targets/{target}/diagnostics", func(w http.ResponseWriter, r *http.Request) {
		result, err := models.DiagnosticsForTarget(store, r.PathValue("target"))
		if err != nil {
			respondError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, result)
	})
	api.HandleFunc("POST /api/v1/experiments", func(w http.ResponseWriter, r *http.Request) {
		mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || mediaType != "application/json" {
			writeJSON(w, http.StatusUnsupportedMediaType, map[string]string{"error": "请提交 JSON"})
			return
		}
		var request models.CreateRequest
		decoder := json.NewDecoder(http.MaxBytesReader(w, r.Body, 16<<10))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&request); err != nil || decoder.Decode(new(any)) != io.EOF {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": "实验表单不是有效的 JSON"})
			return
		}
		result, err := store.Create(request)
		if err != nil {
			respondError(w, err)
			return
		}
		w.Header().Set("Location", "/api/v1/experiments/"+result.Experiment.ID)
		writeJSON(w, http.StatusCreated, result)
	})
	api.HandleFunc("GET /api/v1/experiments/{id}", withExperiment(store, func(w http.ResponseWriter, _ *http.Request, rec models.Record) {
		writeJSON(w, http.StatusOK, rec.Detail)
	}))
	api.HandleFunc("GET /api/v1/experiments/{id}/diagnostic-comparison", withExperiment(store, func(w http.ResponseWriter, _ *http.Request, rec models.Record) {
		comparison, err := models.CompareDiagnostics(rec, store.Get)
		if err != nil {
			respondError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, comparison)
	}))
	diagnosticRoutes(api, store)
	api.HandleFunc("GET /api/v1/experiments/{id}/run-comparison", withExperiment(store, func(w http.ResponseWriter, _ *http.Request, rec models.Record) {
		comparison, err := models.CompareRuns(rec, store.Get)
		if err != nil {
			respondError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, comparison)
	}))
	api.HandleFunc("GET /api/v1/experiments/{id}/run-logs", withExperiment(store, func(w http.ResponseWriter, r *http.Request, rec models.Record) {
		serveRunLogs(w, r, store, rec)
	}))
	api.HandleFunc("GET /api/v1/experiments/{id}/summary", func(w http.ResponseWriter, r *http.Request) {
		summary, err := store.Summary(r.PathValue("id"))
		if err != nil {
			respondError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, summary)
	})
	api.HandleFunc("GET /api/v1/experiments/{id}/artifacts/{artifact}", func(w http.ResponseWriter, r *http.Request) {
		path, err := store.ArtifactFor(r.PathValue("id"), r.PathValue("artifact"))
		if err != nil {
			respondError(w, err)
			return
		}
		serveArtifact(w, r, path)
	})
	api.HandleFunc("GET /api/v1/experiments/{id}/runs/{run}/measurement", func(w http.ResponseWriter, r *http.Request) {
		measurement, err := store.Measurement(r.PathValue("id"), r.PathValue("run"))
		if err != nil {
			respondError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, measurement)
	})
	executionRoutes(api, store, runner)
	site := http.FileServer(http.FS(web))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if strings.HasPrefix(r.URL.Path, "/api/") {
			api.ServeHTTP(w, r)
			return
		}
		site.ServeHTTP(w, r)
	})
}

func withExperiment(store models.Repository, handle func(http.ResponseWriter, *http.Request, models.Record)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rec, err := store.Get(r.PathValue("id"))
		if err != nil {
			respondError(w, err)
			return
		}
		handle(w, r, rec)
	}
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(value); err != nil {
		log.Print(err)
	}
}

func respondError(w http.ResponseWriter, err error) {
	if errors.Is(err, models.ErrInUse) {
		writeJSON(w, http.StatusConflict, map[string]string{"error": strings.TrimPrefix(err.Error(), models.ErrInUse.Error()+": ")})
		return
	}
	if errors.Is(err, models.ErrInvalid) {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": strings.TrimPrefix(err.Error(), models.ErrInvalid.Error()+": ")})
		return
	}
	if errors.Is(err, models.ErrExists) {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "实验 ID 已存在"})
		return
	}
	if errors.Is(err, os.ErrNotExist) {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "实验或产物不存在"})
		return
	}
	log.Print(err)
	writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "无法读取实验归档"})
}

func serveArtifact(w http.ResponseWriter, r *http.Request, path string) {
	f, err := os.Open(path)
	if err != nil {
		respondError(w, err)
		return
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		respondError(w, err)
		return
	}
	if !info.Mode().IsRegular() {
		respondError(w, os.ErrNotExist)
		return
	}
	switch filepath.Ext(path) {
	case ".svg":
		w.Header().Set("Content-Type", "image/svg+xml")
		// The flamegraph's inline zoom/search scripts run with an opaque origin.
		w.Header().Set("Content-Security-Policy", "sandbox allow-scripts; default-src 'none'; script-src 'unsafe-inline'; style-src 'unsafe-inline'")
	case ".json":
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Content-Disposition", "attachment")
	case ".log", ".txt":
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	default:
		respondError(w, os.ErrNotExist)
		return
	}
	http.ServeContent(w, r, info.Name(), info.ModTime(), f)
}
