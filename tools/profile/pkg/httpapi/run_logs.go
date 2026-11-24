// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package httpapi

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"

	"dragonflydb.io/profile/models"
)

func serveRunLogs(w http.ResponseWriter, r *http.Request, store models.Repository, record models.Record) {
	group := r.URL.Query().Get("group")
	if group != "" && group != "baseline" && group != "previous" && group != "current" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "group 必须是 baseline、previous 或 current"})
		return
	}
	comparison, err := models.CompareRuns(record, store.Get)
	if err != nil {
		respondError(w, err)
		return
	}
	sources := []struct {
		group  string
		source *models.RunSource
	}{
		{"baseline", comparison.Baseline},
		{"previous", comparison.Previous},
		{"current", comparison.Current},
	}
	type archivedLog struct {
		group, id string
		trial     int
		file      *os.File
	}
	var logs []archivedLog
	for _, selection := range sources {
		source := selection.source
		if (group != "" && group != selection.group) || source == nil || source.ExperimentID == "" {
			continue
		}
		for _, set := range source.Benchmarks {
			if set.ID != "main" || len(set.Runs) != 3 {
				continue
			}
			runs := append([]models.Run(nil), set.Runs...)
			sort.Slice(runs, func(i, j int) bool { return runs[i].Trial < runs[j].Trial })
			for _, run := range runs {
				path, err := store.ArtifactFor(source.ExperimentID, run.Log)
				if err != nil {
					respondError(w, err)
					return
				}
				file, err := os.Open(path)
				if err != nil {
					respondError(w, err)
					return
				}
				defer file.Close()
				logs = append(logs, archivedLog{selection.group, source.ExperimentID, run.Trial, file})
			}
			break
		}
	}
	if len(logs) == 0 {
		respondError(w, os.ErrNotExist)
		return
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	w.WriteHeader(http.StatusOK)
	if r.Method == http.MethodHead {
		return
	}
	for _, entry := range logs {
		if _, err := fmt.Fprintf(w, "\n===== %s · %s · 第 %d 轮 =====\n", entry.group, entry.id, entry.trial); err != nil {
			return
		}
		if _, err := io.Copy(w, entry.file); err != nil {
			return
		}
	}
}
