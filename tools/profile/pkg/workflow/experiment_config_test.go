// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package workflow

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestForExperimentSelectsBuilds(t *testing.T) {
	base := filepath.Join(t.TempDir(), "dragonfly-io-uring-20261123")
	repo := base + "-v18b"
	release := filepath.Join(repo, "build-opt")
	debug := filepath.Join(repo, "build-dbg")
	config := Config{
		Repo: base, Release: filepath.Join(base, "build-opt"),
		Output:      filepath.Join(base, "runs"),
		Validation:  []Validation{{Build: filepath.Join(base, "build-dbg")}},
		ExtraInputs: []string{filepath.Join(base, "inputs.cfg")},
	}
	writeExperimentCache(t, release, repo, release)
	writeExperimentCache(t, debug, repo, debug)

	got, err := config.ForExperiment("v18b")
	if err != nil {
		t.Fatal(err)
	}
	if got.Experiment != "v18b" || got.Repo != repo || got.Release != release ||
		got.Validation[0].Build != debug || got.ExtraInputs[0] != filepath.Join(repo, "inputs.cfg") ||
		got.Output != config.Output || config.Validation[0].Build != filepath.Join(base, "build-dbg") {
		t.Fatalf("unexpected experiment configuration: %+v (base: %+v)", got, config)
	}

	if err := os.Mkdir(base, 0755); err != nil {
		t.Fatal(err)
	}
	writeExperimentCache(t, debug, base, debug)
	if _, err := config.ForExperiment("v18b"); err == nil || !strings.Contains(err.Error(), "源码目录为") {
		t.Fatalf("expected validation source mismatch, got %v", err)
	}
	writeExperimentCache(t, debug, repo, debug)
	writeExperimentCache(t, release, repo, filepath.Join(base, "build-opt"))
	if _, err := config.ForExperiment("v18b"); err == nil || !strings.Contains(err.Error(), "构建目录为") {
		t.Fatalf("expected relocated build cache error, got %v", err)
	}
	if _, err := config.ForExperiment("v18c"); err == nil || !strings.Contains(err.Error(), base+"-v18c") {
		t.Fatalf("expected missing experiment directory error, got %v", err)
	}
}

func writeExperimentCache(t *testing.T, dir, source, build string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	data := fmt.Sprintf("CMAKE_HOME_DIRECTORY:INTERNAL=%s\nCMAKE_CACHEFILE_DIR:INTERNAL=%s\n", source, build)
	if err := os.WriteFile(filepath.Join(dir, "CMakeCache.txt"), []byte(data), 0644); err != nil {
		t.Fatal(err)
	}
}
