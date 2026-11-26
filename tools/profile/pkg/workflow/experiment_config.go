// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package workflow

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
)

// ForExperiment selects the platform experiment checkout and verifies its configured builds.
func (c Config) ForExperiment(id string) (Config, error) {
	c.Experiment = id
	if name, err := traceExperiment(c, "after"); err != nil || name != id {
		return c, errors.New("实验 ID 必须是 1–64 位的小写字母、数字、连字符或下划线")
	}
	base := c.Repo
	c.Repo = base + "-" + id
	source, err := os.Stat(c.Repo)
	if err != nil {
		return c, fmt.Errorf("无法读取实验源码目录 %s: %w", c.Repo, err)
	}
	if !source.IsDir() {
		return c, fmt.Errorf("实验源码路径 %s 不是目录", c.Repo)
	}
	if shared, err := os.Stat(base); err == nil && os.SameFile(source, shared) {
		return c, fmt.Errorf("实验源码目录 %s 指向共享目录 %s", c.Repo, base)
	}

	rebase := func(path string) string {
		rel, err := filepath.Rel(base, path)
		if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) {
			return path
		}
		return filepath.Join(c.Repo, rel)
	}
	c.Release = rebase(c.Release)
	c.Validation = slices.Clone(c.Validation)
	for i := range c.Validation {
		c.Validation[i].Build = rebase(c.Validation[i].Build)
	}
	c.ExtraInputs = slices.Clone(c.ExtraInputs)
	for i, path := range c.ExtraInputs {
		c.ExtraInputs[i] = rebase(path)
	}

	if err := checkExperimentBuild(c.Release, c.Repo, source); err != nil {
		return c, err
	}
	for _, validation := range c.Validation {
		if err := checkExperimentBuild(validation.Build, c.Repo, source); err != nil {
			return c, err
		}
	}
	return c, nil
}

func checkExperimentBuild(build, repo string, source os.FileInfo) error {
	cache := filepath.Join(build, "CMakeCache.txt")
	data, err := os.ReadFile(cache)
	if err != nil {
		return fmt.Errorf("无法读取实验构建配置 %s: %w", cache, err)
	}
	var sourcePath, buildPath string
	for _, line := range strings.Split(string(data), "\n") {
		key, value, _ := strings.Cut(strings.TrimSuffix(line, "\r"), "=")
		switch key {
		case "CMAKE_HOME_DIRECTORY:INTERNAL":
			sourcePath = value
		case "CMAKE_CACHEFILE_DIR:INTERNAL":
			buildPath = value
		}
	}
	actualSource, err := os.Stat(sourcePath)
	if err != nil || !os.SameFile(source, actualSource) {
		return fmt.Errorf("CMake 配置 %s 的源码目录为 %q，预期 %s", cache, sourcePath, repo)
	}
	actualBuild, err := os.Stat(buildPath)
	if err != nil {
		return fmt.Errorf("CMake 配置 %s 的构建目录为 %q，预期 %s", cache, buildPath, build)
	}
	dir, err := os.Stat(build)
	if err != nil || !os.SameFile(dir, actualBuild) {
		return fmt.Errorf("CMake 配置 %s 的构建目录为 %q，预期 %s", cache, buildPath, build)
	}
	return nil
}
