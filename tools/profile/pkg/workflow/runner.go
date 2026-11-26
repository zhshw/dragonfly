// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

type Record struct {
	Status   string            `json:"status"`
	Attempt  int               `json:"attempt"`
	Dir      string            `json:"artifact_dir"`
	Log      string            `json:"log"`
	Started  string            `json:"started,omitempty"`
	Finished string            `json:"finished,omitempty"`
	Error    string            `json:"error,omitempty"`
	Outputs  map[string]string `json:"output_sha256,omitempty"`
}

type State struct {
	Version int                `json:"version"`
	Config  Config             `json:"config"`
	Digest  string             `json:"input_digest"`
	Status  string             `json:"status"`
	Updated string             `json:"updated"`
	Steps   map[string]*Record `json:"steps"`
}

type Step struct {
	Name string
	Run  func(*Runner, *Record) ([]string, error)
}

type Runner struct {
	Config Config
	State  State
	Ctx    context.Context
	Out    io.Writer
	Log    *os.File
}

// Run executes the configured benchmark and diagnostic steps, preserving completed outputs on resume.
func Run(ctx context.Context, config Config, resume bool, progress io.Writer) error {
	return runWorkflow(ctx, config, plan(config), resume, progress)
}

// ReadState reads the runner checkpoint, which is replaced atomically at each step boundary.
func ReadState(output string) (State, error) {
	var state State
	err := readJSON(filepath.Join(output, "state.json"), &state)
	return state, err
}

func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err = io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func atomicJSON(path string, value any) error {
	b, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	f, err := os.CreateTemp(filepath.Dir(path), ".state-*")
	if err != nil {
		return err
	}
	defer os.Remove(f.Name())
	_, err = f.Write(append(b, '\n'))
	err = errors.Join(err, f.Sync(), f.Close())
	if err != nil {
		return err
	}
	return os.Rename(f.Name(), path)
}

// Hash source/configuration, not generated objects or benchmark payloads. No Git is needed on
// the execution host. External dependency sources can be bound with extra_inputs.
func inputs(c Config) (map[string]string, string, error) {
	files := map[string]string{}
	add := func(path string) error {
		h, err := hashFile(path)
		if err == nil {
			files[path] = h
		}
		return err
	}
	sources := []string{
		"CMakeLists.txt", "src", "helio", "cmake", "tests/integration", "tools/profile",
	}
	for _, name := range sources {
		path := filepath.Join(c.Repo, name)
		if _, err := os.Stat(path); os.IsNotExist(err) && name != "CMakeLists.txt" && name != "src" {
			continue
		}
		if err := filepath.WalkDir(path, func(p string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d.IsDir() {
				if d.Name() == ".git" || d.Name() == "__pycache__" || d.Name() == "third_party" ||
					d.Name() == "node_modules" || d.Name() == "dist" || strings.HasPrefix(d.Name(), "build") {
					return filepath.SkipDir
				}
				return nil
			}
			switch filepath.Ext(p) {
			case ".cc", ".c", ".h", ".hpp", ".inc", ".S", ".cmake", ".sh", ".py", ".go", ".mod":
				return add(p)
			}
			if d.Name() == "CMakeLists.txt" {
				return add(p)
			}
			return nil
		}); err != nil {
			return nil, "", err
		}
	}
	paths := append([]string{}, c.ExtraInputs...)
	paths = append(paths, filepath.Join(c.Release, "CMakeCache.txt"))
	if c.Baseline != "" {
		paths = append(paths, filepath.Join(c.Baseline, "dragonfly"), filepath.Join(c.Baseline, "raft_storage"))
		if c.Diagnostics {
			paths = append(paths, filepath.Join(c.Baseline, "CMakeCache.txt"))
		}
	}
	for _, v := range c.Validation {
		paths = append(paths, filepath.Join(v.Build, "CMakeCache.txt"))
	}
	if c.Diagnostics {
		paths = append(paths, c.Profiler)
	}
	for _, path := range paths {
		if err := add(path); err != nil {
			return nil, "", err
		}
	}
	b, _ := json.Marshal(struct {
		Config Config
		Files  map[string]string
	}{c, files})
	h := sha256.Sum256(b)
	return files, hex.EncodeToString(h[:]), nil
}

func (r *Runner) save() error {
	r.State.Updated = time.Now().UTC().Format(time.RFC3339)
	return atomicJSON(filepath.Join(r.Config.Output, "state.json"), r.State)
}

func completed(s State) int {
	n := 0
	for _, step := range s.Steps {
		if step.Status == "done" {
			n++
		}
	}
	return n
}

func runWorkflow(ctx context.Context, c Config, steps []Step, resume bool, out io.Writer) error {
	names := map[string]bool{}
	for _, step := range steps {
		if step.Name == "" || filepath.Base(step.Name) != step.Name || names[step.Name] {
			return errors.New("invalid or duplicate step name")
		}
		names[step.Name] = true
	}
	// Keep the shared checkout lock compatible with existing executors.
	lockDir := filepath.Join(c.Repo, "docs/cluster/benchmarks")
	if err := os.MkdirAll(lockDir, 0755); err != nil {
		return err
	}
	unlock, err := lockFile(filepath.Join(lockDir, ".raft-workflow.lock"))
	if err != nil {
		return err
	}
	defer unlock()
	files, digest, err := inputs(c)
	if err != nil {
		return err
	}
	r := &Runner{Config: c, Ctx: ctx, Out: out}
	if resume {
		if err = readJSON(filepath.Join(c.Output, "state.json"), &r.State); err != nil {
			return err
		}
		if r.State.Version != 1 || r.State.Digest != digest {
			return errors.New("inputs/config changed; use a new output directory, do not mix experiments")
		}
		if len(r.State.Steps) != len(names) {
			return errors.New("workflow plan changed; use a new output directory")
		}
		for name := range r.State.Steps {
			if !names[name] {
				return errors.New("workflow plan changed; use a new output directory")
			}
		}
		for _, rec := range r.State.Steps {
			if rec.Status == "done" {
				for path, wanted := range rec.Outputs {
					if actual, e := hashFile(path); e != nil || actual != wanted {
						return fmt.Errorf("completed artifact missing or changed: %s", path)
					}
				}
			}
		}
	} else {
		if err = os.MkdirAll(filepath.Dir(c.Output), 0755); err != nil {
			return err
		}
		if err = os.Mkdir(c.Output, 0755); err != nil {
			return fmt.Errorf("create fresh output (use -resume for an existing run): %w", err)
		}
		r.State = State{Version: 1, Config: c, Digest: digest, Steps: map[string]*Record{}}
		for _, step := range steps {
			r.State.Steps[step.Name] = &Record{Status: "pending"}
		}
		if err = atomicJSON(filepath.Join(c.Output, "inputs.json"), files); err != nil {
			return err
		}
	}
	r.State.Status = "running"
	if err = r.save(); err != nil {
		return err
	}
	defer func() {
		if r.State.Status == "running" {
			r.State.Status = "interrupted"
			_ = r.save()
		}
	}()
	for i, step := range steps {
		rec, ok := r.State.Steps[step.Name]
		if !ok {
			return errors.New("workflow plan changed; use a new output directory")
		}
		if rec.Status == "done" {
			continue
		}
		if err = ctx.Err(); err != nil {
			return err
		}
		if _, now, e := inputs(c); e != nil || now != digest {
			return fmt.Errorf("inputs changed before %s; use a new experiment (%v)", step.Name, e)
		}
		rec.Attempt++
		base := filepath.Join(c.Output, "attempts", step.Name, fmt.Sprintf("%03d", rec.Attempt))
		if err = os.MkdirAll(base, 0755); err != nil {
			return err
		}
		rec.Dir, rec.Log = filepath.Join(base, "result"), filepath.Join(base, "command.log")
		rec.Status, rec.Error, rec.Started, rec.Finished = "running", "", time.Now().UTC().Format(time.RFC3339), ""
		r.State.Status = "running"
		if err = r.save(); err != nil {
			return err
		}
		r.Log, err = os.OpenFile(rec.Log, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		fmt.Fprintf(out, "[%d/%d] RUN %s\n", i+1, len(steps), step.Name)
		start := time.Now()
		outputs, runErr := step.Run(r, rec)
		runErr = errors.Join(runErr, r.Log.Close())
		if runErr == nil {
			_, now, e := inputs(c)
			if e != nil || now != digest {
				runErr = fmt.Errorf("inputs changed during %s; discard this attempt (%v)", step.Name, e)
			}
		}
		rec.Outputs = map[string]string{}
		if runErr == nil {
			for _, path := range outputs {
				h, e := hashFile(path)
				if e != nil {
					runErr = e
					break
				}
				rec.Outputs[path] = h
			}
		}
		rec.Finished = time.Now().UTC().Format(time.RFC3339)
		if runErr != nil {
			rec.Status, rec.Error, r.State.Status = "failed", runErr.Error(), "failed"
			if e := r.save(); e != nil {
				return errors.Join(runErr, e)
			}
			return fmt.Errorf("%s failed: %w; log=%s\n%s", step.Name, runErr, rec.Log, tail(rec.Log))
		}
		rec.Status = "done"
		if err = r.save(); err != nil {
			return err
		}
		fmt.Fprintf(out, "[%d/%d] OK  %s (%s)\n", i+1, len(steps), step.Name, time.Since(start).Round(time.Second))
	}
	r.State.Status = "done"
	if err = r.save(); err != nil {
		return err
	}
	if _, ok := r.State.Steps["summarize"]; !ok {
		return nil
	}
	summary := filepath.Join(r.dir("summarize"), "summary.md")
	data, err := os.ReadFile(summary)
	if err == nil {
		fmt.Fprintln(out, string(data))
		fmt.Fprintln(out, "Summary:", summary)
	}
	return err
}

func (r *Runner) dir(step string) string { return r.State.Steps[step].Dir }

func (r *Runner) command(env []string, args ...string) error {
	return r.commandTo(r.Log, env, args...)
}

func (r *Runner) commandTo(log io.Writer, env []string, args ...string) error {
	encoded, _ := json.Marshal(args)
	fmt.Fprintf(r.Log, "argv=%s\n", encoded)
	ctx, cancel := context.WithTimeout(r.Ctx, time.Duration(r.Config.Timeout)*time.Minute)
	defer cancel()
	return execute(ctx, r.Config.Repo, env, args, log, r.Out)
}

func (r *Runner) validationLog(path string, env []string, args ...string) error {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	return errors.Join(r.commandTo(io.MultiWriter(r.Log, f), env, args...), f.Close())
}

func tail(path string) string {
	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()
	st, _ := f.Stat()
	if st != nil && st.Size() > 1600 {
		_, _ = f.Seek(-1600, io.SeekEnd)
	}
	b, _ := io.ReadAll(f)
	return string(b)
}

func copyFiles(source, dest string, names ...string) ([]string, error) {
	if err := os.Mkdir(dest, 0755); err != nil {
		return nil, err
	}
	var outputs []string
	for _, name := range names {
		path := filepath.Join(dest, name)
		if err := copyFile(filepath.Join(source, name), path); err != nil {
			return nil, err
		}
		outputs = append(outputs, path)
	}
	sort.Strings(outputs)
	return outputs, nil
}

func copyFile(source, dest string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	st, err := in.Stat()
	if err != nil {
		return err
	}
	out, err := os.OpenFile(dest, os.O_CREATE|os.O_EXCL|os.O_WRONLY, st.Mode().Perm())
	if err != nil {
		return err
	}
	_, err = io.Copy(out, in)
	return errors.Join(err, out.Close())
}
