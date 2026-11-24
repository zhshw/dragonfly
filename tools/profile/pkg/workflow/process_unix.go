//go:build linux || darwin

// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package workflow

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

func lockFile(path string) (func(), error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	if err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("another workflow owns %s", path)
	}
	return func() { f.Close() }, nil
}

func execute(ctx context.Context, repo string, extra, args []string, log, progress io.Writer) error {
	return executeIO(ctx, repo, extra, args, nil, log, log, progress)
}

func executeIO(ctx context.Context, repo string, extra, args []string, input io.Reader, stdout, stderr, progress io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(args) == 0 {
		return fmt.Errorf("empty command")
	}
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Dir, cmd.Stdin, cmd.Stdout, cmd.Stderr = repo, input, stdout, stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	values := map[string]string{}
	for _, item := range os.Environ() {
		k, v, _ := strings.Cut(item, "=")
		values[k] = v
	}
	values["PYTHONPATH"] = filepath.Join(repo, ".python-packages") + string(os.PathListSeparator) + values["PYTHONPATH"]
	for _, item := range extra {
		k, v, _ := strings.Cut(item, "=")
		values[k] = v
	}
	for k, v := range values {
		cmd.Env = append(cmd.Env, k+"="+v)
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	tick := time.NewTicker(30 * time.Second)
	defer tick.Stop()
	start := time.Now()
	for {
		select {
		case err := <-done:
			return err
		case <-tick.C:
			fmt.Fprintf(progress, "  running %s (%s); full output in command.log\n", filepath.Base(args[0]), time.Since(start).Round(time.Second))
		case <-ctx.Done():
			// Interrupt the entire owned group, allowing unittest fixtures to clean up servers.
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGINT)
			timer := time.NewTimer(5 * time.Second)
			select {
			case <-done:
				timer.Stop()
			case <-timer.C:
				_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
				<-done
			}
			_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
			return ctx.Err()
		}
	}
}
