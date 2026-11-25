// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"syscall"
	"time"

	"dragonflydb.io/profile/models"
	"dragonflydb.io/profile/pkg/execution"
	"dragonflydb.io/profile/pkg/httpapi"
	"dragonflydb.io/profile/pkg/workflow"
)

func executorCLI(ctx context.Context, f *flag.FlagSet, args []string, dsnFile string) error {
	profile := f.String("workflow-config", "", "host benchmark workflow configuration")
	socket := f.String("socket", "", "Unix socket for the container Web service")
	data := f.String("data", models.DefaultRoot, "directory of experiment artifacts")
	dsn := f.String("dsn-file", dsnFile, "host PostgreSQL connection file")
	if err := f.Parse(args); err != nil {
		return err
	}
	if f.NArg() != 0 || *profile == "" || *socket == "" || *dsn == "" {
		return errors.New("executor requires -workflow-config FILE, -socket PATH and -dsn-file FILE")
	}
	config, err := workflow.LoadConfig(*profile)
	if err != nil {
		return err
	}
	pg, err := models.OpenPostgres(ctx, *data, *dsn)
	if err != nil {
		return err
	}
	defer pg.Close()
	runner, err := execution.Open(ctx, pg, config)
	if err != nil {
		return err
	}
	defer runner.Close()
	listener, err := listenExecutor(*socket)
	if err != nil {
		return err
	}
	defer listener.Close()
	server := &http.Server{Handler: httpapi.ExecutorHandler(pg, runner), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		deadline, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = server.Shutdown(deadline)
	}()
	fmt.Printf("profile executor listening on %s\n", listener.Addr())
	if err := server.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// The executor holds its database lease before opening the socket. Remove a stale
// socket after a crash, while leaving unrelated files and live listeners alone.
func listenExecutor(path string) (net.Listener, error) {
	if info, err := os.Lstat(path); err == nil {
		if info.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("executor socket path already exists: %s", path)
		}
		conn, err := net.DialTimeout("unix", path, time.Second)
		if err == nil {
			conn.Close()
			return nil, fmt.Errorf("executor already listening on %s", path)
		}
		if !errors.Is(err, syscall.ECONNREFUSED) {
			return nil, err
		}
		if err := os.Remove(path); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0600); err != nil {
		listener.Close()
		return nil, err
	}
	return listener, nil
}
