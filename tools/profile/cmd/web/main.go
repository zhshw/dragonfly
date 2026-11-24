// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package main

import (
	"cmp"
	"context"
	"errors"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"dragonflydb.io/profile/models"
	"dragonflydb.io/profile/pkg/execution"
	"dragonflydb.io/profile/pkg/httpapi"
	"dragonflydb.io/profile/pkg/workflow"
)

func main() {
	data := flag.String("data", models.DefaultRoot, "directory of experiment records")
	dsn := flag.String("dsn-file", cmp.Or(os.Getenv("PROFILE_DSN_FILE"), os.Getenv("RAFT_WORKFLOW_DSN_FILE")), "PostgreSQL connection file")
	web := flag.String("web", "tools/profile/web/dist", "built web app directory")
	listen := flag.String("listen", "127.0.0.1:8787", "listen address")
	profile := flag.String("workflow-config", "", "host profile for running benchmarks")
	flag.Parse()
	if flag.NArg() != 0 {
		flag.Usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := serve(ctx, *listen, *data, *web, *dsn, *profile); err != nil {
		fmt.Fprintln(os.Stderr, "profile-web:", err)
		os.Exit(1)
	}
}

func serve(ctx context.Context, address, data, web, dsn, profile string) error {
	if st, err := os.Stat(filepath.Join(web, "index.html")); err != nil || !st.Mode().IsRegular() {
		return fmt.Errorf("build the TS web app first; missing %s", filepath.Join(web, "index.html"))
	}
	if profile != "" && dsn == "" {
		return errors.New("-workflow-config requires -dsn-file for experiment and execution records")
	}
	var repo models.Repository = models.Store{Root: data}
	var runner *execution.Manager
	if dsn != "" {
		pg, err := models.OpenPostgres(ctx, data, dsn)
		if err != nil {
			return err
		}
		defer pg.Close()
		repo = pg
		if profile != "" {
			config, err := workflow.LoadConfig(profile)
			if err != nil {
				return fmt.Errorf("workflow config: %w", err)
			}
			runner, err = execution.Open(ctx, pg, config)
			if err != nil {
				return fmt.Errorf("benchmark executor: %w", err)
			}
			defer runner.Close()
		}
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	s := &http.Server{Handler: httpapi.Handler(repo, os.DirFS(web), runner), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		deadline, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = s.Shutdown(deadline)
	}()
	fmt.Printf("profile http://%s/\n", listener.Addr())
	if err := s.Serve(listener); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}
