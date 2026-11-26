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
	"dragonflydb.io/profile/pkg/httpapi"
	"dragonflydb.io/profile/pkg/otel"
)

func main() {
	data := flag.String("data", models.DefaultRoot, "directory of experiment records")
	dsn := flag.String("dsn-file", cmp.Or(os.Getenv("PROFILE_DSN_FILE"), os.Getenv("RAFT_WORKFLOW_DSN_FILE")), "PostgreSQL connection file")
	web := flag.String("web", "tools/profile/web/dist", "built web app directory")
	listen := flag.String("listen", "0.0.0.0:8787", "listen address")
	executor := flag.String("executor-socket", "", "Unix socket for the host benchmark executor")
	check := flag.String("check", "", "probe an HTTP URL and exit")
	flag.Parse()
	if flag.NArg() != 0 {
		flag.Usage()
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	if *check != "" {
		err = checkHTTP(*check)
	} else {
		err = serve(ctx, *listen, *data, *web, *dsn, *executor)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "profile-web:", err)
		os.Exit(1)
	}
}

func checkHTTP(address string) error {
	client := http.Client{Timeout: 4 * time.Second}
	resp, err := client.Get(address)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("health check: %s", resp.Status)
	}
	return nil
}

func serve(ctx context.Context, address, data, web, dsn, socket string) error {
	if st, err := os.Stat(filepath.Join(web, "index.html")); err != nil || !st.Mode().IsRegular() {
		return fmt.Errorf("build the TS web app first; missing %s", filepath.Join(web, "index.html"))
	}
	if socket != "" && dsn == "" {
		return errors.New("-executor-socket requires -dsn-file for experiment records")
	}
	var repo models.Repository = models.Store{Root: data}
	if dsn != "" {
		pg, err := models.OpenPostgres(ctx, data, dsn)
		if err != nil {
			return err
		}
		defer pg.Close()
		repo = pg
	}
	var executor http.Handler
	if socket != "" {
		executor = httpapi.ExecutorProxy(socket)
	}
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}
	envFile := os.Getenv("LANGFUSE_ENV_FILE")
	native := otel.NativeConfig{
		Langfuse: otel.LangfuseConfig{URL: os.Getenv("LANGFUSE_URL"), EnvFile: envFile},
		ClickHouse: otel.ClickHouseConfig{
			URL: os.Getenv("PROFILE_CLICKHOUSE_URL"), User: cmp.Or(os.Getenv("PROFILE_CLICKHOUSE_USER"), "langfuse"),
			EnvFile: envFile,
		},
	}
	s := &http.Server{Handler: httpapi.Handler(repo, os.DirFS(web), executor, native), ReadHeaderTimeout: 5 * time.Second}
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
