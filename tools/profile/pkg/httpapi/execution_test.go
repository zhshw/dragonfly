// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package httpapi

import (
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/fstest"

	"dragonflydb.io/profile/models"
)

func TestWebForwardsOnlyExecutorRoutesOverUnixSocket(t *testing.T) {
	dir, err := os.MkdirTemp("", "profile-sock-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	socket := filepath.Join(dir, "s")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	executor := &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		w.WriteHeader(http.StatusAccepted)
		io.WriteString(w, r.Method+" "+r.URL.Path+" "+string(data))
	})}
	go executor.Serve(listener)
	t.Cleanup(func() { executor.Close() })
	store := models.Store{Root: t.TempDir()}
	web := Handler(store, fstest.MapFS{}, ExecutorProxy(socket))
	for _, route := range []struct{ method, path, body string }{
		{"POST", "/api/v1/experiments/v15/execution", `{"restart":true}`},
		{"GET", "/api/v1/experiments/v15/execution", ""},
		{"GET", "/api/v1/experiments/v15/execution/log", ""},
		{"DELETE", "/api/v1/experiments/v15", ""},
	} {
		response := httptest.NewRecorder()
		web.ServeHTTP(response, httptest.NewRequest(route.method, route.path, strings.NewReader(route.body)))
		if want := route.method + " " + route.path + " " + route.body; response.Code != 202 || response.Body.String() != want {
			t.Fatalf("executor route %s %s: %d %s", route.method, route.path, response.Code, response.Body)
		}
	}
	if err := executor.Close(); err != nil {
		t.Fatal(err)
	}
	for _, route := range []struct {
		path string
		code int
	}{
		{"/api/v1/experiments/v15/execution", http.StatusServiceUnavailable},
		{"/api/v1/experiments", http.StatusOK},
		{"/api/health", http.StatusNotFound},
	} {
		response := httptest.NewRecorder()
		web.ServeHTTP(response, httptest.NewRequest(http.MethodGet, route.path, nil))
		if response.Code != route.code {
			t.Fatalf("%s with executor offline: %d %s", route.path, response.Code, response.Body)
		}
	}
}
