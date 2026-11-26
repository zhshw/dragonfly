// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package otel

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type LangfuseConfig struct {
	URL          string `json:"url,omitempty"`
	EnvFile      string `json:"env_file,omitempty"`
	ArtifactsURL string `json:"artifacts_url,omitempty"`
	Project      string `json:"project,omitempty"`
}

type Receipt struct {
	TraceID   string `json:"trace_id"`
	Verified  int    `json:"verified_observations"`
	FullSpans int    `json:"full_spans"`
	URL       string `json:"url"`
}

type langfuseClient struct {
	base string
	user string
	key  string
	http *http.Client
}

// TraceEnvironment supplies credentials only to the native exporter process environment.
// Callers must not persist or log this slice.
func TraceEnvironment(config LangfuseConfig) ([]string, error) {
	c, err := openLangfuse(config)
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(c.base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return nil, errors.New("invalid Langfuse URL")
	}
	return []string{
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=" + c.base + "/api/public/otel/v1/traces",
		"OTEL_EXPORTER_OTLP_TRACES_HEADERS=Authorization=Basic%20" + base64.StdEncoding.EncodeToString([]byte(c.user+":"+c.key)),
	}, nil
}

func openLangfuse(config LangfuseConfig) (langfuseClient, error) {
	env, err := readEnvFile(config.EnvFile)
	if err != nil {
		return langfuseClient{}, err
	}
	c := langfuseClient{
		base: strings.TrimRight(config.URL, "/"),
		user: env["LANGFUSE_PUBLIC_KEY"], key: env["LANGFUSE_SECRET_KEY"],
		http: newHTTPClient(60 * time.Second),
	}
	if c.user == "" || c.key == "" {
		return langfuseClient{}, errors.New("Langfuse API credentials are missing")
	}
	return c, nil
}

func readEnvFile(path string) (map[string]string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	env := map[string]string{}
	for _, line := range strings.Split(string(data), "\n") {
		if key, value, ok := strings.Cut(line, "="); ok {
			env[key] = strings.TrimSpace(value)
		}
	}
	return env, nil
}

func newHTTPClient(timeout time.Duration) *http.Client {
	return &http.Client{
		Timeout: timeout,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}
}

func (c langfuseClient) request(ctx context.Context, method, path string, body, result any) error {
	var content io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return err
		}
		content = bytes.NewReader(payload)
	}
	request, err := http.NewRequestWithContext(ctx, method, c.base+path, content)
	if err != nil {
		return err
	}
	request.SetBasicAuth(c.user, c.key)
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("x-langfuse-ingestion-version", "4")
	response, err := c.http.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("Langfuse %s %s returned %s", method, request.URL.Path, response.Status)
	}
	decoder := json.NewDecoder(io.LimitReader(response.Body, 8<<20))
	decoder.UseNumber()
	err = decoder.Decode(result)
	if method == http.MethodPost && errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func (c langfuseClient) observed(ctx context.Context, traceID string, begin, end int64) (map[string]bool, error) {
	params := url.Values{
		"traceId": {traceID}, "limit": {"1000"}, "fields": {"core"},
		"fromStartTime": {time.Unix(0, begin-int64(time.Second)).UTC().Format(time.RFC3339Nano)},
		"toStartTime":   {time.Unix(0, end+int64(time.Second)).UTC().Format(time.RFC3339Nano)},
	}
	ids, cursors := map[string]bool{}, map[string]bool{}
	for {
		var page struct {
			Data []struct {
				ID string `json:"id"`
			} `json:"data"`
			Meta struct {
				Cursor string `json:"cursor"`
			} `json:"meta"`
		}
		if err := c.request(ctx, http.MethodGet, "/api/public/v2/observations?"+params.Encode(), nil, &page); err != nil {
			return nil, err
		}
		for _, item := range page.Data {
			ids[item.ID] = true
		}
		cursor := page.Meta.Cursor
		if cursor == "" {
			return ids, nil
		}
		if cursors[cursor] {
			return nil, errors.New("Langfuse observations returned a repeated cursor")
		}
		cursors[cursor] = true
		params.Set("cursor", cursor)
	}
}

func (c langfuseClient) send(ctx context.Context, batch []otlpSpan) error {
	return c.sendTrace(ctx, batch, "dragonfly-replication-profile", "dragonfly.raft.observe", "1")
}

func (c langfuseClient) sendTrace(ctx context.Context, batch []otlpSpan, serviceName, scopeName, version string) error {
	var result struct {
		Partial struct {
			Rejected json.RawMessage `json:"rejectedSpans"`
		} `json:"partialSuccess"`
	}
	service := map[string]any{"attributes": attributes(map[string]any{"service.name": serviceName})}
	scope := map[string]any{"name": scopeName, "version": version}
	group := map[string]any{"scope": scope, "spans": batch}
	resource := map[string]any{"resource": service, "scopeSpans": []any{group}}
	payload := map[string]any{"resourceSpans": []any{resource}}
	if err := c.request(ctx, http.MethodPost, "/api/public/otel/v1/traces", payload, &result); err != nil {
		return err
	}
	if value := result.Partial.Rejected; len(value) != 0 {
		rejected, err := strconv.Atoi(strings.Trim(string(value), `"`))
		if err != nil || rejected != 0 {
			return fmt.Errorf("Langfuse rejected spans: %s", value)
		}
	}
	return nil
}

func metadataFilter(key, value string) map[string]string {
	return map[string]string{"type": "stringObject", "column": "metadata", "key": key, "operator": "=", "value": value}
}

func (c langfuseClient) nativeObservations(ctx context.Context, experiment, capture string, params url.Values) ([]nativeObservation, error) {
	filters := []map[string]string{metadataFilter("schema", "3"), metadataFilter("experiment_id", experiment)}
	if capture != "" {
		filters = append(filters, metadataFilter("capture_id", capture))
	}
	encoded, _ := json.Marshal(filters)
	params.Set("limit", "1000")
	params.Set("filter", string(encoded))
	params.Del("cursor")
	var rows []nativeObservation
	seen := map[string]bool{}
	for {
		var page struct {
			Data []nativeObservation `json:"data"`
			Meta struct {
				Cursor string `json:"cursor"`
			} `json:"meta"`
		}
		if err := c.request(ctx, http.MethodGet, "/api/public/v2/observations?"+params.Encode(), nil, &page); err != nil {
			return nil, err
		}
		rows = append(rows, page.Data...)
		if len(rows) > maxNativeObservations {
			return nil, errors.New("native capture exceeds query safety limit; no partial ranking returned")
		}
		if page.Meta.Cursor == "" {
			return rows, nil
		}
		if seen[page.Meta.Cursor] {
			return nil, errors.New("Langfuse returned a repeated native trace cursor")
		}
		seen[page.Meta.Cursor] = true
		params.Set("cursor", page.Meta.Cursor)
	}
}

func (c langfuseClient) nativeEvents(ctx context.Context, experiment, capture string, params url.Values) ([]NativeEvent, error) {
	rows, err := c.nativeObservations(ctx, experiment, capture, params)
	if err != nil {
		return nil, err
	}
	var events []NativeEvent
	seen := map[string]bool{}
	for _, row := range rows {
		e, err := decodeNative(row)
		if err != nil {
			return nil, err
		}
		key := e.TraceID + ":" + e.SpanID
		if seen[key] {
			continue
		}
		seen[key] = true
		events = append(events, e)
	}
	return events, nil
}
