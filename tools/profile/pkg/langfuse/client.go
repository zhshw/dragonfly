// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package langfuse

import (
	"bytes"
	"context"
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

type Config struct {
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

type client struct {
	base string
	user string
	key  string
	http *http.Client
}

func open(config Config) (client, error) {
	data, err := os.ReadFile(config.EnvFile)
	if err != nil {
		return client{}, err
	}
	c := client{base: strings.TrimRight(config.URL, "/"), http: &http.Client{
		Timeout: 60 * time.Second,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}}
	for _, line := range strings.Split(string(data), "\n") {
		key, value, _ := strings.Cut(line, "=")
		switch key {
		case "LANGFUSE_PUBLIC_KEY":
			c.user = strings.TrimSpace(value)
		case "LANGFUSE_SECRET_KEY":
			c.key = strings.TrimSpace(value)
		}
	}
	if c.user == "" || c.key == "" {
		return client{}, errors.New("Langfuse API credentials are missing")
	}
	return c, nil
}

func (c client) request(ctx context.Context, method, path string, body, result any) error {
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

func (c client) observed(ctx context.Context, traceID string, begin, end int64) (map[string]bool, error) {
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

func (c client) send(ctx context.Context, batch []otlpSpan) error {
	var result struct {
		Partial struct {
			Rejected json.RawMessage `json:"rejectedSpans"`
		} `json:"partialSuccess"`
	}
	service := map[string]any{"attributes": attributes(map[string]any{"service.name": "dragonfly-replication-profile"})}
	scope := map[string]any{"name": "dragonfly.raft.observe", "version": "1"}
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
