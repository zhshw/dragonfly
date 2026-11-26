// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package otel

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type ClickHouseConfig struct {
	URL     string
	User    string
	EnvFile string
}

type clickHouseClient struct {
	endpoint url.URL
	user     string
	password string
	http     *http.Client
}

const nativeClickHouseQuery = `
SELECT span_id AS id, trace_id AS traceId, name,
       mapFromArrays(arrayReverse(metadata_names), arrayReverse(metadata_values)) AS metadata
FROM default.events_full FINAL
WHERE project_id = {project:String}
  AND start_time >= parseDateTime64BestEffort({from:String}, 6, 'UTC')
  AND start_time < parseDateTime64BestEffort({to:String}, 6, 'UTC')
  AND is_deleted = 0
  AND hasAllTokens(metadata_values, tokens({capture:String}))
  AND metadata_values[indexOf(metadata_names, 'capture_id')] = {capture:String}
  AND metadata_values[indexOf(metadata_names, 'experiment_id')] = {experiment:String}
  AND metadata_values[indexOf(metadata_names, 'schema')] = '3'
LIMIT {limit:UInt64}
FORMAT JSONEachRow`

func openClickHouse(config ClickHouseConfig) *clickHouseClient {
	if config.URL == "" || config.User == "" {
		return nil
	}
	endpoint, err := url.Parse(config.URL)
	if err != nil || (endpoint.Scheme != "http" && endpoint.Scheme != "https") ||
		endpoint.Host == "" || endpoint.User != nil || endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil
	}
	env, err := readEnvFile(config.EnvFile)
	if err != nil {
		return nil
	}
	password := env["PROFILE_CLICKHOUSE_PASSWORD"]
	if password == "" && config.User == "langfuse" {
		password = env["CLICKHOUSE_PASSWORD"]
	}
	if password == "" {
		return nil
	}
	return &clickHouseClient{
		endpoint: *endpoint, user: config.User, password: password, http: newHTTPClient(5 * time.Second),
	}
}

func (c *clickHouseClient) nativeEvents(ctx context.Context, project, experiment, capture, from, to string) ([]NativeEvent, error) {
	query := url.Values{
		"readonly": {"1"}, "wait_end_of_query": {"1"},
		"param_project": {project}, "param_experiment": {experiment}, "param_capture": {capture},
		"param_from": {from}, "param_to": {to}, "param_limit": {strconv.Itoa(maxNativeObservations + 1)},
	}
	endpoint := c.endpoint
	endpoint.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), strings.NewReader(nativeClickHouseQuery))
	if err != nil {
		return nil, err
	}
	req.SetBasicAuth(c.user, c.password)
	req.Header.Set("Content-Type", "text/plain; charset=utf-8")
	resp, err := c.http.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ClickHouse returned %s", resp.Status)
	}

	var events []NativeEvent
	seen := make(map[[2]string]bool)
	decoder := json.NewDecoder(resp.Body)
	decoder.UseNumber()
	for {
		var row nativeObservation
		err := decoder.Decode(&row)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if len(events) >= maxNativeObservations {
			return nil, errors.New("native capture exceeds query safety limit")
		}
		event, err := decodeNative(row)
		if err != nil {
			return nil, err
		}
		if event.Fields["experiment_id"] != experiment || event.Fields["capture_id"] != capture || event.TraceID == "" || event.SpanID == "" {
			return nil, errors.New("invalid ClickHouse native event identity")
		}
		id := [2]string{event.TraceID, event.SpanID}
		if seen[id] {
			return nil, errors.New("duplicate ClickHouse native event")
		}
		seen[id] = true

		// Match Langfuse's JSON formatting for fixed-point values without rounding.
		for key, value := range event.Values {
			if strings.Contains(value, ".") && !strings.ContainsAny(value, "eE") {
				event.Values[key] = strings.TrimSuffix(strings.TrimRight(value, "0"), ".")
			}
		}
		events = append(events, event)
	}
	return events, nil
}
