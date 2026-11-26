// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package otel

import (
	"context"
	"errors"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"time"

	"golang.org/x/sync/singleflight"
)

const (
	nativeCacheTTL      = 5 * time.Minute
	nativeCacheCaptures = 4
	nativeCacheSpans    = 32_000
)

type NativeConfig struct {
	Langfuse   LangfuseConfig
	ClickHouse ClickHouseConfig
}

// NativeReader shares validated captures between interactive queries.
type NativeReader struct {
	config NativeConfig
	loads  singleflight.Group
	mu     sync.Mutex
	cache  []nativeCacheEntry
}

type nativeCacheEntry struct {
	experiment, capture string
	data                *nativeCaptureData
	expires             time.Time
}

func NewNativeReader(config NativeConfig) *NativeReader {
	return &NativeReader{config: config}
}

// Query returns a read-only report. Incomplete captures are read again on the next request.
func (r *NativeReader) Query(ctx context.Context, experiment, capture string, q NativeQuery) (NativeReport, error) {
	data, err := r.load(ctx, experiment, capture)
	if err != nil {
		return NativeReport{}, err
	}
	return data.rank(q)
}

func (r *NativeReader) load(ctx context.Context, experiment, capture string) (*nativeCaptureData, error) {
	if capture == "" || experiment == "" {
		return nil, errors.New("experiment and capture are required")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if data := r.cached(experiment, capture); data != nil {
		return data, nil
	}

	// Switching filters can cancel an HTTP request while another request still needs its capture.
	key := strconv.Quote(experiment) + capture
	loaded := r.loads.DoChan(key, func() (any, error) {
		if data := r.cached(experiment, capture); data != nil {
			return data, nil
		}
		fetchCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Minute)
		defer cancel()
		langfuse, err := openLangfuse(r.config.Langfuse)
		if err != nil {
			return nil, err
		}
		clickhouse := openClickHouse(r.config.ClickHouse)
		data, err := loadNativeCapture(fetchCtx, langfuse, clickhouse, experiment, capture)
		if err == nil && data.summary.Complete && len(data.events) <= nativeCacheSpans {
			r.remember(experiment, capture, data)
		}
		return data, err
	})
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case result := <-loaded:
		if result.Err != nil {
			return nil, result.Err
		}
		return result.Val.(*nativeCaptureData), nil
	}
}

func (r *NativeReader) cached(experiment, capture string) *nativeCaptureData {
	r.mu.Lock()
	defer r.mu.Unlock()
	for i, entry := range r.cache {
		if entry.experiment != experiment || entry.capture != capture {
			continue
		}
		r.cache = slices.Delete(r.cache, i, i+1)
		if !time.Now().Before(entry.expires) {
			return nil
		}
		r.cache = append(r.cache, entry)
		return entry.data
	}
	return nil
}

func (r *NativeReader) remember(experiment, capture string, data *nativeCaptureData) {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	r.cache = slices.DeleteFunc(r.cache, func(entry nativeCacheEntry) bool {
		return !now.Before(entry.expires)
	})
	r.cache = append(r.cache, nativeCacheEntry{experiment, capture, data, now.Add(nativeCacheTTL)})
	spans := 0
	for i := len(r.cache) - 1; i >= 0; i-- {
		spans += len(r.cache[i].data.events)
		if len(r.cache)-i > nativeCacheCaptures || spans > nativeCacheSpans {
			r.cache = slices.Delete(r.cache, 0, i+1)
			break
		}
	}
}

func loadNativeCapture(ctx context.Context, langfuse langfuseClient, clickhouse *clickHouseClient, experiment, capture string) (*nativeCaptureData, error) {
	// The C++ exporter sets each manifest's wall times to the bounds of its stream.
	manifests, err := langfuse.nativeObservations(ctx, experiment, capture, url.Values{
		"name": {"raft.capture"}, "fields": {"core"},
	})
	if err == nil {
		from, to, ok := nativeCaptureWindow(manifests)
		if ok {
			if clickhouse != nil {
				if data := readClickHouseCapture(ctx, clickhouse, manifests, experiment, capture, from, to); data != nil {
					return data, nil
				}
			}
			params := url.Values{"fields": {"core,basic,metadata"}, "fromStartTime": {from}, "toStartTime": {to}}
			events, err := langfuse.nativeEvents(ctx, experiment, capture, params)
			if err == nil && nativeMetadataMayBeTruncated(events) {
				params.Set("expandMetadata", "ticket_ids")
				events, err = langfuse.nativeEvents(ctx, experiment, capture, params)
			}
			if err == nil {
				if data, err := prepareNativeCapture(capture, events); err == nil && data.summary.Complete {
					return data, nil
				}
			}
		}
	}

	// Use a full read when manifests are absent, malformed, or disagree with the bounded result.
	events, err := langfuse.nativeEvents(ctx, experiment, capture, url.Values{
		"fields": {"core,basic,metadata"}, "expandMetadata": {"ticket_ids"},
	})
	if err != nil {
		return nil, err
	}
	return prepareNativeCapture(capture, events)
}

func readClickHouseCapture(ctx context.Context, clickhouse *clickHouseClient, manifests []nativeObservation, experiment, capture, from, to string) *nativeCaptureData {
	// Bind the SQL query to the project authenticated by the Langfuse API.
	project := manifests[0].Project
	if project == "" {
		return nil
	}
	expected := make(map[string]string, len(manifests))
	for _, manifest := range manifests {
		if manifest.Project != project || manifest.TraceID == "" || manifest.ID == "" {
			return nil
		}
		if id, exists := expected[manifest.TraceID]; exists && id != manifest.ID {
			return nil
		}
		expected[manifest.TraceID] = manifest.ID
	}
	events, err := clickhouse.nativeEvents(ctx, project, experiment, capture, from, to)
	if err != nil {
		return nil
	}
	for _, event := range events {
		if event.Name == "raft.capture" {
			if expected[event.TraceID] != event.SpanID {
				return nil
			}
			delete(expected, event.TraceID)
		}
	}
	if len(expected) != 0 {
		return nil
	}
	data, err := prepareNativeCapture(capture, events)
	if err != nil || !data.summary.Complete {
		return nil
	}
	return data
}

func nativeCaptureWindow(manifests []nativeObservation) (string, string, bool) {
	if len(manifests) == 0 {
		return "", "", false
	}
	var from, to time.Time
	for _, row := range manifests {
		start, startErr := time.Parse(time.RFC3339Nano, row.Start)
		end, endErr := time.Parse(time.RFC3339Nano, row.End)
		if startErr != nil || endErr != nil || end.Before(start) {
			return "", "", false
		}
		if from.IsZero() || start.Before(from) {
			from = start
		}
		if to.IsZero() || end.After(to) {
			to = end
		}
	}

	// Langfuse's API rounds wall times to milliseconds and may use an exclusive upper bound.
	return from.UTC().Truncate(time.Millisecond).Add(-time.Millisecond).Format(time.RFC3339Nano),
		to.UTC().Truncate(time.Millisecond).Add(time.Millisecond).Format(time.RFC3339Nano), true
}

func nativeMetadataMayBeTruncated(events []NativeEvent) bool {
	for _, event := range events {
		for _, fields := range []map[string]string{event.Fields, event.Values, event.States} {
			for _, value := range fields {
				// events_core retains 200 UTF-8 characters. Bytes give a conservative check.
				if len(value) >= 200 {
					return true
				}
			}
		}
	}
	return false
}
