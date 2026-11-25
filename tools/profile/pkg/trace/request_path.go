// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package trace

import (
	"errors"
	"io"
)

type RequestPath struct {
	Phases    []RequestPhase `json:"phases"`
	ReplyGaps []ReplyGap     `json:"reply_gaps"`
	Fetches   []FetchDelay   `json:"fetches,omitempty"`
}

// AnalyzeRequestPath ranks reply gaps for one client batch and includes any Fetch timing
// that can be aligned with its log offsets.
func AnalyzeRequestPath(timeline io.Reader, batch int) (RequestPath, error) {
	if batch < 1 {
		return RequestPath{}, errors.New("batch must be positive")
	}
	events, err := readTraceEvents(timeline)
	if err != nil {
		return RequestPath{}, err
	}
	request, err := analyzeRequestTrace(events, RequestTraceQuery{Slow: true, Batch: batch, Limit: len(events)})
	if err != nil {
		return RequestPath{}, err
	}
	path := RequestPath{Phases: request.Phases, ReplyGaps: request.Slow.ReplyGaps}
	if fetches, err := analyzeFetchDelay(events, batch, len(events)); err == nil {
		path.Fetches = fetches.Delays
	}
	return path, nil
}
