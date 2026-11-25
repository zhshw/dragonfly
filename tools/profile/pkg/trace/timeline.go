// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

// Package trace analyzes complete Profile diagnostic timelines.
package trace

import (
	"encoding/json"
	"fmt"
	"io"
	"math"
)

// Event is a span in a Profile timeline, with time and duration in microseconds.
type Event struct {
	Name string           `json:"name"`
	Cat  string           `json:"cat"`
	Kind string           `json:"ph"`
	Node string           `json:"pid"`
	TID  int64            `json:"tid"`
	Time float64          `json:"ts"`
	Dur  float64          `json:"dur"`
	Args map[string]int64 `json:"args"`
}

func traceNS(us float64) int64 {
	return int64(math.Round(us * 1000))
}

func traceEnd(e Event) int64 {
	return traceNS(e.Time + e.Dur)
}

func readTraceEvents(timeline io.Reader) ([]Event, error) {
	var data struct {
		Events []Event `json:"traceEvents"`
	}
	err := json.NewDecoder(timeline).Decode(&data)
	return data.Events, err
}

func selectTraceBatch(batches []Event, batch int) (int, error) {
	if batch > len(batches) {
		return 0, fmt.Errorf("batch must be between 1 and %d", len(batches))
	}
	if batch != 0 {
		return batch, nil
	}
	slowest := 0
	for i, b := range batches {
		if b.Dur > batches[slowest].Dur {
			slowest = i
		}
	}
	return slowest + 1, nil
}
