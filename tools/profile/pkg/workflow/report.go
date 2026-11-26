// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package workflow

import (
	"encoding/json"
	"errors"

	"dragonflydb.io/profile/pkg/otel"
)

type ReportOptions struct {
	Go         string
	Flamegraph string
}

func decodeReport(report otel.Report) (otel.Metadata, BenchRow, error) {
	var metadata otel.Metadata
	var row BenchRow
	if err := errors.Join(json.Unmarshal(report.Metadata, &metadata), json.Unmarshal(report.Result, &row)); err != nil {
		return metadata, row, err
	}
	return metadata, row, validWorkload(row)
}
