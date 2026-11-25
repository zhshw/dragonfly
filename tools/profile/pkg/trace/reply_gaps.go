// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package trace

import "io"

type ReplyGapStats struct {
	Batches     int        `json:"batches"`
	Count       int        `json:"count"`
	MaxMS       float64    `json:"max_ms"`
	MinMS       float64    `json:"min_ms"`
	MeanMS      float64    `json:"mean_ms"`
	VarianceMS2 float64    `json:"variance_ms2"`
	BatchMaxMS  []*float64 `json:"batch_max_ms"`
}

type replyGapAccumulator struct {
	stats ReplyGapStats
	m2    float64
}

func (a *replyGapAccumulator) addBatch(gaps []ReplyGap) {
	s := &a.stats
	if len(gaps) == 0 {
		s.BatchMaxMS = append(s.BatchMaxMS, nil)
		return
	}
	var longest float64
	for _, gap := range gaps {
		ms := float64(gap.EndNS-gap.BeginNS) / 1e6
		longest = max(longest, ms)
		if s.Count == 0 {
			s.MinMS, s.MaxMS = ms, ms
		}
		s.Count++
		s.MinMS, s.MaxMS = min(s.MinMS, ms), max(s.MaxMS, ms)
		delta := ms - s.MeanMS
		s.MeanMS += delta / float64(s.Count)
		a.m2 += delta * (ms - s.MeanMS)
	}
	s.BatchMaxMS = append(s.BatchMaxMS, &longest)
}

func (a *replyGapAccumulator) result() ReplyGapStats {
	s := a.stats
	s.Batches = len(s.BatchMaxMS)
	if s.Count != 0 {
		s.VarianceMS2 = a.m2 / float64(s.Count)
	}
	return s
}

// AnalyzeReplyGapStats aggregates the observable reply gaps across all SET batches in one capture.
func AnalyzeReplyGapStats(timeline io.Reader) (ReplyGapStats, error) {
	report, err := AnalyzeRequestTrace(timeline, RequestTraceQuery{})
	return report.replyGapStats, err
}
