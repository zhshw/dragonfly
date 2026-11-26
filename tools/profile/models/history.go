// Copyright 2026, DragonflyDB authors. All rights reserved.
// See LICENSE for licensing terms.

package models

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"gorm.io/gorm/clause"
)

// Backfill reads the existing profile archives into PostgreSQL.
func (p *Postgres) Backfill(ctx context.Context, archive string, out io.Writer) error {
	path := func(name string) string {
		if name == "" {
			return name
		}
		return filepath.Join(archive, name)
	}
	for _, source := range historicalArchives() {
		id := source.Experiment.ID
		prior, err := p.Summary(id)
		if err == nil && prior.Experiment.State == "complete" {
			if source.Targets.Previous != "" && prior.Comparisons.Previous == nil {
				if err := p.linkArchivedPrevious(ctx, prior, source.Targets.Previous); err != nil {
					return fmt.Errorf("%s previous experiment: %w", id, err)
				}
				fmt.Fprintf(out, "%s linked to %s\n", id, source.Targets.Previous)
				continue
			}
			fmt.Fprintf(out, "%s already recorded\n", id)
			continue
		}
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		source.Compare, source.ValidationDir = path(source.Compare), path(source.ValidationDir)
		for i := range source.Additional {
			source.Additional[i].Dir = path(source.Additional[i].Dir)
		}
		for role, capture := range source.Diagnostics {
			source.Diagnostics[role] = DiagnosticSource{Report: path(capture.Report), Sources: path(capture.Sources)}
		}
		result, err := p.Import(ctx, source)
		if err != nil {
			return fmt.Errorf("%s: %w", id, err)
		}
		fmt.Fprintf(out, "%s: %d main benchmark rounds, %d diagnostics, %d benchmark sets\n",
			id, result.Evidence.Benchmark, result.Evidence.Diagnostics, 1+len(source.Additional))
	}
	return nil
}

func (p *Postgres) linkArchivedPrevious(ctx context.Context, summary Summary, targetID string) error {
	targets, err := p.Targets()
	if err != nil {
		return err
	}
	for _, target := range targets {
		if target.ID != targetID {
			continue
		}
		// The retest hash establishes lineage. The benchmark snapshot comes from the prior archive.
		if target.ExperimentID == "" || target.Variant != "current" || target.SHA == "" ||
			target.SHA != summary.Variants["baseline"].SHA {
			return fmt.Errorf("archive %s does not match the previous release binary", targetID)
		}
		current, ok := summary.Variants["current"]
		if !ok {
			return fmt.Errorf("missing current benchmark")
		}
		comparisons := Comparisons{Previous: &TargetComparison{Target: target}}
		if err := comparisons.measure(summary.Workload, summary.Harness, current.Metrics); err != nil {
			return err
		}
		row := comparisonRow{ExperimentID: summary.Experiment.ID, Role: "previous", TargetID: target.ID,
			Comparison: *comparisons.Previous}
		return p.db.WithContext(ctx).Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "experiment_id"}, {Name: "role"}}, DoNothing: true,
		}).Create(&row).Error
	}
	return fmt.Errorf("previous archive %s is missing", targetID)
}

// Paths map document experiment IDs to the archive names from their original runs.
func historicalArchives() []ImportConfig {
	return []ImportConfig{
		{
			Experiment: Label{ID: "v3", Title: "初始基准与原 Cluster 对照", Date: "2026-11-23", Decision: "retained", Description: "初版 Raft memory 的正式吞吐和与原 Cluster 的诊断对照。"},
			Compare:    "ack-modes-20261123-c1",
			Targets:    ChoiceIDs{Cumulative: "v3", Reference: "v3:cluster"},
			Aliases:    map[string]string{"memory": "current"},
			Diagnostics: map[string]DiagnosticSource{
				"current": {Report: "observability-20261123/memory-c1-p64-v3/report", Sources: "observability-20261123/probe-v3/sources.json"},
				"cluster": {Report: "observability-20261123/cluster-c1-p64-v3/report", Sources: "observability-20261123/probe-v3/sources.json"},
			},
		},
		{
			Experiment: Label{ID: "v4", Title: "共享 WAL 缓冲与直接编码", Optimization: "P0-1", Date: "2026-11-23", Decision: "retained", Description: "减少 SET 编码的中间分配和复制，保留共享缓冲筛选结果。"},
			Compare:    "profile-buffer-20261123/encoded-c1",
			Targets:    ChoiceIDs{Cumulative: "v3", Reference: "v4:cluster", Previous: "v3"},
			Diagnostics: map[string]DiagnosticSource{
				"baseline": {Report: "profile-buffer-20261123/memory-baseline-c1-p64-v4/report", Sources: "observability-20261123/probe-v3/sources.json"},
				"current":  {Report: "profile-buffer-20261123/memory-encoded-c1-p64-v4/report", Sources: "profile-buffer-20261123/probe-encoded/sources.json"},
				"cluster":  {Report: "profile-buffer-20261123/cluster-c1-p64-v4/report", Sources: "profile-buffer-20261123/probe-encoded/sources.json"},
			},
			Additional: []BenchmarkSource{
				{ID: "shared", Title: "共享缓冲筛选", Dir: "profile-buffer-20261123/shared-c1"},
			},
		},
		{
			Experiment: Label{ID: "v5", Title: "批量应用与借用参数", Optimization: "P0-2", Date: "2026-11-23", Decision: "retained", Description: "用有界批次投递已提交 effect，并借用 SET 参数。"},
			Compare:    "profile-apply-20261123/apply-c1",
			Targets:    ChoiceIDs{Cumulative: "v3", Reference: "v5:cluster", Previous: "v4"},
			Diagnostics: map[string]DiagnosticSource{
				"baseline": {Report: "profile-apply-20261123/memory-encoded-c1-p64-v5/report", Sources: "profile-buffer-20261123/probe-encoded/sources.json"},
				"current":  {Report: "profile-apply-20261123/memory-apply-c1-p64-v5/report", Sources: "profile-apply-20261123/probe-apply/sources.json"},
				"cluster":  {Report: "profile-apply-20261123/cluster-c1-p64-v5/report", Sources: "profile-apply-20261123/probe-apply/sources.json"},
			},
			ValidationDir: "profile-apply-20261123/validation",
		},
		{
			Experiment: Label{ID: "v6", Title: "聚合编码与 effect/WAL 共享", Optimization: "P0-1", Date: "2026-11-23", Decision: "retained", Description: "现场四版本交错测试，直接聚合为中间版本。完整共享降低局部 CPU，确认吞吐收益有限。"},
			Compare:    "profile-drain-20261123/shared-c1",
			Targets:    ChoiceIDs{Cumulative: "v3", Reference: "v6:cluster", Previous: "v5"},
			Aliases:    map[string]string{"previous": "baseline", "baseline": "direct"},
			Diagnostics: map[string]DiagnosticSource{
				"baseline": {Report: "profile-drain-20261123/memory-before-c1-p64-v6/report", Sources: "profile-apply-20261123/probe-apply/sources.json"},
				"current":  {Report: "profile-drain-20261123/memory-shared-c1-p64-v6/report", Sources: "profile-drain-20261123/probe-shared/sources.json"},
				"cluster":  {Report: "profile-drain-20261123/cluster-c1-p64-v6/report", Sources: "profile-drain-20261123/probe-shared/sources.json"},
			},
			ValidationDir: "profile-drain-20261123/validation",
			Additional: []BenchmarkSource{
				{ID: "direct-screening", Title: "直接聚合筛选", Dir: "profile-drain-20261123/direct-c1-v3"},
			},
		},
		{
			Experiment: Label{ID: "v7", Title: "Follower 复用已验证批次", Optimization: "P1-4", Date: "2026-11-23", Decision: "retained", Description: "避免 follower 应用时的 WAL 读回、CRC 和重复解码。"},
			Compare:    "profile-follower-20261123/compare-c1",
			Targets:    ChoiceIDs{Cumulative: "v3", Reference: "v7:cluster", Previous: "v6"},
			Diagnostics: map[string]DiagnosticSource{
				"baseline": {Report: "profile-follower-20261123/memory-before-c1-p64-v7/report", Sources: "profile-drain-20261123/probe-shared/sources.json"},
				"current":  {Report: "profile-follower-20261123/memory-follower-c1-p64-v7/report", Sources: "profile-follower-20261123/probe-follower/sources.json"},
				"cluster":  {Report: "profile-follower-20261123/cluster-c1-p64-v7/report", Sources: "profile-follower-20261123/probe-follower/sources.json"},
			},
			ValidationDir: "profile-follower-20261123/validation",
		},
		{
			Experiment: Label{ID: "v8", Title: "Fetch 直接封帧", Optimization: "P1-7", Date: "2026-11-23", Decision: "retained", Description: "预计算长度，只预留一次缓冲，直接输出 Fetch frame。"},
			Compare:    "profile-frame-20261123/compare-c1",
			Targets:    ChoiceIDs{Cumulative: "v3", Reference: "v8:cluster", Previous: "v7"},
			Diagnostics: map[string]DiagnosticSource{
				"baseline": {Report: "profile-frame-20261123/memory-before-c1-p64-v8/report", Sources: "profile-follower-20261123/probe-follower/sources.json"},
				"current":  {Report: "profile-frame-20261123/memory-frame-c1-p64-v8/report", Sources: "profile-frame-20261123/probe-frame/sources.json"},
				"cluster":  {Report: "profile-frame-20261123/cluster-c1-p64-v8/report", Sources: "profile-frame-20261123/probe-frame/sources.json"},
			},
			ValidationDir: "profile-frame-20261123/validation",
		},
		{
			Experiment: Label{ID: "v9", Title: "SET 输入所有权转交", Optimization: "P1-5", Date: "2026-11-23", Decision: "retained", Description: "普通 SET 接管已解析参数，减少输入值复制。"},
			Compare:    "profile-input-20261123/compare-c1",
			Targets:    ChoiceIDs{Cumulative: "v3", Reference: "v9:cluster", Previous: "v8"},
			Diagnostics: map[string]DiagnosticSource{
				"baseline": {Report: "profile-input-20261123/memory-before-c1-p64-v9/report", Sources: "profile-frame-20261123/probe-frame/sources.json"},
				"current":  {Report: "profile-input-20261123/memory-input-c1-p64-v9/report", Sources: "profile-input-20261123/probe-input/sources.json"},
				"cluster":  {Report: "profile-input-20261123/cluster-c1-p64-v9/report", Sources: "profile-input-20261123/probe-input/sources.json"},
			},
			ValidationDir: "profile-input-20261123/validation",
		},
		{
			Experiment: Label{ID: "p0-1b", Title: "Follower 接收确认快路径", Optimization: "P0-1b", Date: "2026-11-24", Decision: "reverted", Description: "提前 Fetch 的正式吞吐没有稳定收益，保留数据，代码已回退。"},
			Compare:    "profile-receipt-20261123/compare-c1",
			Targets:    ChoiceIDs{Cumulative: "v3", Reference: "p0-1b:cluster", Previous: "v9"},
			Diagnostics: map[string]DiagnosticSource{
				"baseline": {Report: "profile-receipt-20261123/memory-before-c1-p64-v10/report", Sources: "profile-receipt-20261123/probe-before/sources.json"},
				"current":  {Report: "profile-receipt-20261123/memory-receipt-c1-p64-v10/report", Sources: "profile-receipt-20261123/probe-receipt-final/sources.json"},
				"cluster":  {Report: "profile-receipt-20261123/cluster-c1-p64-v10/report", Sources: "profile-receipt-20261123/probe-receipt-final/sources.json"},
			},
			ValidationDir: "profile-receipt-20261123/validation",
		},
		{
			Experiment: Label{ID: "v10", Title: "Memory 写入与后台同步并行", Optimization: "P1-6", Date: "2026-11-24", Decision: "retained", Description: "同任期、同段的普通写入可在后台 fsync 期间继续追加与复制。"},
			Compare:    "profile-flush-overlap-20261124/compare-c1",
			Targets:    ChoiceIDs{Cumulative: "v3", Reference: "v0:cluster", Previous: "v9"},
			Only:       []string{"candidate", "cluster"},
			Diagnostics: map[string]DiagnosticSource{
				"current": {Report: "profile-flush-overlap-20261124/memory-overlap-c1-p64-v11/report", Sources: "profile-flush-overlap-20261124/probe-overlap/sources.json"},
				"cluster": {Report: "profile-flush-overlap-20261124/cluster-c1-p64-v11/report", Sources: "profile-flush-overlap-20261124/probe-overlap/sources.json"},
			},
			ValidationDir: "profile-flush-overlap-20261124/validation",
			Additional: []BenchmarkSource{
				{ID: "c2", Title: "2 连接历史样本", Dir: "profile-flush-overlap-20261124/compare-c2", Only: []string{"candidate", "cluster"}},
				{ID: "c8", Title: "8 连接历史样本", Dir: "profile-flush-overlap-20261124/compare-c8", Only: []string{"candidate", "cluster"}},
			},
		},
	}
}
