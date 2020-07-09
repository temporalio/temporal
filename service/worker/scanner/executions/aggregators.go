// The MIT License (MIT)
//
// Copyright (c) 2017-2020 Uber Technologies Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package executions

import (
	"fmt"

	"github.com/uber/cadence/service/worker/scanner/executions/common"
)

type (
	shardFixResultAggregator struct {
		minShard int
		maxShard int

		reports       map[int]common.ShardFixReport
		status        ShardStatusResult
		statusSummary ShardStatusSummaryResult
		aggregation   AggregateFixReportResult
	}

	shardScanResultAggregator struct {
		minShard int
		maxShard int

		reports        map[int]common.ShardScanReport
		status         ShardStatusResult
		statusSummary  ShardStatusSummaryResult
		aggregation    AggregateScanReportResult
		shardSizes     ShardSizeQueryResult
		corruptionKeys map[int]common.Keys
	}
)

func newShardFixResultAggregator(
	corruptKeys []CorruptedKeysEntry,
	minShard int,
	maxShard int,
) *shardFixResultAggregator {
	status := make(map[int]ShardStatus)
	for _, s := range corruptKeys {
		status[s.ShardID] = ShardStatusRunning
	}
	statusSummary := map[ShardStatus]int{
		ShardStatusRunning:            len(corruptKeys),
		ShardStatusSuccess:            0,
		ShardStatusControlFlowFailure: 0,
	}
	return &shardFixResultAggregator{
		minShard: minShard,
		maxShard: maxShard,

		reports:       make(map[int]common.ShardFixReport),
		status:        status,
		statusSummary: statusSummary,
		aggregation:   AggregateFixReportResult{},
	}
}

func (a *shardFixResultAggregator) getStatusResult(req PaginatedShardQueryRequest) (*ShardStatusQueryResult, error) {
	return getStatusResult(a.minShard, a.maxShard, req, a.status)
}

func (a *shardFixResultAggregator) addReport(report common.ShardFixReport) {
	a.reports[report.ShardID] = report
	a.statusSummary[ShardStatusRunning]--
	if report.Result.ControlFlowFailure != nil {
		a.status[report.ShardID] = ShardStatusControlFlowFailure
		a.statusSummary[ShardStatusControlFlowFailure]++
	} else {
		a.status[report.ShardID] = ShardStatusSuccess
		a.statusSummary[ShardStatusSuccess]++
	}
	if report.Result.ShardFixKeys != nil {
		a.adjustAggregation(report.Stats, func(a, b int64) int64 { return a + b })
	}
}

func (a *shardFixResultAggregator) getReport(shardID int) (*common.ShardFixReport, error) {
	if _, ok := a.status[shardID]; !ok {
		return nil, fmt.Errorf("shard %v is not included in shards which will be processed", shardID)
	}
	if report, ok := a.reports[shardID]; ok {
		return &report, nil
	}
	return nil, fmt.Errorf("shard %v has not finished yet, check back later for report", shardID)
}

func (a *shardFixResultAggregator) adjustAggregation(stats common.ShardFixStats, fn func(a, b int64) int64) {
	a.aggregation.ExecutionCount = fn(a.aggregation.ExecutionCount, stats.ExecutionCount)
	a.aggregation.SkippedCount = fn(a.aggregation.SkippedCount, stats.SkippedCount)
	a.aggregation.FailedCount = fn(a.aggregation.FailedCount, stats.FailedCount)
	a.aggregation.FixedCount = fn(a.aggregation.FixedCount, stats.FixedCount)
}

func newShardScanResultAggregator(
	shards []int,
	minShard int,
	maxShard int,
) *shardScanResultAggregator {
	status := make(map[int]ShardStatus)
	for _, s := range shards {
		status[s] = ShardStatusRunning
	}
	statusSummary := map[ShardStatus]int{
		ShardStatusSuccess:            0,
		ShardStatusControlFlowFailure: 0,
		ShardStatusRunning:            len(shards),
	}
	return &shardScanResultAggregator{
		minShard: minShard,
		maxShard: maxShard,

		reports:       make(map[int]common.ShardScanReport),
		status:        status,
		statusSummary: statusSummary,
		shardSizes:    nil,
		aggregation: AggregateScanReportResult{
			CorruptionByType: make(map[common.InvariantType]int64),
		},
		corruptionKeys: make(map[int]common.Keys),
	}
}

func (a *shardScanResultAggregator) getShardSizeQueryResult(req ShardSizeQueryRequest) (ShardSizeQueryResult, error) {
	if req.StartIndex < 0 || req.StartIndex >= req.EndIndex || req.EndIndex > len(a.shardSizes) {
		return nil, fmt.Errorf("index out of bounds exception (required startIndex >= 0 && startIndex < endIndex && endIndex <= %v)", len(a.shardSizes))
	}
	if req.EndIndex-req.StartIndex > maxShardQueryResult {
		return nil, fmt.Errorf("too many shards requested, the limit is %v", maxShardQueryResult)
	}
	return a.shardSizes[req.StartIndex:req.EndIndex], nil
}

func (a *shardScanResultAggregator) getCorruptionKeys(req PaginatedShardQueryRequest) (*ShardCorruptKeysQueryResult, error) {
	startingShardID := a.minShard
	if req.StartingShardID != nil {
		startingShardID = *req.StartingShardID
	}
	if err := shardInBounds(a.minShard, a.maxShard, startingShardID); err != nil {
		return nil, err
	}
	limit := maxShardQueryResult
	if req.LimitShards != nil && *req.LimitShards > 0 && *req.LimitShards < maxShardQueryResult {
		limit = *req.LimitShards
	}
	result := make(map[int]common.Keys)
	currentShardID := startingShardID
	for len(result) < limit && currentShardID <= a.maxShard {
		keys, ok := a.corruptionKeys[currentShardID]
		if !ok {
			currentShardID++
			continue
		}
		result[currentShardID] = keys
		currentShardID++
	}
	if currentShardID > a.maxShard {
		return &ShardCorruptKeysQueryResult{
			Result: result,
			ShardQueryPaginationToken: ShardQueryPaginationToken{
				NextShardID: nil,
				IsDone:      true,
			},
		}, nil
	}
	return &ShardCorruptKeysQueryResult{
		Result: result,
		ShardQueryPaginationToken: ShardQueryPaginationToken{
			NextShardID: &currentShardID,
			IsDone:      false,
		},
	}, nil
}

func (a *shardScanResultAggregator) getStatusResult(req PaginatedShardQueryRequest) (*ShardStatusQueryResult, error) {
	return getStatusResult(a.minShard, a.maxShard, req, a.status)
}

func (a *shardScanResultAggregator) addReport(report common.ShardScanReport) {
	if report.Result.ShardScanKeys != nil {
		a.insertReportIntoSizes(report)
	}
	a.reports[report.ShardID] = report
	a.statusSummary[ShardStatusRunning]--
	if report.Result.ControlFlowFailure != nil {
		a.status[report.ShardID] = ShardStatusControlFlowFailure
		a.statusSummary[ShardStatusControlFlowFailure]++
	} else {
		a.status[report.ShardID] = ShardStatusSuccess
		a.statusSummary[ShardStatusSuccess]++
	}
	if report.Result.ShardScanKeys != nil {
		a.adjustAggregation(report.Stats, func(a, b int64) int64 { return a + b })
		if report.Result.ShardScanKeys.Corrupt != nil {
			a.corruptionKeys[report.ShardID] = *report.Result.ShardScanKeys.Corrupt
		}
	}
}

func (a *shardScanResultAggregator) insertReportIntoSizes(report common.ShardScanReport) {
	tuple := ShardSizeTuple{
		ShardID:         report.ShardID,
		ExecutionsCount: report.Stats.ExecutionsCount,
	}
	insertIndex := 0
	for insertIndex < len(a.shardSizes) {
		if a.shardSizes[insertIndex].ExecutionsCount < tuple.ExecutionsCount {
			break
		}
		insertIndex++
	}
	newShardSizes := append([]ShardSizeTuple{}, a.shardSizes[0:insertIndex]...)
	newShardSizes = append(newShardSizes, tuple)
	newShardSizes = append(newShardSizes, a.shardSizes[insertIndex:]...)
	a.shardSizes = newShardSizes
}

func (a *shardScanResultAggregator) getShardDistributionStats() ShardDistributionStats {
	return ShardDistributionStats{
		Max:    a.shardSizes[0].ExecutionsCount,
		Median: a.shardSizes[int(float64(len(a.shardSizes))*.5)].ExecutionsCount,
		Min:    a.shardSizes[len(a.shardSizes)-1].ExecutionsCount,
		P90:    a.shardSizes[int(float64(len(a.shardSizes))*.1)].ExecutionsCount,
		P75:    a.shardSizes[int(float64(len(a.shardSizes))*.25)].ExecutionsCount,
		P25:    a.shardSizes[int(float64(len(a.shardSizes))*.75)].ExecutionsCount,
		P10:    a.shardSizes[int(float64(len(a.shardSizes))*.9)].ExecutionsCount,
	}
}

func (a *shardScanResultAggregator) getReport(shardID int) (*common.ShardScanReport, error) {
	if _, ok := a.status[shardID]; !ok {
		return nil, fmt.Errorf("shard %v is not included in shards which will be processed", shardID)
	}
	if report, ok := a.reports[shardID]; ok {
		return &report, nil
	}
	return nil, fmt.Errorf("shard %v has not finished yet, check back later for report", shardID)
}

func (a *shardScanResultAggregator) adjustAggregation(stats common.ShardScanStats, fn func(a, b int64) int64) {
	a.aggregation.ExecutionsCount = fn(a.aggregation.ExecutionsCount, stats.ExecutionsCount)
	a.aggregation.CorruptedCount = fn(a.aggregation.CorruptedCount, stats.CorruptedCount)
	a.aggregation.CheckFailedCount = fn(a.aggregation.CheckFailedCount, stats.CheckFailedCount)
	a.aggregation.CorruptedOpenExecutionCount = fn(a.aggregation.CorruptedOpenExecutionCount, stats.CorruptedOpenExecutionCount)
	for k, v := range stats.CorruptionByType {
		a.aggregation.CorruptionByType[k] = fn(a.aggregation.CorruptionByType[k], v)
	}
}

func getStatusResult(
	minShardID int,
	maxShardID int,
	req PaginatedShardQueryRequest,
	status ShardStatusResult,
) (*ShardStatusQueryResult, error) {
	startingShardID := minShardID
	if req.StartingShardID != nil {
		startingShardID = *req.StartingShardID
	}
	if err := shardInBounds(minShardID, maxShardID, startingShardID); err != nil {
		return nil, err
	}
	limit := maxShardQueryResult
	if req.LimitShards != nil && *req.LimitShards > 0 && *req.LimitShards < maxShardQueryResult {
		limit = *req.LimitShards
	}
	result := make(map[int]ShardStatus)
	currentShardID := startingShardID
	for len(result) < limit && currentShardID <= maxShardID {
		status, ok := status[currentShardID]
		if !ok {
			currentShardID++
			continue
		}
		result[currentShardID] = status
		currentShardID++
	}
	if currentShardID > maxShardID {
		return &ShardStatusQueryResult{
			Result: result,
			ShardQueryPaginationToken: ShardQueryPaginationToken{
				NextShardID: nil,
				IsDone:      true,
			},
		}, nil
	}
	return &ShardStatusQueryResult{
		Result: result,
		ShardQueryPaginationToken: ShardQueryPaginationToken{
			NextShardID: &currentShardID,
			IsDone:      false,
		},
	}, nil
}
