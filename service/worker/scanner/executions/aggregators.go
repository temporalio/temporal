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

		reports     map[int]common.ShardFixReport
		status      ShardStatusResult
		aggregation AggregateFixReportResult
	}

	shardScanResultAggregator struct {
		minShard int
		maxShard int

		reports                 map[int]common.ShardScanReport
		status                  ShardStatusResult
		aggregation             AggregateScanReportResult
		corruptionKeys          map[int]common.Keys
		controlFlowFailureCount int
		successCount            int
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
	return &shardFixResultAggregator{
		minShard: minShard,
		maxShard: maxShard,

		reports:     make(map[int]common.ShardFixReport),
		status:      status,
		aggregation: AggregateFixReportResult{},
	}
}

func (a *shardFixResultAggregator) getStatusResult(req PaginatedShardQueryRequest) (*ShardStatusQueryResult, error) {
	return getStatusResult(a.minShard, a.maxShard, req, a.status)
}

func (a *shardFixResultAggregator) addReport(report common.ShardFixReport) {
	a.removeReport(report.ShardID)
	a.reports[report.ShardID] = report
	if report.Result.ControlFlowFailure != nil {
		a.status[report.ShardID] = ShardStatusControlFlowFailure
	} else {
		a.status[report.ShardID] = ShardStatusSuccess
	}
	if report.Result.ShardFixKeys != nil {
		a.adjustAggregation(report.Stats, func(a, b int64) int64 { return a + b })
	}
}

func (a *shardFixResultAggregator) removeReport(shardID int) {
	report, ok := a.reports[shardID]
	if !ok {
		return
	}
	delete(a.reports, shardID)
	delete(a.status, shardID)
	if report.Result.ShardFixKeys != nil {
		a.adjustAggregation(report.Stats, func(a, b int64) int64 { return a - b })
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
	return &shardScanResultAggregator{
		minShard: minShard,
		maxShard: maxShard,

		reports: make(map[int]common.ShardScanReport),
		status:  status,
		aggregation: AggregateScanReportResult{
			CorruptionByType: make(map[common.InvariantType]int64),
		},
		corruptionKeys:          make(map[int]common.Keys),
		controlFlowFailureCount: 0,
		successCount:            0,
	}
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
	a.removeReport(report.ShardID)
	a.reports[report.ShardID] = report
	if report.Result.ControlFlowFailure != nil {
		a.status[report.ShardID] = ShardStatusControlFlowFailure
		a.controlFlowFailureCount++
	} else {
		a.status[report.ShardID] = ShardStatusSuccess
		a.successCount++
	}
	if report.Result.ShardScanKeys != nil {
		a.adjustAggregation(report.Stats, func(a, b int64) int64 { return a + b })
		if report.Result.ShardScanKeys.Corrupt != nil {
			a.corruptionKeys[report.ShardID] = *report.Result.ShardScanKeys.Corrupt
		}
	}
}

func (a *shardScanResultAggregator) removeReport(shardID int) {
	report, ok := a.reports[shardID]
	if !ok {
		return
	}
	delete(a.reports, shardID)
	delete(a.status, shardID)
	delete(a.corruptionKeys, shardID)
	if report.Result.ShardScanKeys != nil {
		a.adjustAggregation(report.Stats, func(a, b int64) int64 { return a - b })
		a.successCount--
	} else {
		a.controlFlowFailureCount--
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
