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
	"errors"

	"go.uber.org/cadence/workflow"

	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/reconciliation/common"
	"github.com/uber/cadence/common/resource"
)

const (
	// ConcreteScannerContextKey is the key used to access ScannerContext in activities for concrete executions
	ConcreteScannerContextKey = ContextKey(0)
	// ConcreteFixerContextKey is the key used to access FixerContext in activities for concrete executions
	ConcreteFixerContextKey = ContextKey(1)
	// CurrentScannerContextKey is the key used to access ScannerContext in activities for current executions
	CurrentScannerContextKey = ContextKey(2)
	// CurrentFixerContextKey is the key used to access FixerContext in activities for current executions
	CurrentFixerContextKey = ContextKey(3)

	// ShardReportQuery is the query name for the query used to get a single shard's report
	ShardReportQuery = "shard_report"
	// ShardStatusQuery is the query name for the query used to get the status of all shards
	ShardStatusQuery = "shard_status"
	// ShardStatusSummaryQuery is the query name for the query used to get the shard status -> counts map
	ShardStatusSummaryQuery = "shard_status_summary"
	// AggregateReportQuery is the query name for the query used to get the aggregate result of all finished shards
	AggregateReportQuery = "aggregate_report"
	// ShardCorruptKeysQuery is the query name for the query used to get all completed shards with at least one corruption
	ShardCorruptKeysQuery = "shard_corrupt_keys"
	// ShardSizeQuery is the query name for the query used to get the number of executions per shard in sorted order
	ShardSizeQuery = "shard_size"

	// ShardStatusRunning indicates the shard has not completed yet
	ShardStatusRunning ShardStatus = "running"
	// ShardStatusSuccess indicates the scan on the shard ran successfully
	ShardStatusSuccess ShardStatus = "success"
	// ShardStatusControlFlowFailure indicates the scan on the shard failed
	ShardStatusControlFlowFailure ShardStatus = "control_flow_failure"

	scanShardReportChan = "scanShardReportChan"
	fixShardReportChan  = "fixShardReportChan"

	maxShardQueryResult = 1000
)

// ScanTypeScannerContextKeyMap maps execution type to the context key used by scanner
var ScanTypeScannerContextKeyMap = map[common.ScanType]interface{}{
	common.ConcreteExecutionType: ConcreteScannerContextKey,
	common.CurrentExecutionType:  CurrentScannerContextKey,
}

// ScanTypeFixerContextKeyMap maps execution type to the context key used by fixer
var ScanTypeFixerContextKeyMap = map[common.ScanType]interface{}{
	common.ConcreteExecutionType: ConcreteFixerContextKey,
	common.CurrentExecutionType:  CurrentFixerContextKey,
}

type (
	// ContextKey is the type which identifies context keys
	ContextKey int

	// ScannerContext is the resource that is available in activities under ConcreteScannerContextKey context key
	ScannerContext struct {
		Resource                     resource.Resource
		Scope                        metrics.Scope
		ScannerWorkflowDynamicConfig *ScannerWorkflowDynamicConfig
	}

	// FixerContext is the resource that is available to activities under ConcreteFixerContextKey
	FixerContext struct {
		Resource resource.Resource
		Scope    metrics.Scope
	}

	// ScannerWorkflowParams are the parameters to the scan workflow
	ScannerWorkflowParams struct {
		Shards                          Shards
		ScannerWorkflowConfigOverwrites ScannerWorkflowConfigOverwrites
		ScanType                        common.ScanType
	}

	// FixerWorkflowParams are the parameters to the fix workflow
	FixerWorkflowParams struct {
		ScannerWorkflowWorkflowID     string
		ScannerWorkflowRunID          string
		FixerWorkflowConfigOverwrites FixerWorkflowConfigOverwrites
		ScanType                      common.ScanType
	}

	// Shards identify the shards that should be scanned.
	// Exactly one of List or Range should be non-nil.
	Shards struct {
		List  []int
		Range *ShardRange
	}

	// ShardRange identifies a set of shards based on min (inclusive) and max (exclusive)
	ShardRange struct {
		Min int
		Max int
	}

	// ShardStatusResult indicates the status for all shards
	ShardStatusResult map[int]ShardStatus

	// ShardStatusSummaryResult indicates the counts of shards in each status
	ShardStatusSummaryResult map[ShardStatus]int

	// AggregateScanReportResult indicates the result of summing together all
	// shard reports which have finished scan.
	AggregateScanReportResult common.ShardScanStats

	// AggregateFixReportResult indicates the result of summing together all
	// shard reports that have finished for fix.
	AggregateFixReportResult common.ShardFixStats

	// ShardCorruptKeysResult is a map of all shards which have finished scan successfully and have at least one corruption
	ShardCorruptKeysResult map[int]common.Keys

	// ShardStatus is the type which indicates the status of a shard scan.
	ShardStatus string

	// ScanReportError is a type that is used to send either error or report on a channel.
	// Exactly one of Report and ErrorStr should be non-nil.
	ScanReportError struct {
		Reports  []common.ShardScanReport
		ErrorStr *string
	}

	// FixReportError is a type that is used to send either error or report on a channel.
	// Exactly one of Report and ErrorStr should be non-nil.
	FixReportError struct {
		Reports  []common.ShardFixReport
		ErrorStr *string
	}

	// ShardSizeQueryRequest is the request used for ShardSizeQuery.
	// The following must be true: 0 <= StartIndex < EndIndex <= len(shards successfully finished)
	// The following must be true: EndIndex - StartIndex <= maxShardQueryResult.
	// StartIndex is inclusive, EndIndex is exclusive.
	ShardSizeQueryRequest struct {
		StartIndex int
		EndIndex   int
	}

	// PaginatedShardQueryRequest is the request used for queries which return results over all shards
	PaginatedShardQueryRequest struct {
		// StartingShardID is the first shard to start iteration from.
		// Setting to nil will start iteration from the beginning of the shards.
		StartingShardID *int
		// LimitShards indicates the maximum number of results that can be returned.
		// If nil or larger than allowed maximum, will default to maximum allowed.
		LimitShards *int
	}

	// ShardQueryPaginationToken is used to return information used to make the next query
	ShardQueryPaginationToken struct {
		// NextShardID is one greater than the highest shard returned in the current query.
		// NextShardID is nil if IsDone is true.
		// It is possible to get NextShardID != nil and on the next call to get an empty result with IsDone = true.
		NextShardID *int
		IsDone      bool
	}

	// ShardStatusQueryResult is the query result for ShardStatusQuery
	ShardStatusQueryResult struct {
		Result                    ShardStatusResult
		ShardQueryPaginationToken ShardQueryPaginationToken
	}

	// ShardCorruptKeysQueryResult is the query result for ShardCorruptKeysQuery
	ShardCorruptKeysQueryResult struct {
		Result                    ShardCorruptKeysResult
		ShardQueryPaginationToken ShardQueryPaginationToken
	}

	// ShardSizeQueryResult is the result from ShardSizeQuery.
	// Contains sorted list of shards, sorted by the number of executions per shard.
	ShardSizeQueryResult []ShardSizeTuple

	// ShardSizeTuple indicates the size and sorted index of a single shard
	ShardSizeTuple struct {
		ShardID         int
		ExecutionsCount int64
	}
)

var (
	errQueryNotReady = errors.New("query is not yet ready to be handled, please try again shortly")
)

// Validate validates shard list or range
func (s Shards) Validate() error {
	if s.List == nil && s.Range == nil {
		return errors.New("must provide either List or Range")
	}
	if s.List != nil && s.Range != nil {
		return errors.New("only one of List or Range can be provided")
	}
	if s.List != nil && len(s.List) == 0 {
		return errors.New("empty List provided")
	}
	if s.Range != nil && s.Range.Max <= s.Range.Min {
		return errors.New("empty Range provided")
	}
	return nil
}

// Flatten flattens Shards to a list of shard IDs and finds the min/max shardID
func (s Shards) Flatten() ([]int, int, int) {
	shardList := s.List
	if len(shardList) == 0 {
		shardList = []int{}
		for i := s.Range.Min; i < s.Range.Max; i++ {
			shardList = append(shardList, i)
		}
	}
	min := shardList[0]
	max := shardList[0]
	for i := 1; i < len(shardList); i++ {
		if shardList[i] < min {
			min = shardList[i]
		}
		if shardList[i] > max {
			max = shardList[i]
		}
	}

	return shardList, min, max
}

// ScannerWorkflow is the workflow that scans over all executions for the given scan type
func ScannerWorkflow(
	ctx workflow.Context,
	params ScannerWorkflowParams,
) error {
	if err := params.Shards.Validate(); err != nil {
		return err
	}
	shards, minShard, maxShard := params.Shards.Flatten()
	aggregator := newShardScanResultAggregator(shards, minShard, maxShard)
	if err := workflow.SetQueryHandler(ctx, ShardReportQuery, func(shardID int) (*common.ShardScanReport, error) {
		return aggregator.getReport(shardID)
	}); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(ctx, ShardStatusQuery, func(req PaginatedShardQueryRequest) (*ShardStatusQueryResult, error) {
		return aggregator.getStatusResult(req)
	}); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(ctx, ShardStatusSummaryQuery, func() (ShardStatusSummaryResult, error) {
		return aggregator.statusSummary, nil
	}); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(ctx, AggregateReportQuery, func() (AggregateScanReportResult, error) {
		return aggregator.aggregation, nil
	}); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(ctx, ShardCorruptKeysQuery, func(req PaginatedShardQueryRequest) (*ShardCorruptKeysQueryResult, error) {
		return aggregator.getCorruptionKeys(req)
	}); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(ctx, ShardSizeQuery, func(req ShardSizeQueryRequest) (ShardSizeQueryResult, error) {
		return aggregator.getShardSizeQueryResult(req)
	}); err != nil {
		return err
	}

	activityCtx := getShortActivityContext(ctx)
	var resolvedConfig ResolvedScannerWorkflowConfig
	if err := workflow.ExecuteActivity(activityCtx, ScannerConfigActivityName, ScannerConfigActivityParams{
		Overwrites: params.ScannerWorkflowConfigOverwrites,
		ScanType:   params.ScanType,
	}).Get(ctx, &resolvedConfig); err != nil {
		return err
	}

	if !resolvedConfig.Enabled {
		return nil
	}

	shardReportChan := workflow.GetSignalChannel(ctx, scanShardReportChan)
	for i := 0; i < resolvedConfig.Concurrency; i++ {
		idx := i
		workflow.Go(ctx, func(ctx workflow.Context) {
			batches := getShardBatches(resolvedConfig.ActivityBatchSize, resolvedConfig.Concurrency, shards, idx)
			for _, batch := range batches {
				activityCtx = getLongActivityContext(ctx)
				var reports []common.ShardScanReport
				if err := workflow.ExecuteActivity(activityCtx, ScannerScanShardActivityName, ScanShardActivityParams{
					Shards:                  batch,
					ExecutionsPageSize:      resolvedConfig.ExecutionsPageSize,
					BlobstoreFlushThreshold: resolvedConfig.BlobstoreFlushThreshold,
					InvariantCollections:    resolvedConfig.InvariantCollections,
					ScanType:                params.ScanType,
				}).Get(ctx, &reports); err != nil {
					errStr := err.Error()
					shardReportChan.Send(ctx, ScanReportError{
						Reports:  nil,
						ErrorStr: &errStr,
					})
					return
				}
				shardReportChan.Send(ctx, ScanReportError{
					Reports:  reports,
					ErrorStr: nil,
				})
			}
		})
	}

	for i := 0; i < len(shards); {
		var reportErr ScanReportError
		shardReportChan.Receive(ctx, &reportErr)
		if reportErr.ErrorStr != nil {
			return errors.New(*reportErr.ErrorStr)
		}
		for _, report := range reportErr.Reports {
			aggregator.addReport(report)
			i++
		}
	}

	activityCtx = getShortActivityContext(ctx)
	if err := workflow.ExecuteActivity(activityCtx, ScannerEmitMetricsActivityName, ScannerEmitMetricsActivityParams{
		ShardSuccessCount:            aggregator.statusSummary[ShardStatusSuccess],
		ShardControlFlowFailureCount: aggregator.statusSummary[ShardStatusControlFlowFailure],
		AggregateReportResult:        aggregator.aggregation,
		ShardDistributionStats:       aggregator.getShardDistributionStats(),
		ScanType:                     params.ScanType,
	}).Get(ctx, nil); err != nil {
		return err
	}

	return nil
}

// FixerWorkflow is the workflow that fixes all concrete executions from a scan output.
func FixerWorkflow(
	ctx workflow.Context,
	params FixerWorkflowParams,
) error {
	var aggregator *shardFixResultAggregator
	if err := workflow.SetQueryHandler(ctx, ShardReportQuery, func(shardID int) (*common.ShardFixReport, error) {
		if aggregator == nil {
			return nil, errQueryNotReady
		}
		return aggregator.getReport(shardID)
	}); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(ctx, ShardStatusQuery, func(req PaginatedShardQueryRequest) (*ShardStatusQueryResult, error) {
		if aggregator == nil {
			return nil, errQueryNotReady
		}
		return aggregator.getStatusResult(req)
	}); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(ctx, ShardStatusSummaryQuery, func() (ShardStatusSummaryResult, error) {
		if aggregator == nil {
			return nil, errQueryNotReady
		}
		return aggregator.statusSummary, nil
	}); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(ctx, AggregateReportQuery, func() (AggregateFixReportResult, error) {
		if aggregator == nil {
			return AggregateFixReportResult{}, errQueryNotReady
		}
		return aggregator.aggregation, nil
	}); err != nil {
		return err
	}

	resolvedConfig := resolveFixerConfig(params.FixerWorkflowConfigOverwrites)
	shardReportChan := workflow.GetSignalChannel(ctx, fixShardReportChan)
	corruptKeys, err := getCorruptedKeys(ctx, params)
	if err != nil {
		return err
	}
	if len(corruptKeys.CorruptedKeys) == 0 {
		return nil
	}

	for i := 0; i < resolvedConfig.Concurrency; i++ {
		idx := i
		workflow.Go(ctx, func(ctx workflow.Context) {
			batches := getCorruptedKeysBatches(resolvedConfig.ActivityBatchSize, resolvedConfig.Concurrency, corruptKeys.CorruptedKeys, idx)
			for _, batch := range batches {
				activityCtx := getLongActivityContext(ctx)
				var reports []common.ShardFixReport
				if err := workflow.ExecuteActivity(activityCtx, FixerFixShardActivityName, FixShardActivityParams{
					CorruptedKeysEntries:        batch,
					ResolvedFixerWorkflowConfig: resolvedConfig,
					ScanType:                    params.ScanType,
				}).Get(ctx, &reports); err != nil {
					errStr := err.Error()
					shardReportChan.Send(ctx, FixReportError{
						Reports:  nil,
						ErrorStr: &errStr,
					})
					return
				}
				shardReportChan.Send(ctx, FixReportError{
					Reports:  reports,
					ErrorStr: nil,
				})
			}
		})
	}

	aggregator = newShardFixResultAggregator(corruptKeys.CorruptedKeys, *corruptKeys.MinShard, *corruptKeys.MaxShard)
	for i := 0; i < len(corruptKeys.CorruptedKeys); {
		var reportErr FixReportError
		shardReportChan.Receive(ctx, &reportErr)
		if reportErr.ErrorStr != nil {
			return errors.New(*reportErr.ErrorStr)
		}
		for _, report := range reportErr.Reports {
			aggregator.addReport(report)
			i++
		}
	}
	return nil
}

func getCorruptedKeys(
	ctx workflow.Context,
	params FixerWorkflowParams,
) (*FixerCorruptedKeysActivityResult, error) {
	fixerCorruptedKeysActivityParams := FixerCorruptedKeysActivityParams{
		ScannerWorkflowWorkflowID: params.ScannerWorkflowWorkflowID,
		ScannerWorkflowRunID:      params.ScannerWorkflowRunID,
		StartingShardID:           nil,
		ScanType:                  params.ScanType,
	}
	var minShardID *int
	var maxShardID *int
	var corruptKeys []CorruptedKeysEntry
	isFirst := true
	for isFirst || fixerCorruptedKeysActivityParams.StartingShardID != nil {
		isFirst = false
		corruptedKeysResult := &FixerCorruptedKeysActivityResult{}
		activityCtx := getShortActivityContext(ctx)
		if err := workflow.ExecuteActivity(
			activityCtx,
			FixerCorruptedKeysActivityName,
			fixerCorruptedKeysActivityParams).Get(ctx, &corruptedKeysResult); err != nil {
			return nil, err
		}

		fixerCorruptedKeysActivityParams.StartingShardID = corruptedKeysResult.ShardQueryPaginationToken.NextShardID
		if len(corruptedKeysResult.CorruptedKeys) == 0 {
			continue
		}
		corruptKeys = append(corruptKeys, corruptedKeysResult.CorruptedKeys...)
		if corruptedKeysResult.MinShard != nil && (minShardID == nil || *minShardID > *corruptedKeysResult.MinShard) {
			minShardID = corruptedKeysResult.MinShard
		}
		if corruptedKeysResult.MaxShard != nil && (maxShardID == nil || *maxShardID < *corruptedKeysResult.MaxShard) {
			maxShardID = corruptedKeysResult.MaxShard
		}
	}
	return &FixerCorruptedKeysActivityResult{
		CorruptedKeys: corruptKeys,
		MinShard:      minShardID,
		MaxShard:      maxShardID,
		ShardQueryPaginationToken: ShardQueryPaginationToken{
			NextShardID: nil,
			IsDone:      true,
		},
	}, nil
}
