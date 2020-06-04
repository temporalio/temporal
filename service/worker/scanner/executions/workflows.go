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
	"github.com/uber/cadence/common/resource"
	"github.com/uber/cadence/service/worker/scanner/executions/common"
)

const (
	// ScannerContextKey is the key used to access ScannerContext in activities
	ScannerContextKey = ContextKey(0)
	// FixerContextKey is the key used to access FixerContext in activities
	FixerContextKey = ContextKey(1)

	// ShardReportQuery is the query name for the query used to get a single shard's report
	ShardReportQuery = "shard_report"
	// ShardStatusQuery is the query name for the query used to get the status of all shards
	ShardStatusQuery = "shard_status_query"
	// AggregateReportQuery is the query name for the query used to get the aggregate result of all finished shards
	AggregateReportQuery = "aggregate_report"
	// ShardCorruptKeysQuery is the query name for the query used to get all completed shards with at least one corruption
	ShardCorruptKeysQuery = "shard_corrupt_keys"

	// ShardStatusRunning indicates the shard has not completed yet
	ShardStatusRunning ShardStatus = "running"
	// ShardStatusSuccess indicates the scan on the shard ran successfully
	ShardStatusSuccess ShardStatus = "success"
	// ShardStatusControlFlowFailure indicates the scan on the shard failed
	ShardStatusControlFlowFailure ShardStatus = "control_flow_failure"

	scanShardReportChan = "scanShardReportChan"
	fixShardReportChan  = "fixShardReportChan"
)

type (
	// ContextKey is the type which identifies context keys
	ContextKey int

	// ScannerContext is the resource that is available in activities under ScannerContextKey context key
	ScannerContext struct {
		Resource                     resource.Resource
		Scope                        metrics.Scope
		ScannerWorkflowDynamicConfig *ScannerWorkflowDynamicConfig
	}

	// FixerContext is the resource that is available to activities under FixerContextKey
	FixerContext struct {
		Resource resource.Resource
		Scope    metrics.Scope
	}

	// ScannerWorkflowParams are the parameters to the scan workflow
	ScannerWorkflowParams struct {
		Shards                          Shards
		ScannerWorkflowConfigOverwrites ScannerWorkflowConfigOverwrites
	}

	// FixerWorkflowParams are the parameters to the fix workflow
	FixerWorkflowParams struct {
		FixerCorruptedKeysActivityParams FixerCorruptedKeysActivityParams
		FixerWorkflowConfigOverwrites    FixerWorkflowConfigOverwrites
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
		Report   *common.ShardScanReport
		ErrorStr *string
	}

	// FixReportError is a type that is used to send either error or report on a channel.
	// Exactly one of Report and ErrorStr should be non-nil.
	FixReportError struct {
		Report   *common.ShardFixReport
		ErrorStr *string
	}
)

var (
	errQueryNotReady = errors.New("query is not yet ready to be handled, please try again shortly")
)

// ScannerWorkflow is the workflow that scans over all concrete executions
func ScannerWorkflow(
	ctx workflow.Context,
	params ScannerWorkflowParams,
) error {
	shards := flattenShards(params.Shards)
	aggregator := newShardScanResultAggregator(shards)
	if err := workflow.SetQueryHandler(ctx, ShardReportQuery, func(shardID int) (*common.ShardScanReport, error) {
		return aggregator.getReport(shardID)
	}); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(ctx, ShardStatusQuery, func() (ShardStatusResult, error) {
		return aggregator.status, nil
	}); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(ctx, AggregateReportQuery, func() (AggregateScanReportResult, error) {
		return aggregator.aggregation, nil
	}); err != nil {
		return err
	}
	if err := workflow.SetQueryHandler(ctx, ShardCorruptKeysQuery, func() (ShardCorruptKeysResult, error) {
		return aggregator.corruptionKeys, nil
	}); err != nil {
		return err
	}

	activityCtx := getShortActivityContext(ctx)
	var resolvedConfig ResolvedScannerWorkflowConfig
	if err := workflow.ExecuteActivity(activityCtx, ScannerConfigActivityName, ScannerConfigActivityParams{
		Overwrites: params.ScannerWorkflowConfigOverwrites,
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
			for _, shard := range shards {
				if shard%resolvedConfig.Concurrency == idx {
					activityCtx = getLongActivityContext(ctx)
					report := &common.ShardScanReport{}
					if err := workflow.ExecuteActivity(activityCtx, ScannerScanShardActivityName, ScanShardActivityParams{
						ShardID:                 shard,
						ExecutionsPageSize:      resolvedConfig.ExecutionsPageSize,
						BlobstoreFlushThreshold: resolvedConfig.BlobstoreFlushThreshold,
						InvariantCollections:    resolvedConfig.InvariantCollections,
					}).Get(ctx, &report); err != nil {
						errStr := err.Error()
						shardReportChan.Send(ctx, ScanReportError{
							Report:   nil,
							ErrorStr: &errStr,
						})
						return
					}
					shardReportChan.Send(ctx, ScanReportError{
						Report:   report,
						ErrorStr: nil,
					})
				}
			}
		})
	}

	for i := 0; i < len(shards); i++ {
		var reportErr ScanReportError
		shardReportChan.Receive(ctx, &reportErr)
		if reportErr.ErrorStr != nil {
			return errors.New(*reportErr.ErrorStr)
		}
		aggregator.addReport(*reportErr.Report)
	}

	activityCtx = getShortActivityContext(ctx)
	if err := workflow.ExecuteActivity(activityCtx, ScannerEmitMetricsActivityName, ScannerEmitMetricsActivityParams{
		ShardStatusResult:     aggregator.status,
		AggregateReportResult: aggregator.aggregation,
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
	if err := workflow.SetQueryHandler(ctx, ShardStatusQuery, func() (ShardStatusResult, error) {
		if aggregator == nil {
			return nil, errQueryNotReady
		}
		return aggregator.status, nil
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
	activityCtx := getShortActivityContext(ctx)
	corruptedKeysResult := &FixerCorruptedKeysActivityResult{}
	if err := workflow.ExecuteActivity(
		activityCtx, FixerCorruptedKeysActivityName, params.FixerCorruptedKeysActivityParams).Get(ctx, &corruptedKeysResult); err != nil {
		return err
	}

	resolvedConfig := resolveFixerConfig(params.FixerWorkflowConfigOverwrites)
	shardReportChan := workflow.GetSignalChannel(ctx, fixShardReportChan)
	for i := 0; i < resolvedConfig.Concurrency; i++ {
		idx := i
		workflow.Go(ctx, func(ctx workflow.Context) {
			for j, corrupted := range corruptedKeysResult.CorruptedKeys {
				if j%resolvedConfig.Concurrency == idx {
					activityCtx = getLongActivityContext(ctx)
					report := &common.ShardFixReport{}
					if err := workflow.ExecuteActivity(activityCtx, FixerFixShardActivityName, FixShardActivityParams{
						CorruptedKeysEntry:          corrupted,
						ResolvedFixerWorkflowConfig: resolvedConfig,
					}).Get(ctx, &report); err != nil {
						errStr := err.Error()
						shardReportChan.Send(ctx, FixReportError{
							Report:   nil,
							ErrorStr: &errStr,
						})
						return
					}
					shardReportChan.Send(ctx, FixReportError{
						Report:   report,
						ErrorStr: nil,
					})
				}
			}
		})
	}

	aggregator = newShardFixResultAggregator(corruptedKeysResult.Shards)
	for i := 0; i < len(corruptedKeysResult.CorruptedKeys); i++ {
		var reportErr FixReportError
		shardReportChan.Receive(ctx, &reportErr)
		if reportErr.ErrorStr != nil {
			return errors.New(*reportErr.ErrorStr)
		}
		aggregator.addReport(*reportErr.Report)
	}
	return nil
}
