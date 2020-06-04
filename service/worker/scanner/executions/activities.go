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
	"context"
	"encoding/json"
	"errors"

	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/activity"

	c "github.com/uber/cadence/common"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/service/worker/scanner/executions/common"
	"github.com/uber/cadence/service/worker/scanner/executions/shard"
)

const (
	// ScannerConfigActivityName is the activity name ScannerConfigActivity
	ScannerConfigActivityName = "cadence-sys-executions-scanner-config-activity"
	// ScannerScanShardActivityName is the activity name for ScanShardActivity
	ScannerScanShardActivityName = "cadence-sys-executions-scanner-scan-shard-activity"
	// ScannerEmitMetricsActivityName is the activity name for ScannerEmitMetricsActivity
	ScannerEmitMetricsActivityName = "cadence-sys-executions-scanner-emit-metrics-activity"
	// FixerCorruptedKeysActivityName is the activity name for FixerCorruptedKeysActivity
	FixerCorruptedKeysActivityName = "cadence-sys-executions-fixer-corrupted-keys-activity"
	// FixerFixShardActivityName is the activity name for FixShardActivity
	FixerFixShardActivityName = "cadence-sys-executions-fixer-fix-shard-activity"
)

type (
	// ScannerConfigActivityParams is the parameter for ScannerConfigActivity
	ScannerConfigActivityParams struct {
		Overwrites ScannerWorkflowConfigOverwrites
	}

	// ScanShardActivityParams is the parameter for ScanShardActivity
	ScanShardActivityParams struct {
		ShardID                 int
		ExecutionsPageSize      int
		BlobstoreFlushThreshold int
		InvariantCollections    InvariantCollections
	}

	// ScannerEmitMetricsActivityParams is the parameter for ScannerEmitMetricsActivity
	ScannerEmitMetricsActivityParams struct {
		ShardStatusResult     ShardStatusResult
		AggregateReportResult AggregateScanReportResult
	}

	// FixerCorruptedKeysActivityParams is the parameter for FixerCorruptedKeysActivity
	FixerCorruptedKeysActivityParams struct {
		ScannerWorkflowWorkflowID string
		ScannerWorkflowRunID      string
	}

	// FixShardActivityParams is the parameter for FixShardActivity
	FixShardActivityParams struct {
		CorruptedKeysEntry          CorruptedKeysEntry
		ResolvedFixerWorkflowConfig ResolvedFixerWorkflowConfig
	}

	FixerCorruptedKeysActivityResult struct {
		CorruptedKeys []CorruptedKeysEntry
		Shards        []int
	}

	// CorruptedKeysEntry is a pair of shardID and corrupted keys
	CorruptedKeysEntry struct {
		ShardID       int
		CorruptedKeys common.Keys
	}
)

// ScannerEmitMetricsActivity will emit metrics for a complete run of scanner
func ScannerEmitMetricsActivity(
	activityCtx context.Context,
	params ScannerEmitMetricsActivityParams,
) error {
	scope := activityCtx.Value(ScannerContextKey).(ScannerContext).Scope.Tagged(metrics.ActivityTypeTag(ScannerEmitMetricsActivityName))
	shardSuccess := 0
	shardControlFlowFailure := 0
	for _, v := range params.ShardStatusResult {
		switch v {
		case ShardStatusSuccess:
			shardSuccess++
		case ShardStatusControlFlowFailure:
			shardControlFlowFailure++
		}
	}
	scope.UpdateGauge(metrics.CadenceShardSuccessGauge, float64(shardSuccess))
	scope.UpdateGauge(metrics.CadenceShardFailureGauge, float64(shardControlFlowFailure))

	agg := params.AggregateReportResult
	scope.UpdateGauge(metrics.ScannerExecutionsGauge, float64(agg.ExecutionsCount))
	scope.UpdateGauge(metrics.ScannerCorruptedGauge, float64(agg.CorruptedCount))
	scope.UpdateGauge(metrics.ScannerCheckFailedGauge, float64(agg.CheckFailedCount))
	scope.UpdateGauge(metrics.ScannerCorruptedOpenExecutionGauge, float64(agg.CorruptedOpenExecutionCount))
	for k, v := range agg.CorruptionByType {
		scope.Tagged(metrics.InvariantTypeTag(string(k))).UpdateGauge(metrics.ScannerCorruptionByTypeGauge, float64(v))
	}
	return nil
}

// ScanShardActivity will scan all executions in a shard and check for invariant violations.
func ScanShardActivity(
	activityCtx context.Context,
	params ScanShardActivityParams,
) (*common.ShardScanReport, error) {
	ctx := activityCtx.Value(ScannerContextKey).(ScannerContext)
	resources := ctx.Resource
	scope := ctx.Scope.Tagged(metrics.ActivityTypeTag(ScannerScanShardActivityName))
	sw := scope.StartTimer(metrics.CadenceLatency)
	defer sw.Stop()
	execManager, err := resources.GetExecutionManager(params.ShardID)
	if err != nil {
		scope.IncCounter(metrics.CadenceFailures)
		return nil, err
	}
	var collections []common.InvariantCollection
	if params.InvariantCollections.InvariantCollectionHistory {
		collections = append(collections, common.InvariantCollectionHistory)
	}
	if params.InvariantCollections.InvariantCollectionMutableState {
		collections = append(collections, common.InvariantCollectionMutableState)
	}
	pr := common.NewPersistenceRetryer(execManager, resources.GetHistoryManager())
	scanner := shard.NewScanner(
		params.ShardID,
		pr,
		params.ExecutionsPageSize,
		resources.GetBlobstoreClient(),
		params.BlobstoreFlushThreshold,
		collections,
		func() { activity.RecordHeartbeat(activityCtx) })
	report := scanner.Scan()
	if report.Result.ControlFlowFailure != nil {
		scope.IncCounter(metrics.CadenceFailures)
	}
	return &report, nil
}

// ScannerConfigActivity will read dynamic config, apply overwrites and return a resolved config.
func ScannerConfigActivity(
	activityCtx context.Context,
	params ScannerConfigActivityParams,
) (ResolvedScannerWorkflowConfig, error) {
	dc := activityCtx.Value(ScannerContextKey).(ScannerContext).ScannerWorkflowDynamicConfig
	result := ResolvedScannerWorkflowConfig{
		Enabled:                 dc.Enabled(),
		Concurrency:             dc.Concurrency(),
		ExecutionsPageSize:      dc.ExecutionsPageSize(),
		BlobstoreFlushThreshold: dc.BlobstoreFlushThreshold(),
		InvariantCollections: InvariantCollections{
			InvariantCollectionMutableState: dc.DynamicConfigInvariantCollections.InvariantCollectionMutableState(),
			InvariantCollectionHistory:      dc.DynamicConfigInvariantCollections.InvariantCollectionHistory(),
		},
	}
	overwrites := params.Overwrites
	if overwrites.Enabled != nil {
		result.Enabled = *overwrites.Enabled
	}
	if overwrites.Concurrency != nil {
		result.Concurrency = *overwrites.Concurrency
	}
	if overwrites.ExecutionsPageSize != nil {
		result.ExecutionsPageSize = *overwrites.ExecutionsPageSize
	}
	if overwrites.BlobstoreFlushThreshold != nil {
		result.BlobstoreFlushThreshold = *overwrites.BlobstoreFlushThreshold
	}
	if overwrites.InvariantCollections != nil {
		result.InvariantCollections = *overwrites.InvariantCollections
	}
	return result, nil
}

// FixerCorruptedKeysActivity will check that provided scanner workflow is closed
// get corrupt keys from it, and flatten these keys into a list. If provided scanner
// workflow is not closed or query fails then error will be returned.
func FixerCorruptedKeysActivity(
	activityCtx context.Context,
	params FixerCorruptedKeysActivityParams,
) (*FixerCorruptedKeysActivityResult, error) {
	resource := activityCtx.Value(FixerContextKey).(FixerContext).Resource
	client := resource.GetSDKClient()
	descResp, err := client.DescribeWorkflowExecution(activityCtx, &shared.DescribeWorkflowExecutionRequest{
		Domain: c.StringPtr(c.SystemLocalDomainName),
		Execution: &shared.WorkflowExecution{
			WorkflowId: c.StringPtr(params.ScannerWorkflowWorkflowID),
			RunId:      c.StringPtr(params.ScannerWorkflowRunID),
		},
	})
	if err != nil {
		return nil, err
	}
	if descResp.WorkflowExecutionInfo.CloseStatus == nil {
		return nil, errors.New("provided scan workflow is not closed, can only use finished scan")
	}
	queryResp, err := client.QueryWorkflow(activityCtx, &shared.QueryWorkflowRequest{
		Domain: c.StringPtr(c.SystemLocalDomainName),
		Execution: &shared.WorkflowExecution{
			WorkflowId: c.StringPtr(params.ScannerWorkflowWorkflowID),
			RunId:      c.StringPtr(params.ScannerWorkflowRunID),
		},
		Query: &shared.WorkflowQuery{
			QueryType: c.StringPtr(ShardCorruptKeysQuery),
		},
	})
	if err != nil {
		return nil, err
	}
	var corruptedKeys ShardCorruptKeysResult
	if err := json.Unmarshal(queryResp.QueryResult, &corruptedKeys); err != nil {
		return nil, err
	}
	var corrupted []CorruptedKeysEntry
	var shards []int
	for k, v := range corruptedKeys {
		corrupted = append(corrupted, CorruptedKeysEntry{
			ShardID:       k,
			CorruptedKeys: v,
		})
		shards = append(shards, k)
	}
	return &FixerCorruptedKeysActivityResult{
		CorruptedKeys: corrupted,
		Shards:        shards,
	}, nil
}

// FixShardActivity will fetch blobs of corrupted executions from scan workflow.
// It will then iterate over all corrupted executions and run fix on them.
func FixShardActivity(
	activityCtx context.Context,
	params FixShardActivityParams,
) (*common.ShardFixReport, error) {
	ctx := activityCtx.Value(FixerContextKey).(FixerContext)
	resources := ctx.Resource
	scope := ctx.Scope.Tagged(metrics.ActivityTypeTag(FixerFixShardActivityName))
	sw := scope.StartTimer(metrics.CadenceLatency)
	defer sw.Stop()
	execManager, err := resources.GetExecutionManager(params.CorruptedKeysEntry.ShardID)
	if err != nil {
		scope.IncCounter(metrics.CadenceFailures)
		return nil, err
	}
	var collections []common.InvariantCollection
	if params.ResolvedFixerWorkflowConfig.InvariantCollections.InvariantCollectionHistory {
		collections = append(collections, common.InvariantCollectionHistory)
	}
	if params.ResolvedFixerWorkflowConfig.InvariantCollections.InvariantCollectionMutableState {
		collections = append(collections, common.InvariantCollectionMutableState)
	}
	pr := common.NewPersistenceRetryer(execManager, resources.GetHistoryManager())
	fixer := shard.NewFixer(
		params.CorruptedKeysEntry.ShardID,
		pr,
		resources.GetBlobstoreClient(),
		params.CorruptedKeysEntry.CorruptedKeys,
		params.ResolvedFixerWorkflowConfig.BlobstoreFlushThreshold,
		collections,
		func() { activity.RecordHeartbeat(activityCtx) })
	report := fixer.Fix()
	if report.Result.ControlFlowFailure != nil {
		scope.IncCounter(metrics.CadenceFailures)
	}
	return &report, nil
}
