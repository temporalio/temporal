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

	"go.uber.org/cadence"

	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/activity"

	c "github.com/uber/cadence/common"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/reconciliation/common"
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

	// ErrScanWorkflowNotClosed indicates fix was attempted on scan workflow which was not finished
	ErrScanWorkflowNotClosed = "scan workflow is not closed, only can run fix on output of finished scan workflow"
	// ErrSerialization indicates a serialization or deserialization error occurred
	ErrSerialization = "encountered serialization error"
)

var scanTypePrefixMap = map[common.ScanType]string{
	common.ConcreteExecutionType: "", // leave it empty for now to be backwards compatible
	common.CurrentExecutionType:  "current_executions_",
}

type (
	// ScannerConfigActivityParams is the parameter for ScannerConfigActivity
	ScannerConfigActivityParams struct {
		Overwrites ScannerWorkflowConfigOverwrites
		ScanType   common.ScanType
	}

	// ScanShardActivityParams is the parameter for ScanShardActivity
	ScanShardActivityParams struct {
		Shards                  []int
		ExecutionsPageSize      int
		BlobstoreFlushThreshold int
		InvariantCollections    InvariantCollections
		ScanType                common.ScanType
	}

	// ScannerEmitMetricsActivityParams is the parameter for ScannerEmitMetricsActivity
	ScannerEmitMetricsActivityParams struct {
		ShardSuccessCount            int
		ShardControlFlowFailureCount int
		AggregateReportResult        AggregateScanReportResult
		ShardDistributionStats       ShardDistributionStats
		ScanType                     common.ScanType
	}

	// ShardDistributionStats contains stats on the distribution of executions in shards.
	// It is used by the ScannerEmitMetricsActivityParams.
	ShardDistributionStats struct {
		Max    int64
		Median int64
		Min    int64
		P90    int64
		P75    int64
		P25    int64
		P10    int64
	}

	// FixerCorruptedKeysActivityParams is the parameter for FixerCorruptedKeysActivity
	FixerCorruptedKeysActivityParams struct {
		ScannerWorkflowWorkflowID string
		ScannerWorkflowRunID      string
		StartingShardID           *int
		ScanType                  common.ScanType
	}

	// FixShardActivityParams is the parameter for FixShardActivity
	FixShardActivityParams struct {
		CorruptedKeysEntries        []CorruptedKeysEntry
		ResolvedFixerWorkflowConfig ResolvedFixerWorkflowConfig
		ScanType                    common.ScanType
	}

	// FixerCorruptedKeysActivityResult is the result of FixerCorruptedKeysActivity
	FixerCorruptedKeysActivityResult struct {
		CorruptedKeys             []CorruptedKeysEntry
		MinShard                  *int
		MaxShard                  *int
		ShardQueryPaginationToken ShardQueryPaginationToken
	}

	// CorruptedKeysEntry is a pair of shardID and corrupted keys
	CorruptedKeysEntry struct {
		ShardID       int
		CorruptedKeys common.Keys
	}

	// ScanShardHeartbeatDetails is the heartbeat details for scan shard
	ScanShardHeartbeatDetails struct {
		LastShardIndexHandled int
		Reports               []common.ShardScanReport
	}

	// FixShardHeartbeatDetails is the heartbeat details for the fix shard
	FixShardHeartbeatDetails struct {
		LastShardIndexHandled int
		Reports               []common.ShardFixReport
	}
)

// ScannerEmitMetricsActivity will emit metrics for a complete run of scanner
func ScannerEmitMetricsActivity(
	activityCtx context.Context,
	params ScannerEmitMetricsActivityParams,
) error {
	scope := activityCtx.Value(ScanTypeScannerContextKeyMap[params.ScanType]).(ScannerContext).Scope.
		Tagged(metrics.ActivityTypeTag(scanTypePrefixMap[params.ScanType] + ScannerEmitMetricsActivityName))
	scope.UpdateGauge(metrics.CadenceShardSuccessGauge, float64(params.ShardSuccessCount))
	scope.UpdateGauge(metrics.CadenceShardFailureGauge, float64(params.ShardControlFlowFailureCount))

	agg := params.AggregateReportResult
	scope.UpdateGauge(metrics.ScannerExecutionsGauge, float64(agg.ExecutionsCount))
	scope.UpdateGauge(metrics.ScannerCorruptedGauge, float64(agg.CorruptedCount))
	scope.UpdateGauge(metrics.ScannerCheckFailedGauge, float64(agg.CheckFailedCount))
	scope.UpdateGauge(metrics.ScannerCorruptedOpenExecutionGauge, float64(agg.CorruptedOpenExecutionCount))
	for k, v := range agg.CorruptionByType {
		scope.Tagged(metrics.InvariantTypeTag(string(k))).UpdateGauge(metrics.ScannerCorruptionByTypeGauge, float64(v))
	}
	shardStats := params.ShardDistributionStats
	scope.UpdateGauge(metrics.ScannerShardSizeMaxGauge, float64(shardStats.Max))
	scope.UpdateGauge(metrics.ScannerShardSizeMedianGauge, float64(shardStats.Median))
	scope.UpdateGauge(metrics.ScannerShardSizeMinGauge, float64(shardStats.Min))
	scope.UpdateGauge(metrics.ScannerShardSizeNinetyGauge, float64(shardStats.P90))
	scope.UpdateGauge(metrics.ScannerShardSizeSeventyFiveGauge, float64(shardStats.P75))
	scope.UpdateGauge(metrics.ScannerShardSizeTwentyFiveGauge, float64(shardStats.P25))
	scope.UpdateGauge(metrics.ScannerShardSizeTenGauge, float64(shardStats.P10))
	return nil
}

// ScanShardActivity will scan a collection of shards for invariant violations.
func ScanShardActivity(
	activityCtx context.Context,
	params ScanShardActivityParams,
) ([]common.ShardScanReport, error) {
	heartbeatDetails := ScanShardHeartbeatDetails{
		LastShardIndexHandled: -1,
		Reports:               nil,
	}
	if activity.HasHeartbeatDetails(activityCtx) {
		if err := activity.GetHeartbeatDetails(activityCtx, &heartbeatDetails); err != nil {
			return nil, err
		}
	}
	for i := heartbeatDetails.LastShardIndexHandled + 1; i < len(params.Shards); i++ {
		currentShardID := params.Shards[i]
		shardReport, err := scanShard(activityCtx, params, currentShardID, heartbeatDetails)
		if err != nil {
			return nil, err
		}
		heartbeatDetails = ScanShardHeartbeatDetails{
			LastShardIndexHandled: i,
			Reports:               append(heartbeatDetails.Reports, *shardReport),
		}
	}
	return heartbeatDetails.Reports, nil
}

func scanShard(
	activityCtx context.Context,
	params ScanShardActivityParams,
	shardID int,
	heartbeatDetails ScanShardHeartbeatDetails,
) (*common.ShardScanReport, error) {
	ctx := activityCtx.Value(ScanTypeScannerContextKeyMap[params.ScanType]).(ScannerContext)
	resources := ctx.Resource
	scope := ctx.Scope.Tagged(metrics.ActivityTypeTag(scanTypePrefixMap[params.ScanType] + ScannerScanShardActivityName))
	sw := scope.StartTimer(metrics.CadenceLatency)
	defer sw.Stop()
	execManager, err := resources.GetExecutionManager(shardID)
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
		shardID,
		pr,
		params.ExecutionsPageSize,
		resources.GetBlobstoreClient(),
		params.BlobstoreFlushThreshold,
		collections,
		func() { activity.RecordHeartbeat(activityCtx, heartbeatDetails) },
		params.ScanType)
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
	dc := activityCtx.Value(ScanTypeScannerContextKeyMap[params.ScanType]).(ScannerContext).ScannerWorkflowDynamicConfig
	result := ResolvedScannerWorkflowConfig{
		Enabled:                 dc.Enabled(),
		Concurrency:             dc.Concurrency(),
		ExecutionsPageSize:      dc.ExecutionsPageSize(),
		BlobstoreFlushThreshold: dc.BlobstoreFlushThreshold(),
		ActivityBatchSize:       dc.ActivityBatchSize(),
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
	if overwrites.ActivityBatchSize != nil {
		result.ActivityBatchSize = *overwrites.ActivityBatchSize
	}
	return result, nil
}

// FixerCorruptedKeysActivity will fetch the keys of blobs from shards with corruptions from a completed scan workflow.
// If scan workflow is not closed or if query fails activity will return an error.
// Accepts as input the shard to start query at and returns a next page token, therefore this activity can
// be used to do pagination.
func FixerCorruptedKeysActivity(
	activityCtx context.Context,
	params FixerCorruptedKeysActivityParams,
) (*FixerCorruptedKeysActivityResult, error) {
	resource := activityCtx.Value(ScanTypeFixerContextKeyMap[params.ScanType]).(FixerContext).Resource
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
		return nil, cadence.NewCustomError(ErrScanWorkflowNotClosed)
	}
	queryArgs := PaginatedShardQueryRequest{
		StartingShardID: params.StartingShardID,
	}
	queryArgsBytes, err := json.Marshal(queryArgs)
	if err != nil {
		return nil, cadence.NewCustomError(ErrSerialization)
	}
	queryResp, err := client.QueryWorkflow(activityCtx, &shared.QueryWorkflowRequest{
		Domain: c.StringPtr(c.SystemLocalDomainName),
		Execution: &shared.WorkflowExecution{
			WorkflowId: c.StringPtr(params.ScannerWorkflowWorkflowID),
			RunId:      c.StringPtr(params.ScannerWorkflowRunID),
		},
		Query: &shared.WorkflowQuery{
			QueryType: c.StringPtr(ShardCorruptKeysQuery),
			QueryArgs: queryArgsBytes,
		},
	})
	if err != nil {
		return nil, err
	}
	queryResult := &ShardCorruptKeysQueryResult{}
	if err := json.Unmarshal(queryResp.QueryResult, &queryResult); err != nil {
		return nil, cadence.NewCustomError(ErrSerialization)
	}
	var corrupted []CorruptedKeysEntry
	var minShardID *int
	var maxShardID *int
	for sid, keys := range queryResult.Result {
		if minShardID == nil || *minShardID > sid {
			minShardID = c.IntPtr(sid)
		}
		if maxShardID == nil || *maxShardID < sid {
			maxShardID = c.IntPtr(sid)
		}
		corrupted = append(corrupted, CorruptedKeysEntry{
			ShardID:       sid,
			CorruptedKeys: keys,
		})
	}
	return &FixerCorruptedKeysActivityResult{
		CorruptedKeys:             corrupted,
		MinShard:                  minShardID,
		MaxShard:                  maxShardID,
		ShardQueryPaginationToken: queryResult.ShardQueryPaginationToken,
	}, nil
}

// FixShardActivity will fix a collection of shards.
func FixShardActivity(
	activityCtx context.Context,
	params FixShardActivityParams,
) ([]common.ShardFixReport, error) {
	heartbeatDetails := FixShardHeartbeatDetails{
		LastShardIndexHandled: -1,
		Reports:               nil,
	}
	if activity.HasHeartbeatDetails(activityCtx) {
		if err := activity.GetHeartbeatDetails(activityCtx, &heartbeatDetails); err != nil {
			return nil, err
		}
	}
	for i := heartbeatDetails.LastShardIndexHandled + 1; i < len(params.CorruptedKeysEntries); i++ {
		currentShardID := params.CorruptedKeysEntries[i].ShardID
		currentKeys := params.CorruptedKeysEntries[i].CorruptedKeys
		shardReport, err := fixShard(activityCtx, params, currentShardID, currentKeys, heartbeatDetails)
		if err != nil {
			return nil, err
		}
		heartbeatDetails = FixShardHeartbeatDetails{
			LastShardIndexHandled: i,
			Reports:               append(heartbeatDetails.Reports, *shardReport),
		}
	}
	return heartbeatDetails.Reports, nil
}

func fixShard(
	activityCtx context.Context,
	params FixShardActivityParams,
	shardID int,
	corruptedKeys common.Keys,
	heartbeatDetails FixShardHeartbeatDetails,
) (*common.ShardFixReport, error) {
	ctx := activityCtx.Value(ScanTypeFixerContextKeyMap[params.ScanType]).(FixerContext)
	resources := ctx.Resource
	scope := ctx.Scope.Tagged(metrics.ActivityTypeTag(FixerFixShardActivityName))
	sw := scope.StartTimer(metrics.CadenceLatency)
	defer sw.Stop()
	execManager, err := resources.GetExecutionManager(shardID)
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
		shardID,
		pr,
		resources.GetBlobstoreClient(),
		corruptedKeys,
		params.ResolvedFixerWorkflowConfig.BlobstoreFlushThreshold,
		collections,
		func() { activity.RecordHeartbeat(activityCtx, heartbeatDetails) },
		params.ScanType)
	report := fixer.Fix()
	if report.Result.ControlFlowFailure != nil {
		scope.IncCounter(metrics.CadenceFailures)
	}
	return &report, nil
}
