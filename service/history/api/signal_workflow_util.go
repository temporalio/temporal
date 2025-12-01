package api

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/service/history/consts"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func ValidateSignal(
	ctx context.Context,
	shard historyi.ShardContext,
	mutableState historyi.MutableState,
	signalPayloadSize int,
	signalHeaderSize int,
	operation string,
) error {
	config := shard.GetConfig()
	namespaceEntry := mutableState.GetNamespaceEntry()
	namespaceID := namespaceEntry.ID().String()
	namespaceName := namespaceEntry.Name().String()
	workflowID := mutableState.GetExecutionInfo().WorkflowId
	runID := mutableState.GetExecutionState().RunId

	executionInfo := mutableState.GetExecutionInfo()
	maxAllowedSignals := config.MaximumSignalsPerExecution(namespaceName)
	blobSizeLimitWarn := config.BlobSizeLimitWarn(namespaceName)
	blobSizeLimitError := config.BlobSizeLimitError(namespaceName)

	metricsHandler := interceptor.GetMetricsHandlerFromContext(ctx, shard.GetLogger())
	metrics.HeaderSize.With(metricsHandler.WithTags(metrics.HeaderCallsiteTag(operation))).Record(int64(signalHeaderSize))
	if err := common.CheckEventBlobSizeLimit(
		signalPayloadSize,
		blobSizeLimitWarn,
		blobSizeLimitError,
		namespaceName,
		workflowID,
		runID,
		metricsHandler.WithTags(
			metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String()),
		),
		shard.GetThrottledLogger(),
		tag.BlobSizeViolationOperation(operation),
	); err != nil {
		return err
	}

	if maxAllowedSignals > 0 && int(executionInfo.SignalCount) >= maxAllowedSignals {
		shard.GetLogger().Info("Execution limit reached for maximum signals",
			tag.WorkflowNamespaceID(namespaceID),
			tag.WorkflowID(workflowID),
			tag.WorkflowRunID(runID),
			tag.WorkflowSignalCount(executionInfo.SignalCount),
		)
		return consts.ErrSignalsLimitExceeded
	}

	if mutableState.IsWorkflowCloseAttempted() && mutableState.HasStartedWorkflowTask() {
		shard.GetThrottledLogger().Info("Signal rejected because workflow is closing",
			tag.WorkflowNamespaceID(namespaceID),
			tag.WorkflowID(workflowID),
			tag.WorkflowRunID(runID),
		)
		return consts.ErrWorkflowClosing
	}

	return nil
}
