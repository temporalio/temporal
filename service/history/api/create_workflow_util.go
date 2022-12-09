// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package api

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/historyservice/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	CreateWorkflowCASPredicate struct {
		RunID            string
		LastWriteVersion int64
	}
)

func NewWorkflowWithSignal(
	ctx context.Context,
	shard shard.Context,
	namespaceEntry *namespace.Namespace,
	workflowID string,
	runID string,
	startRequest *historyservice.StartWorkflowExecutionRequest,
	signalWithStartRequest *workflowservice.SignalWithStartWorkflowExecutionRequest,
) (WorkflowContext, error) {
	newMutableState, err := CreateMutableState(
		ctx,
		shard,
		namespaceEntry,
		startRequest.StartRequest.WorkflowExecutionTimeout,
		startRequest.StartRequest.WorkflowRunTimeout,
		runID,
	)
	if err != nil {
		return nil, err
	}

	startEvent, err := newMutableState.AddWorkflowExecutionStartedEvent(
		commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		startRequest,
	)
	if err != nil {
		return nil, err
	}

	if signalWithStartRequest != nil {
		if signalWithStartRequest.GetRequestId() != "" {
			newMutableState.AddSignalRequested(signalWithStartRequest.GetRequestId())
		}
		if _, err := newMutableState.AddWorkflowExecutionSignaled(
			signalWithStartRequest.GetSignalName(),
			signalWithStartRequest.GetSignalInput(),
			signalWithStartRequest.GetIdentity(),
			signalWithStartRequest.GetHeader(),
		); err != nil {
			return nil, err
		}
	}

	// Generate first workflow task event if not child WF and no first workflow task backoff
	if err := GenerateFirstWorkflowTask(
		newMutableState,
		startRequest.ParentExecutionInfo,
		startEvent,
	); err != nil {
		return nil, err
	}

	newWorkflowContext := workflow.NewContext(
		shard,
		definition.NewWorkflowKey(
			namespaceEntry.ID().String(),
			workflowID,
			runID,
		),
		shard.GetLogger(),
	)
	return NewWorkflowContext(newWorkflowContext, wcache.NoopReleaseFn, newMutableState), nil
}

func CreateMutableState(
	ctx context.Context,
	shard shard.Context,
	namespaceEntry *namespace.Namespace,
	executionTimeout *time.Duration,
	runTimeout *time.Duration,
	runID string,
) (workflow.MutableState, error) {
	newMutableState := workflow.NewMutableState(
		shard,
		shard.GetEventsCache(),
		shard.GetLogger(),
		namespaceEntry,
		shard.GetTimeSource().Now(),
	)
	if err := newMutableState.SetHistoryTree(ctx, executionTimeout, runTimeout, runID); err != nil {
		return nil, err
	}
	return newMutableState, nil
}

func GenerateFirstWorkflowTask(
	mutableState workflow.MutableState,
	parentInfo *workflowspb.ParentExecutionInfo,
	startEvent *historypb.HistoryEvent,
) error {

	if parentInfo == nil {
		// WorkflowTask is only created when it is not a Child Workflow and no backoff is needed
		if err := mutableState.AddFirstWorkflowTaskScheduled(
			startEvent,
		); err != nil {
			return err
		}
	}
	return nil
}

func NewWorkflowVersionCheck(
	shard shard.Context,
	prevLastWriteVersion int64,
	newMutableState workflow.MutableState,
) error {
	if prevLastWriteVersion == common.EmptyVersion {
		return nil
	}

	if prevLastWriteVersion > newMutableState.GetCurrentVersion() {
		clusterMetadata := shard.GetClusterMetadata()
		namespaceEntry := newMutableState.GetNamespaceEntry()
		clusterName := clusterMetadata.ClusterNameForFailoverVersion(namespaceEntry.IsGlobalNamespace(), prevLastWriteVersion)
		return serviceerror.NewNamespaceNotActive(
			namespaceEntry.Name().String(),
			clusterMetadata.GetCurrentClusterName(),
			clusterName,
		)
	}
	return nil
}

func ValidateStart(
	ctx context.Context,
	shard shard.Context,
	namespaceEntry *namespace.Namespace,
	workflowID string,
	workflowInputSize int,
	workflowMemoSize int,
	operation string,
) error {
	config := shard.GetConfig()
	logger := shard.GetLogger()
	throttledLogger := shard.GetThrottledLogger()
	namespaceName := namespaceEntry.Name().String()

	if err := common.CheckEventBlobSizeLimit(
		workflowInputSize,
		config.BlobSizeLimitWarn(namespaceName),
		config.BlobSizeLimitError(namespaceName),
		namespaceName,
		workflowID,
		"",
		interceptor.GetMetricsHandlerFromContext(ctx, logger).WithTags(metrics.CommandTypeTag(operation)),
		throttledLogger,
		tag.BlobSizeViolationOperation(operation),
	); err != nil {
		return err
	}

	handler := interceptor.GetMetricsHandlerFromContext(ctx, logger).WithTags(metrics.CommandTypeTag(operation))
	handler.Histogram(metrics.MemoSize.GetMetricName(), metrics.MemoSize.GetMetricUnit()).Record(int64(workflowMemoSize))
	if err := common.CheckEventBlobSizeLimit(
		workflowMemoSize,
		config.MemoSizeLimitWarn(namespaceName),
		config.MemoSizeLimitError(namespaceName),
		namespaceName,
		workflowID,
		"",
		handler,
		throttledLogger,
		tag.BlobSizeViolationOperation(operation),
	); err != nil {
		return common.ErrMemoSizeExceedsLimit
	}

	return nil
}

func ValidateStartWorkflowExecutionRequest(
	ctx context.Context,
	request *workflowservice.StartWorkflowExecutionRequest,
	shard shard.Context,
	namespaceEntry *namespace.Namespace,
	operation string,
) error {

	workflowID := request.GetWorkflowId()
	maxIDLengthLimit := shard.GetConfig().MaxIDLengthLimit()

	if len(request.GetRequestId()) == 0 {
		return serviceerror.NewInvalidArgument("Missing request ID.")
	}
	if timestamp.DurationValue(request.GetWorkflowExecutionTimeout()) < 0 {
		return serviceerror.NewInvalidArgument("Invalid WorkflowExecutionTimeoutSeconds.")
	}
	if timestamp.DurationValue(request.GetWorkflowRunTimeout()) < 0 {
		return serviceerror.NewInvalidArgument("Invalid WorkflowRunTimeoutSeconds.")
	}
	if timestamp.DurationValue(request.GetWorkflowTaskTimeout()) < 0 {
		return serviceerror.NewInvalidArgument("Invalid WorkflowTaskTimeoutSeconds.")
	}
	if request.TaskQueue == nil || request.TaskQueue.GetName() == "" {
		return serviceerror.NewInvalidArgument("Missing Taskqueue.")
	}
	if request.WorkflowType == nil || request.WorkflowType.GetName() == "" {
		return serviceerror.NewInvalidArgument("Missing WorkflowType.")
	}
	if len(request.GetNamespace()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("Namespace exceeds length limit.")
	}
	if len(request.GetWorkflowId()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("WorkflowId exceeds length limit.")
	}
	if len(request.TaskQueue.GetName()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("TaskQueue exceeds length limit.")
	}
	if len(request.WorkflowType.GetName()) > maxIDLengthLimit {
		return serviceerror.NewInvalidArgument("WorkflowType exceeds length limit.")
	}
	if err := common.ValidateRetryPolicy(request.RetryPolicy); err != nil {
		return err
	}

	if err := ValidateStart(
		ctx,
		shard,
		namespaceEntry,
		workflowID,
		request.GetInput().Size(),
		request.GetMemo().Size(),
		operation,
	); err != nil {
		return err
	}

	return nil
}

func OverrideStartWorkflowExecutionRequest(
	request *workflowservice.StartWorkflowExecutionRequest,
	operation string,
	shard shard.Context,
	metricsHandler metrics.MetricsHandler,
) {
	// workflow execution timeout is left as is
	//  if workflow execution timeout == 0 -> infinity

	namespace := request.GetNamespace()

	workflowRunTimeout := common.OverrideWorkflowRunTimeout(
		timestamp.DurationValue(request.GetWorkflowRunTimeout()),
		timestamp.DurationValue(request.GetWorkflowExecutionTimeout()),
	)
	if workflowRunTimeout != timestamp.DurationValue(request.GetWorkflowRunTimeout()) {
		request.WorkflowRunTimeout = timestamp.DurationPtr(workflowRunTimeout)
		metricsHandler.Counter(metrics.WorkflowRunTimeoutOverrideCount.GetMetricName()).Record(
			1,
			metrics.OperationTag(operation),
			metrics.NamespaceTag(namespace),
		)
	}

	workflowTaskStartToCloseTimeout := common.OverrideWorkflowTaskTimeout(
		namespace,
		timestamp.DurationValue(request.GetWorkflowTaskTimeout()),
		timestamp.DurationValue(request.GetWorkflowRunTimeout()),
		shard.GetConfig().DefaultWorkflowTaskTimeout,
	)
	if workflowTaskStartToCloseTimeout != timestamp.DurationValue(request.GetWorkflowTaskTimeout()) {
		request.WorkflowTaskTimeout = timestamp.DurationPtr(workflowTaskStartToCloseTimeout)
		metricsHandler.Counter(metrics.WorkflowTaskTimeoutOverrideCount.GetMetricName()).Record(
			1,
			metrics.OperationTag(operation),
			metrics.NamespaceTag(namespace),
		)
	}
}
