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

	"go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/sdk"
	schedulerhsm "go.temporal.io/server/components/scheduler"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/worker/scheduler"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/server/api/historyservice/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/retrypolicy"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	VersionedRunID struct {
		RunID            string
		LastWriteVersion int64
	}
)

func NewWorkflowWithSignal(
	shard shard.Context,
	namespaceEntry *namespace.Namespace,
	workflowID string,
	runID string,
	startRequest *historyservice.StartWorkflowExecutionRequest,
	signalWithStartRequest *workflowservice.SignalWithStartWorkflowExecutionRequest,
) (WorkflowLease, error) {
	newMutableState, err := CreateMutableState(
		shard,
		namespaceEntry,
		startRequest.StartRequest.WorkflowExecutionTimeout,
		startRequest.StartRequest.WorkflowRunTimeout,
		workflowID,
		runID,
	)
	if err != nil {
		return nil, err
	}

	startEvent, err := newMutableState.AddWorkflowExecutionStartedEvent(
		&commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		startRequest,
	)
	if err != nil {
		return nil, err
	}

	// This workflow is created for the hsm attached to it and will not generate actual workflow tasks.
	// This is a bit of a hacky way to distinguish between a "real" workflow and a top level state machine.
	// This code will change as the scheduler HSM project progresses.
	hsmOnlyWorkflow := startRequest.StartRequest.WorkflowType.Name == scheduler.WorkflowType && shard.GetConfig().UseExperimentalHsmScheduler(startRequest.NamespaceId)
	if hsmOnlyWorkflow {
		args := schedule.StartScheduleArgs{}
		if err := sdk.PreferProtoDataConverter.FromPayloads(startRequest.StartRequest.Input, &args); err != nil {
			return nil, err
		}

		// Key ID is left empty as the scheduler machine is a singleton.
		node, err := newMutableState.HSM().AddChild(hsm.Key{Type: schedulerhsm.StateMachineType}, schedulerhsm.NewScheduler(&args))
		if err != nil {
			return nil, err
		}
		err = hsm.MachineTransition(node, func(scheduler schedulerhsm.Scheduler) (hsm.TransitionOutput, error) {
			return schedulerhsm.TransitionSchedulerActivate.Apply(scheduler, schedulerhsm.EventSchedulerActivate{})
		})
		if err != nil {
			return nil, err
		}
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
			signalWithStartRequest.GetSkipGenerateWorkflowTask(),
		); err != nil {
			return nil, err
		}
	}
	requestEagerExecution := startRequest.StartRequest.GetRequestEagerExecution()

	var scheduledEventID int64
	if !hsmOnlyWorkflow {
		// Generate first workflow task event if not child WF and no first workflow task backoff
		scheduledEventID, err = GenerateFirstWorkflowTask(
			newMutableState,
			startRequest.ParentExecutionInfo,
			startEvent,
			requestEagerExecution,
		)
		if err != nil {
			return nil, err
		}
	}

	// If first workflow task should back off (e.g. cron or workflow retry) a workflow task will not be scheduled.
	if requestEagerExecution && newMutableState.HasPendingWorkflowTask() {
		// TODO: get build ID from Starter so eager workflows can be versioned
		_, _, err = newMutableState.AddWorkflowTaskStartedEvent(
			scheduledEventID,
			startRequest.StartRequest.RequestId,
			startRequest.StartRequest.TaskQueue,
			startRequest.StartRequest.Identity,
			nil,
			nil,
		)
		if err != nil {
			// Unable to add WorkflowTaskStarted event to history
			return nil, err
		}
	}

	newWorkflowContext := workflow.NewContext(
		shard.GetConfig(),
		definition.NewWorkflowKey(
			namespaceEntry.ID().String(),
			workflowID,
			runID,
		),
		shard.GetLogger(),
		shard.GetThrottledLogger(),
		shard.GetMetricsHandler(),
	)
	return NewWorkflowLease(newWorkflowContext, wcache.NoopReleaseFn, newMutableState), nil
}

func CreateMutableState(
	shard shard.Context,
	namespaceEntry *namespace.Namespace,
	executionTimeout *durationpb.Duration,
	runTimeout *durationpb.Duration,
	workflowID string,
	runID string,
) (workflow.MutableState, error) {
	newMutableState := workflow.NewMutableState(
		shard,
		shard.GetEventsCache(),
		shard.GetLogger(),
		namespaceEntry,
		workflowID,
		runID,
		shard.GetTimeSource().Now(),
	)
	if err := newMutableState.SetHistoryTree(executionTimeout, runTimeout, runID); err != nil {
		return nil, err
	}
	return newMutableState, nil
}

func GenerateFirstWorkflowTask(
	mutableState workflow.MutableState,
	parentInfo *workflowspb.ParentExecutionInfo,
	startEvent *historypb.HistoryEvent,
	bypassTaskGeneration bool,
) (int64, error) {
	if parentInfo == nil {
		// WorkflowTask is only created when it is not a Child Workflow and no backoff is needed
		return mutableState.AddFirstWorkflowTaskScheduled(nil, startEvent, bypassTaskGeneration)
	}
	return 0, nil
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
	metrics.MemoSize.With(handler).Record(int64(workflowMemoSize))
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
	if err := retrypolicy.Validate(request.RetryPolicy); err != nil {
		return err
	}
	return ValidateStart(
		ctx,
		shard,
		namespaceEntry,
		workflowID,
		request.GetInput().Size(),
		request.GetMemo().Size(),
		operation,
	)
}

func OverrideStartWorkflowExecutionRequest(
	request *workflowservice.StartWorkflowExecutionRequest,
	operation string,
	shard shard.Context,
	metricsHandler metrics.Handler,
) {
	// workflow execution timeout is left as is
	//  if workflow execution timeout == 0 -> infinity

	namespace := request.GetNamespace()

	workflowRunTimeout := common.OverrideWorkflowRunTimeout(
		timestamp.DurationValue(request.GetWorkflowRunTimeout()),
		timestamp.DurationValue(request.GetWorkflowExecutionTimeout()),
	)
	if workflowRunTimeout != timestamp.DurationValue(request.GetWorkflowRunTimeout()) {
		request.WorkflowRunTimeout = durationpb.New(workflowRunTimeout)
		metrics.WorkflowRunTimeoutOverrideCount.With(metricsHandler).Record(
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
		request.WorkflowTaskTimeout = durationpb.New(workflowTaskStartToCloseTimeout)
		metrics.WorkflowTaskTimeoutOverrideCount.With(metricsHandler).Record(
			1,
			metrics.OperationTag(operation),
			metrics.NamespaceTag(namespace),
		)
	}
}
