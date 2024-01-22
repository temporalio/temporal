// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package history

import (
	"context"
	"fmt"
	"net/http"
	"slices"

	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/service/history/callbacks"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

var retryable4xxErrorTypes = []int{
	http.StatusRequestTimeout,
	http.StatusTooManyRequests,
}

// HTTPCaller is a method that can be used to invoke HTTP requests.
type HTTPCaller func(*http.Request) (*http.Response, error)

type callbackQueueActiveTaskExecutor struct {
	taskExecutor
	config            *configs.Config
	payloadSerializer commonnexus.PayloadSerializer
	clusterName       string
	callerProvider    func(namespaceID, destination string) HTTPCaller
}

var _ queues.Executor = &callbackQueueActiveTaskExecutor{}

func newCallbackQueueActiveTaskExecutor(
	shardCtx shard.Context,
	workflowCache wcache.Cache,
	logger log.Logger,
	metricsHandler metrics.Handler,
	config *configs.Config,
	callerProvider func(namespaceID, destination string) HTTPCaller,
) *callbackQueueActiveTaskExecutor {
	return &callbackQueueActiveTaskExecutor{
		taskExecutor: taskExecutor{
			shardContext:   shardCtx,
			cache:          workflowCache,
			logger:         logger,
			metricsHandler: metricsHandler.WithTags(metrics.OperationTag(metrics.OperationCallbackQueueProcessorScope)),
		},
		config:         config,
		clusterName:    shardCtx.GetClusterMetadata().GetCurrentClusterName(),
		callerProvider: callerProvider,
	}
}

func (t *callbackQueueActiveTaskExecutor) Execute(
	ctx context.Context,
	executable queues.Executable,
) queues.ExecuteResponse {
	task := executable.GetTask()
	taskType := "Active" + task.GetType().String()

	namespaceTag, replicationState := getNamespaceTagAndReplicationStateByID(
		t.shardContext.GetNamespaceRegistry(),
		task.GetNamespaceID(),
	)
	metricsTags := []metrics.Tag{
		namespaceTag,
		metrics.TaskTypeTag(taskType),
		metrics.OperationTag(taskType),
	}

	// We don't want to execute callback tasks when handing over a namespace to avoid starting work that may not be
	// committed and cause duplicate requests.
	// We Check namespace handover state **once** when processing is started. Callback tasks may take up to 10
	// seconds (by default), but we avoid checking again later, before committing the result, to attempt to commit
	// results of inflight tasks and not lose the progress.
	if replicationState == enumspb.REPLICATION_STATE_HANDOVER {
		// TODO: Move this logic to queues.Executable when metrics tags don't need to be returned from task executor.
		return queues.ExecuteResponse{
			ExecutionMetricTags: metricsTags,
			ExecutedAsActive:    true,
			ExecutionErr:        consts.ErrNamespaceHandover,
		}
	}

	var err error
	switch task := task.(type) {
	case *tasks.CallbackTask:
		err = t.processCallbackTask(ctx, task)
	default:
		err = queues.NewUnprocessableTaskError("unknown task type")
	}

	return queues.ExecuteResponse{
		ExecutionMetricTags: metricsTags,
		ExecutedAsActive:    true,
		ExecutionErr:        err,
	}
}

func (t *callbackQueueActiveTaskExecutor) processCallbackTask(ctx context.Context, task *tasks.CallbackTask) error {
	ctx, cancel := context.WithTimeout(ctx, t.config.CallbackTaskTimeout())
	defer cancel()

	var callback *persistencespb.CallbackInfo
	_, release, ms, err := t.getValidatedMutableStateForTask(
		ctx, task, func(ms workflow.MutableState) error {
			var err error
			callback, err = t.validateCallbackTask(ms, task)
			return err
		},
	)
	if err != nil {
		return err
	}
	// We're just reading mutable state here, no need to unload from cache.
	defer release(nil)

	switch variant := callback.PublicInfo.GetCallback().GetVariant().(type) {
	case *commonpb.Callback_Nexus_:
		completion, err := t.getNexusCompletion(ctx, ms)
		// We got what we need from mutable state; release now and reacquire later when processing is done.
		release(nil)
		if err != nil {
			return err
		}
		return t.processNexusCallbackTask(ctx, task, variant.Nexus.GetUrl(), completion)
	default:
		return queues.NewUnprocessableTaskError(fmt.Sprintf("unprocessable callback variant: %v", variant))
	}
}

// getNexusCompletion converts a workflow completion event into a [nexus.OperationCompletion].
// Completions may be sent to arbitrary third parties, we intentionally do not include any termination reasons, and
// expose only failure messages.
func (t *callbackQueueActiveTaskExecutor) getNexusCompletion(ctx context.Context, ms workflow.MutableState) (nexus.OperationCompletion, error) {
	ce, err := ms.GetCompletionEvent(ctx)
	if err != nil {
		return nil, err
	}
	switch ce.GetEventType() {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		payloads := ce.GetWorkflowExecutionCompletedEventAttributes().GetResult().GetPayloads()
		var payload *commonpb.Payload
		if len(payloads) > 0 {
			// All of our SDKs support returning a single value from workflows, we can safely ignore the
			// rest of the payloads. Additionally, even if a workflow could return more than a single value,
			// Nexus does not support it.
			payload = payloads[0]
		} else {
			payload = &commonpb.Payload{}
		}
		completion, err := nexus.NewOperationCompletionSuccessful(payload, nexus.OperationCompletionSuccesfulOptions{
			Serializer: t.payloadSerializer,
		})
		if err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("failed to construct Nexus completion: %v", err))
		}
		return completion, nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		failure := commonnexus.APIFailureToNexusFailure(ce.GetWorkflowExecutionFailedEventAttributes().GetFailure())
		return &nexus.OperationCompletionUnsuccessful{
			State:   nexus.OperationStateFailed,
			Failure: failure,
		}, nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CANCELED:
		return &nexus.OperationCompletionUnsuccessful{
			State:   nexus.OperationStateCanceled,
			Failure: &nexus.Failure{Message: "operation canceled"},
		}, nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TERMINATED:
		return &nexus.OperationCompletionUnsuccessful{
			State:   nexus.OperationStateFailed,
			Failure: &nexus.Failure{Message: "operation terminated"},
		}, nil
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		return &nexus.OperationCompletionUnsuccessful{
			State:   nexus.OperationStateFailed,
			Failure: &nexus.Failure{Message: "operation exceeded internal timeout"},
		}, nil
	}
	return nil, queues.NewUnprocessableTaskError(fmt.Sprintf("invalid workflow execution status: %v", ce.GetEventType()))
}

func (t *callbackQueueActiveTaskExecutor) processNexusCallbackTask(ctx context.Context, task *tasks.CallbackTask, url string, completion nexus.OperationCompletion) (retErr error) {
	request, err := nexus.NewCompletionHTTPRequest(ctx, url, completion)
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("failed to construct Nexus request: %v", err))
	}

	caller := t.callerProvider(task.NamespaceID, task.DestinationAddress)
	response, callErr := caller(request)
	var callback *persistencespb.CallbackInfo
	wfCtx, release, ms, err := t.getValidatedMutableStateForTask(
		ctx, task, func(ms workflow.MutableState) error {
			var err error
			callback, err = t.validateCallbackTask(ms, task)
			return err
		},
	)
	if err != nil {
		return nil
	}
	defer func() { release(retErr) }()

	if err := t.transitionCallbackBasedOnResponse(ms, callback, response, callErr); err != nil {
		return err
	}
	if ms.GetExecutionInfo().CloseTime == nil {
		// This is here to bring attention to future implementors of callbacks triggered on open workflows.
		return queues.NewUnprocessableTaskError("triggered safeguard preventing from mutating closed workflows")
	}
	// Can't use UpdateWorkflowExecutionAsActive since it updates the current run, and we are operating on closed
	// workflows.
	// When we support workflow-update callbacks, we'll need to revisit this code.
	return wfCtx.SubmitClosedWorkflowSnapshot(ctx, t.shardContext, workflow.TransactionPolicyActive)
}

func (t *callbackQueueActiveTaskExecutor) transitionCallbackBasedOnResponse(
	ms workflow.MutableState,
	callback *persistencespb.CallbackInfo,
	response *http.Response,
	err error,
) error {
	if err == nil {
		if response.StatusCode >= 200 && response.StatusCode < 300 {
			return callbacks.TransitionSucceeded.Apply(callback, callbacks.EventSucceeded{}, ms)
		}
		if response.StatusCode >= 400 && response.StatusCode < 500 && !slices.Contains(retryable4xxErrorTypes, response.StatusCode) {
			return callbacks.TransitionFailed.Apply(callback, callbacks.EventFailed(err), ms)
		}
		err = fmt.Errorf("request failed with: %v", response.Status) // nolint:goerr113
	}
	return callbacks.TransitionAttemptFailed.Apply(callback, callbacks.EventAttemptFailed(err), ms)
}
