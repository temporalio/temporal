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

package recordworkflowtaskstarted

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"google.golang.org/protobuf/types/known/timestamppb"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/history/workflow/update"
)

//nolint:revive // cyclomatic complexity
func Invoke(
	ctx context.Context,
	req *historyservice.RecordWorkflowTaskStartedRequest,
	shardContext shard.Context,
	config *configs.Config,
	eventNotifier events.Notifier,
	persistenceVisibilityMgr manager.VisibilityManager,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (*historyservice.RecordWorkflowTaskStartedResponse, error) {
	namespaceEntry, err := api.GetActiveNamespace(shardContext, namespace.ID(req.GetNamespaceId()))
	if err != nil {
		return nil, err
	}

	scheduledEventID := req.GetScheduledEventId()
	requestID := req.GetRequestId()

	var workflowKey definition.WorkflowKey
	var resp *historyservice.RecordWorkflowTaskStartedResponse

	err = api.GetAndUpdateWorkflowWithNew(
		ctx,
		req.Clock,
		api.BypassMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(
			req.NamespaceId,
			req.WorkflowExecution.WorkflowId,
			req.WorkflowExecution.RunId,
		),
		func(workflowLease api.WorkflowLease) (*api.UpdateWorkflowAction, error) {
			mutableState := workflowLease.GetMutableState()
			updateRegistry := workflowLease.GetContext().UpdateRegistry(ctx, nil)
			if !mutableState.IsWorkflowExecutionRunning() {
				return nil, consts.ErrWorkflowCompleted
			}

			workflowTask := mutableState.GetWorkflowTaskByID(scheduledEventID)
			metricsScope := shardContext.GetMetricsHandler().WithTags(metrics.OperationTag(metrics.HistoryRecordWorkflowTaskStartedScope))

			// First check to see if cache needs to be refreshed as we could potentially have stale workflow execution in
			// some extreme cassandra failure cases.
			if workflowTask == nil && scheduledEventID >= mutableState.GetNextEventID() {
				metrics.StaleMutableStateCounter.With(metricsScope).Record(1)
				// Reload workflow execution history
				// ErrStaleState will trigger updateWorkflow function to reload the mutable state
				return nil, consts.ErrStaleState
			}

			// Check execution state to make sure task is in the list of outstanding tasks and it is not yet started.  If
			// task is not outstanding than it is most probably a duplicate and complete the task.
			if workflowTask == nil {
				// Looks like WorkflowTask already completed as a result of another call.
				// It is OK to drop the task at this point.
				return nil, serviceerror.NewNotFound("Workflow task not found.")
			}

			workflowKey = mutableState.GetWorkflowKey()
			updateAction := &api.UpdateWorkflowAction{}

			if workflowTask.StartedEventID != common.EmptyEventID {
				// If workflow task is started as part of the current request scope then return a positive response
				if workflowTask.RequestID == requestID {
					resp, err = CreateRecordWorkflowTaskStartedResponse(ctx, mutableState, updateRegistry, workflowTask, req.PollRequest.GetIdentity(), false)
					if err != nil {
						return nil, err
					}
					updateAction.Noop = true
					return updateAction, nil
				}

				// Looks like WorkflowTask already started as a result of another call.
				// It is OK to drop the task at this point.
				return nil, serviceerrors.NewTaskAlreadyStarted("Workflow")
			}

			// Assuming a workflow is running on a sticky task queue by a workerA.
			// After workerA is dead for more than 10s, matching will return StickyWorkerUnavailable error when history
			// tries to push a new workflow task. When history sees that error, it will fall back to push the task to
			// its original normal task queue without clear its stickiness to avoid an extra persistence write.
			// We will clear the stickiness here when that task is delivered to another worker polling from normal queue.
			// The stickiness info is used by frontend to decide if it should send down partial history or full history.
			// Sending down partial history will cost the worker an extra fetch to server for the full history.
			currentTaskQueue := mutableState.CurrentTaskQueue()
			if currentTaskQueue.Kind == enumspb.TASK_QUEUE_KIND_STICKY &&
				currentTaskQueue.GetName() != req.PollRequest.TaskQueue.GetName() {
				// req.PollRequest.TaskQueue.GetName() may include partition, but we only check when sticky is enabled,
				// and sticky queue never has partition, so it does not matter.
				mutableState.ClearStickyTaskQueue()
			}

			_, workflowTask, err = mutableState.AddWorkflowTaskStartedEvent(
				scheduledEventID,
				requestID,
				req.PollRequest.TaskQueue,
				req.PollRequest.Identity,
				worker_versioning.StampFromCapabilities(req.PollRequest.WorkerVersionCapabilities),
				req.GetBuildIdRedirectInfo(),
			)
			if err != nil {
				// Unable to add WorkflowTaskStarted event to history
				return nil, err
			}

			if workflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE {
				updateAction.Noop = true
			}

			workflowScheduleToStartLatency := workflowTask.StartedTime.Sub(workflowTask.ScheduledTime)
			namespaceName := namespaceEntry.Name()
			taskQueue := workflowTask.TaskQueue
			metrics.TaskScheduleToStartLatency.With(metrics.GetPerTaskQueueScope(
				metricsScope,
				namespaceName.String(),
				taskQueue.GetName(),
				taskQueue.GetKind(),
			)).Record(workflowScheduleToStartLatency,
				metrics.TaskQueueTypeTag(enumspb.TASK_QUEUE_TYPE_WORKFLOW),
			)

			resp, err = CreateRecordWorkflowTaskStartedResponse(
				ctx,
				mutableState,
				updateRegistry,
				workflowTask,
				req.PollRequest.GetIdentity(),
				false,
			)
			if err != nil {
				return nil, err
			}

			return updateAction, nil
		},
		nil,
		shardContext,
		workflowConsistencyChecker,
	)

	if err != nil {
		return nil, err
	}

	if dynamicconfig.AccessHistory(config.FrontendAccessHistoryFraction, shardContext.GetMetricsHandler().WithTags(metrics.NamespaceTag(namespaceEntry.Name().String()), metrics.OperationTag(metrics.HistoryHandleWorkflowTaskStartedTag))) {
		maxHistoryPageSize := int32(config.HistoryMaxPageSize(namespaceEntry.Name().String()))
		err = setHistoryForRecordWfTaskStartedResp(
			ctx,
			shardContext,
			workflowKey,
			maxHistoryPageSize,
			workflowConsistencyChecker,
			eventNotifier,
			persistenceVisibilityMgr,
			resp,
		)
	}
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func setHistoryForRecordWfTaskStartedResp(
	ctx context.Context,
	shardContext shard.Context,
	workflowKey definition.WorkflowKey,
	maximumPageSize int32,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	eventNotifier events.Notifier,
	persistenceVisibilityMgr manager.VisibilityManager,
	response *historyservice.RecordWorkflowTaskStartedResponse,
) (retError error) {

	firstEventID := common.FirstEventID
	nextEventID := response.GetNextEventId()
	if response.GetStickyExecutionEnabled() {
		// sticky tasks only need partial history
		firstEventID = response.GetPreviousStartedEventId() + 1
	}

	// TODO below is a temporal solution to guard against invalid event batch
	//  when data inconsistency occurs
	//  long term solution should check event batch pointing backwards within history store
	defer func() {
		if _, ok := retError.(*serviceerror.DataLoss); ok {
			api.TrimHistoryNode(
				ctx,
				shardContext,
				workflowConsistencyChecker,
				eventNotifier,
				workflowKey.GetNamespaceID(),
				workflowKey.GetWorkflowID(),
				workflowKey.GetRunID(),
			)
		}
	}()

	history, persistenceToken, err := api.GetHistory(
		ctx,
		shardContext,
		namespace.ID(workflowKey.GetNamespaceID()),
		&commonpb.WorkflowExecution{WorkflowId: workflowKey.GetWorkflowID(), RunId: workflowKey.GetRunID()},
		firstEventID,
		nextEventID,
		maximumPageSize,
		nil,
		response.GetTransientWorkflowTask(),
		response.GetBranchToken(),
		persistenceVisibilityMgr,
	)
	if err != nil {
		return err
	}

	var continuation []byte
	if len(persistenceToken) != 0 {
		continuation, err = api.SerializeHistoryToken(&tokenspb.HistoryContinuation{
			RunId:                 workflowKey.GetRunID(),
			FirstEventId:          firstEventID,
			NextEventId:           nextEventID,
			PersistenceToken:      persistenceToken,
			TransientWorkflowTask: response.GetTransientWorkflowTask(),
			BranchToken:           response.GetBranchToken(),
		})
		if err != nil {
			return err
		}
	}

	response.History = history
	response.NextPageToken = continuation
	return nil
}

func CreateRecordWorkflowTaskStartedResponse(
	ctx context.Context,
	ms workflow.MutableState,
	updateRegistry update.Registry,
	workflowTask *workflow.WorkflowTaskInfo,
	identity string,
	wtHeartbeat bool,
) (*historyservice.RecordWorkflowTaskStartedResponse, error) {
	response := &historyservice.RecordWorkflowTaskStartedResponse{}
	response.WorkflowType = ms.GetWorkflowType()
	executionInfo := ms.GetExecutionInfo()
	if executionInfo.LastWorkflowTaskStartedEventId != common.EmptyEventID {
		response.PreviousStartedEventId = executionInfo.LastWorkflowTaskStartedEventId
	}

	// Starting workflowTask could result in different scheduledEventID if workflowTask was transient and new events came in
	// before it was started.
	response.ScheduledEventId = workflowTask.ScheduledEventID
	response.StartedEventId = workflowTask.StartedEventID
	response.StickyExecutionEnabled = ms.IsStickyTaskQueueSet()
	response.NextEventId = ms.GetNextEventID()
	response.Attempt = workflowTask.Attempt
	response.WorkflowExecutionTaskQueue = &taskqueuepb.TaskQueue{
		Name: executionInfo.TaskQueue,
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}
	response.ScheduledTime = timestamppb.New(workflowTask.ScheduledTime)
	response.StartedTime = timestamppb.New(workflowTask.StartedTime)
	response.Version = workflowTask.Version

	// TODO (alex-update): Transient needs to be renamed to "TransientOrSpeculative"
	response.TransientWorkflowTask = ms.GetTransientWorkflowTaskInfo(workflowTask, identity)

	currentBranchToken, err := ms.GetCurrentBranchToken()
	if err != nil {
		return nil, err
	}
	response.BranchToken = currentBranchToken

	qr := ms.GetQueryRegistry()
	bufferedQueryIDs := qr.GetBufferedIDs()
	if len(bufferedQueryIDs) > 0 {
		response.Queries = make(map[string]*querypb.WorkflowQuery, len(bufferedQueryIDs))
		for _, bufferedQueryID := range bufferedQueryIDs {
			input, err := qr.GetQueryInput(bufferedQueryID)
			if err != nil {
				continue
			}
			response.Queries[bufferedQueryID] = input
		}
	}

	// If there are updates in the registry which were already sent to worker but still
	// are not processed and not rejected by server, it means that WT that was used to
	// deliver those updates failed, got timed out, or got lost.
	// Resend these updates if this is not a heartbeat WT (includeAlreadySent = !wtHeartbeat).
	// Heartbeat WT delivers only new updates that come while this WT was running (similar to queries and buffered events).
	response.Messages = updateRegistry.Send(ctx, !wtHeartbeat, workflowTask.StartedEventID)

	if workflowTask.Type == enumsspb.WORKFLOW_TASK_TYPE_SPECULATIVE && len(response.GetMessages()) == 0 {
		return nil, serviceerror.NewNotFound("No messages for speculative workflow task.")
	}

	return response, nil
}
