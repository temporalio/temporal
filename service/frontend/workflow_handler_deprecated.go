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

package frontend

import (
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"

	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/common/tasktoken"
)

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/api/getworkflowexecutionhistory/api.go
func (wh *WorkflowHandler) getWorkflowExecutionHistory(
	ctx context.Context,
	request *workflowservice.GetWorkflowExecutionHistoryRequest,
) (_ *workflowservice.GetWorkflowExecutionHistoryResponse, retError error) {
	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	// this function returns the following 7 things,
	// 1. the current branch token (to use to retrieve history events)
	// 2. the workflow run ID
	// 3. the last first event ID (the event ID of the last batch of events in the history)
	// 4. the last first event transaction id
	// 5. the next event ID
	// 6. whether the workflow is running
	// 7. error if any
	queryHistory := func(
		namespaceUUID namespace.ID,
		execution *commonpb.WorkflowExecution,
		expectedNextEventID int64,
		currentBranchToken []byte,
		verionHistoryItem *historyspb.VersionHistoryItem,
	) ([]byte, string, int64, int64, bool, *historyspb.VersionHistoryItem, error) {
		response, err := wh.historyClient.PollMutableState(ctx, &historyservice.PollMutableStateRequest{
			NamespaceId:         namespaceUUID.String(),
			Execution:           execution,
			ExpectedNextEventId: expectedNextEventID,
			CurrentBranchToken:  currentBranchToken,
			VersionHistoryItem:  verionHistoryItem,
		})
		if err != nil {
			return nil, "", 0, 0, false, nil, err
		}

		isWorkflowRunning := response.GetWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
		currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(response.GetVersionHistories())
		if err != nil {
			return nil, "", 0, 0, false, nil, err
		}
		lastVersionHistoryItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
		if err != nil {
			return nil, "", 0, 0, false, nil, err
		}
		return response.CurrentBranchToken,
			response.Execution.GetRunId(),
			response.GetLastFirstEventId(),
			response.GetNextEventId(),
			isWorkflowRunning,
			lastVersionHistoryItem,
			nil
	}

	isLongPoll := request.GetWaitNewEvent()
	isCloseEventOnly := request.GetHistoryEventFilterType() == enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT
	execution := request.Execution
	var continuationToken *tokenspb.HistoryContinuation

	var runID string
	lastFirstEventID := common.FirstEventID
	var nextEventID int64
	var isWorkflowRunning bool

	// process the token for paging
	queryNextEventID := common.EndEventID
	if request.NextPageToken != nil {
		continuationToken, err = deserializeHistoryToken(request.NextPageToken)
		if err != nil {
			return nil, errInvalidNextPageToken
		}
		if execution.GetRunId() != "" && execution.GetRunId() != continuationToken.GetRunId() {
			return nil, errNextPageTokenRunIDMismatch
		}

		execution.RunId = continuationToken.GetRunId()

		// we need to update the current next event ID and whether workflow is running
		if len(continuationToken.PersistenceToken) == 0 && isLongPoll && continuationToken.IsWorkflowRunning {
			if !isCloseEventOnly {
				queryNextEventID = continuationToken.GetNextEventId()
			}
			continuationToken.BranchToken, _, lastFirstEventID, nextEventID, isWorkflowRunning, continuationToken.VersionHistoryItem, err =
				queryHistory(namespaceID, execution, queryNextEventID, continuationToken.BranchToken, continuationToken.VersionHistoryItem)
			if err != nil {
				return nil, err
			}
			continuationToken.FirstEventId = continuationToken.GetNextEventId()
			continuationToken.NextEventId = nextEventID
			continuationToken.IsWorkflowRunning = isWorkflowRunning
		}
	} else {
		continuationToken = &tokenspb.HistoryContinuation{}
		if !isCloseEventOnly {
			queryNextEventID = common.FirstEventID
		}
		continuationToken.BranchToken, runID, lastFirstEventID, nextEventID, isWorkflowRunning, continuationToken.VersionHistoryItem, err =
			queryHistory(namespaceID, execution, queryNextEventID, nil, nil)
		if err != nil {
			return nil, err
		}

		execution.RunId = runID

		continuationToken.RunId = runID
		continuationToken.FirstEventId = common.FirstEventID
		continuationToken.NextEventId = nextEventID
		continuationToken.IsWorkflowRunning = isWorkflowRunning
		continuationToken.PersistenceToken = nil
	}

	// TODO below is a temporal solution to guard against invalid event batch
	//  when data inconsistency occurs
	//  long term solution should check event batch pointing backwards within history store
	defer func() {
		if _, ok := retError.(*serviceerror.DataLoss); ok {
			wh.trimHistoryNode(ctx, namespaceID.String(), execution.GetWorkflowId(), execution.GetRunId())
		}
	}()

	rawHistoryQueryEnabled := wh.config.SendRawWorkflowHistory(request.GetNamespace())

	history := &historypb.History{}
	history.Events = []*historypb.HistoryEvent{}
	var historyBlob []*commonpb.DataBlob
	if isCloseEventOnly {
		if !isWorkflowRunning {
			if rawHistoryQueryEnabled {
				historyBlob, _, err = wh.getRawHistory(
					ctx,
					wh.metricsScope(ctx),
					namespaceID,
					execution,
					lastFirstEventID,
					nextEventID,
					request.GetMaximumPageSize(),
					nil,
					continuationToken.TransientWorkflowTask,
					continuationToken.BranchToken,
				)
				if err != nil {
					return nil, err
				}

				// since getHistory func will not return empty history, so the below is safe
				historyBlob = historyBlob[len(historyBlob)-1:]
			} else {
				history, _, err = wh.getHistory(
					ctx,
					wh.metricsScope(ctx),
					namespaceID,
					namespace.Name(request.GetNamespace()),
					execution,
					lastFirstEventID,
					nextEventID,
					request.GetMaximumPageSize(),
					nil,
					continuationToken.TransientWorkflowTask,
					continuationToken.BranchToken,
				)
				if err != nil {
					return nil, err
				}
				// since getHistory func will not return empty history, so the below is safe
				history.Events = history.Events[len(history.Events)-1 : len(history.Events)]
			}
			continuationToken = nil
		} else if isLongPoll {
			// set the persistence token to be nil so next time we will query history for updates
			continuationToken.PersistenceToken = nil
		} else {
			continuationToken = nil
		}
	} else {
		// return all events
		if continuationToken.FirstEventId >= continuationToken.NextEventId {
			// currently there is no new event
			history.Events = []*historypb.HistoryEvent{}
			if !isWorkflowRunning {
				continuationToken = nil
			}
		} else {
			if rawHistoryQueryEnabled {
				historyBlob, continuationToken.PersistenceToken, err = wh.getRawHistory(
					ctx,
					wh.metricsScope(ctx),
					namespaceID,
					execution,
					continuationToken.FirstEventId,
					continuationToken.NextEventId,
					request.GetMaximumPageSize(),
					continuationToken.PersistenceToken,
					continuationToken.TransientWorkflowTask,
					continuationToken.BranchToken,
				)
			} else {
				history, continuationToken.PersistenceToken, err = wh.getHistory(
					ctx,
					wh.metricsScope(ctx),
					namespaceID,
					namespace.Name(request.GetNamespace()),
					execution,
					continuationToken.FirstEventId,
					continuationToken.NextEventId,
					request.GetMaximumPageSize(),
					continuationToken.PersistenceToken,
					continuationToken.TransientWorkflowTask,
					continuationToken.BranchToken,
				)
			}

			if err != nil {
				return nil, err
			}

			// here, for long pull on history events, we need to intercept the paging token from cassandra
			// and do something clever
			if len(continuationToken.PersistenceToken) == 0 && (!continuationToken.IsWorkflowRunning || !isLongPoll) {
				// meaning, there is no more history to be returned
				continuationToken = nil
			}
		}
	}

	nextToken, err := serializeHistoryToken(continuationToken)
	if err != nil {
		return nil, err
	}

	// Backwards-compatibility fix for retry events after #1866: older SDKs don't know how to "follow"
	// subsequent runs linked in WorkflowExecutionFailed or TimedOut events, so they'll get the wrong result
	// when trying to "get" the result of a workflow run. (This applies to cron runs also but "get" on a cron
	// workflow isn't really sensible.)
	//
	// To handle this in a backwards-compatible way, we'll pretend the completion event is actually
	// ContinuedAsNew, if it's Failed or TimedOut. We want to do this only when the client is looking for a
	// completion event, and not when it's getting the history to display for other purposes. The best signal
	// for that purpose is `isCloseEventOnly`. (We can't use `isLongPoll` also because in some cases, older
	// versions of the Java SDK don't set that flag.)
	//
	// TODO: We can remove this once we no longer support SDK versions prior to around September 2021.
	// Revisit this once we have an SDK deprecation policy.
	if isCloseEventOnly &&
		!wh.versionChecker.ClientSupportsFeature(ctx, headers.FeatureFollowsNextRunID) &&
		len(history.Events) > 0 {
		lastEvent := history.Events[len(history.Events)-1]
		fakeEvent, err := wh.makeFakeContinuedAsNewEvent(ctx, lastEvent)
		if err != nil {
			return nil, err
		}
		if fakeEvent != nil {
			history.Events[len(history.Events)-1] = fakeEvent
		}
	}

	return &workflowservice.GetWorkflowExecutionHistoryResponse{
		History:       history,
		RawHistory:    historyBlob,
		NextPageToken: nextToken,
		Archived:      false,
	}, nil
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/api/getworkflowexecutionhistoryreverse/api.go
func (wh *WorkflowHandler) getWorkflowExecutionHistoryReverse(
	ctx context.Context,
	request *workflowservice.GetWorkflowExecutionHistoryReverseRequest,
) (_ *workflowservice.GetWorkflowExecutionHistoryReverseResponse, retError error) {
	namespaceID, err := wh.namespaceRegistry.GetNamespaceID(namespace.Name(request.GetNamespace()))
	if err != nil {
		return nil, err
	}

	queryMutableState := func(
		namespaceUUID namespace.ID,
		execution *commonpb.WorkflowExecution,
		expectedNextEventID int64,
		currentBranchToken []byte,
		versionHistoryItem *historyspb.VersionHistoryItem,
	) ([]byte, string, int64, *historyspb.VersionHistoryItem, error) {
		response, err := wh.historyClient.PollMutableState(ctx, &historyservice.PollMutableStateRequest{
			NamespaceId:         namespaceUUID.String(),
			Execution:           execution,
			ExpectedNextEventId: expectedNextEventID,
			CurrentBranchToken:  currentBranchToken,
			VersionHistoryItem:  versionHistoryItem,
		})
		if err != nil {
			return nil, "", 0, nil, err
		}

		currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(response.GetVersionHistories())
		if err != nil {
			return nil, "", 0, nil, err
		}
		lastVersionHistoryItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
		if err != nil {
			return nil, "", 0, nil, err
		}

		return response.CurrentBranchToken,
			response.Execution.GetRunId(),
			response.GetLastFirstEventTxnId(),
			lastVersionHistoryItem,
			nil
	}

	execution := request.Execution
	var continuationToken *tokenspb.HistoryContinuation

	var runID string
	var lastFirstTxnID int64

	if request.NextPageToken == nil {
		continuationToken = &tokenspb.HistoryContinuation{}
		continuationToken.BranchToken, runID, lastFirstTxnID, continuationToken.VersionHistoryItem, err =
			queryMutableState(namespaceID, execution, common.FirstEventID, nil, nil)
		if err != nil {
			return nil, err
		}

		execution.RunId = runID
		continuationToken.RunId = runID
		continuationToken.FirstEventId = common.FirstEventID
		continuationToken.NextEventId = common.EmptyEventID
		continuationToken.PersistenceToken = nil
	} else {
		continuationToken, err = deserializeHistoryToken(request.NextPageToken)
		if err != nil {
			return nil, errInvalidNextPageToken
		}
		if execution.GetRunId() != "" && execution.GetRunId() != continuationToken.GetRunId() {
			return nil, errNextPageTokenRunIDMismatch
		}

		execution.RunId = continuationToken.GetRunId()
	}

	// TODO below is a temporal solution to guard against invalid event batch
	//  when data inconsistency occurs
	//  long term solution should check event batch pointing backwards within history store
	defer func() {
		if _, ok := retError.(*serviceerror.DataLoss); ok {
			wh.trimHistoryNode(ctx, namespaceID.String(), execution.GetWorkflowId(), execution.GetRunId())
		}
	}()

	history := &historypb.History{}
	history.Events = []*historypb.HistoryEvent{}
	// return all events
	history, continuationToken.PersistenceToken, continuationToken.NextEventId, err = wh.getHistoryReverse(
		ctx,
		wh.metricsScope(ctx),
		namespaceID,
		namespace.Name(request.GetNamespace()),
		execution,
		continuationToken.NextEventId,
		lastFirstTxnID,
		request.GetMaximumPageSize(),
		continuationToken.PersistenceToken,
		continuationToken.BranchToken,
	)

	if err != nil {
		return nil, err
	}

	if continuationToken.NextEventId < continuationToken.FirstEventId {
		continuationToken = nil
	}

	nextToken, err := serializeHistoryToken(continuationToken)
	if err != nil {
		return nil, err
	}

	return &workflowservice.GetWorkflowExecutionHistoryReverseResponse{
		History:       history,
		NextPageToken: nextToken,
	}, nil
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO WorkflowHandler.RespondWorkflowTaskCompleted() and ./service/history/workflowTaskHandlerCallbacks.go
func (wh *WorkflowHandler) respondWorkflowTaskCompleted(
	ctx context.Context,
	request *workflowservice.RespondWorkflowTaskCompletedRequest,
) (_ *workflowservice.RespondWorkflowTaskCompletedResponse, retError error) {
	taskToken, err := wh.tokenSerializer.Deserialize(request.TaskToken)
	if err != nil {
		return nil, err
	}
	namespaceId := namespace.ID(taskToken.GetNamespaceId())

	histResp, err := wh.historyClient.RespondWorkflowTaskCompleted(ctx, &historyservice.RespondWorkflowTaskCompletedRequest{
		NamespaceId:     namespaceId.String(),
		CompleteRequest: request,
	})
	if err != nil {
		return nil, err
	}

	completedResp := &workflowservice.RespondWorkflowTaskCompletedResponse{
		ActivityTasks:       histResp.ActivityTasks,
		ResetHistoryEventId: histResp.ResetHistoryEventId,
	}
	if request.GetReturnNewWorkflowTask() && histResp != nil && histResp.StartedResponse != nil {
		taskToken := tasktoken.NewWorkflowTaskToken(
			taskToken.GetNamespaceId(),
			taskToken.GetWorkflowId(),
			taskToken.GetRunId(),
			histResp.StartedResponse.GetScheduledEventId(),
			histResp.StartedResponse.GetStartedEventId(),
			histResp.StartedResponse.GetStartedTime(),
			histResp.StartedResponse.GetAttempt(),
			histResp.StartedResponse.GetClock(),
			histResp.StartedResponse.GetVersion(),
		)
		token, err := wh.tokenSerializer.Serialize(taskToken)
		if err != nil {
			return nil, err
		}
		workflowExecution := &commonpb.WorkflowExecution{
			WorkflowId: taskToken.GetWorkflowId(),
			RunId:      taskToken.GetRunId(),
		}
		matchingResp := common.CreateMatchingPollWorkflowTaskQueueResponse(histResp.StartedResponse, workflowExecution, token)

		newWorkflowTask, err := wh.createPollWorkflowTaskQueueResponse(ctx, namespaceId, matchingResp, matchingResp.GetBranchToken())
		if err != nil {
			return nil, err
		}
		completedResp.WorkflowTask = newWorkflowTask
	}

	return completedResp, nil
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/api/utils/get_history_util.go
func (wh *WorkflowHandler) getRawHistory(
	ctx context.Context,
	metricsHandler metrics.Handler,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	firstEventID int64,
	nextEventID int64,
	pageSize int32,
	nextPageToken []byte,
	transientWorkflowTaskInfo *historyspb.TransientWorkflowTaskInfo,
	branchToken []byte,
) ([]*commonpb.DataBlob, []byte, error) {
	shardID := common.WorkflowIDToHistoryShard(namespaceID.String(), execution.GetWorkflowId(), wh.config.NumHistoryShards)

	resp, err := wh.persistenceExecutionManager.ReadRawHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      int(pageSize),
		NextPageToken: nextPageToken,
		ShardID:       shardID,
	})
	if err != nil {
		return nil, nil, err
	}

	rawHistory := resp.HistoryEventBlobs
	if len(resp.NextPageToken) == 0 && transientWorkflowTaskInfo != nil {
		if err := wh.validateTransientWorkflowTaskEvents(nextEventID, transientWorkflowTaskInfo); err != nil {
			metrics.ServiceErrIncompleteHistoryCounter.With(metricsHandler).Record(1)
			wh.logger.Error("getHistory error",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(execution.GetWorkflowId()),
				tag.WorkflowRunID(execution.GetRunId()),
				tag.Error(err))
			return nil, nil, err
		}

		for _, event := range transientWorkflowTaskInfo.HistorySuffix {
			blob, err := wh.payloadSerializer.SerializeEvent(event, enumspb.ENCODING_TYPE_PROTO3)
			if err != nil {
				return nil, nil, err
			}
			rawHistory = append(rawHistory, blob)
		}
	}

	return rawHistory, resp.NextPageToken, nil
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/api/utils/get_history_util.go
func (wh *WorkflowHandler) getHistory(
	ctx context.Context,
	metricsHandler metrics.Handler,
	namespaceID namespace.ID,
	namespace namespace.Name,
	execution *commonpb.WorkflowExecution,
	firstEventID int64,
	nextEventID int64,
	pageSize int32,
	nextPageToken []byte,
	transientWorkflowTaskInfo *historyspb.TransientWorkflowTaskInfo,
	branchToken []byte,
) (*historypb.History, []byte, error) {

	var size int
	isFirstPage := len(nextPageToken) == 0
	shardID := common.WorkflowIDToHistoryShard(namespaceID.String(), execution.GetWorkflowId(), wh.config.NumHistoryShards)
	var err error
	var historyEvents []*historypb.HistoryEvent
	historyEvents, size, nextPageToken, err = persistence.ReadFullPageEvents(ctx, wh.persistenceExecutionManager, &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      int(pageSize),
		NextPageToken: nextPageToken,
		ShardID:       shardID,
	})
	switch err.(type) {
	case nil:
		// noop
	case *serviceerror.DataLoss:
		// log event
		wh.logger.Error("encountered data loss event",
			tag.WorkflowNamespaceID(namespaceID.String()),
			tag.WorkflowID(execution.GetWorkflowId()),
			tag.WorkflowRunID(execution.GetRunId()),
			tag.Error(err),
		)
		return nil, nil, err
	default:
		return nil, nil, err
	}

	metricsHandler.Histogram(metrics.HistorySize.Name(), metrics.HistorySize.Unit()).Record(int64(size))

	isLastPage := len(nextPageToken) == 0
	if err := wh.verifyHistoryIsComplete(
		historyEvents,
		firstEventID,
		nextEventID-1,
		isFirstPage,
		isLastPage,
		int(pageSize)); err != nil {
		metrics.ServiceErrIncompleteHistoryCounter.With(metricsHandler).Record(1)
		wh.logger.Error("getHistory: incomplete history",
			tag.WorkflowNamespaceID(namespaceID.String()),
			tag.WorkflowID(execution.GetWorkflowId()),
			tag.WorkflowRunID(execution.GetRunId()),
			tag.Error(err))
	}

	if len(nextPageToken) == 0 && transientWorkflowTaskInfo != nil {
		if err := wh.validateTransientWorkflowTaskEvents(nextEventID, transientWorkflowTaskInfo); err != nil {
			metrics.ServiceErrIncompleteHistoryCounter.With(metricsHandler).Record(1)
			wh.logger.Error("getHistory error",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(execution.GetWorkflowId()),
				tag.WorkflowRunID(execution.GetRunId()),
				tag.Error(err))
		}
		// Append the transient workflow task events once we are done enumerating everything from the events table
		historyEvents = append(historyEvents, transientWorkflowTaskInfo.HistorySuffix...)
	}

	if err := wh.processOutgoingSearchAttributes(historyEvents, namespace); err != nil {
		return nil, nil, err
	}

	executionHistory := &historypb.History{
		Events: historyEvents,
	}
	return executionHistory, nextPageToken, nil
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/api/utils/get_history_util.go
func (wh *WorkflowHandler) getHistoryReverse(
	ctx context.Context,
	metricsHandler metrics.Handler,
	namespaceID namespace.ID,
	namespace namespace.Name,
	execution *commonpb.WorkflowExecution,
	nextEventID int64,
	lastFirstTxnID int64,
	pageSize int32,
	nextPageToken []byte,
	branchToken []byte,
) (*historypb.History, []byte, int64, error) {
	var size int
	shardID := common.WorkflowIDToHistoryShard(namespaceID.String(), execution.GetWorkflowId(), wh.config.NumHistoryShards)
	var err error
	var historyEvents []*historypb.HistoryEvent

	historyEvents, size, nextPageToken, err = persistence.ReadFullPageEventsReverse(ctx, wh.persistenceExecutionManager, &persistence.ReadHistoryBranchReverseRequest{
		BranchToken:            branchToken,
		MaxEventID:             nextEventID,
		LastFirstTransactionID: lastFirstTxnID,
		PageSize:               int(pageSize),
		NextPageToken:          nextPageToken,
		ShardID:                shardID,
	})

	switch err.(type) {
	case nil:
		// noop
	case *serviceerror.DataLoss:
		// log event
		wh.logger.Error("encountered data loss event", tag.WorkflowNamespaceID(namespaceID.String()), tag.WorkflowID(execution.GetWorkflowId()), tag.WorkflowRunID(execution.GetRunId()))
		return nil, nil, 0, err
	default:
		return nil, nil, 0, err
	}

	metricsHandler.Histogram(metrics.HistorySize.Name(), metrics.HistorySize.Unit()).Record(int64(size))

	if err := wh.processOutgoingSearchAttributes(historyEvents, namespace); err != nil {
		return nil, nil, 0, err
	}

	executionHistory := &historypb.History{
		Events: historyEvents,
	}

	var newNextEventID int64
	if len(historyEvents) > 0 {
		newNextEventID = historyEvents[len(historyEvents)-1].EventId - 1
	} else {
		newNextEventID = nextEventID
	}

	return executionHistory, nextPageToken, newNextEventID, nil
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/workflowTaskHandlerCallbacks.go
func (wh *WorkflowHandler) createPollWorkflowTaskQueueResponse(
	ctx context.Context,
	namespaceID namespace.ID,
	matchingResp *matchingservice.PollWorkflowTaskQueueResponse,
	branchToken []byte,
) (_ *workflowservice.PollWorkflowTaskQueueResponse, retError error) {

	if matchingResp.WorkflowExecution == nil {
		// this will happen if there is no workflow task to be send to worker / caller
		return &workflowservice.PollWorkflowTaskQueueResponse{}, nil
	}

	var history *historypb.History
	var continuation []byte
	var err error

	if matchingResp.GetStickyExecutionEnabled() && matchingResp.Query != nil {
		// meaning sticky query, we should not return any events to worker
		// since query task only check the current status
		history = &historypb.History{
			Events: []*historypb.HistoryEvent{},
		}
	} else {
		// here we have 3 cases:
		// 1. sticky && non query task
		// 2. non sticky &&  non query task
		// 3. non sticky && query task
		// for 1, partial history have to be send back
		// for 2 and 3, full history have to be send back

		var persistenceToken []byte

		firstEventID := common.FirstEventID
		nextEventID := matchingResp.GetNextEventId()
		if matchingResp.GetStickyExecutionEnabled() {
			firstEventID = matchingResp.GetPreviousStartedEventId() + 1
		}
		namespaceEntry, dErr := wh.namespaceRegistry.GetNamespaceByID(namespaceID)
		if dErr != nil {
			return nil, dErr
		}

		// TODO below is a temporal solution to guard against invalid event batch
		//  when data inconsistency occurs
		//  long term solution should check event batch pointing backwards within history store
		defer func() {
			if _, ok := retError.(*serviceerror.DataLoss); ok {
				wh.trimHistoryNode(ctx, namespaceID.String(), matchingResp.WorkflowExecution.GetWorkflowId(), matchingResp.WorkflowExecution.GetRunId())
			}
		}()
		history, persistenceToken, err = wh.getHistory(
			ctx,
			wh.metricsScope(ctx),
			namespaceID,
			namespaceEntry.Name(),
			matchingResp.GetWorkflowExecution(),
			firstEventID,
			nextEventID,
			int32(wh.config.HistoryMaxPageSize(namespaceEntry.Name().String())),
			nil,
			matchingResp.GetTransientWorkflowTask(),
			branchToken,
		)
		if err != nil {
			return nil, err
		}

		if len(persistenceToken) != 0 {
			continuation, err = serializeHistoryToken(&tokenspb.HistoryContinuation{
				RunId:                 matchingResp.WorkflowExecution.GetRunId(),
				FirstEventId:          firstEventID,
				NextEventId:           nextEventID,
				PersistenceToken:      persistenceToken,
				TransientWorkflowTask: matchingResp.GetTransientWorkflowTask(),
				BranchToken:           branchToken,
			})
			if err != nil {
				return nil, err
			}
		}
	}

	resp := &workflowservice.PollWorkflowTaskQueueResponse{
		TaskToken:                  matchingResp.TaskToken,
		WorkflowExecution:          matchingResp.WorkflowExecution,
		WorkflowType:               matchingResp.WorkflowType,
		PreviousStartedEventId:     matchingResp.PreviousStartedEventId,
		StartedEventId:             matchingResp.StartedEventId,
		Query:                      matchingResp.Query,
		BacklogCountHint:           matchingResp.BacklogCountHint,
		Attempt:                    matchingResp.Attempt,
		History:                    history,
		NextPageToken:              continuation,
		WorkflowExecutionTaskQueue: matchingResp.WorkflowExecutionTaskQueue,
		ScheduledTime:              matchingResp.ScheduledTime,
		StartedTime:                matchingResp.StartedTime,
		Queries:                    matchingResp.Queries,
		Messages:                   matchingResp.Messages,
	}

	return resp, nil
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/api/getworkflowexecutionhistory/api.go
func (wh *WorkflowHandler) makeFakeContinuedAsNewEvent(
	_ context.Context,
	lastEvent *historypb.HistoryEvent,
) (*historypb.HistoryEvent, error) {
	switch lastEvent.EventType {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		if lastEvent.GetWorkflowExecutionCompletedEventAttributes().GetNewExecutionRunId() == "" {
			return nil, nil
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		if lastEvent.GetWorkflowExecutionFailedEventAttributes().GetNewExecutionRunId() == "" {
			return nil, nil
		}
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		if lastEvent.GetWorkflowExecutionTimedOutEventAttributes().GetNewExecutionRunId() == "" {
			return nil, nil
		}
	default:
		return nil, nil
	}

	// We need to replace the last event with a continued-as-new event that has at least the
	// NewExecutionRunId field. We don't actually need any other fields, since that's the only one
	// the client looks at in this case, but copy the last result or failure from the real completed
	// event just so it's clear what the result was.
	newAttrs := &historypb.WorkflowExecutionContinuedAsNewEventAttributes{}
	switch lastEvent.EventType {
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED:
		attrs := lastEvent.GetWorkflowExecutionCompletedEventAttributes()
		newAttrs.NewExecutionRunId = attrs.NewExecutionRunId
		newAttrs.LastCompletionResult = attrs.Result
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED:
		attrs := lastEvent.GetWorkflowExecutionFailedEventAttributes()
		newAttrs.NewExecutionRunId = attrs.NewExecutionRunId
		newAttrs.Failure = attrs.Failure
	case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_TIMED_OUT:
		attrs := lastEvent.GetWorkflowExecutionTimedOutEventAttributes()
		newAttrs.NewExecutionRunId = attrs.NewExecutionRunId
		newAttrs.Failure = failure.NewTimeoutFailure("workflow timeout", enumspb.TIMEOUT_TYPE_START_TO_CLOSE)
	}

	return &historypb.HistoryEvent{
		EventId:   lastEvent.EventId,
		EventTime: lastEvent.EventTime,
		EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW,
		Version:   lastEvent.Version,
		TaskId:    lastEvent.TaskId,
		Attributes: &historypb.HistoryEvent_WorkflowExecutionContinuedAsNewEventAttributes{
			WorkflowExecutionContinuedAsNewEventAttributes: newAttrs,
		},
	}, nil
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/api/utils/trim_history_util.go
func (wh *WorkflowHandler) trimHistoryNode(
	ctx context.Context,
	namespaceID string,
	workflowID string,
	runID string,
) {
	response, err := wh.historyClient.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
		NamespaceId: namespaceID,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	})
	if err != nil {
		return // abort
	}

	_, err = wh.persistenceExecutionManager.TrimHistoryBranch(ctx, &persistence.TrimHistoryBranchRequest{
		ShardID:       common.WorkflowIDToHistoryShard(namespaceID, workflowID, wh.config.NumHistoryShards),
		BranchToken:   response.CurrentBranchToken,
		NodeID:        response.GetLastFirstEventId(),
		TransactionID: response.GetLastFirstEventTxnId(),
	})
	if err != nil {
		// best effort
		wh.logger.Error("unable to trim history branch",
			tag.WorkflowNamespaceID(namespaceID),
			tag.WorkflowID(workflowID),
			tag.WorkflowRunID(runID),
			tag.Error(err),
		)
	}
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/api/utils/get_history_util.go
func (wh *WorkflowHandler) processOutgoingSearchAttributes(events []*historypb.HistoryEvent, namespace namespace.Name) error {
	saTypeMap, err := wh.saProvider.GetSearchAttributes(wh.visibilityMgr.GetIndexName(), false)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf(errUnableToGetSearchAttributesMessage, err))
	}
	for _, event := range events {
		var searchAttributes *commonpb.SearchAttributes
		switch event.EventType {
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED:
			searchAttributes = event.GetWorkflowExecutionStartedEventAttributes().GetSearchAttributes()
		case enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW:
			searchAttributes = event.GetWorkflowExecutionContinuedAsNewEventAttributes().GetSearchAttributes()
		case enumspb.EVENT_TYPE_START_CHILD_WORKFLOW_EXECUTION_INITIATED:
			searchAttributes = event.GetStartChildWorkflowExecutionInitiatedEventAttributes().GetSearchAttributes()
		case enumspb.EVENT_TYPE_UPSERT_WORKFLOW_SEARCH_ATTRIBUTES:
			searchAttributes = event.GetUpsertWorkflowSearchAttributesEventAttributes().GetSearchAttributes()
		}
		if searchAttributes != nil {
			searchattribute.ApplyTypeMap(searchAttributes, saTypeMap)
			aliasedSas, err := searchattribute.AliasFields(wh.saMapperProvider, searchAttributes, namespace.String())
			if err != nil {
				return err
			}
			if aliasedSas != searchAttributes {
				searchAttributes.IndexedFields = aliasedSas.IndexedFields
			}
		}
	}

	return nil
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/api/utils/get_history_util.go
func (wh *WorkflowHandler) validateTransientWorkflowTaskEvents(
	eventIDOffset int64,
	transientWorkflowTaskInfo *historyspb.TransientWorkflowTaskInfo,
) error {
	for i, event := range transientWorkflowTaskInfo.HistorySuffix {
		expectedEventID := eventIDOffset + int64(i)
		if event.GetEventId() != expectedEventID {
			return serviceerror.NewInternal(
				fmt.Sprintf(
					"invalid transient workflow task at position %v; expected event ID %v, found event ID %v",
					i,
					expectedEventID,
					event.GetEventId()))
		}
	}

	return nil
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/api/utils/get_history_util.go
func (wh *WorkflowHandler) verifyHistoryIsComplete(
	events []*historypb.HistoryEvent,
	expectedFirstEventID int64,
	expectedLastEventID int64,
	isFirstPage bool,
	isLastPage bool,
	pageSize int,
) error {

	nEvents := len(events)
	if nEvents == 0 {
		if isLastPage {
			// we seem to be returning a non-nil pageToken on the lastPage which
			// in turn cases the client to call getHistory again - only to find
			// there are no more events to consume - bail out if this is the case here
			return nil
		}
		return serviceerror.NewDataLoss("History contains zero events.")
	}

	firstEventID := events[0].GetEventId()
	lastEventID := events[nEvents-1].GetEventId()

	if !isFirstPage { // at least one page of history has been read previously
		if firstEventID <= expectedFirstEventID {
			// not first page and no events have been read in the previous pages - not possible
			return serviceerror.NewDataLoss(fmt.Sprintf("Invalid history: expected first eventID to be > %v but got %v", expectedFirstEventID, firstEventID))
		}
		expectedFirstEventID = firstEventID
	}

	if !isLastPage {
		// estimate lastEventID based on pageSize. This is a lower bound
		// since the persistence layer counts "batch of events" as a single page
		expectedLastEventID = expectedFirstEventID + int64(pageSize) - 1
	}

	nExpectedEvents := expectedLastEventID - expectedFirstEventID + 1

	if firstEventID == expectedFirstEventID &&
		((isLastPage && lastEventID == expectedLastEventID && int64(nEvents) == nExpectedEvents) ||
			(!isLastPage && lastEventID >= expectedLastEventID && int64(nEvents) >= nExpectedEvents)) {
		return nil
	}

	return serviceerror.NewDataLoss(fmt.Sprintf("Incomplete history: expected events [%v-%v] but got events [%v-%v] of length %v: isFirstPage=%v,isLastPage=%v,pageSize=%v",
		expectedFirstEventID,
		expectedLastEventID,
		firstEventID,
		lastEventID,
		nEvents,
		isFirstPage,
		isLastPage,
		pageSize))
}
