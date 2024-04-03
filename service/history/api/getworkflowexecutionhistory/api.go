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

package getworkflowexecutionhistory

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/failure"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
)

func Invoke(
	ctx context.Context,
	shardContext shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	versionChecker headers.VersionChecker,
	eventNotifier events.Notifier,
	request *historyservice.GetWorkflowExecutionHistoryRequest,
	persistenceVisibilityMgr manager.VisibilityManager,
) (_ *historyservice.GetWorkflowExecutionHistoryResponse, retError error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	err := api.ValidateNamespaceUUID(namespaceID)
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
		versionHistoryItem *historyspb.VersionHistoryItem,
	) ([]byte, string, int64, int64, bool, *historyspb.VersionHistoryItem, error) {
		response, err := api.GetOrPollMutableState(
			ctx,
			shardContext,
			&historyservice.GetMutableStateRequest{
				NamespaceId:         namespaceUUID.String(),
				Execution:           execution,
				ExpectedNextEventId: expectedNextEventID,
				CurrentBranchToken:  currentBranchToken,
				VersionHistoryItem:  versionHistoryItem,
			},
			workflowConsistencyChecker,
			eventNotifier,
		)
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

	isLongPoll := request.Request.GetWaitNewEvent()
	isCloseEventOnly := request.Request.GetHistoryEventFilterType() == enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT
	execution := request.Request.Execution
	var continuationToken *tokenspb.HistoryContinuation

	var runID string
	lastFirstEventID := common.FirstEventID
	var nextEventID int64
	var isWorkflowRunning bool

	// process the token for paging
	queryNextEventID := common.EndEventID
	if request.Request.NextPageToken != nil {
		continuationToken, err = api.DeserializeHistoryToken(request.Request.NextPageToken)
		if err != nil {
			return nil, consts.ErrInvalidNextPageToken
		}
		if execution.GetRunId() != "" && execution.GetRunId() != continuationToken.GetRunId() {
			return nil, consts.ErrNextPageTokenRunIDMismatch
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

	// TODO below is a temporary solution to guard against invalid event batch
	// when data inconsistency occurs. Long term solution should check event
	// batch pointing backwards within history store.
	defer func() {
		if _, ok := retError.(*serviceerror.DataLoss); ok {
			api.TrimHistoryNode(
				ctx,
				shardContext,
				workflowConsistencyChecker,
				eventNotifier,
				namespaceID.String(),
				execution.GetWorkflowId(),
				execution.GetRunId(),
			)
		}
	}()

	history := &historypb.History{}
	history.Events = []*historypb.HistoryEvent{}
	var historyBlob []*commonpb.DataBlob
	if isCloseEventOnly {
		if !isWorkflowRunning {
			if shardContext.GetConfig().SendRawWorkflowHistory(request.Request.GetNamespace()) {
				historyBlob, _, err = api.GetRawHistory(
					ctx,
					shardContext,
					namespaceID,
					execution,
					lastFirstEventID,
					nextEventID,
					request.Request.GetMaximumPageSize(),
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
				history, _, err = api.GetHistory(
					ctx,
					shardContext,
					namespaceID,
					execution,
					lastFirstEventID,
					nextEventID,
					request.Request.GetMaximumPageSize(),
					nil,
					continuationToken.TransientWorkflowTask,
					continuationToken.BranchToken,
					persistenceVisibilityMgr,
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
			if shardContext.GetConfig().SendRawWorkflowHistory(request.Request.GetNamespace()) {
				historyBlob, continuationToken.PersistenceToken, err = api.GetRawHistory(
					ctx,
					shardContext,
					namespaceID,
					execution,
					continuationToken.FirstEventId,
					continuationToken.NextEventId,
					request.Request.GetMaximumPageSize(),
					continuationToken.PersistenceToken,
					continuationToken.TransientWorkflowTask,
					continuationToken.BranchToken,
				)
			} else {
				history, continuationToken.PersistenceToken, err = api.GetHistory(
					ctx,
					shardContext,
					namespaceID,
					execution,
					continuationToken.FirstEventId,
					continuationToken.NextEventId,
					request.Request.GetMaximumPageSize(),
					continuationToken.PersistenceToken,
					continuationToken.TransientWorkflowTask,
					continuationToken.BranchToken,
					persistenceVisibilityMgr,
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

	nextToken, err := api.SerializeHistoryToken(continuationToken)
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
	followsNextRunId := versionChecker.ClientSupportsFeature(ctx, headers.FeatureFollowsNextRunID)
	if isCloseEventOnly && !followsNextRunId && len(history.Events) > 0 {
		lastEvent := history.Events[len(history.Events)-1]
		fakeEvent, err := makeFakeContinuedAsNewEvent(ctx, lastEvent)
		if err != nil {
			return nil, err
		}
		if fakeEvent != nil {
			history.Events[len(history.Events)-1] = fakeEvent
		}
	}

	return &historyservice.GetWorkflowExecutionHistoryResponse{
		Response: &workflowservice.GetWorkflowExecutionHistoryResponse{
			History:       history,
			RawHistory:    historyBlob,
			NextPageToken: nextToken,
			Archived:      false,
		},
	}, nil
}

func makeFakeContinuedAsNewEvent(
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
