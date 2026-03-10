package getworkflowexecutionhistory

import (
	"context"
	"errors"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/transitionhistory"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/persistence/visibility/manager"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/softassert"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	historyi "go.temporal.io/server/service/history/interfaces"
)

// appendTransientEvents queries mutable state for transient/speculative events and appends them to the response.
// This function attempts to use cached transient tasks from the initial mutable state call to avoid a second call.
// If cached tasks are nil or stale (validation fails), it falls back to querying mutable state.
// Validation of event IDs is handled by validateTransientWorkflowTaskEvents.
func appendTransientTasks(
	ctx context.Context,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	eventNotifier events.Notifier,
	namespaceID namespace.ID,
	execution *commonpb.WorkflowExecution,
	useRawHistory bool,
	history *historypb.History,
	historyBlob *[]*commonpb.DataBlob,
	cachedTransientTasks *historyspb.TransientWorkflowTaskInfo,
	nextEventID int64,
) {
	// CLI and UI clients should not receive transient events for backward compatibility
	// Check this FIRST before doing any work
	clientName, _ := headers.GetClientNameAndVersion(ctx)
	if clientName == headers.ClientNameCLI || clientName == headers.ClientNameUI {
		return
	}

	var transientWorkflowTask *historyspb.TransientWorkflowTaskInfo

	// Try cached tasks first
	if cachedTransientTasks != nil {
		// Validate cached tasks are still valid (not stale)
		if err := api.ValidateTransientWorkflowTaskEvents(nextEventID, cachedTransientTasks); err == nil {
			transientWorkflowTask = cachedTransientTasks
		}
	}

	// If no valid cache, fetch fresh from mutable state
	if transientWorkflowTask == nil {
		msResp, err := api.GetOrPollWorkflowMutableState(
			ctx,
			shardContext,
			&historyservice.GetMutableStateRequest{
				NamespaceId: namespaceID.String(),
				Execution:   execution,
			},
			workflowConsistencyChecker,
			eventNotifier,
		)
		if err != nil || msResp.GetTransientOrSpeculativeTasks() == nil {
			// Transient events don't exist or are already committed - this is OK
			// Just return without appending (events are in persisted history)
			if err != nil {
				shardContext.GetLogger().Warn("Failed to refetch transient events",
					tag.WorkflowNamespaceID(namespaceID.String()),
					tag.WorkflowID(execution.GetWorkflowId()),
					tag.WorkflowRunID(execution.GetRunId()),
					tag.Error(err))
			}
			return
		}
		transientWorkflowTask = msResp.GetTransientOrSpeculativeTasks()

		if msResp.GetWorkflowStatus() != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
			return
		}

		if err := api.ValidateTransientWorkflowTaskEvents(nextEventID, transientWorkflowTask); err != nil {
			return
		}
	}

	// Manually append transient events to the response
	if useRawHistory {
		transientEventsBlob, err := shardContext.GetPayloadSerializer().SerializeEvents(transientWorkflowTask.GetHistorySuffix())
		if err == nil {
			*historyBlob = append(*historyBlob, transientEventsBlob)
		} else {
			softassert.Fail(shardContext.GetLogger(), "GetWorkflowExecutionHistory could not serialize transient workflow tasks")
			shardContext.GetLogger().Error("Failed to serialize transient workflow tasks",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(execution.GetWorkflowId()),
				tag.WorkflowRunID(execution.GetRunId()),
				tag.Error(err),
			)
		}
	} else {
		history.Events = append(history.Events, transientWorkflowTask.GetHistorySuffix()...)
	}
}

func Invoke(
	ctx context.Context,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	versionChecker headers.VersionChecker,
	eventNotifier events.Notifier,
	request *historyservice.GetWorkflowExecutionHistoryRequest,
	persistenceVisibilityMgr manager.VisibilityManager,
) (_ *historyservice.GetWorkflowExecutionHistoryResponseWithRaw, retError error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	namespaceName := namespace.Name(request.GetRequest().GetNamespace())
	err := api.ValidateNamespaceUUID(namespaceID)
	if err != nil {
		return nil, err
	}

	isCloseEventOnly := request.Request.GetHistoryEventFilterType() == enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT

	queryMutableState := func(
		namespaceUUID namespace.ID,
		execution *commonpb.WorkflowExecution,
		expectedNextEventID int64,
		currentBranchToken []byte,
		versionHistoryItem *historyspb.VersionHistoryItem,
		versionedTransition *persistencespb.VersionedTransition,
	) (
		[]byte, // current branch token (to use to retrieve history events)
		string, // workflow run ID
		int64, // last first event ID (the event ID of the last batch of events in the history)
		int64, // last first event transaction id
		bool, // whether the workflow is running
		*historyspb.VersionHistoryItem, // version history item for the current branch
		*persistencespb.VersionedTransition, // last versioned transition
		*historyspb.TransientWorkflowTaskInfo, // transient workflow task info
		error, // error if any
	) {
		response, err := api.GetOrPollWorkflowMutableState(
			ctx,
			shardContext,
			&historyservice.GetMutableStateRequest{
				NamespaceId:         namespaceUUID.String(),
				Execution:           execution,
				ExpectedNextEventId: expectedNextEventID,
				CurrentBranchToken:  currentBranchToken,
				VersionHistoryItem:  versionHistoryItem,
				VersionedTransition: versionedTransition,
			},
			workflowConsistencyChecker,
			eventNotifier,
		)

		var branchErr *serviceerrors.CurrentBranchChanged
		if errors.As(err, &branchErr) && isCloseEventOnly {
			shardContext.GetLogger().Info("Got CurrentBranchChanged, retry with empty branch token",
				tag.WorkflowNamespaceID(namespaceUUID.String()),
				tag.WorkflowID(execution.GetWorkflowId()),
				tag.WorkflowRunID(execution.GetRunId()),
				tag.Error(err),
			)
			// if we are only querying for close event, and encounter CurrentBranchChanged error, then we retry with empty branch token to get the close event
			response, err = api.GetOrPollWorkflowMutableState(
				ctx,
				shardContext,
				&historyservice.GetMutableStateRequest{
					NamespaceId:         namespaceUUID.String(),
					Execution:           execution,
					ExpectedNextEventId: expectedNextEventID,
					CurrentBranchToken:  nil,
					VersionHistoryItem:  nil,
					VersionedTransition: nil,
				},
				workflowConsistencyChecker,
				eventNotifier,
			)
		}
		if err != nil {
			return nil, "", 0, 0, false, nil, nil, nil, err
		}

		isWorkflowRunning := response.GetWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
		currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(response.GetVersionHistories())
		if err != nil {
			return nil, "", 0, 0, false, nil, nil, nil, err
		}
		lastVersionHistoryItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
		if err != nil {
			return nil, "", 0, 0, false, nil, nil, nil, err
		}

		lastVersionedTransition := transitionhistory.LastVersionedTransition(response.GetTransitionHistory())
		return response.CurrentBranchToken,
			response.Execution.GetRunId(),
			response.GetLastFirstEventId(),
			response.GetNextEventId(),
			isWorkflowRunning,
			lastVersionHistoryItem,
			lastVersionedTransition,
			response.GetTransientOrSpeculativeTasks(),
			nil
	}

	isLongPoll := request.Request.GetWaitNewEvent()
	execution := request.Request.Execution
	var continuationToken *tokenspb.HistoryContinuation

	var runID string
	lastFirstEventID := common.FirstEventID
	var nextEventID int64
	var isWorkflowRunning bool
	var cachedTransientTasks *historyspb.TransientWorkflowTaskInfo

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

		// Set isWorkflowRunning from continuation token for pagination
		isWorkflowRunning = continuationToken.IsWorkflowRunning

		// we need to update the current next event ID and whether workflow is running
		if len(continuationToken.PersistenceToken) == 0 {
			if !isCloseEventOnly {
				queryNextEventID = continuationToken.GetNextEventId()
			}
			continuationToken.BranchToken, _, lastFirstEventID, nextEventID, isWorkflowRunning, continuationToken.VersionHistoryItem, continuationToken.VersionedTransition, cachedTransientTasks, err =
				queryMutableState(namespaceID, execution, queryNextEventID, continuationToken.BranchToken, continuationToken.VersionHistoryItem, continuationToken.VersionedTransition)
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
		continuationToken.BranchToken, runID, lastFirstEventID, nextEventID, isWorkflowRunning, continuationToken.VersionHistoryItem, continuationToken.VersionedTransition, cachedTransientTasks, err =
			queryMutableState(namespaceID, execution, queryNextEventID, nil, nil, nil)
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
		var dataLossErr *serviceerror.DataLoss
		if errors.As(retError, &dataLossErr) {
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
	config := shardContext.GetConfig()
	sendRawHistoryBetweenInternalServices := config.SendRawHistoryBetweenInternalServices()
	sendRawWorkflowHistoryForNamespace := config.SendRawWorkflowHistory(request.Request.GetNamespace())
	if isCloseEventOnly {
		if !isWorkflowRunning {
			if sendRawWorkflowHistoryForNamespace || sendRawHistoryBetweenInternalServices {
				historyBlob, _, err = api.GetRawHistory(
					ctx,
					shardContext,
					namespaceName,
					namespaceID,
					execution,
					lastFirstEventID,
					nextEventID,
					request.Request.GetMaximumPageSize(),
					nil,
					nil,
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
					namespaceName,
					namespaceID,
					execution,
					lastFirstEventID,
					nextEventID,
					request.Request.GetMaximumPageSize(),
					nil,
					nil,
					continuationToken.BranchToken,
					persistenceVisibilityMgr,
				)
				if err != nil {
					return nil, err
				}
				// GetHistory func will not return empty history. Log workflow details if that is not the case
				if len(history.Events) == 0 {
					dataLossErr := softassert.UnexpectedDataLoss(
						shardContext.GetLogger(),
						"no events in workflow history",
						nil,
						tag.WorkflowNamespaceID(namespaceID.String()),
						tag.WorkflowNamespace(request.GetRequest().GetNamespace()),
						tag.WorkflowID(execution.GetWorkflowId()),
						tag.WorkflowRunID(execution.GetRunId()),
					)
					// Emit dataloss metric
					if shardContext.GetConfig().EnableDataLossMetrics() {
						persistence.EmitDataLossMetric(
							shardContext.GetMetricsHandler(),
							request.GetRequest().GetNamespace(),
							execution.GetWorkflowId(),
							execution.GetRunId(),
							"GetWorkflowExecutionHistory",
							dataLossErr,
						)
					}
					return nil, dataLossErr
				}
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
			if sendRawWorkflowHistoryForNamespace || sendRawHistoryBetweenInternalServices {
				historyBlob, continuationToken.PersistenceToken, err = api.GetRawHistory(
					ctx,
					shardContext,
					namespaceName,
					namespaceID,
					execution,
					continuationToken.FirstEventId,
					continuationToken.NextEventId,
					request.Request.GetMaximumPageSize(),
					continuationToken.PersistenceToken,
					nil,
					continuationToken.BranchToken,
				)
			} else {
				history, continuationToken.PersistenceToken, err = api.GetHistory(
					ctx,
					shardContext,
					namespaceName,
					namespaceID,
					execution,
					continuationToken.FirstEventId,
					continuationToken.NextEventId,
					request.Request.GetMaximumPageSize(),
					continuationToken.PersistenceToken,
					nil,
					continuationToken.BranchToken,
					persistenceVisibilityMgr,
				)
			}

			if err != nil {
				return nil, err
			}

			// Query and append transient/speculative tasks if on last page
			if len(continuationToken.PersistenceToken) == 0 {
				appendTransientTasks(
					ctx,
					shardContext,
					workflowConsistencyChecker,
					eventNotifier,
					namespaceID,
					execution,
					sendRawWorkflowHistoryForNamespace || sendRawHistoryBetweenInternalServices,
					history,
					&historyBlob,
					cachedTransientTasks,
					continuationToken.NextEventId,
				)
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

	// if SendRawHistoryBetweenInternalServices is enabled, we do this check in frontend service
	if len(history.Events) > 0 {
		err = api.FixFollowEvents(ctx, versionChecker, isCloseEventOnly, history)
		if err != nil {
			return nil, err
		}
	}

	var rawHistory [][]byte
	// if sendRawHistoryBetweenInternalServices is true and SendRawWorkflowHistory is not enabled for this namespace,
	// send history in raw format in History field of historyservice.GetWorkflowExecutionHistoryResponseWithRaw.
	// If SendRawWorkflowHistory is enabled for this namespace, raw history will be appended to RawHistory field in
	// workflowservice.GetWorkflowExecutionHistoryResponse.
	if sendRawHistoryBetweenInternalServices && !sendRawWorkflowHistoryForNamespace {
		rawHistory = make([][]byte, 0, len(historyBlob))
		for _, blob := range historyBlob {
			rawHistory = append(rawHistory, blob.Data)
		}
		historyBlob = nil
	}
	return &historyservice.GetWorkflowExecutionHistoryResponseWithRaw{
		Response: &workflowservice.GetWorkflowExecutionHistoryResponse{
			History:       history,
			RawHistory:    historyBlob,
			NextPageToken: nextToken,
			Archived:      false,
		},

		History: rawHistory,
	}, nil
}
