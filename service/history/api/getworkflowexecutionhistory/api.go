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

	isCloseEventOnly := request.GetRequest().GetHistoryEventFilterType() == enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT

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
		versionedTransition *persistencespb.VersionedTransition,
	) ([]byte, string, int64, int64, bool, *historyspb.VersionHistoryItem, *persistencespb.VersionedTransition, error) {
		response, err := api.GetOrPollWorkflowMutableState(
			ctx,
			shardContext,
			historyservice.GetMutableStateRequest_builder{
				NamespaceId:         namespaceUUID.String(),
				Execution:           execution,
				ExpectedNextEventId: expectedNextEventID,
				CurrentBranchToken:  currentBranchToken,
				VersionHistoryItem:  versionHistoryItem,
				VersionedTransition: versionedTransition,
			}.Build(),
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
				historyservice.GetMutableStateRequest_builder{
					NamespaceId:         namespaceUUID.String(),
					Execution:           execution,
					ExpectedNextEventId: expectedNextEventID,
					CurrentBranchToken:  nil,
					VersionHistoryItem:  nil,
					VersionedTransition: nil,
				}.Build(),
				workflowConsistencyChecker,
				eventNotifier,
			)
		}
		if err != nil {
			return nil, "", 0, 0, false, nil, nil, err
		}

		isWorkflowRunning := response.GetWorkflowStatus() == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
		currentVersionHistory, err := versionhistory.GetCurrentVersionHistory(response.GetVersionHistories())
		if err != nil {
			return nil, "", 0, 0, false, nil, nil, err
		}
		lastVersionHistoryItem, err := versionhistory.GetLastVersionHistoryItem(currentVersionHistory)
		if err != nil {
			return nil, "", 0, 0, false, nil, nil, err
		}

		lastVersionedTransition := transitionhistory.LastVersionedTransition(response.GetTransitionHistory())
		return response.GetCurrentBranchToken(),
			response.GetExecution().GetRunId(),
			response.GetLastFirstEventId(),
			response.GetNextEventId(),
			isWorkflowRunning,
			lastVersionHistoryItem,
			lastVersionedTransition,
			nil
	}

	isLongPoll := request.GetRequest().GetWaitNewEvent()
	execution := request.GetRequest().GetExecution()
	var continuationToken *tokenspb.HistoryContinuation

	var runID string
	lastFirstEventID := common.FirstEventID
	var nextEventID int64
	var isWorkflowRunning bool

	// process the token for paging
	queryNextEventID := common.EndEventID
	if len(request.GetRequest().GetNextPageToken()) != 0 {
		continuationToken, err = api.DeserializeHistoryToken(request.GetRequest().GetNextPageToken())
		if err != nil {
			return nil, consts.ErrInvalidNextPageToken
		}
		if execution.GetRunId() != "" && execution.GetRunId() != continuationToken.GetRunId() {
			return nil, consts.ErrNextPageTokenRunIDMismatch
		}

		execution.SetRunId(continuationToken.GetRunId())

		// we need to update the current next event ID and whether workflow is running
		if len(continuationToken.GetPersistenceToken()) == 0 && isLongPoll && continuationToken.GetIsWorkflowRunning() {
			if !isCloseEventOnly {
				queryNextEventID = continuationToken.GetNextEventId()
			}
			var branchToken []byte
			var versionHistoryItem *historyspb.VersionHistoryItem
			var versionedTransition *persistencespb.VersionedTransition
			branchToken, _, lastFirstEventID, nextEventID, isWorkflowRunning, versionHistoryItem, versionedTransition, err =
				queryHistory(namespaceID, execution, queryNextEventID, continuationToken.GetBranchToken(), continuationToken.GetVersionHistoryItem(), continuationToken.GetVersionedTransition())
			if err != nil {
				return nil, err
			}
			continuationToken.SetBranchToken(branchToken)
			continuationToken.SetVersionHistoryItem(versionHistoryItem)
			continuationToken.SetVersionedTransition(versionedTransition)
			continuationToken.SetFirstEventId(continuationToken.GetNextEventId())
			continuationToken.SetNextEventId(nextEventID)
			continuationToken.SetIsWorkflowRunning(isWorkflowRunning)
		}
	} else {
		continuationToken = &tokenspb.HistoryContinuation{}
		if !isCloseEventOnly {
			queryNextEventID = common.FirstEventID
		}
		var branchToken []byte
		var versionHistoryItem *historyspb.VersionHistoryItem
		var versionedTransition *persistencespb.VersionedTransition
		branchToken, runID, lastFirstEventID, nextEventID, isWorkflowRunning, versionHistoryItem, versionedTransition, err =
			queryHistory(namespaceID, execution, queryNextEventID, nil, nil, nil)
		if err != nil {
			return nil, err
		}
		continuationToken.SetBranchToken(branchToken)
		continuationToken.SetVersionHistoryItem(versionHistoryItem)
		continuationToken.SetVersionedTransition(versionedTransition)

		execution.SetRunId(runID)

		continuationToken.SetRunId(runID)
		continuationToken.SetFirstEventId(common.FirstEventID)
		continuationToken.SetNextEventId(nextEventID)
		continuationToken.SetIsWorkflowRunning(isWorkflowRunning)
		continuationToken.SetPersistenceToken(nil)
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
	history.SetEvents([]*historypb.HistoryEvent{})
	var historyBlob []*commonpb.DataBlob
	config := shardContext.GetConfig()
	sendRawHistoryBetweenInternalServices := config.SendRawHistoryBetweenInternalServices()
	sendRawWorkflowHistoryForNamespace := config.SendRawWorkflowHistory(request.GetRequest().GetNamespace())
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
					request.GetRequest().GetMaximumPageSize(),
					nil,
					continuationToken.GetTransientWorkflowTask(),
					continuationToken.GetBranchToken(),
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
					request.GetRequest().GetMaximumPageSize(),
					nil,
					continuationToken.GetTransientWorkflowTask(),
					continuationToken.GetBranchToken(),
					persistenceVisibilityMgr,
				)
				if err != nil {
					return nil, err
				}
				// GetHistory func will not return empty history. Log workflow details if that is not the case
				if len(history.GetEvents()) == 0 {
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
				history.SetEvents(history.GetEvents()[len(history.GetEvents())-1 : len(history.GetEvents())])
			}
			continuationToken = nil
		} else if isLongPoll {
			// set the persistence token to be nil so next time we will query history for updates
			continuationToken.SetPersistenceToken(nil)
		} else {
			continuationToken = nil
		}
	} else {
		// return all events
		if continuationToken.GetFirstEventId() >= continuationToken.GetNextEventId() {
			// currently there is no new event
			history.SetEvents([]*historypb.HistoryEvent{})
			if !isWorkflowRunning {
				continuationToken = nil
			}
		} else {
			var persistenceToken []byte
			if sendRawWorkflowHistoryForNamespace || sendRawHistoryBetweenInternalServices {
				historyBlob, persistenceToken, err = api.GetRawHistory(
					ctx,
					shardContext,
					namespaceName,
					namespaceID,
					execution,
					continuationToken.GetFirstEventId(),
					continuationToken.GetNextEventId(),
					request.GetRequest().GetMaximumPageSize(),
					continuationToken.GetPersistenceToken(),
					continuationToken.GetTransientWorkflowTask(),
					continuationToken.GetBranchToken(),
				)
			} else {
				history, persistenceToken, err = api.GetHistory(
					ctx,
					shardContext,
					namespaceName,
					namespaceID,
					execution,
					continuationToken.GetFirstEventId(),
					continuationToken.GetNextEventId(),
					request.GetRequest().GetMaximumPageSize(),
					continuationToken.GetPersistenceToken(),
					continuationToken.GetTransientWorkflowTask(),
					continuationToken.GetBranchToken(),
					persistenceVisibilityMgr,
				)
			}
			continuationToken.SetPersistenceToken(persistenceToken)

			if err != nil {
				return nil, err
			}

			// here, for long pull on history events, we need to intercept the paging token from cassandra
			// and do something clever
			if len(continuationToken.GetPersistenceToken()) == 0 && (!continuationToken.GetIsWorkflowRunning() || !isLongPoll) {
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
	if len(history.GetEvents()) > 0 {
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
			rawHistory = append(rawHistory, blob.GetData())
		}
		historyBlob = nil
	}
	return historyservice.GetWorkflowExecutionHistoryResponseWithRaw_builder{
		Response: workflowservice.GetWorkflowExecutionHistoryResponse_builder{
			History:       history,
			RawHistory:    historyBlob,
			NextPageToken: nextToken,
			Archived:      false,
		}.Build(),

		History: rawHistory,
	}.Build(), nil
}
