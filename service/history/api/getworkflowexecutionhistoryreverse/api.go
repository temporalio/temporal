package getworkflowexecutionhistoryreverse

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/events"
	historyi "go.temporal.io/server/service/history/interfaces"
)

func Invoke(
	ctx context.Context,
	shardContext historyi.ShardContext,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	eventNotifier events.Notifier,
	request *historyservice.GetWorkflowExecutionHistoryReverseRequest,
	persistenceVisibilityMgr manager.VisibilityManager,
) (_ *historyservice.GetWorkflowExecutionHistoryReverseResponse, retError error) {
	namespaceID := namespace.ID(request.GetNamespaceId())
	err := api.ValidateNamespaceUUID(namespaceID)
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
		response, err := api.GetOrPollWorkflowMutableState(
			ctx,
			shardContext,
			historyservice.GetMutableStateRequest_builder{
				NamespaceId:         namespaceUUID.String(),
				Execution:           execution,
				ExpectedNextEventId: expectedNextEventID,
				CurrentBranchToken:  currentBranchToken,
				VersionHistoryItem:  versionHistoryItem,
			}.Build(),
			workflowConsistencyChecker,
			eventNotifier,
		)
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
		return response.GetCurrentBranchToken(),
			response.GetExecution().GetRunId(),
			response.GetLastFirstEventTxnId(),
			lastVersionHistoryItem,
			nil
	}

	req := request.GetRequest()
	execution := req.GetExecution()
	var continuationToken *tokenspb.HistoryContinuation

	var runID string
	var lastFirstTxnID int64

	if len(req.GetNextPageToken()) == 0 {
		continuationToken = &tokenspb.HistoryContinuation{}
		var branchToken []byte
		var versionHistoryItem *historyspb.VersionHistoryItem
		branchToken, runID, lastFirstTxnID, versionHistoryItem, err =
			queryMutableState(namespaceID, execution, common.FirstEventID, nil, nil)
		if err != nil {
			return nil, err
		}
		continuationToken.SetBranchToken(branchToken)
		continuationToken.SetVersionHistoryItem(versionHistoryItem)

		execution.SetRunId(runID)
		continuationToken.SetRunId(runID)
		continuationToken.SetFirstEventId(common.FirstEventID)
		continuationToken.SetNextEventId(common.EmptyEventID)
		continuationToken.SetPersistenceToken(nil)
	} else {
		continuationToken, err = api.DeserializeHistoryToken(req.GetNextPageToken())
		if err != nil {
			return nil, consts.ErrInvalidNextPageToken
		}
		if execution.GetRunId() != "" && execution.GetRunId() != continuationToken.GetRunId() {
			return nil, consts.ErrNextPageTokenRunIDMismatch
		}

		execution.SetRunId(continuationToken.GetRunId())
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
				namespaceID.String(),
				execution.GetWorkflowId(),
				execution.GetRunId(),
			)
		}
	}()

	history := &historypb.History{}
	history.SetEvents([]*historypb.HistoryEvent{})
	// return all events
	var persistenceToken []byte
	var nextEventId int64
	history, persistenceToken, nextEventId, err = api.GetHistoryReverse(
		ctx,
		shardContext,
		namespaceID,
		execution,
		continuationToken.GetNextEventId(),
		lastFirstTxnID,
		req.GetMaximumPageSize(),
		continuationToken.GetPersistenceToken(),
		continuationToken.GetBranchToken(),
		persistenceVisibilityMgr,
	)
	continuationToken.SetPersistenceToken(persistenceToken)
	continuationToken.SetNextEventId(nextEventId)

	if err != nil {
		return nil, err
	}

	if continuationToken.GetNextEventId() < continuationToken.GetFirstEventId() {
		continuationToken = nil
	}

	nextToken, err := api.SerializeHistoryToken(continuationToken)
	if err != nil {
		return nil, err
	}

	return historyservice.GetWorkflowExecutionHistoryReverseResponse_builder{
		Response: workflowservice.GetWorkflowExecutionHistoryReverseResponse_builder{
			History:       history,
			NextPageToken: nextToken,
		}.Build(),
	}.Build(), nil
}
