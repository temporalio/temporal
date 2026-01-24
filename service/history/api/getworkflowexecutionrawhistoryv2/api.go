package getworkflowexecutionrawhistoryv2

import (
	"context"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/rpc/interceptor"
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
	request *historyservice.GetWorkflowExecutionRawHistoryV2Request,
) (_ *historyservice.GetWorkflowExecutionRawHistoryV2Response, retError error) {
	ns, err := shardContext.GetNamespaceRegistry().GetNamespaceByID(namespace.ID(request.GetNamespaceId()))
	if err != nil {
		return nil, err
	}

	req := request.GetRequest()
	execution := req.GetExecution()
	var pageToken *tokenspb.RawHistoryContinuation
	var targetVersionHistory *historyspb.VersionHistory
	if len(req.GetNextPageToken()) == 0 {
		response, err := api.GetOrPollWorkflowMutableState(
			ctx,
			shardContext,
			historyservice.GetMutableStateRequest_builder{
				NamespaceId: ns.ID().String(),
				Execution:   execution,
			}.Build(),
			workflowConsistencyChecker,
			eventNotifier,
		)
		if err != nil {
			return nil, err
		}

		targetVersionHistory, err = SetRequestDefaultValueAndGetTargetVersionHistory(
			request,
			response.GetVersionHistories(),
		)
		if err != nil {
			return nil, err
		}

		pageToken = api.GeneratePaginationTokenV2Request(request, response.GetVersionHistories())
	} else {
		pageToken, err = api.DeserializeRawHistoryToken(req.GetNextPageToken())
		if err != nil {
			return nil, err
		}
		versionHistories := pageToken.GetVersionHistories()
		if versionHistories == nil {
			return nil, consts.ErrInvalidVersionHistories
		}
		targetVersionHistory, err = SetRequestDefaultValueAndGetTargetVersionHistory(
			request,
			versionHistories,
		)
		if err != nil {
			return nil, err
		}
	}

	if err := api.ValidatePaginationTokenV2Request(
		request,
		pageToken,
	); err != nil {
		return nil, err
	}

	if pageToken.GetStartEventId()+1 == pageToken.GetEndEventId() {
		// API is exclusive-exclusive. Return empty response here.
		return historyservice.GetWorkflowExecutionRawHistoryV2Response_builder{
			Response: adminservice.GetWorkflowExecutionRawHistoryV2Response_builder{
				HistoryBatches: []*commonpb.DataBlob{},
				NextPageToken:  nil, // no further pagination
				VersionHistory: targetVersionHistory,
			}.Build(),
		}.Build(), nil
	}
	pageSize := int(req.GetMaximumPageSize())
	shardID := common.WorkflowIDToHistoryShard(
		ns.ID().String(),
		execution.GetWorkflowId(),
		shardContext.GetConfig().NumberOfShards,
	)
	rawHistoryResponse, err := shardContext.GetExecutionManager().ReadRawHistoryBranch(ctx, &persistence.ReadHistoryBranchRequest{
		BranchToken: targetVersionHistory.GetBranchToken(),
		// GetWorkflowExecutionRawHistoryV2 is exclusive exclusive.
		// ReadRawHistoryBranch is inclusive exclusive.
		MinEventID:    pageToken.GetStartEventId() + 1,
		MaxEventID:    pageToken.GetEndEventId(),
		PageSize:      pageSize,
		NextPageToken: pageToken.GetPersistenceToken(),
		ShardID:       shardID,
	})
	if err != nil {
		if _, isNotFound := err.(*serviceerror.NotFound); isNotFound {
			// when no events can be returned from DB, DB layer will return
			// EntityNotExistsError, this API shall return empty response
			return historyservice.GetWorkflowExecutionRawHistoryV2Response_builder{
				Response: adminservice.GetWorkflowExecutionRawHistoryV2Response_builder{
					HistoryBatches: []*commonpb.DataBlob{},
					NextPageToken:  nil, // no further pagination
					VersionHistory: targetVersionHistory,
				}.Build(),
			}.Build(), nil
		}
		return nil, err
	}

	pageToken.SetPersistenceToken(rawHistoryResponse.NextPageToken)
	size := rawHistoryResponse.Size
	metricsHandler := interceptor.GetMetricsHandlerFromContext(ctx, shardContext.GetLogger()).WithTags(metrics.OperationTag(metrics.HistoryGetWorkflowExecutionRawHistoryV2Scope))
	metrics.HistorySize.With(metricsHandler).Record(
		int64(size),
		metrics.NamespaceTag(ns.Name().String()),
		metrics.OperationTag(metrics.AdminGetWorkflowExecutionRawHistoryV2Scope),
	)

	result :=
		adminservice.GetWorkflowExecutionRawHistoryV2Response_builder{
			HistoryBatches: rawHistoryResponse.HistoryEventBlobs,
			VersionHistory: targetVersionHistory,
			HistoryNodeIds: rawHistoryResponse.NodeIDs,
		}.Build()
	if len(pageToken.GetPersistenceToken()) == 0 {
		result.SetNextPageToken(nil)
	} else {
		nextPageToken, err := api.SerializeRawHistoryToken(pageToken)
		if err != nil {
			return nil, err
		}
		result.SetNextPageToken(nextPageToken)
	}

	return historyservice.GetWorkflowExecutionRawHistoryV2Response_builder{
		Response: result,
	}.Build(), nil
}

func SetRequestDefaultValueAndGetTargetVersionHistory(
	request *historyservice.GetWorkflowExecutionRawHistoryV2Request,
	versionHistories *historyspb.VersionHistories,
) (*historyspb.VersionHistory, error) {

	req := request.GetRequest()

	targetBranch, err := versionhistory.GetCurrentVersionHistory(versionHistories)
	if err != nil {
		return nil, err
	}
	firstItem, err := versionhistory.GetFirstVersionHistoryItem(targetBranch)
	if err != nil {
		return nil, err
	}
	lastItem, err := versionhistory.GetLastVersionHistoryItem(targetBranch)
	if err != nil {
		return nil, err
	}

	if req.GetStartEventId() == common.EmptyEventID || req.GetStartEventVersion() == common.EmptyVersion {
		// If start event is not set, get the events from the first event
		// As the API is exclusive-exclusive, use first event id - 1 here
		req.SetStartEventId(common.FirstEventID - 1)
		req.SetStartEventVersion(firstItem.GetVersion())
	}
	if req.GetEndEventId() == common.EmptyEventID || req.GetEndEventVersion() == common.EmptyVersion {
		// If end event is not set, get the events until the end event
		// As the API is exclusive-exclusive, use end event id + 1 here
		req.SetEndEventId(lastItem.GetEventId() + 1)
		req.SetEndEventVersion(lastItem.GetVersion())
	}

	if req.GetStartEventId() < 0 {
		return nil, consts.ErrInvalidFirstNextEventCombination
	}

	// get branch based on the end event if end event is defined in the request
	if req.GetEndEventId() == lastItem.GetEventId()+1 &&
		req.GetEndEventVersion() == lastItem.GetVersion() {
		// this is a special case, target branch remains the same
	} else {
		endItem := versionhistory.NewVersionHistoryItem(req.GetEndEventId(), req.GetEndEventVersion())
		idx, err := versionhistory.FindFirstVersionHistoryIndexByVersionHistoryItem(versionHistories, endItem)
		if err != nil {
			return nil, err
		}

		targetBranch, err = versionhistory.GetVersionHistory(versionHistories, idx)
		if err != nil {
			return nil, err
		}
	}

	startItem := versionhistory.NewVersionHistoryItem(req.GetStartEventId(), req.GetStartEventVersion())
	// If the request start event is defined. The start event may be on a different branch as current branch.
	// We need to find the LCA of the start event and the current branch.
	if req.GetStartEventId() == common.FirstEventID-1 &&
		req.GetStartEventVersion() == firstItem.GetVersion() {
		// this is a special case, start event is on the same branch as target branch
	} else {
		if !versionhistory.ContainsVersionHistoryItem(targetBranch, startItem) {
			idx, err := versionhistory.FindFirstVersionHistoryIndexByVersionHistoryItem(versionHistories, startItem)
			if err != nil {
				return nil, err
			}
			startBranch, err := versionhistory.GetVersionHistory(versionHistories, idx)
			if err != nil {
				return nil, err
			}
			startItem, err = versionhistory.FindLCAVersionHistoryItem(targetBranch, startBranch)
			if err != nil {
				return nil, err
			}
			req.SetStartEventId(startItem.GetEventId())
			req.SetStartEventVersion(startItem.GetVersion())
		}
	}

	return targetBranch, nil
}
