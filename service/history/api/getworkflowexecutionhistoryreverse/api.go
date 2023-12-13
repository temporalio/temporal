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
	"go.temporal.io/server/service/history/shard"
)

func Invoke(
	ctx context.Context,
	shardContext shard.Context,
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

	req := request.Request
	execution := req.Execution
	var continuationToken *tokenspb.HistoryContinuation

	var runID string
	var lastFirstTxnID int64

	if req.NextPageToken == nil {
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
		continuationToken, err = api.DeserializeHistoryToken(req.NextPageToken)
		if err != nil {
			return nil, consts.ErrInvalidNextPageToken
		}
		if execution.GetRunId() != "" && execution.GetRunId() != continuationToken.GetRunId() {
			return nil, consts.ErrNextPageTokenRunIDMismatch
		}

		execution.RunId = continuationToken.GetRunId()
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
	history.Events = []*historypb.HistoryEvent{}
	// return all events
	history, continuationToken.PersistenceToken, continuationToken.NextEventId, err = api.GetHistoryReverse(
		ctx,
		shardContext,
		namespaceID,
		execution,
		continuationToken.NextEventId,
		lastFirstTxnID,
		req.GetMaximumPageSize(),
		continuationToken.PersistenceToken,
		continuationToken.BranchToken,
		persistenceVisibilityMgr,
	)

	if err != nil {
		return nil, err
	}

	if continuationToken.NextEventId < continuationToken.FirstEventId {
		continuationToken = nil
	}

	nextToken, err := api.SerializeHistoryToken(continuationToken)
	if err != nil {
		return nil, err
	}

	return &historyservice.GetWorkflowExecutionHistoryReverseResponse{
		Response: &workflowservice.GetWorkflowExecutionHistoryReverseResponse{
			History:       history,
			NextPageToken: nextToken,
		},
	}, nil
}
