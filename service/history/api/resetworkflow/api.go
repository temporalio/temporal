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

package resetworkflow

import (
	"context"

	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/ndc"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	resetRequest *historyservice.ResetWorkflowExecutionRequest,
	shard shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (_ *historyservice.ResetWorkflowExecutionResponse, retError error) {
	namespaceID := namespace.ID(resetRequest.GetNamespaceId())
	err := api.ValidateNamespaceUUID(namespaceID)
	if err != nil {
		return nil, err
	}

	request := resetRequest.ResetRequest
	workflowID := request.WorkflowExecution.GetWorkflowId()
	baseRunID := request.WorkflowExecution.GetRunId()

	baseWFContext, err := workflowConsistencyChecker.GetWorkflowContext(
		ctx,
		nil,
		api.BypassMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(
			namespaceID.String(),
			workflowID,
			baseRunID,
		),
		workflow.LockPriorityHigh,
	)
	if err != nil {
		return nil, err
	}
	defer func() { baseWFContext.GetReleaseFn()(retError) }()

	baseMutableState := baseWFContext.GetMutableState()
	if request.GetWorkflowTaskFinishEventId() <= common.FirstEventID ||
		request.GetWorkflowTaskFinishEventId() >= baseMutableState.GetNextEventID() {
		return nil, serviceerror.NewInvalidArgument("Workflow task finish ID must be > 1 && <= workflow last event ID.")
	}

	// also load the current run of the workflow, it can be different from the base runID
	currentRunID, err := workflowConsistencyChecker.GetCurrentRunID(
		ctx,
		namespaceID.String(),
		request.WorkflowExecution.GetWorkflowId(),
		workflow.LockPriorityHigh,
	)
	if err != nil {
		return nil, err
	}
	if baseRunID == "" {
		baseRunID = currentRunID
	}

	var currentWFContext api.WorkflowContext
	if currentRunID == baseRunID {
		currentWFContext = baseWFContext
	} else {
		currentWFContext, err = workflowConsistencyChecker.GetWorkflowContext(
			ctx,
			nil,
			api.BypassMutableStateConsistencyPredicate,
			definition.NewWorkflowKey(
				namespaceID.String(),
				workflowID,
				currentRunID,
			),
			workflow.LockPriorityHigh,
		)
		if err != nil {
			return nil, err
		}
		defer func() { currentWFContext.GetReleaseFn()(retError) }()
	}

	// dedup by requestID
	if currentWFContext.GetMutableState().GetExecutionState().CreateRequestId == request.GetRequestId() {
		shard.GetLogger().Info("Duplicated reset request",
			tag.WorkflowID(workflowID),
			tag.WorkflowRunID(currentRunID),
			tag.WorkflowNamespaceID(namespaceID.String()))
		return &historyservice.ResetWorkflowExecutionResponse{
			RunId: currentRunID,
		}, nil
	}

	resetRunID := uuid.New().String()
	baseRebuildLastEventID := request.GetWorkflowTaskFinishEventId() - 1
	baseVersionHistories := baseMutableState.GetExecutionInfo().GetVersionHistories()
	baseCurrentVersionHistory, err := versionhistory.GetCurrentVersionHistory(baseVersionHistories)
	if err != nil {
		return nil, err
	}
	baseRebuildLastEventVersion, err := versionhistory.GetVersionHistoryEventVersion(baseCurrentVersionHistory, baseRebuildLastEventID)
	if err != nil {
		return nil, err
	}
	baseCurrentBranchToken := baseCurrentVersionHistory.GetBranchToken()
	baseNextEventID := baseMutableState.GetNextEventID()

	if err := ndc.NewWorkflowResetter(
		shard,
		workflowConsistencyChecker.GetWorkflowCache(),
		shard.GetLogger(),
	).ResetWorkflow(
		ctx,
		namespaceID,
		workflowID,
		baseRunID,
		baseCurrentBranchToken,
		baseRebuildLastEventID,
		baseRebuildLastEventVersion,
		baseNextEventID,
		resetRunID,
		request.GetRequestId(),
		ndc.NewWorkflow(
			shard.GetClusterMetadata(),
			currentWFContext.GetContext(),
			currentWFContext.GetMutableState(),
			currentWFContext.GetReleaseFn(),
		),
		request.GetReason(),
		nil,
		getResetReapplyExcludeTypes(request),
	); err != nil {
		return nil, err
	}
	return &historyservice.ResetWorkflowExecutionResponse{
		RunId: resetRunID,
	}, nil
}

// getResetReapplyExcludeTypes computes the set of requested exclude types. It
// uses the reset_reapply_exclude_types request field (a set of event types to
// exclude from reapply), as well as the deprecated reset_reapply_type request
// field (a specification of what to include).
func getResetReapplyExcludeTypes(request *workflowservice.ResetWorkflowExecutionRequest) []enumspb.ResetReapplyExcludeType {
	includeSpec := request.GetResetReapplyType()
	excludeTypes := request.GetResetReapplyExcludeTypes()

	excludeSet := map[enumspb.ResetReapplyExcludeType]bool{}
	switch includeSpec {
	case enumspb.RESET_REAPPLY_TYPE_SIGNAL:
		// A client sending this value of the deprecated reset_reapply_type
		// field will not have any events other than signal reapplied. We may
		// implement reapplication of event types other than signal in the
		// future; any such event type should be added as an exclusion to this
		// switch case. A client who wishes to have reapplication of all
		// supported event types should not send the deprecated
		// reset_reapply_type field (since its default value is
		// RESET_REAPPLY_TYPE_ALL_ELIGIBLE).
		// TODO (dan) exclude update
	case enumspb.RESET_REAPPLY_TYPE_NONE:
		// TODO (dan) exclude update
		excludeSet[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL] = true
	}
	for _, e := range excludeTypes {
		excludeSet[e] = true
	}

	exclude := []enumspb.ResetReapplyExcludeType{}
	for e := range excludeSet {
		exclude = append(exclude, e)
	}
	return exclude
}
