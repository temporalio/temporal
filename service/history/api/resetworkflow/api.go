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
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/locks"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/ndc"
	"go.temporal.io/server/service/history/shard"
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

	baseWorkflowLease, err := workflowConsistencyChecker.GetWorkflowLease(
		ctx,
		nil,
		definition.NewWorkflowKey(
			namespaceID.String(),
			workflowID,
			baseRunID,
		),
		locks.PriorityHigh,
	)
	if err != nil {
		return nil, err
	}
	defer func() { baseWorkflowLease.GetReleaseFn()(retError) }()

	baseMutableState := baseWorkflowLease.GetMutableState()
	if request.GetWorkflowTaskFinishEventId() <= common.FirstEventID ||
		request.GetWorkflowTaskFinishEventId() >= baseMutableState.GetNextEventID() {
		return nil, serviceerror.NewInvalidArgument("Workflow task finish ID must be > 1 && <= workflow last event ID.")
	}

	// also load the current run of the workflow, it can be different from the base runID
	currentRunID, err := workflowConsistencyChecker.GetCurrentRunID(
		ctx,
		namespaceID.String(),
		request.WorkflowExecution.GetWorkflowId(),
		locks.PriorityHigh,
	)
	if err != nil {
		return nil, err
	}
	if baseRunID == "" {
		baseRunID = currentRunID
	}

	var currentWorkflowLease api.WorkflowLease
	if currentRunID == baseRunID {
		currentWorkflowLease = baseWorkflowLease
	} else {
		currentWorkflowLease, err = workflowConsistencyChecker.GetWorkflowLease(
			ctx,
			nil,
			definition.NewWorkflowKey(
				namespaceID.String(),
				workflowID,
				currentRunID,
			),
			locks.PriorityHigh,
		)
		if err != nil {
			return nil, err
		}
		defer func() { currentWorkflowLease.GetReleaseFn()(retError) }()
	}

	// dedup by requestID
	if currentWorkflowLease.GetMutableState().GetExecutionState().CreateRequestId == request.GetRequestId() {
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
	baseWorkflow := ndc.NewWorkflow(
		shard.GetClusterMetadata(),
		baseWorkflowLease.GetContext(),
		baseWorkflowLease.GetMutableState(),
		baseWorkflowLease.GetReleaseFn(),
	)

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
		baseWorkflow,
		ndc.NewWorkflow(
			shard.GetClusterMetadata(),
			currentWorkflowLease.GetContext(),
			currentWorkflowLease.GetMutableState(),
			currentWorkflowLease.GetReleaseFn(),
		),
		request.GetReason(),
		nil,
		GetResetReapplyExcludeTypes(request.GetResetReapplyExcludeTypes(), request.GetResetReapplyType()),
	); err != nil {
		return nil, err
	}
	return &historyservice.ResetWorkflowExecutionResponse{
		RunId: resetRunID,
	}, nil
}

// GetResetReapplyExcludeTypes computes the set of requested exclude types. It
// uses the reset_reapply_exclude_types request field (a set of event types to
// exclude from reapply), as well as the deprecated reset_reapply_type request
// field (a specification of what to include).
func GetResetReapplyExcludeTypes(
	excludeTypes []enumspb.ResetReapplyExcludeType,
	includeType enumspb.ResetReapplyType,
) map[enumspb.ResetReapplyExcludeType]struct{} {
	// A client who wishes to have reapplication of all supported event types should omit the deprecated
	// reset_reapply_type field (since its default value is RESET_REAPPLY_TYPE_ALL_ELIGIBLE).
	exclude := map[enumspb.ResetReapplyExcludeType]struct{}{}
	switch includeType {
	case enumspb.RESET_REAPPLY_TYPE_SIGNAL:
		// A client sending this value of the deprecated reset_reapply_type field will not have any events other than
		// signal reapplied.
		exclude[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE] = struct{}{}
	case enumspb.RESET_REAPPLY_TYPE_NONE:
		exclude[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_SIGNAL] = struct{}{}
		exclude[enumspb.RESET_REAPPLY_EXCLUDE_TYPE_UPDATE] = struct{}{}
	case enumspb.RESET_REAPPLY_TYPE_UNSPECIFIED, enumspb.RESET_REAPPLY_TYPE_ALL_ELIGIBLE:
		// Do nothing.
	}
	for _, e := range excludeTypes {
		exclude[e] = struct{}{}
	}
	return exclude
}
