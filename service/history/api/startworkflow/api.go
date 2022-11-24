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

package startworkflow

import (
	"context"

	"github.com/google/uuid"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

func Invoke(
	ctx context.Context,
	startRequest *historyservice.StartWorkflowExecutionRequest,
	shard shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (resp *historyservice.StartWorkflowExecutionResponse, retError error) {
	namespaceEntry, err := api.GetActiveNamespace(shard, namespace.ID(startRequest.GetNamespaceId()))
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.ID()

	request := startRequest.StartRequest
	api.OverrideStartWorkflowExecutionRequest(request, metrics.HistoryStartWorkflowExecutionScope, shard, shard.GetMetricsHandler())
	err = api.ValidateStartWorkflowExecutionRequest(ctx, request, shard, namespaceEntry, "StartWorkflowExecution")
	if err != nil {
		return nil, err
	}

	workflowID := request.GetWorkflowId()
	runID := uuid.New()
	workflowContext, err := api.NewWorkflowWithSignal(
		ctx,
		shard,
		namespaceEntry,
		workflowID,
		runID.String(),
		startRequest,
		nil,
	)
	if err != nil {
		return nil, err
	}

	now := shard.GetTimeSource().Now()
	newWorkflow, newWorkflowEventsSeq, err := workflowContext.GetMutableState().CloseTransactionAsSnapshot(
		now,
		workflow.TransactionPolicyActive,
	)
	if err != nil {
		return nil, err
	}
	if len(newWorkflowEventsSeq) != 1 {
		return nil, serviceerror.NewInternal("unable to create 1st event batch")
	}

	// create as brand new
	createMode := persistence.CreateWorkflowModeBrandNew
	prevRunID := ""
	prevLastWriteVersion := int64(0)
	err = workflowContext.GetContext().CreateWorkflowExecution(
		ctx,
		now,
		createMode,
		prevRunID,
		prevLastWriteVersion,
		workflowContext.GetMutableState(),
		newWorkflow,
		newWorkflowEventsSeq,
	)
	if err == nil {
		return &historyservice.StartWorkflowExecutionResponse{
			RunId: runID.String(),
		}, nil
	}

	t, ok := err.(*persistence.CurrentWorkflowConditionFailedError)
	if !ok {
		return nil, err
	}

	// handle CurrentWorkflowConditionFailedError
	if t.RequestID == request.GetRequestId() {
		return &historyservice.StartWorkflowExecutionResponse{
			RunId: t.RunID,
		}, nil
		// delete history is expected here because duplicate start request will create history with different rid
	}

	// create as ID reuse
	prevRunID = t.RunID
	prevLastWriteVersion = t.LastWriteVersion
	if workflowContext.GetMutableState().GetCurrentVersion() < prevLastWriteVersion {
		clusterMetadata := shard.GetClusterMetadata()
		clusterName := clusterMetadata.ClusterNameForFailoverVersion(namespaceEntry.IsGlobalNamespace(), prevLastWriteVersion)
		return nil, serviceerror.NewNamespaceNotActive(
			request.GetNamespace(),
			clusterMetadata.GetCurrentClusterName(),
			clusterName,
		)
	}

	prevExecutionUpdateAction, err := api.ApplyWorkflowIDReusePolicy(
		t.RequestID,
		prevRunID,
		t.State,
		t.Status,
		workflowID,
		runID.String(),
		startRequest.StartRequest.GetWorkflowIdReusePolicy(),
	)
	if err != nil {
		return nil, err
	}

	if prevExecutionUpdateAction != nil {
		// update prev execution and create new execution in one transaction
		err := api.GetAndUpdateWorkflowWithNew(
			ctx,
			nil,
			api.BypassMutableStateConsistencyPredicate,
			definition.NewWorkflowKey(
				namespaceID.String(),
				workflowID,
				prevRunID,
			),
			prevExecutionUpdateAction,
			func() (workflow.Context, workflow.MutableState, error) {
				workflowContext, err := api.NewWorkflowWithSignal(
					ctx,
					shard,
					namespaceEntry,
					workflowID,
					runID.String(),
					startRequest,
					nil)
				if err != nil {
					return nil, nil, err
				}
				return workflowContext.GetContext(), workflowContext.GetMutableState(), nil
			},
			shard,
			workflowConsistencyChecker,
		)
		switch err {
		case nil:
			return &historyservice.StartWorkflowExecutionResponse{
				RunId: runID.String(),
			}, nil
		case consts.ErrWorkflowCompleted:
			// previous workflow already closed
			// fallthough to the logic for only creating the new workflow below
		default:
			return nil, err
		}
	}

	if err = workflowContext.GetContext().CreateWorkflowExecution(
		ctx,
		now,
		persistence.CreateWorkflowModeUpdateCurrent,
		prevRunID,
		prevLastWriteVersion,
		workflowContext.GetMutableState(),
		newWorkflow,
		newWorkflowEventsSeq,
	); err != nil {
		return nil, err
	}
	return &historyservice.StartWorkflowExecutionResponse{
		RunId: runID.String(),
	}, nil

}
