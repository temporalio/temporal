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

package api

import (
	commonpb "go.temporal.io/api/common/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/historyservice/v1"
	workflowspb "go.temporal.io/server/api/workflow/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

func NewWorkflowWithSignal(
	shard shard.Context,
	namespaceEntry *namespace.Namespace,
	execution commonpb.WorkflowExecution,
	startRequest *historyservice.StartWorkflowExecutionRequest,
	signalWithStartRequest *workflowservice.SignalWithStartWorkflowExecutionRequest,
) (workflow.Context, workflow.MutableState, error) {
	newMutableState, err := CreateMutableState(shard, namespaceEntry, execution.GetRunId())
	if err != nil {
		return nil, nil, err
	}

	startEvent, err := newMutableState.AddWorkflowExecutionStartedEvent(
		execution,
		startRequest,
	)
	if err != nil {
		return nil, nil, err
	}

	if signalWithStartRequest != nil {
		if signalWithStartRequest.GetRequestId() != "" {
			newMutableState.AddSignalRequested(signalWithStartRequest.GetRequestId())
		}
		if _, err := newMutableState.AddWorkflowExecutionSignaled(
			signalWithStartRequest.GetSignalName(),
			signalWithStartRequest.GetSignalInput(),
			signalWithStartRequest.GetIdentity(),
			signalWithStartRequest.GetHeader(),
		); err != nil {
			return nil, nil, err
		}
	}

	// Generate first workflow task event if not child WF and no first workflow task backoff
	if err := GenerateFirstWorkflowTask(
		newMutableState,
		startRequest.ParentExecutionInfo,
		startEvent,
	); err != nil {
		return nil, nil, err
	}

	newWorkflowContext := workflow.NewContext(
		shard,
		definition.NewWorkflowKey(
			namespaceEntry.ID().String(),
			execution.GetWorkflowId(),
			execution.GetRunId(),
		),
		shard.GetLogger(),
	)
	return newWorkflowContext, newMutableState, nil
}

func CreateMutableState(
	shard shard.Context,
	namespaceEntry *namespace.Namespace,
	runID string,
) (workflow.MutableState, error) {
	newMutableState := workflow.NewMutableState(
		shard,
		shard.GetEventsCache(),
		shard.GetLogger(),
		namespaceEntry,
		shard.GetTimeSource().Now(),
	)
	if err := newMutableState.SetHistoryTree(runID); err != nil {
		return nil, err
	}
	return newMutableState, nil
}

func GenerateFirstWorkflowTask(
	mutableState workflow.MutableState,
	parentInfo *workflowspb.ParentExecutionInfo,
	startEvent *historypb.HistoryEvent,
) error {

	if parentInfo == nil {
		// WorkflowTask is only created when it is not a Child Workflow and no backoff is needed
		if err := mutableState.AddFirstWorkflowTaskScheduled(
			startEvent,
		); err != nil {
			return err
		}
	}
	return nil
}
