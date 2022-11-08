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

package signalwithstartworkflow

import (
	"context"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/shard"
)

func Invoke(
	ctx context.Context,
	signalWithStartRequest *historyservice.SignalWithStartWorkflowExecutionRequest,
	shard shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
) (_ *historyservice.SignalWithStartWorkflowExecutionResponse, retError error) {
	namespaceEntry, err := api.GetActiveNamespace(shard, namespace.ID(signalWithStartRequest.GetNamespaceId()))
	if err != nil {
		return nil, err
	}
	namespaceID := namespaceEntry.ID()

	var currentWorkflowContext api.WorkflowContext
	currentWorkflowContext, err = workflowConsistencyChecker.GetWorkflowContext(
		ctx,
		nil,
		api.BypassMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(
			string(namespaceID),
			signalWithStartRequest.SignalWithStartRequest.WorkflowId,
			"",
		),
	)
	switch err.(type) {
	case nil:
		defer func() { currentWorkflowContext.GetReleaseFn()(retError) }()
	case *serviceerror.NotFound:
		currentWorkflowContext = nil
	default:
		return nil, err
	}

	// Start workflow and signal
	startRequest := ConvertToStartRequest(
		namespaceID,
		signalWithStartRequest.SignalWithStartRequest,
		shard.GetTimeSource().Now(),
	)
	request := startRequest.StartRequest
	api.OverrideStartWorkflowExecutionRequest(request, metrics.HistorySignalWithStartWorkflowExecutionScope, shard, shard.GetMetricsHandler())
	err = api.ValidateStartWorkflowExecutionRequest(ctx, request, shard, namespaceEntry, "SignalWithStartWorkflowExecution")
	if err != nil {
		return nil, err
	}
	runID, err := SignalWithStartWorkflow(
		ctx,
		shard,
		namespaceEntry,
		currentWorkflowContext,
		startRequest,
		signalWithStartRequest.SignalWithStartRequest,
	)
	if err != nil {
		return nil, err
	}
	return &historyservice.SignalWithStartWorkflowExecutionResponse{
		RunId: runID,
	}, nil
}
