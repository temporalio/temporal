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

package multioperationworkflow

import (
	"context"

	historyservicepb "go.temporal.io/server/api/historyservice/v1"
	matchinservicepb "go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/api/startworkflow"
	"go.temporal.io/server/service/history/api/updateworkflow"
	"go.temporal.io/server/service/history/shard"
)

func Invoke(
	ctx context.Context,
	req *historyservicepb.MultiOperationWorkflowExecutionRequest,
	shardContext shard.Context,
	workflowConsistencyChecker api.WorkflowConsistencyChecker,
	tokenSerializer common.TaskTokenSerializer,
	visibilityManager manager.VisibilityManager,
	matchingClient matchinservicepb.MatchingServiceClient,
) (*historyservicepb.MultiOperationWorkflowExecutionResponse, error) {
	startReq := req.Operations[0].GetStartWorkflow()
	starter, err := startworkflow.NewStarter(
		shardContext,
		workflowConsistencyChecker,
		tokenSerializer,
		visibilityManager,
		startReq,
	)
	if err != nil {
		return nil, err
	}
	startResp, err := starter.Invoke(ctx)
	if err != nil {
		return nil, err
	}

	updateReq := req.Operations[1]
	updateResp, err := updateworkflow.Invoke(ctx, updateReq.GetUpdateWorkflow(), shardContext, workflowConsistencyChecker, matchingClient)
	if err != nil {
		return nil, err
	}

	return &historyservicepb.MultiOperationWorkflowExecutionResponse{
		Operations: []*historyservicepb.MultiOperationWorkflowExecutionResponse_Operation{
			{
				Operation: &historyservicepb.MultiOperationWorkflowExecutionResponse_Operation_StartWorkflow{
					StartWorkflow: startResp,
				},
			},
			{
				Operation: &historyservicepb.MultiOperationWorkflowExecutionResponse_Operation_UpdateWorkflow{
					UpdateWorkflow: updateResp,
				},
			},
		},
	}, nil
}
