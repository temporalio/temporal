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

package tests

import (
	"context"
	"time"

	"github.com/google/uuid"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
)

func (s *clientIntegrationSuite) TestAdminBackfillMutableState() {

	workflowFn := func(ctx workflow.Context) error {
		var randomUUID string
		err := workflow.SideEffect(
			ctx,
			func(workflow.Context) interface{} { return uuid.New().String() },
		).Get(&randomUUID)
		s.NoError(err)

		_ = workflow.Sleep(ctx, 10*time.Minute)

		return nil
	}

	s.worker.RegisterWorkflow(workflowFn)

	workflowID := "integration-admin-backfill-mutable-state-test"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          s.taskQueue,
		WorkflowRunTimeout: 20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	workflowRun, err := s.sdkClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)
	baseRunID := workflowRun.GetRunID()

	// there are total 6 events, 3 state transitions
	//  1. WorkflowExecutionStarted
	//  2. WorkflowTaskScheduled
	//
	//  3. WorkflowTaskStarted
	//
	//  4. WorkflowTaskCompleted
	//  5. MarkerRecord
	//  6. TimerStarted

	var responseBase *adminservice.DescribeMutableStateResponse
	for {
		responseBase, err = s.adminClient.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      baseRunID,
			},
		})
		s.NoError(err)
		if responseBase.DatabaseMutableState.ExecutionInfo.StateTransitionCount == 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	baseCurrentVersionHistory, err := versionhistory.GetCurrentVersionHistory(responseBase.DatabaseMutableState.ExecutionInfo.VersionHistories)
	s.NoError(err)
	baseBranchToken := baseCurrentVersionHistory.BranchToken

	shardID := common.WorkflowIDToHistoryShard(
		responseBase.DatabaseMutableState.ExecutionInfo.NamespaceId,
		responseBase.DatabaseMutableState.ExecutionInfo.WorkflowId,
		s.testClusterConfig.HistoryConfig.NumHistoryShards,
	)
	err = s.testCluster.GetExecutionManager().DeleteCurrentWorkflowExecution(context.Background(), &persistence.DeleteCurrentWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: responseBase.DatabaseMutableState.ExecutionInfo.NamespaceId,
		WorkflowID:  responseBase.DatabaseMutableState.ExecutionInfo.WorkflowId,
		RunID:       responseBase.DatabaseMutableState.ExecutionState.RunId,
	})
	s.NoError(err)
	err = s.testCluster.GetExecutionManager().DeleteWorkflowExecution(context.Background(), &persistence.DeleteWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: responseBase.DatabaseMutableState.ExecutionInfo.NamespaceId,
		WorkflowID:  responseBase.DatabaseMutableState.ExecutionInfo.WorkflowId,
		RunID:       responseBase.DatabaseMutableState.ExecutionState.RunId,
	})
	s.NoError(err)
	_, err = s.adminClient.CloseShard(context.Background(), &adminservice.CloseShardRequest{
		ShardId: shardID,
	})
	s.NoError(err)
	_, err = s.adminClient.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      baseRunID,
		},
	})
	s.IsType(serviceerror.NewNotFound(""), err)

	newRunID := uuid.New().String()
	_, err = s.adminClient.RebuildMutableState(ctx, &adminservice.RebuildMutableStateRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      newRunID,
		},
		BranchToken: baseBranchToken,
	})
	s.NoError(err)

	responseNew, err := s.adminClient.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      newRunID,
		},
	})
	s.NoError(err)
	newCurrentVersionHistory, err := versionhistory.GetCurrentVersionHistory(responseNew.DatabaseMutableState.ExecutionInfo.VersionHistories)
	s.NoError(err)
	newBranchToken := newCurrentVersionHistory.BranchToken

	s.NotEqual(baseBranchToken, newBranchToken)
	baseCurrentVersionHistory.BranchToken = nil
	newCurrentVersionHistory.BranchToken = nil
	s.Equal(responseBase.DatabaseMutableState.ExecutionInfo.VersionHistories, responseNew.DatabaseMutableState.ExecutionInfo.VersionHistories)
	// state transition count is not accurate in this case, due to
	//  1. activity heartbeat
	//  2. buffered events
	// s.Equal(responseBase.DatabaseMutableState.ExecutionInfo.StateTransitionCount, responseNew.DatabaseMutableState.ExecutionInfo.StateTransitionCount)
	responseBase.DatabaseMutableState.ExecutionState.CreateRequestId = ""
	responseBase.DatabaseMutableState.ExecutionState.RunId = ""
	responseNew.DatabaseMutableState.ExecutionState.CreateRequestId = ""
	responseNew.DatabaseMutableState.ExecutionState.RunId = ""
	s.Equal(responseBase.DatabaseMutableState.ExecutionState, responseNew.DatabaseMutableState.ExecutionState)
}

func (s *clientIntegrationSuite) TestAdminRebuildMutableState() {

	workflowFn := func(ctx workflow.Context) error {
		var randomUUID string
		err := workflow.SideEffect(
			ctx,
			func(workflow.Context) interface{} { return uuid.New().String() },
		).Get(&randomUUID)
		s.NoError(err)

		_ = workflow.Sleep(ctx, 10*time.Minute)
		return nil
	}

	s.worker.RegisterWorkflow(workflowFn)

	workflowID := "integration-admin-rebuild-mutable-state-test"
	workflowOptions := sdkclient.StartWorkflowOptions{
		ID:                 workflowID,
		TaskQueue:          s.taskQueue,
		WorkflowRunTimeout: 20 * time.Second,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	workflowRun, err := s.sdkClient.ExecuteWorkflow(ctx, workflowOptions, workflowFn)
	s.NoError(err)
	runID := workflowRun.GetRunID()

	// there are total 6 events, 3 state transitions
	//  1. WorkflowExecutionStarted
	//  2. WorkflowTaskScheduled
	//
	//  3. WorkflowTaskStarted
	//
	//  4. WorkflowTaskCompleted
	//  5. MarkerRecord
	//  6. TimerStarted

	var response1 *adminservice.DescribeMutableStateResponse
	for {
		response1, err = s.adminClient.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: workflowID,
				RunId:      runID,
			},
		})
		s.NoError(err)
		if response1.DatabaseMutableState.ExecutionInfo.StateTransitionCount == 3 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	_, err = s.adminClient.RebuildMutableState(ctx, &adminservice.RebuildMutableStateRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	})
	s.NoError(err)

	response2, err := s.adminClient.DescribeMutableState(ctx, &adminservice.DescribeMutableStateRequest{
		Namespace: s.namespace,
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
	})
	s.NoError(err)
	s.Equal(response1.DatabaseMutableState.ExecutionInfo.VersionHistories, response2.DatabaseMutableState.ExecutionInfo.VersionHistories)
	s.Equal(response1.DatabaseMutableState.ExecutionInfo.StateTransitionCount, response2.DatabaseMutableState.ExecutionInfo.StateTransitionCount)
	s.Equal(response1.DatabaseMutableState.ExecutionState, response2.DatabaseMutableState.ExecutionState)
}
