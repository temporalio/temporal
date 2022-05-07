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

package host

import (
	"fmt"
	"time"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common/primitives/timestamp"
)

func (s *integrationSuite) TestDeleteCompetedWorkflowExecution() {
	id := "integration-delete-completed-workflow-test"
	wt := "integration-delete-completed-workflow-test-type"
	tl := "integration-delete-completed-workflow-test-taskqueue"
	identity := "worker1"

	// Start workflow execution.
	we, err := s.engine.StartWorkflowExecution(NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	})
	s.NoError(err)

	// Complete workflow.
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType, previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
		}}, nil
	}

	atHandler := func(execution *commonpb.WorkflowExecution, activityType *commonpb.ActivityType, activityID string, input *commonpb.Payloads, taskToken []byte) (*commonpb.Payloads, bool, error) {
		return nil, false, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		ActivityTaskHandler: atHandler,
		Logger:              s.Logger,
		T:                   s.T(),
	}

	_, err = poller.PollAndProcessWorkflowTask(false, false)
	s.NoError(err)

	// Verify that workflow is completed and visibility is updated.
	executionsCount := 0
	for i := 0; i < 10; i++ {
		visibilityResponse, err := s.engine.ListWorkflowExecutions(NewContext(), &workflowservice.ListWorkflowExecutionsRequest{
			Namespace:     s.namespace,
			PageSize:      1,
			NextPageToken: nil,
			Query:         fmt.Sprintf("WorkflowId='%s'", id),
		})
		s.NoError(err)
		if len(visibilityResponse.Executions) == 0 {
			time.Sleep(100 * time.Millisecond)
		} else {
			executionsCount = len(visibilityResponse.Executions)
			break
		}
	}
	s.Equal(1, executionsCount)

	// Delete workflow execution.
	_, err = s.operatorClient.DeleteWorkflowExecution(NewContext(), &operatorservice.DeleteWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
	})
	s.NoError(err)

	executionDeleted := false
	for i := 0; i < 10; i++ {
		// Check execution is deleted.
		describeResponse, err := s.engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
		if err == nil {
			s.Logger.Warn("Execution not deleted yet")
			time.Sleep(100 * time.Millisecond)
			continue
		}
		var notFoundErr *serviceerror.NotFound
		s.ErrorAs(err, &notFoundErr)
		s.Nil(describeResponse)

		// Check history is deleted.
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
		var invalidArgumentErr *serviceerror.InvalidArgument
		s.ErrorAs(err, &invalidArgumentErr)
		s.Nil(historyResponse)

		// Check visibility is updated.
		for i := 0; i < 10; i++ {
			visibilityResponse, err := s.engine.ListWorkflowExecutions(NewContext(), &workflowservice.ListWorkflowExecutionsRequest{
				Namespace:     s.namespace,
				PageSize:      1,
				NextPageToken: nil,
				Query:         fmt.Sprintf("WorkflowId='%s'", id),
			})
			s.NoError(err)
			if len(visibilityResponse.Executions) != 0 {
				time.Sleep(100 * time.Millisecond)
			} else {
				executionsCount = len(visibilityResponse.Executions)
				break
			}
		}
		s.Equal(0, executionsCount)

		executionDeleted = true
		break
	}

	s.True(executionDeleted)
}

func (s *integrationSuite) TestDeleteRunningWorkflowExecution() {
	id := "integration-delete-running-workflow-test"
	wt := "integration-delete-running-workflow-test-type"
	tl := "integration-delete-running-workflow-test-taskqueue"
	identity := "worker1"

	// Start workflow execution.
	we, err := s.engine.StartWorkflowExecution(NewContext(), &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           s.namespace,
		WorkflowId:          id,
		WorkflowType:        &commonpb.WorkflowType{Name: wt},
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Input:               nil,
		WorkflowRunTimeout:  timestamp.DurationPtr(100 * time.Second),
		WorkflowTaskTimeout: timestamp.DurationPtr(1 * time.Second),
		Identity:            identity,
	})
	s.NoError(err)

	// Verify that workflow is running and visibility is updated.
	executionsCount := 0
	for i := 0; i < 10; i++ {
		visibilityResponse, err := s.engine.ListWorkflowExecutions(NewContext(), &workflowservice.ListWorkflowExecutionsRequest{
			Namespace:     s.namespace,
			PageSize:      1,
			NextPageToken: nil,
			Query:         fmt.Sprintf("WorkflowId='%s'", id),
		})
		s.NoError(err)
		if len(visibilityResponse.Executions) == 0 {
			time.Sleep(100 * time.Millisecond)
		} else {
			executionsCount = len(visibilityResponse.Executions)
			break
		}
	}
	s.Equal(1, executionsCount)

	// Delete workflow execution.
	_, err = s.operatorClient.DeleteWorkflowExecution(NewContext(), &operatorservice.DeleteWorkflowExecutionRequest{
		Namespace: s.namespace,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: id,
			RunId:      we.RunId,
		},
	})
	s.NoError(err)

	executionDeleted := false
	for i := 0; i < 10; i++ {
		// Check execution is deleted.
		describeResponse, err := s.engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
		if err == nil {
			s.Logger.Warn("Execution not deleted yet")
			time.Sleep(100 * time.Millisecond)
			continue
		}
		var notFoundErr *serviceerror.NotFound
		s.ErrorAs(err, &notFoundErr)
		s.Nil(describeResponse)

		// Check history is deleted.
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(NewContext(), &workflowservice.GetWorkflowExecutionHistoryRequest{
			Namespace: s.namespace,
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: id,
				RunId:      we.RunId,
			},
		})
		var invalidArgumentErr *serviceerror.InvalidArgument
		s.ErrorAs(err, &invalidArgumentErr)
		s.Nil(historyResponse)

		// Check visibility is updated.
		for i := 0; i < 10; i++ {
			visibilityResponse, err := s.engine.ListWorkflowExecutions(NewContext(), &workflowservice.ListWorkflowExecutionsRequest{
				Namespace:     s.namespace,
				PageSize:      1,
				NextPageToken: nil,
				Query:         fmt.Sprintf("WorkflowId='%s'", id),
			})
			s.NoError(err)
			if len(visibilityResponse.Executions) != 0 {
				time.Sleep(100 * time.Millisecond)
			} else {
				executionsCount = len(visibilityResponse.Executions)
				break
			}
		}
		s.Equal(0, executionsCount)

		executionDeleted = true
		break
	}

	s.True(executionDeleted)
}
