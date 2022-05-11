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
	"strconv"
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

func (s *integrationSuite) Test_DeleteWorkflowExecution_Competed() {
	id := "integration-delete-workflow-completed-test"
	wt := "integration-delete-workflow-completed-test-type"
	tl := "integration-delete-workflow-completed-test-taskqueue"
	identity := "worker1"

	const numExecutions = 5

	var wes []commonpb.WorkflowExecution
	// Start numExecutions workflow executions.
	for i := 0; i < numExecutions; i++ {
		wid := id + strconv.Itoa(i)
		we, err := s.engine.StartWorkflowExecution(NewContext(), &workflowservice.StartWorkflowExecutionRequest{
			RequestId:             uuid.New(),
			Namespace:             s.namespace,
			WorkflowId:            wid,
			WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			WorkflowType:          &commonpb.WorkflowType{Name: wt},
			TaskQueue:             &taskqueuepb.TaskQueue{Name: tl},
			Input:                 nil,
			WorkflowRunTimeout:    timestamp.DurationPtr(100 * time.Second),
			WorkflowTaskTimeout:   timestamp.DurationPtr(1 * time.Second),
			Identity:              identity,
		})
		s.NoError(err)
		wes = append(wes, commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      we.RunId,
		})
	}

	// Complete workflow.
	wtHandler := func(execution *commonpb.WorkflowExecution, wt *commonpb.WorkflowType, previousStartedEventID, startedEventID int64, history *historypb.History) ([]*commandpb.Command, error) {
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		// ActivityTaskHandler: atHandler,
		Logger: s.Logger,
		T:      s.T(),
	}

	for range wes {
		_, err := poller.PollAndProcessWorkflowTask(false, false)
		s.NoError(err)
	}

	// Verify that workflow is completed and visibility is updated.
	for _, we := range wes {
		executionsCount := 0
		for i := 0; i < 10; i++ {
			visibilityResponse, err := s.engine.ListWorkflowExecutions(NewContext(), &workflowservice.ListWorkflowExecutionsRequest{
				Namespace:     s.namespace,
				PageSize:      1,
				NextPageToken: nil,
				Query:         fmt.Sprintf("WorkflowId='%s'", we.WorkflowId),
			})
			s.NoError(err)
			if len(visibilityResponse.Executions) != 1 || visibilityResponse.Executions[0].Status != enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
				time.Sleep(100 * time.Millisecond)
			} else {
				executionsCount = len(visibilityResponse.Executions)
				break
			}
		}
		s.Equal(1, executionsCount)
	}

	// Delete workflow executions.
	for _, we := range wes {
		var err error
		for {
			_, err = s.operatorClient.DeleteWorkflowExecution(NewContext(), &operatorservice.DeleteWorkflowExecutionRequest{
				Namespace: s.namespace,
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: we.WorkflowId,
					RunId:      we.RunId,
				},
			})
			if _, isNotReady := err.(*serviceerror.WorkflowNotReady); err == nil || !isNotReady {
				break
			}
			// Overrides are defined in onebox.go:770 to iterate faster.
			time.Sleep(500 * time.Millisecond)
		}
		s.NoError(err)
	}

	for _, we := range wes {
		executionDeleted := false
		for i := 0; i < 10; i++ {
			// Check execution is deleted.
			describeResponse, err := s.engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: s.namespace,
				Execution: &we,
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
				Execution: &we,
			})
			var invalidArgumentErr *serviceerror.InvalidArgument
			s.ErrorAs(err, &invalidArgumentErr)
			s.Nil(historyResponse)

			// Check visibility is updated.
			visibilityResponse, err := s.engine.ListWorkflowExecutions(NewContext(), &workflowservice.ListWorkflowExecutionsRequest{
				Namespace:     s.namespace,
				PageSize:      1,
				NextPageToken: nil,
				Query:         fmt.Sprintf("WorkflowId='%s'", we.WorkflowId),
			})
			s.NoError(err)
			if len(visibilityResponse.Executions) != 0 {
				time.Sleep(100 * time.Millisecond)
				s.Logger.Warn("Visibility is not deleted yet")
				continue
			}
			s.Equal(0, len(visibilityResponse.Executions))

			executionDeleted = true
			break
		}
		s.True(executionDeleted)
	}
}

func (s *integrationSuite) Test_DeleteWorkflowExecution_Running() {
	id := "integration-delete-workflow-running-test"
	wt := "integration-delete-workflow-running-test-type"
	tl := "integration-delete-workflow-running-test-taskqueue"
	identity := "worker1"

	const numExecutions = 5

	var wes []commonpb.WorkflowExecution
	// Start numExecutions workflow executions.
	for i := 0; i < numExecutions; i++ {
		wid := id + strconv.Itoa(i)
		we, err := s.engine.StartWorkflowExecution(NewContext(), &workflowservice.StartWorkflowExecutionRequest{
			RequestId:             uuid.New(),
			Namespace:             s.namespace,
			WorkflowId:            wid,
			WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			WorkflowType:          &commonpb.WorkflowType{Name: wt},
			TaskQueue:             &taskqueuepb.TaskQueue{Name: tl},
			Input:                 nil,
			WorkflowRunTimeout:    timestamp.DurationPtr(100 * time.Second),
			WorkflowTaskTimeout:   timestamp.DurationPtr(1 * time.Second),
			Identity:              identity,
		})
		s.NoError(err)
		wes = append(wes, commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      we.RunId,
		})
	}

	// Verify that workflow is running and visibility is updated.
	for _, we := range wes {
		executionsCount := 0
		for i := 0; i < 10; i++ {
			visibilityResponse, err := s.engine.ListWorkflowExecutions(NewContext(), &workflowservice.ListWorkflowExecutionsRequest{
				Namespace:     s.namespace,
				PageSize:      1,
				NextPageToken: nil,
				Query:         fmt.Sprintf("WorkflowId='%s'", we.WorkflowId),
			})
			s.NoError(err)
			if len(visibilityResponse.Executions) != 1 || visibilityResponse.Executions[0].Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				time.Sleep(100 * time.Millisecond)
			} else {
				executionsCount = len(visibilityResponse.Executions)
				break
			}
		}
		s.Equal(1, executionsCount)
	}

	// Delete workflow executions.
	for _, we := range wes {
		_, err := s.operatorClient.DeleteWorkflowExecution(NewContext(), &operatorservice.DeleteWorkflowExecutionRequest{
			Namespace: s.namespace,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: we.WorkflowId,
				RunId:      we.RunId,
			},
		})
		s.NoError(err)
	}

	for _, we := range wes {
		executionDeleted := false
		for i := 0; i < 10; i++ {
			// Check execution is deleted.
			describeResponse, err := s.engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: s.namespace,
				Execution: &we,
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
				Execution: &we,
			})
			var invalidArgumentErr *serviceerror.InvalidArgument
			s.ErrorAs(err, &invalidArgumentErr)
			s.Nil(historyResponse)

			// Check visibility is updated.
			visibilityResponse, err := s.engine.ListWorkflowExecutions(NewContext(), &workflowservice.ListWorkflowExecutionsRequest{
				Namespace:     s.namespace,
				PageSize:      1,
				NextPageToken: nil,
				Query:         fmt.Sprintf("WorkflowId='%s'", we.WorkflowId),
			})
			s.NoError(err)
			if len(visibilityResponse.Executions) != 0 {
				time.Sleep(100 * time.Millisecond)
				s.Logger.Warn("Visibility is not deleted yet")
				continue
			}
			s.Equal(0, len(visibilityResponse.Executions))

			executionDeleted = true
			break
		}
		s.True(executionDeleted)
	}
}

func (s *integrationSuite) Test_DeleteWorkflowExecution_RunningWithTerminate() {
	id := "integration-delete-workflow-running-with-terminate-test"
	wt := "integration-delete-workflow-running-with-terminate-test-type"
	tl := "integration-delete-workflow-running-with-terminate-test-taskqueue"
	identity := "worker1"

	const numExecutions = 3

	var wes []commonpb.WorkflowExecution
	// Start numExecutions workflow executions.
	for i := 0; i < numExecutions; i++ {
		wid := id + strconv.Itoa(i)
		we, err := s.engine.StartWorkflowExecution(NewContext(), &workflowservice.StartWorkflowExecutionRequest{
			RequestId:             uuid.New(),
			Namespace:             s.namespace,
			WorkflowId:            wid,
			WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			WorkflowType:          &commonpb.WorkflowType{Name: wt},
			TaskQueue:             &taskqueuepb.TaskQueue{Name: tl},
			Input:                 nil,
			WorkflowRunTimeout:    timestamp.DurationPtr(100 * time.Second),
			WorkflowTaskTimeout:   timestamp.DurationPtr(1 * time.Second),
			Identity:              identity,
		})
		s.NoError(err)
		wes = append(wes, commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      we.RunId,
		})
	}

	// Verify that workflow is running and visibility is updated.
	for _, we := range wes {
		executionsCount := 0
		for i := 0; i < 10; i++ {
			visibilityResponse, err := s.engine.ListWorkflowExecutions(NewContext(), &workflowservice.ListWorkflowExecutionsRequest{
				Namespace:     s.namespace,
				PageSize:      1,
				NextPageToken: nil,
				Query:         fmt.Sprintf("WorkflowId='%s'", we.WorkflowId),
			})
			s.NoError(err)
			if len(visibilityResponse.Executions) != 1 || visibilityResponse.Executions[0].Status != enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING {
				time.Sleep(100 * time.Millisecond)
			} else {
				executionsCount = len(visibilityResponse.Executions)
				break
			}
		}
		s.Equal(1, executionsCount)
	}

	// Terminate and delete workflow executions.
	for _, we := range wes {
		_, err := s.engine.TerminateWorkflowExecution(NewContext(), &workflowservice.TerminateWorkflowExecutionRequest{
			Namespace: s.namespace,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: we.WorkflowId,
				RunId:      we.RunId,
			},
		})
		s.NoError(err)
		for {
			_, err = s.operatorClient.DeleteWorkflowExecution(NewContext(), &operatorservice.DeleteWorkflowExecutionRequest{
				Namespace: s.namespace,
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: we.WorkflowId,
					RunId:      we.RunId,
				},
			})
			if _, isNotReady := err.(*serviceerror.WorkflowNotReady); err == nil || !isNotReady {
				break
			}
			// Overrides are defined in onebox.go:770 to iterate faster.
			time.Sleep(500 * time.Millisecond)
		}
		s.NoError(err)
	}

	for _, we := range wes {
		executionDeleted := false
		for i := 0; i < 10; i++ {
			// Check execution is deleted.
			describeResponse, err := s.engine.DescribeWorkflowExecution(NewContext(), &workflowservice.DescribeWorkflowExecutionRequest{
				Namespace: s.namespace,
				Execution: &we,
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
				Execution: &we,
			})
			var invalidArgumentErr *serviceerror.InvalidArgument
			s.ErrorAs(err, &invalidArgumentErr)
			s.Nil(historyResponse)

			// Check visibility is updated.
			visibilityResponse, err := s.engine.ListWorkflowExecutions(NewContext(), &workflowservice.ListWorkflowExecutionsRequest{
				Namespace:     s.namespace,
				PageSize:      1,
				NextPageToken: nil,
				Query:         fmt.Sprintf("WorkflowId='%s'", we.WorkflowId),
			})
			s.NoError(err)
			if len(visibilityResponse.Executions) != 0 {
				time.Sleep(100 * time.Millisecond)
				s.Logger.Warn("Visibility is not deleted yet")
				continue
			}
			s.Equal(0, len(visibilityResponse.Executions))

			executionDeleted = true
			break
		}
		s.True(executionDeleted)
	}
}
