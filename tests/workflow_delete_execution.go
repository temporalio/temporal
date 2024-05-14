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
	"fmt"
	"strconv"
	"time"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"google.golang.org/protobuf/types/known/durationpb"
)

const (
	waitForPersistence = 10 * time.Second
)

func (s *FunctionalSuite) Test_DeleteWorkflowExecution_Competed() {
	id := "functional-delete-workflow-completed-test"
	wt := "functional-delete-workflow-completed-test-type"
	tl := "functional-delete-workflow-completed-test-taskqueue"
	identity := "worker1"

	const numExecutions = 5

	var wes []*commonpb.WorkflowExecution
	// Start numExecutions workflow executions.
	for i := 0; i < numExecutions; i++ {
		wid := id + strconv.Itoa(i)
		we, err := s.engine.StartWorkflowExecution(NewContext(), &workflowservice.StartWorkflowExecutionRequest{
			RequestId:             uuid.New(),
			Namespace:             s.namespace,
			WorkflowId:            wid,
			WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			WorkflowType:          &commonpb.WorkflowType{Name: wt},
			TaskQueue:             &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Input:                 nil,
			WorkflowRunTimeout:    durationpb.New(100 * time.Second),
			WorkflowTaskTimeout:   durationpb.New(1 * time.Second),
			Identity:              identity,
		})
		s.NoError(err)
		wes = append(wes, &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      we.RunId,
		})
	}

	// Complete workflow.
	wtHandler := func(task *workflowservice.PollWorkflowTaskQueueResponse) ([]*commandpb.Command, error) {
		return []*commandpb.Command{{
			CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
			Attributes:  &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{}},
		}}, nil
	}

	poller := &TaskPoller{
		Engine:              s.engine,
		Namespace:           s.namespace,
		TaskQueue:           &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Identity:            identity,
		WorkflowTaskHandler: wtHandler,
		// ActivityTaskHandler: atHandler,
		Logger: s.Logger,
		T:      s.T(),
	}

	for range wes {
		_, err := poller.PollAndProcessWorkflowTask()
		s.NoError(err)
	}

	// Verify that workflow is completed and visibility is updated.
	for _, we := range wes {
		s.Eventually(
			func() bool {
				visibilityResponse, err := s.engine.ListWorkflowExecutions(
					NewContext(),
					&workflowservice.ListWorkflowExecutionsRequest{
						Namespace:     s.namespace,
						PageSize:      1,
						NextPageToken: nil,
						Query:         fmt.Sprintf("WorkflowId='%s'", we.WorkflowId),
					},
				)
				s.NoError(err)
				if len(visibilityResponse.Executions) == 1 &&
					visibilityResponse.Executions[0].Status == enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED {
					return true
				}
				return false
			},
			waitForESToSettle,
			100*time.Millisecond,
		)
	}

	// Delete workflow executions.
	for _, we := range wes {
		_, err := s.engine.DeleteWorkflowExecution(NewContext(), &workflowservice.DeleteWorkflowExecutionRequest{
			Namespace: s.namespace,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: we.WorkflowId,
				RunId:      we.RunId,
			},
		})
		s.NoError(err)
	}

	for _, we := range wes {
		s.Eventually(
			func() bool {
				// Check execution is deleted.
				describeResponse, err := s.engine.DescribeWorkflowExecution(
					NewContext(),
					&workflowservice.DescribeWorkflowExecutionRequest{
						Namespace: s.namespace,
						Execution: we,
					},
				)
				if err == nil {
					s.Logger.Warn("Execution not deleted yet")
					return false
				}
				var notFoundErr *serviceerror.NotFound
				s.ErrorAs(err, &notFoundErr)
				s.Nil(describeResponse)
				return true
			},
			waitForPersistence,
			100*time.Millisecond,
		)

		// Check history is deleted.
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(
			NewContext(),
			&workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace: s.namespace,
				Execution: we,
			},
		)
		var invalidArgumentErr *serviceerror.InvalidArgument
		s.ErrorAs(err, &invalidArgumentErr)
		s.Nil(historyResponse)

		s.Eventually(
			func() bool {
				// Check visibility is updated.
				visibilityResponse, err := s.engine.ListWorkflowExecutions(
					NewContext(),
					&workflowservice.ListWorkflowExecutionsRequest{
						Namespace:     s.namespace,
						PageSize:      1,
						NextPageToken: nil,
						Query:         fmt.Sprintf("WorkflowId='%s'", we.WorkflowId),
					},
				)
				s.NoError(err)
				if len(visibilityResponse.Executions) != 0 {
					s.Logger.Warn("Visibility is not deleted yet")
					return false
				}
				return true
			},
			waitForESToSettle,
			100*time.Millisecond,
		)
	}
}

func (s *FunctionalSuite) Test_DeleteWorkflowExecution_Running() {
	id := "functional-delete-workflow-running-test"
	wt := "functional-delete-workflow-running-test-type"
	tl := "functional-delete-workflow-running-test-taskqueue"
	identity := "worker1"

	const numExecutions = 5

	var wes []*commonpb.WorkflowExecution
	// Start numExecutions workflow executions.
	for i := 0; i < numExecutions; i++ {
		wid := id + strconv.Itoa(i)
		we, err := s.engine.StartWorkflowExecution(NewContext(), &workflowservice.StartWorkflowExecutionRequest{
			RequestId:             uuid.New(),
			Namespace:             s.namespace,
			WorkflowId:            wid,
			WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			WorkflowType:          &commonpb.WorkflowType{Name: wt},
			TaskQueue:             &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Input:                 nil,
			WorkflowRunTimeout:    durationpb.New(100 * time.Second),
			WorkflowTaskTimeout:   durationpb.New(1 * time.Second),
			Identity:              identity,
		})
		s.NoError(err)
		wes = append(wes, &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      we.RunId,
		})
	}

	// Verify that workflow is running and visibility is updated.
	for _, we := range wes {
		s.Eventually(
			func() bool {
				visibilityResponse, err := s.engine.ListWorkflowExecutions(
					NewContext(),
					&workflowservice.ListWorkflowExecutionsRequest{
						Namespace:     s.namespace,
						PageSize:      1,
						NextPageToken: nil,
						Query:         fmt.Sprintf("WorkflowId='%s'", we.WorkflowId),
					},
				)
				s.NoError(err)
				return len(visibilityResponse.Executions) == 1 &&
					visibilityResponse.Executions[0].Status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
			},
			waitForESToSettle,
			100*time.Millisecond,
		)
	}

	// Delete workflow executions.
	for _, we := range wes {
		_, err := s.engine.DeleteWorkflowExecution(NewContext(), &workflowservice.DeleteWorkflowExecutionRequest{
			Namespace: s.namespace,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: we.WorkflowId,
				RunId:      we.RunId,
			},
		})
		s.NoError(err)
	}

	for _, we := range wes {
		s.Eventually(
			func() bool {
				// Check execution is deleted.
				describeResponse, err := s.engine.DescribeWorkflowExecution(
					NewContext(),
					&workflowservice.DescribeWorkflowExecutionRequest{
						Namespace: s.namespace,
						Execution: we,
					},
				)
				if err == nil {
					s.Logger.Warn("Execution not deleted yet")
					return false
				}
				var notFoundErr *serviceerror.NotFound
				s.ErrorAs(err, &notFoundErr)
				s.Nil(describeResponse)
				return true
			},
			waitForPersistence,
			100*time.Millisecond,
		)

		// Check history is deleted.
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(
			NewContext(),
			&workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace: s.namespace,
				Execution: we,
			},
		)
		var invalidArgumentErr *serviceerror.InvalidArgument
		s.ErrorAs(err, &invalidArgumentErr)
		s.Nil(historyResponse)

		s.Eventually(
			func() bool {
				// Check visibility is updated.
				visibilityResponse, err := s.engine.ListWorkflowExecutions(
					NewContext(),
					&workflowservice.ListWorkflowExecutionsRequest{
						Namespace:     s.namespace,
						PageSize:      1,
						NextPageToken: nil,
						Query:         fmt.Sprintf("WorkflowId='%s'", we.WorkflowId),
					},
				)
				s.NoError(err)
				if len(visibilityResponse.Executions) != 0 {
					s.Logger.Warn("Visibility is not deleted yet")
					return false
				}
				return true
			},
			waitForESToSettle,
			100*time.Millisecond,
		)
	}
}

func (s *FunctionalSuite) Test_DeleteWorkflowExecution_RunningWithTerminate() {
	id := "functional-delete-workflow-running-with-terminate-test"
	wt := "functional-delete-workflow-running-with-terminate-test-type"
	tl := "functional-delete-workflow-running-with-terminate-test-taskqueue"
	identity := "worker1"

	const numExecutions = 3

	var wes []*commonpb.WorkflowExecution
	// Start numExecutions workflow executions.
	for i := 0; i < numExecutions; i++ {
		wid := id + strconv.Itoa(i)
		we, err := s.engine.StartWorkflowExecution(NewContext(), &workflowservice.StartWorkflowExecutionRequest{
			RequestId:             uuid.New(),
			Namespace:             s.namespace,
			WorkflowId:            wid,
			WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
			WorkflowType:          &commonpb.WorkflowType{Name: wt},
			TaskQueue:             &taskqueuepb.TaskQueue{Name: tl, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
			Input:                 nil,
			WorkflowRunTimeout:    durationpb.New(100 * time.Second),
			WorkflowTaskTimeout:   durationpb.New(1 * time.Second),
			Identity:              identity,
		})
		s.NoError(err)
		wes = append(wes, &commonpb.WorkflowExecution{
			WorkflowId: wid,
			RunId:      we.RunId,
		})
	}

	// Verify that workflow is running and visibility is updated.
	for _, we := range wes {
		s.Eventually(
			func() bool {
				visibilityResponse, err := s.engine.ListWorkflowExecutions(
					NewContext(),
					&workflowservice.ListWorkflowExecutionsRequest{
						Namespace:     s.namespace,
						PageSize:      1,
						NextPageToken: nil,
						Query:         fmt.Sprintf("WorkflowId='%s'", we.WorkflowId),
					},
				)
				s.NoError(err)
				return len(visibilityResponse.Executions) == 1 &&
					visibilityResponse.Executions[0].Status == enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING
			},
			waitForESToSettle,
			100*time.Millisecond,
		)
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
		_, err = s.engine.DeleteWorkflowExecution(NewContext(), &workflowservice.DeleteWorkflowExecutionRequest{
			Namespace: s.namespace,
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: we.WorkflowId,
				RunId:      we.RunId,
			},
		})
		s.NoError(err)
	}

	for _, we := range wes {
		s.Eventually(
			func() bool {
				// Check execution is deleted.
				describeResponse, err := s.engine.DescribeWorkflowExecution(
					NewContext(),
					&workflowservice.DescribeWorkflowExecutionRequest{
						Namespace: s.namespace,
						Execution: we,
					},
				)
				if err == nil {
					s.Logger.Warn("Execution not deleted yet")
					return false
				}
				var notFoundErr *serviceerror.NotFound
				s.ErrorAs(err, &notFoundErr)
				s.Nil(describeResponse)
				return true
			},
			waitForPersistence,
			1*time.Second,
		)

		// Check history is deleted.
		historyResponse, err := s.engine.GetWorkflowExecutionHistory(
			NewContext(),
			&workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace: s.namespace,
				Execution: we,
			},
		)
		var invalidArgumentErr *serviceerror.InvalidArgument
		s.ErrorAs(err, &invalidArgumentErr)
		s.Nil(historyResponse)

		s.Eventually(
			func() bool {
				// Check visibility is updated.
				visibilityResponse, err := s.engine.ListWorkflowExecutions(
					NewContext(),
					&workflowservice.ListWorkflowExecutionsRequest{
						Namespace:     s.namespace,
						PageSize:      1,
						NextPageToken: nil,
						Query:         fmt.Sprintf("WorkflowId='%s'", we.WorkflowId),
					},
				)
				s.NoError(err)
				if len(visibilityResponse.Executions) != 0 {
					s.Logger.Warn("Visibility is not deleted yet")
					return false
				}
				return true
			},
			waitForESToSettle,
			100*time.Millisecond,
		)
	}
}
