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
	"time"

	"github.com/pborman/uuid"
	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/converter"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func (s *FunctionalSuite) defaultWorkflowID() string {
	return fmt.Sprintf("functional-%v", s.T().Name())
}

func (s *FunctionalSuite) defaultTaskQueue() *taskqueuepb.TaskQueue {
	name := fmt.Sprintf("functional-queue-%v", s.T().Name())
	return &taskqueuepb.TaskQueue{Name: name, Kind: enumspb.TASK_QUEUE_KIND_NORMAL}
}

func (s *FunctionalSuite) startEagerWorkflow(baseOptions *workflowservice.StartWorkflowExecutionRequest) *workflowservice.StartWorkflowExecutionResponse {
	options := proto.Clone(baseOptions).(*workflowservice.StartWorkflowExecutionRequest)
	options.RequestEagerExecution = true

	if options.Namespace == "" {
		options.Namespace = s.namespace
	}
	if options.Identity == "" {
		options.Identity = "test"
	}
	if options.WorkflowId == "" {
		options.WorkflowId = s.defaultWorkflowID()
	}
	if options.WorkflowType == nil {
		options.WorkflowType = &commonpb.WorkflowType{Name: "Workflow"}
	}
	if options.TaskQueue == nil {
		options.TaskQueue = s.defaultTaskQueue()
	}
	if options.RequestId == "" {
		options.RequestId = uuid.New()
	}

	response, err := s.engine.StartWorkflowExecution(NewContext(), options)
	s.Require().NoError(err)

	return response
}

func (s *FunctionalSuite) respondWorkflowTaskCompleted(task *workflowservice.PollWorkflowTaskQueueResponse, result interface{}) {
	dataConverter := converter.GetDefaultDataConverter()
	payloads, err := dataConverter.ToPayloads(result)
	s.Require().NoError(err)
	completion := workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: s.namespace,
		Identity:  "test",
		TaskToken: task.TaskToken,
		Commands: []*commandpb.Command{{CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION, Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
			CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: payloads,
			},
		}}},
	}
	_, err = s.engine.RespondWorkflowTaskCompleted(NewContext(), &completion)
	s.Require().NoError(err)
}

func (s *FunctionalSuite) pollWorkflowTaskQueue() *workflowservice.PollWorkflowTaskQueueResponse {
	task, err := s.engine.PollWorkflowTaskQueue(NewContext(), &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.namespace,
		TaskQueue: s.defaultTaskQueue(),
		Identity:  "test",
	})
	s.Require().NotNil(task, "PollWorkflowTaskQueue response was empty")
	s.Require().NoError(err)
	return task
}

func (s *FunctionalSuite) getWorkflowStringResult(workflowID, runID string) string {
	hostPort := "127.0.0.1:7134"
	if TestFlags.FrontendAddr != "" {
		hostPort = TestFlags.FrontendAddr
	}
	c, err := client.Dial(client.Options{HostPort: hostPort, Namespace: s.namespace})
	s.Require().NoError(err)
	run := c.GetWorkflow(NewContext(), workflowID, runID)
	var result string
	err = run.Get(NewContext(), &result)
	s.Require().NoError(err)
	return result
}

func (s *FunctionalSuite) TestEagerWorkflowStart_StartNew() {
	// Add a search attribute to verify that per namespace search attribute mapping is properly applied in the
	// response.
	response := s.startEagerWorkflow(&workflowservice.StartWorkflowExecutionRequest{
		SearchAttributes: &commonpb.SearchAttributes{
			IndexedFields: map[string]*commonpb.Payload{
				"CustomKeywordField": {
					Metadata: map[string][]byte{"encoding": []byte("json/plain")},
					Data:     []byte(`"value"`),
				},
			},
		},
	})
	task := response.GetEagerWorkflowTask()
	s.Require().NotNil(task, "StartWorkflowExecution response did not contain a workflow task")
	kwData := task.History.Events[0].GetWorkflowExecutionStartedEventAttributes().SearchAttributes.IndexedFields["CustomKeywordField"].Data
	s.Require().Equal(`"value"`, string(kwData))
	s.respondWorkflowTaskCompleted(task, "ok")
	// Verify workflow completes and client can get the result
	result := s.getWorkflowStringResult(s.defaultWorkflowID(), response.RunId)
	s.Require().Equal("ok", result)
}

func (s *FunctionalSuite) TestEagerWorkflowStart_RetryTaskAfterTimeout() {
	response := s.startEagerWorkflow(&workflowservice.StartWorkflowExecutionRequest{
		// Should give enough grace time even in slow CI
		WorkflowTaskTimeout: durationpb.New(2 * time.Second),
	})
	task := response.GetEagerWorkflowTask()
	s.Require().NotNil(task, "StartWorkflowExecution response did not contain a workflow task")
	// Let it timeout so it can be polled via standard matching based dispatch
	task = s.pollWorkflowTaskQueue()
	s.respondWorkflowTaskCompleted(task, "ok")
	// Verify workflow completes and client can get the result
	result := s.getWorkflowStringResult(s.defaultWorkflowID(), response.RunId)
	s.Require().Equal("ok", result)
}

func (s *FunctionalSuite) TestEagerWorkflowStart_RetryStartAfterTimeout() {
	request := &workflowservice.StartWorkflowExecutionRequest{
		// Should give enough grace time even in slow CI
		WorkflowTaskTimeout: durationpb.New(2 * time.Second),
		RequestId:           uuid.New(),
	}
	response := s.startEagerWorkflow(request)
	task := response.GetEagerWorkflowTask()
	s.Require().NotNil(task, "StartWorkflowExecution response did not contain a workflow task")

	// Let it timeout
	time.Sleep(request.WorkflowTaskTimeout.AsDuration())
	response = s.startEagerWorkflow(request)
	task = response.GetEagerWorkflowTask()
	s.Require().Nil(task, "StartWorkflowExecution response contained a workflow task")

	task = s.pollWorkflowTaskQueue()
	s.respondWorkflowTaskCompleted(task, "ok")
	// Verify workflow completes and client can get the result
	result := s.getWorkflowStringResult(s.defaultWorkflowID(), response.RunId)
	s.Require().Equal("ok", result)
}

func (s *FunctionalSuite) TestEagerWorkflowStart_RetryStartImmediately() {
	request := &workflowservice.StartWorkflowExecutionRequest{RequestId: uuid.New()}
	response := s.startEagerWorkflow(request)
	task := response.GetEagerWorkflowTask()
	s.Require().NotNil(task, "StartWorkflowExecution response did not contain a workflow task")
	response = s.startEagerWorkflow(request)
	task = response.GetEagerWorkflowTask()
	s.Require().NotNil(task, "StartWorkflowExecution response did not contain a workflow task")

	s.respondWorkflowTaskCompleted(task, "ok")
	// Verify workflow completes and client can get the result
	result := s.getWorkflowStringResult(s.defaultWorkflowID(), response.RunId)
	s.Require().Equal("ok", result)
}

func (s *FunctionalSuite) TestEagerWorkflowStart_TerminateDuplicate() {
	request := &workflowservice.StartWorkflowExecutionRequest{
		WorkflowIdReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
	}
	s.startEagerWorkflow(request)
	response := s.startEagerWorkflow(request)
	task := response.GetEagerWorkflowTask()
	s.Require().NotNil(task, "StartWorkflowExecution response did not contain a workflow task")

	s.respondWorkflowTaskCompleted(task, "ok")
	// Verify workflow completes and client can get the result
	result := s.getWorkflowStringResult(s.defaultWorkflowID(), response.RunId)
	s.Require().Equal("ok", result)
}
