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
	"time"

	"github.com/pborman/uuid"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/protobuf/types/known/durationpb"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/payloads"
)

func (s *FunctionalSuite) TestServerRejectsInvalidRequests() {
	sut := newSystemUnderTestConnector(s)

	customersNamespace := namespace.Name(s.namespace)
	err := sut.startWorkflowExecution(customersNamespace)
	s.NoError(err)

	task, err := sut.pollWorkflowTaskQueue(customersNamespace)
	s.NoError(err)

	// customer tries to use another namespace
	err = sut.respondWorkflowTaskCompleted(task, namespace.Name("another-namespace"))
	s.Error(err, "Invalid request was processed successfully, make sure NamespaceRequestValidator interceptor is used")

	err = sut.respondWorkflowTaskCompleted(task, customersNamespace)
	s.NoError(err, "Valid request was rejected")
}

type sutConnector struct {
	suite           *FunctionalSuite
	identity        string
	taskQueue       *taskqueuepb.TaskQueue
	stickyTaskQueue *taskqueuepb.TaskQueue
	id              string
	taskToken       []byte
}

func newSystemUnderTestConnector(s *FunctionalSuite) *sutConnector {
	id := uuid.New()
	return &sutConnector{
		suite:           s,
		identity:        "worker-1",
		taskQueue:       &taskqueuepb.TaskQueue{Name: id, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		stickyTaskQueue: &taskqueuepb.TaskQueue{Name: "test-sticky-taskqueue", Kind: enumspb.TASK_QUEUE_KIND_STICKY, NormalName: id},
		id:              id,
	}

}

func (b *sutConnector) startWorkflowExecution(ns namespace.Name) error {
	request := newStartWorkflowExecutionRequest(ns, b.id, b.identity, b.taskQueue)

	_, err := b.suite.engine.StartWorkflowExecution(NewContext(), request)
	return err
}

func (b *sutConnector) pollWorkflowTaskQueue(ns namespace.Name) ([]byte, error) {
	request := newPollWorkflowTaskQueueRequest(ns, b.identity, b.taskQueue)
	resp, err := b.suite.engine.PollWorkflowTaskQueue(NewContext(), request)
	if err != nil {
		return nil, err
	}
	b.taskToken = resp.GetTaskToken()
	return b.taskToken, nil
}

func (b *sutConnector) respondWorkflowTaskCompleted(token []byte, ns namespace.Name) error {
	request := newRespondWorkflowTaskCompletedRequest(ns, b.stickyTaskQueue, token)
	_, err := b.suite.engine.RespondWorkflowTaskCompleted(NewContext(), request)
	return err
}

func newStartWorkflowExecutionRequest(ns namespace.Name, workflowId string, identity string, queue *taskqueuepb.TaskQueue) *workflowservice.StartWorkflowExecutionRequest {
	wt := "functional-workflow-namespace-validator-interceptor"
	workflowType := &commonpb.WorkflowType{Name: wt}
	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:           uuid.New(),
		Namespace:           ns.String(),
		WorkflowId:          workflowId,
		WorkflowType:        workflowType,
		TaskQueue:           queue,
		Input:               nil,
		WorkflowRunTimeout:  durationpb.New(20 * time.Second),
		WorkflowTaskTimeout: durationpb.New(3 * time.Second),
		Identity:            identity,
	}
	return request
}

func newPollWorkflowTaskQueueRequest(ns namespace.Name, identity string, queue *taskqueuepb.TaskQueue) *workflowservice.PollWorkflowTaskQueueRequest {
	return &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: ns.String(),
		TaskQueue: queue,
		Identity:  identity,
	}
}

func newRespondWorkflowTaskCompletedRequest(ns namespace.Name, stickyQueue *taskqueuepb.TaskQueue, token []byte) *workflowservice.RespondWorkflowTaskCompletedRequest {
	return &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: ns.String(),
		TaskToken: token,
		Commands: []*commandpb.Command{
			{
				CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
				Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
					CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
						Result: payloads.EncodeString("efg"),
					},
				},
			}},
		StickyAttributes: &taskqueuepb.StickyExecutionAttributes{
			WorkerTaskQueue:        stickyQueue,
			ScheduleToStartTimeout: durationpb.New(5 * time.Second),
		},
		ReturnNewWorkflowTask:      true,
		ForceCreateNewWorkflowTask: false,
	}
}
