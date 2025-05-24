// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
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
// FITNESS FOR A PARTICULAR PURPOSE AND NGetONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package action

import (
	"encoding/base64"

	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
)

type PollWorkflowTaskQueue struct {
	stamp.ActionActor[*model.WorkflowWorker]
	stamp.ActionTarget[*model.WorkflowTask]
	TaskQueue *model.TaskQueue `validate:"required"`
}

func (t PollWorkflowTaskQueue) Next(_ stamp.GenContext) *workflowservice.PollWorkflowTaskQueueRequest {
	return &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: string(t.TaskQueue.GetNamespace().GetID()),
		TaskQueue: &taskqueue.TaskQueue{
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			Name: string(t.TaskQueue.GetID()),
		},
		Identity: string(t.GetActor().GetID()),
	}
}

type WorkflowTaskResponse interface {
	Complete() *RespondWorkflowTaskCompleted
	Fail() *FailWorkflowTask
}

type RespondWorkflowTaskCompleted struct {
	stamp.ActionActor[*model.WorkflowWorker]
	stamp.ActionTarget[*model.WorkflowTask]
	WorkflowTask *model.WorkflowTask `validate:"required"`
	Commands     stamp.ListGen[*commandpb.Command]
	Messages     stamp.ListGen[*protocolpb.Message]
}

func (t RespondWorkflowTaskCompleted) Next(ctx stamp.GenContext) *workflowservice.RespondWorkflowTaskCompletedRequest {
	if t.WorkflowTask.Token == "" {
		panic("WorkflowTask Token is empty")
	}
	tokenBytes, err := base64.StdEncoding.DecodeString(t.WorkflowTask.Token)
	if err != nil {
		panic(err)
	}

	// TODO: randomize order
	var commands []*commandpb.Command
	for _, cmd := range t.Commands {
		commands = append(commands, cmd.Next(ctx))
	}

	// TODO: randomize order
	var messages []*protocolpb.Message
	for _, msg := range t.Messages {
		m := msg.Next(ctx)
		messages = append(messages, m)

		// auto-generate a ProtocolCommand for each Message
		// TODO: allow to turn this off
		commands = append(commands, ProtocolCommand{MessageID: m.Id}.Next(ctx))
	}

	return &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace: string(t.WorkflowTask.GetNamespace().GetID()),
		Identity:  string(t.GetActor().GetID()),
		TaskToken: tokenBytes,
		Commands:  commands,
		Messages:  messages,
	}
}

type FailWorkflowTask struct {
	stamp.ActionActor[*model.WorkflowWorker]
	stamp.ActionTarget[*model.WorkflowTask]
	WorkflowTask *model.WorkflowTask `validate:"required"`
	// TODO
}

func (t FailWorkflowTask) Next(ctx stamp.GenContext) *workflowservice.RespondWorkflowTaskFailedRequest {
	return &workflowservice.RespondWorkflowTaskFailedRequest{
		// TODO
	}
}
