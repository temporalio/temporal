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
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
	"google.golang.org/protobuf/types/known/durationpb"
)

var (
	GenWorkflowIdConflictPolicy = stamp.GenChoice("GenWorkflowIdConflictPolicy",
		enumspb.WORKFLOW_ID_CONFLICT_POLICY_UNSPECIFIED,
		enumspb.WORKFLOW_ID_CONFLICT_POLICY_USE_EXISTING,
		enumspb.WORKFLOW_ID_CONFLICT_POLICY_TERMINATE_EXISTING,
	)
	GenWorkflowIdReusePolicy = stamp.GenChoice("GenWorkflowIdReusePolicy",
		enumspb.WORKFLOW_ID_REUSE_POLICY_UNSPECIFIED,
		enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE,
		enumspb.WORKFLOW_ID_REUSE_POLICY_ALLOW_DUPLICATE_FAILED_ONLY,
		enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		enumspb.WORKFLOW_ID_REUSE_POLICY_TERMINATE_IF_RUNNING,
	)
	GenRequestID = stamp.GenChoice("GenRequestID",
		"", // empty request ID
		"custom-request-id",
	)
)

type StartWorkflowExecution struct {
	stamp.ActionActor[*model.WorkflowClient]
	stamp.ActionTarget[*model.WorkflowExecution]
	TaskQueue                *model.TaskQueue `validate:"required"`
	Input                    Payloads
	Identity                 stamp.Gen[string] // defaults to actor's identity
	WorkflowID               stamp.Gen[stamp.ID]
	WorkflowType             stamp.Gen[stamp.ID]
	WorkflowExecutionTimeout stamp.Gen[time.Duration]
	WorkflowRunTimeout       stamp.Gen[time.Duration]
	WorkflowTaskTimeout      stamp.Gen[time.Duration]
	RequestId                stamp.Gen[string] // not an ID as it is empty by default
	WorkflowIdReusePolicy    stamp.Gen[enumspb.WorkflowIdReusePolicy]
	WorkflowIdConflictPolicy stamp.Gen[enumspb.WorkflowIdConflictPolicy]
}

func (w StartWorkflowExecution) Next(ctx stamp.GenContext) *workflowservice.StartWorkflowExecutionRequest {
	return &workflowservice.StartWorkflowExecutionRequest{
		Namespace:  string(w.TaskQueue.GetNamespace().GetID()),
		WorkflowId: string(w.WorkflowID.Next(ctx.AllowRandom())),
		WorkflowType: &commonpb.WorkflowType{
			Name: string(w.WorkflowType.Next(ctx.AllowRandom())),
		},
		TaskQueue: &taskqueue.TaskQueue{
			Name: string(w.TaskQueue.GetID()),
		},
		Input:                    w.Input.Next(ctx),
		WorkflowExecutionTimeout: durationpb.New(w.WorkflowExecutionTimeout.Next(ctx)),
		WorkflowRunTimeout:       durationpb.New(w.WorkflowRunTimeout.Next(ctx)),
		WorkflowTaskTimeout:      durationpb.New(w.WorkflowTaskTimeout.Next(ctx)),
		Identity:                 w.Identity.NextOrDefault(ctx, string(w.GetActor().GetID())),
		RequestId:                w.RequestId.Next(ctx.AllowRandom()),
		WorkflowIdReusePolicy:    w.WorkflowIdReusePolicy.Next(ctx),
		WorkflowIdConflictPolicy: w.WorkflowIdConflictPolicy.Next(ctx),
	}
}

type GetWorkflowExecutionHistory struct {
	stamp.ActionActor[*model.WorkflowClient]
	stamp.ActionTarget[*model.WorkflowExecutionHistory]
	WorkflowExecution *model.WorkflowExecution `validate:"required"`
	// TODO ...
}

func (w GetWorkflowExecutionHistory) Next(ctx stamp.GenContext) *workflowservice.GetWorkflowExecutionHistoryRequest {
	return &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace: string(w.WorkflowExecution.GetNamespace().GetID()),
		Execution: w.WorkflowExecution.Next(ctx),
	}
}

type TerminateWorkflowExecution struct {
	stamp.ActionActor[*model.WorkflowClient]
	stamp.ActionTarget[*model.WorkflowExecution]
	WorkflowExecution *model.WorkflowExecution `validate:"required"`
	Identity          stamp.Gen[string]        // defaults to actor's identity
	Reason            stamp.Gen[string]
	// TODO ...
}

func (w TerminateWorkflowExecution) Next(ctx stamp.GenContext) *workflowservice.TerminateWorkflowExecutionRequest {
	return &workflowservice.TerminateWorkflowExecutionRequest{
		Namespace:         string(w.WorkflowExecution.GetNamespace().GetID()),
		WorkflowExecution: w.WorkflowExecution.Next(ctx),
		Identity:          w.Identity.NextOrDefault(ctx, string(w.GetActor().GetID())),
		Reason:            w.Reason.NextOrDefault(ctx, "<reason>"),
	}
}
