// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package taskpoller

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/testing/testvars"
)

type (
	TaskPoller struct {
		t         *testing.T
		client    workflowservice.WorkflowServiceClient
		namespace string
	}
	Options struct {
		tv                  *testvars.TestVars
		timeout             time.Duration
		pollStickyTaskQueue bool
	}
	// OptionFunc is a function to change an Options instance
	OptionFunc func(*Options)
)

var (
	// DrainWorkflowTask returns an empty RespondWorkflowTaskCompletedRequest response
	DrainWorkflowTask = func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error) {
		return &workflowservice.RespondWorkflowTaskCompletedRequest{}, nil
	}
	// WithTimeout defines a timeout for a task poller method (includes *all* RPC calls it has to make)
	WithTimeout = func(timeout time.Duration) OptionFunc {
		return func(o *Options) {
			o.timeout = timeout
		}
	}
	// WithPollSticky will make the poller use the sticky task queue instead of the normal task queue
	WithPollSticky OptionFunc = func(o *Options) {
		o.pollStickyTaskQueue = true
	}
)

func New(
	t *testing.T,
	client workflowservice.WorkflowServiceClient,
	namespace string,
) TaskPoller {
	return TaskPoller{
		t:         t,
		client:    client,
		namespace: namespace,
	}
}

// PollWorkflowTask issues PollWorkflowTaskQueueRequests to obtain a new workflow task.
func (p *TaskPoller) PollWorkflowTask(
	tv *testvars.TestVars,
	funcs ...OptionFunc,
) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
	p.t.Helper()
	options := newOptions(tv, funcs)
	ctx, cancel := newContext(options)
	defer cancel()
	return p.pollWorkflowTask(ctx, options)
}

// PollAndHandleWorkflowTask issues PollWorkflowTaskQueueRequests to obtain a new workflow task,
// invokes the handler with the task, and completes/fails the task accordingly.
func (p *TaskPoller) PollAndHandleWorkflowTask(
	tv *testvars.TestVars,
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error),
	funcs ...OptionFunc,
) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	p.t.Helper()
	options := newOptions(tv, funcs)
	ctx, cancel := newContext(options)
	defer cancel()
	return p.pollAndHandleWorkflowTask(ctx, options, handler)
}

// HandleWorkflowTask invokes the provided handler with the provided task,
// and completes/fails the task accordingly.
func (p *TaskPoller) HandleWorkflowTask(
	tv *testvars.TestVars,
	task *workflowservice.PollWorkflowTaskQueueResponse,
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error),
	funcs ...OptionFunc,
) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	p.t.Helper()
	options := newOptions(tv, funcs)
	ctx, cancel := newContext(options)
	defer cancel()
	return p.handleWorkflowTask(ctx, options, task, handler)
}

//revive:disable-next-line:cognitive-complexity
func (p *TaskPoller) pollWorkflowTask(
	ctx context.Context,
	opts *Options,
) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
	p.t.Helper()
	taskQueue := opts.tv.TaskQueue()
	if opts.pollStickyTaskQueue {
		taskQueue = opts.tv.StickyTaskQueue()
	}

	resp, err := p.client.PollWorkflowTaskQueue(
		ctx,
		&workflowservice.PollWorkflowTaskQueueRequest{
			Namespace: p.namespace,
			TaskQueue: taskQueue,
			Identity:  opts.tv.WorkerIdentity(),
		})
	if err != nil {
		return nil, err
	}
	if resp == nil || len(resp.TaskToken) == 0 {
		return nil, errors.New("no task available")
	}

	var events []*historypb.HistoryEvent
	history := resp.History
	if history == nil {
		return nil, errors.New("history is nil")
	}

	events = history.Events
	if len(events) == 0 {
		return nil, errors.New("history events are empty")
	}

	nextPageToken := resp.NextPageToken
	for nextPageToken != nil {
		resp, err := p.client.GetWorkflowExecutionHistory(
			ctx,
			&workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace:     p.namespace,
				Execution:     resp.WorkflowExecution,
				NextPageToken: nextPageToken,
			})
		if err != nil {
			return nil, err
		}
		events = append(events, resp.History.Events...)
		nextPageToken = resp.NextPageToken
	}

	return resp, err
}

func (p *TaskPoller) pollAndHandleWorkflowTask(
	ctx context.Context,
	opts *Options,
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error),
) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	p.t.Helper()
	task, err := p.pollWorkflowTask(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("failed to poll workflow task: %w", err)
	}
	return p.handleWorkflowTask(ctx, opts, task, handler)
}

func (p *TaskPoller) handleWorkflowTask(
	ctx context.Context,
	opts *Options,
	task *workflowservice.PollWorkflowTaskQueueResponse,
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error),
) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	p.t.Helper()
	reply, err := handler(task)
	if err != nil {
		return nil, p.respondWorkflowTaskFailed(ctx, opts, task.TaskToken, err)
	}

	resp, err := p.respondWorkflowTaskCompleted(ctx, opts, task, reply)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (p *TaskPoller) respondWorkflowTaskCompleted(
	ctx context.Context,
	opts *Options,
	task *workflowservice.PollWorkflowTaskQueueResponse,
	reply *workflowservice.RespondWorkflowTaskCompletedRequest,
) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	p.t.Helper()
	if reply == nil {
		return nil, errors.New("missing RespondWorkflowTaskCompletedRequest return")
	}
	if reply.Namespace == "" {
		reply.Namespace = p.namespace
	}
	if len(reply.TaskToken) == 0 {
		reply.TaskToken = task.TaskToken
	}
	if reply.Identity == "" {
		reply.Identity = opts.tv.WorkerIdentity()
	}
	reply.ReturnNewWorkflowTask = true

	return p.client.RespondWorkflowTaskCompleted(ctx, reply)
}

func (p *TaskPoller) respondWorkflowTaskFailed(
	ctx context.Context,
	opts *Options,
	taskToken []byte,
	taskErr error,
) error {
	p.t.Helper()
	_, err := p.client.RespondWorkflowTaskFailed(
		ctx,
		&workflowservice.RespondWorkflowTaskFailedRequest{
			Namespace: p.namespace,
			TaskToken: taskToken,
			Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
			Failure:   temporal.GetDefaultFailureConverter().ErrorToFailure(taskErr),
			Identity:  opts.tv.WorkerIdentity(),
		})
	return err
}

func newOptions(
	tv *testvars.TestVars,
	funcs []OptionFunc,
) *Options {
	res := &Options{
		tv: tv,
	}

	// default options
	WithTimeout(10 * time.Second)(res)

	// custom options
	for _, f := range funcs {
		f(res)
	}

	return res
}

func newContext(opts *Options) (context.Context, context.CancelFunc) {
	return rpc.NewContextWithTimeoutAndVersionHeaders(opts.timeout * debug.TimeoutMultiplier)
}
