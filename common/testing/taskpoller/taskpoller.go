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

package taskpoller

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/service/history/consts"
)

type (
	TaskPoller struct {
		t         *testing.T
		logger    log.Logger
		client    workflowservice.WorkflowServiceClient
		namespace string
	}
	Options struct {
		ctx                 context.Context
		tv                  *testvars.TestVars
		maxRetries          int
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
			o.ctx, _ = rpc.NewContextWithTimeoutAndVersionHeaders(timeout * debug.TimeoutMultiplier)
		}
	}
	// WithPollSticky will make the poller use the sticky task queue instead of the normal task queue
	WithPollSticky OptionFunc = func(o *Options) {
		o.pollStickyTaskQueue = true
	}
	// WithoutPollRetries disables any polling retries
	WithoutPollRetries OptionFunc = func(o *Options) {
		o.maxRetries = 1
	}
)

func New(
	t *testing.T,
	client workflowservice.WorkflowServiceClient,
	namespace string,
	logger log.Logger,
) TaskPoller {
	return TaskPoller{
		t:         t,
		client:    client,
		namespace: namespace,
		logger:    logger,
	}
}

// PollWorkflowTask issues PollWorkflowTaskQueueRequests to obtain a new workflow task.
func (p *TaskPoller) PollWorkflowTask(
	tv *testvars.TestVars,
	funcs ...OptionFunc,
) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
	p.t.Helper()
	return p.pollWorkflowTask(newOptions(tv, funcs))
}

// PollAndHandleWorkflowTask issues PollWorkflowTaskQueueRequests to obtain a new workflow task,
// invokes the handler with the task, and completes/fails the task accordingly.
func (p *TaskPoller) PollAndHandleWorkflowTask(
	tv *testvars.TestVars,
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error),
	funcs ...OptionFunc,
) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	p.t.Helper()
	return p.pollAndProcessWorkflowTask(newOptions(tv, funcs), handler)
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
	return p.handleWorkflowTask(newOptions(tv, funcs), task, handler)
}

//revive:disable-next-line
func (p *TaskPoller) pollWorkflowTask(
	opts *Options,
) (*workflowservice.PollWorkflowTaskQueueResponse, error) {
	p.t.Helper()
	var attempt int
	for {
		select {
		case <-opts.ctx.Done():
			return nil, opts.ctx.Err()
		default:
			attempt++
			if opts.maxRetries > 0 && attempt-1 >= opts.maxRetries {
				return nil, fmt.Errorf("max retries (%v) exhausted", opts.maxRetries)
			}

			taskQueue := opts.tv.TaskQueue()
			if opts.pollStickyTaskQueue {
				taskQueue = opts.tv.StickyTaskQueue()
			}

			resp, err := p.client.PollWorkflowTaskQueue(
				opts.ctx,
				&workflowservice.PollWorkflowTaskQueueRequest{
					Namespace: p.namespace,
					TaskQueue: taskQueue,
					Identity:  opts.tv.WorkerIdentity(),
				})
			if err != nil {
				if common.IsServiceTransientError(err) {
					continue
				}
				if err == consts.ErrDuplicate {
					continue
				}
				return nil, err
			}
			if resp == nil || len(resp.TaskToken) == 0 {
				continue
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
					opts.ctx,
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
	}
}

func (p *TaskPoller) pollAndProcessWorkflowTask(
	opts *Options,
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error),
) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	p.t.Helper()
	task, err := p.pollWorkflowTask(opts)
	if err != nil {
		return nil, fmt.Errorf("failed to poll workflow task: %w", err)
	}
	return p.handleWorkflowTask(opts, task, handler)
}

func (p *TaskPoller) handleWorkflowTask(
	opts *Options,
	task *workflowservice.PollWorkflowTaskQueueResponse,
	handler func(task *workflowservice.PollWorkflowTaskQueueResponse) (*workflowservice.RespondWorkflowTaskCompletedRequest, error),
) (*workflowservice.RespondWorkflowTaskCompletedResponse, error) {
	p.t.Helper()
	reply, err := handler(task)
	if err != nil {
		return nil, p.respondWorkflowTaskFailed(opts, task.TaskToken, err)
	}

	resp, err := p.respondWorkflowTaskCompleted(opts, task, reply)
	if err != nil {
		return nil, err
	}

	return resp, nil
}

func (p *TaskPoller) respondWorkflowTaskCompleted(
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

	return p.client.RespondWorkflowTaskCompleted(opts.ctx, reply)
}

func (p *TaskPoller) respondWorkflowTaskFailed(
	opts *Options,
	taskToken []byte,
	taskErr error,
) error {
	p.t.Helper()
	_, err := p.client.RespondWorkflowTaskFailed(
		opts.ctx,
		&workflowservice.RespondWorkflowTaskFailedRequest{
			Namespace: p.namespace,
			TaskToken: taskToken,
			Cause:     enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE,
			Failure:   newApplicationFailure(taskErr, false, nil),
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

func newApplicationFailure(
	err error,
	nonRetryable bool,
	details *commonpb.Payloads,
) *failurepb.Failure {
	var applicationErr *temporal.ApplicationError
	if errors.As(err, &applicationErr) {
		nonRetryable = applicationErr.NonRetryable()
	}

	return &failurepb.Failure{
		Message: err.Error(),
		Source:  "Functional Tests",
		FailureInfo: &failurepb.Failure_ApplicationFailureInfo{ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
			Type:         getErrorType(err),
			NonRetryable: nonRetryable,
			Details:      details,
		}},
	}
}

func getErrorType(err error) string {
	var t reflect.Type
	for t = reflect.TypeOf(err); t.Kind() == reflect.Ptr; t = t.Elem() {
	}
	return t.Name()
}
