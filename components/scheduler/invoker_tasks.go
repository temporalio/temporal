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

package scheduler

import (
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/service/history/hsm"
)

type ProcessBufferTask struct {
	deadline time.Time
}

// EventEnqueue is fired to trigger immediate processing of queued
// BufferedStarts. The BufferedStarts field is appended to the Invoker's queue.
// For retrying execution, use EventRetry.
type EventEnqueue struct {
	Node *hsm.Node

	BufferedStarts []*schedulespb.BufferedStart
}

type processBufferResult struct {
	StartWorkflows     []*schedulespb.BufferedStart
	CancelWorkflows    []*commonpb.WorkflowExecution
	TerminateWorkflows []*commonpb.WorkflowExecution

	// DiscardStarts will be dropped from the Invoker's BufferedStarts without execution.
	DiscardStarts []*schedulespb.BufferedStart

	// Number of buffered starts dropped due to overlap policy during processing.
	OverlapSkipped int64

	// Nunmber of buffered starts dropped from missing the catchup window.
	MissedCatchupWindow int64

	// Number of buffered starts dropped due to the max buffer size having been exceeded.
	// TODO - set this from Generator/Backfiller
	BufferDropped int64
}

type executeResult struct {
	// Starts that executed successfully can be removed from the buffer.
	CompletedStarts []*schedulespb.BufferedStart

	// Starts that failed with a retryable error should be updated and kept in the buffer.
	RetryableStarts []*schedulespb.BufferedStart

	// Starts that failed with a non-retryable error can be removed from the buffer.
	FailedStarts []*schedulespb.BufferedStart

	CompletedCancels    []*commonpb.WorkflowExecution
	CompletedTerminates []*commonpb.WorkflowExecution
}

// Append combines two executeResults (no deduplication is done).
func (e executeResult) Append(o executeResult) executeResult {
	return executeResult{
		CompletedStarts:     append(e.CompletedStarts, o.CompletedStarts...),
		RetryableStarts:     append(e.RetryableStarts, o.RetryableStarts...),
		FailedStarts:        append(e.FailedStarts, o.FailedStarts...),
		CompletedCancels:    append(e.CompletedCancels, o.CompletedCancels...),
		CompletedTerminates: append(e.CompletedTerminates, o.CompletedTerminates...),
	}
}

type ExecuteTask struct{}

// EventFinishProcessing is fired by the Invoker after executing a ProcessBufferTask,
// updating the Invoker state.
//
// When this event is processed, BufferedStarts specified in StartWorkflows
// will have their Attempt count set to 1, allowing ExecuteTask to begin their
// execution.
type EventFinishProcessing struct {
	Node *hsm.Node

	LastProcessedTime time.Time
	processBufferResult
}

// EventWait is fired when the Invoker should return to a waiting state until
// more actions are buffered.
type EventWait struct {
	Node *hsm.Node

	LastProcessedTime time.Time
}

// EventRetryProcessing is fired when the Invoker should back off and retry failed
// executions. As with EventFinish, processBufferResult can be used to update
// Invoker state.
//
// The ProcessBufferTask will be delayed based on the earliest retryable start's
// backoff timer, which may be earlier than the start specified on this event's
// BufferedStart.
type EventRetryProcessing struct {
	Node *hsm.Node

	LastProcessedTime time.Time
	processBufferResult
}

// EventCompleteExecution is fired ExecuteTask completes. The Invoker's state is
// updated to remove completed items from the buffers, and update retry counters.
//
// See also EventRecordAction.
type EventCompleteExecution struct {
	Node *hsm.Node

	executeResult
}

const TaskTypeProcessBuffer = "scheduler.invoker.ProcessBuffer"
const TaskTypeExecute = "scheduler.invoker.Execute"

var _ hsm.Task = ProcessBufferTask{}
var _ hsm.Task = ExecuteTask{}

func (ExecuteTask) Type() string {
	return TaskTypeExecute
}

func (e ExecuteTask) Deadline() time.Time {
	return hsm.Immediate
}

func (ExecuteTask) Destination() string {
	return ""
}

func (ExecuteTask) Validate(_ *persistencespb.StateMachineRef, node *hsm.Node) error {
	return validateTaskTransition(node, TransitionFinishProcessing)
}

func (ProcessBufferTask) Type() string {
	return TaskTypeProcessBuffer
}

func (p ProcessBufferTask) Deadline() time.Time {
	return p.deadline
}

func (ProcessBufferTask) Destination() string {
	return ""
}

func (ProcessBufferTask) Validate(_ *persistencespb.StateMachineRef, node *hsm.Node) error {
	return validateTaskTransition(node, TransitionEnqueue)
}

func (i Invoker) tasks() (tasks []hsm.Task, err error) {
	// Add ProcessBufferTask for both backoff/processing states.
	switch i.State() { //nolint:exhaustive
	case enumsspb.SCHEDULER_INVOKER_STATE_BACKING_OFF:
		tasks = append(tasks, ProcessBufferTask{
			deadline: i.earliestRetryTime(),
		})
	case enumsspb.SCHEDULER_INVOKER_STATE_PROCESSING:
		tasks = append(tasks, ProcessBufferTask{
			deadline: hsm.Immediate,
		})
	}

	// Eligible buffer starts are those that are at Attempt > 0, and whose
	// BackoffTime is before the Invoker's LastProcessedTime. After the ProcessBuffer
	// timer task fires from the backoff deadline, the LastProcessedTime is updated,
	// and backing off starts can become eligible for an execution task.
	readyStarts := i.getEligibleBufferedStarts()

	// Add an ExecuteTask if any actions are pending execution.
	if len(i.CancelWorkflows) > 0 || len(i.TerminateWorkflows) > 0 || len(readyStarts) > 0 {
		tasks = append(tasks, ExecuteTask{})
	}

	return tasks, nil
}

func (i Invoker) output() (hsm.TransitionOutput, error) {
	tasks, err := i.tasks()
	if err != nil {
		return hsm.TransitionOutput{}, err
	}
	return hsm.TransitionOutput{Tasks: tasks}, nil
}
