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
	"fmt"
	"time"

	commonpb "go.temporal.io/api/common/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/service/history/hsm"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	// The Invoker sub state machine is responsible for executing buffered actions.
	Invoker struct {
		*schedulespb.InvokerInternal
	}

	// The machine definition provides serialization/deserialization and type information.
	invokerMachineDefinition struct{}
)

const (
	// Unique identifier for the Invoker sub state machine.
	InvokerMachineType = "scheduler.Invoker"
)

var (
	_ hsm.StateMachine[enumsspb.SchedulerInvokerState] = Invoker{}
	_ hsm.StateMachineDefinition                       = &invokerMachineDefinition{}

	// Each sub state machine is a singleton of the top-level Scheduler, accessed with
	// a fixed key.
	InvokerMachineKey = hsm.Key{Type: InvokerMachineType, ID: ""}
)

// NewInvoker returns an intialized Invoker sub state machine, which should
// be parented under a Scheduler root node.
func NewInvoker() *Invoker {
	return &Invoker{
		InvokerInternal: &schedulespb.InvokerInternal{
			State:          enumsspb.SCHEDULER_INVOKER_STATE_WAITING,
			BufferedStarts: []*schedulespb.BufferedStart{},
		},
	}
}

func (i Invoker) State() enumsspb.SchedulerInvokerState {
	return i.InvokerInternal.State
}

func (i Invoker) SetState(state enumsspb.SchedulerInvokerState) {
	i.InvokerInternal.State = state
}

func (i Invoker) RegenerateTasks(node *hsm.Node) ([]hsm.Task, error) {
	return i.tasks()
}

func (invokerMachineDefinition) Type() string {
	return InvokerMachineType
}

func (invokerMachineDefinition) Serialize(state any) ([]byte, error) {
	if state, ok := state.(Invoker); ok {
		return proto.Marshal(state.InvokerInternal)
	}
	return nil, fmt.Errorf("invalid Invoker state provided: %v", state)
}

func (invokerMachineDefinition) Deserialize(body []byte) (any, error) {
	state := &schedulespb.InvokerInternal{}
	return Invoker{state}, proto.Unmarshal(body, state)
}

func (invokerMachineDefinition) CompareState(a any, b any) (int, error) {
	panic("TODO: CompareState not yet implemented for Invoker")
}

// TransitionEnqueue adds buffered starts to the Invoker's queue.
var TransitionEnqueue = hsm.NewTransition(
	[]enumsspb.SchedulerInvokerState{
		enumsspb.SCHEDULER_INVOKER_STATE_UNSPECIFIED,
		enumsspb.SCHEDULER_INVOKER_STATE_WAITING,
		enumsspb.SCHEDULER_INVOKER_STATE_PROCESSING,
		enumsspb.SCHEDULER_INVOKER_STATE_BACKING_OFF,
	},
	enumsspb.SCHEDULER_INVOKER_STATE_PROCESSING,
	func(i Invoker, event EventEnqueue) (hsm.TransitionOutput, error) {
		i.BufferedStarts = append(i.GetBufferedStarts(), event.BufferedStarts...)
		return i.output()
	},
)

// TransitionCompleteExecution records actions completed during an ExecuteTask by
// updating the Invoker's internal state.
//
// See also TransitionRecordAction on Scheduler.
var TransitionCompleteExecution = hsm.NewTransition(
	[]enumsspb.SchedulerInvokerState{
		enumsspb.SCHEDULER_INVOKER_STATE_WAITING,
		enumsspb.SCHEDULER_INVOKER_STATE_PROCESSING,
		enumsspb.SCHEDULER_INVOKER_STATE_BACKING_OFF,
	},
	// We always move to BackingOff to make the backoff timer effective immediately
	// on transition (before more Execute tasks can be queued). If no starts are backing
	// off, the ProcessBuffer task will immediately fire and move back to Waiting.
	enumsspb.SCHEDULER_INVOKER_STATE_BACKING_OFF,
	func(i Invoker, event EventCompleteExecution) (hsm.TransitionOutput, error) {
		i.recordExecution(&event.executeResult)
		return i.output()
	},
)

// TransitionFinishProcessing records the results of processing the buffer to the
// Invoker's internal state. Any BufferedStarts on StartWorkflows will also be
// removed from the Invoker's BufferedStarts field. Use when no buffered starts need
// to be retried.
var TransitionFinishProcessing = hsm.NewTransition(
	[]enumsspb.SchedulerInvokerState{
		enumsspb.SCHEDULER_INVOKER_STATE_PROCESSING,
		enumsspb.SCHEDULER_INVOKER_STATE_BACKING_OFF,
	},
	enumsspb.SCHEDULER_INVOKER_STATE_WAITING,
	func(i Invoker, event EventFinishProcessing) (hsm.TransitionOutput, error) {
		i.LastProcessedTime = timestamppb.New(event.LastProcessedTime)
		i.update(&event.processBufferResult)
		return i.output()
	},
)

// TransitionRetryProcessing is similar to TransitionFinishProcessing, except
// it moves the Invoker into the BACKING_OFF state to retry failed tasks after
// a delay. The event's ProcessBufferResult field is used to kick off immediate
// tasks corresponding to requested actions, if any are set.
var TransitionRetryProcessing = hsm.NewTransition(
	[]enumsspb.SchedulerInvokerState{
		enumsspb.SCHEDULER_INVOKER_STATE_WAITING, // A service call could fail after the Invoker goes idle.
		enumsspb.SCHEDULER_INVOKER_STATE_PROCESSING,
		enumsspb.SCHEDULER_INVOKER_STATE_BACKING_OFF,
	},
	enumsspb.SCHEDULER_INVOKER_STATE_BACKING_OFF,
	func(i Invoker, event EventRetryProcessing) (hsm.TransitionOutput, error) {
		i.LastProcessedTime = timestamppb.New(event.LastProcessedTime)
		i.update(&event.processBufferResult)
		return i.output()
	},
)

// TransitionWait moves the Invoker to waiting state. No new tasks will be
// created until the Invoker's state changes.
var TransitionWait = hsm.NewTransition(
	[]enumsspb.SchedulerInvokerState{
		enumsspb.SCHEDULER_INVOKER_STATE_UNSPECIFIED,
		enumsspb.SCHEDULER_INVOKER_STATE_WAITING,
		enumsspb.SCHEDULER_INVOKER_STATE_PROCESSING,
	},
	enumsspb.SCHEDULER_INVOKER_STATE_WAITING,
	func(i Invoker, event EventWait) (hsm.TransitionOutput, error) {
		i.LastProcessedTime = timestamppb.New(event.LastProcessedTime)
		return i.output()
	},
)

// update updates the Invoker's internal state based on result. Should be
// called only during transitions.
func (i Invoker) update(result *processBufferResult) {
	discards := make(map[string]bool) // request ID -> is present
	ready := make(map[string]bool)
	for _, start := range result.DiscardStarts {
		discards[start.RequestId] = true
	}
	for _, start := range result.StartWorkflows {
		ready[start.RequestId] = true
	}

	// Drop discarded starts, and update requested starts for execution.
	var starts []*schedulespb.BufferedStart
	for _, start := range i.GetBufferedStarts() {
		if discards[start.RequestId] {
			continue
		}

		// Starts ready for execution are set to their first attempt.
		if ready[start.RequestId] && start.Attempt < 1 {
			start.Attempt = 1
		}

		starts = append(starts, start)
	}

	// Update internal state.
	i.InvokerInternal.BufferedStarts = starts
	i.InvokerInternal.CancelWorkflows = append(i.GetCancelWorkflows(), result.CancelWorkflows...)
	i.InvokerInternal.TerminateWorkflows = append(i.GetTerminateWorkflows(), result.TerminateWorkflows...)
}

func (i Invoker) recordExecution(result *executeResult) {
	completed := make(map[string]bool)                       // request ID -> is present
	failed := make(map[string]bool)                          // request ID -> is present
	retryable := make(map[string]*schedulespb.BufferedStart) // request ID -> *BufferedStart
	canceled := make(map[string]bool)                        // run ID -> is present
	terminated := make(map[string]bool)                      // run ID -> is present

	for _, start := range result.CompletedStarts {
		completed[start.RequestId] = true
	}
	for _, start := range result.FailedStarts {
		failed[start.RequestId] = true
	}
	for _, start := range result.RetryableStarts {
		retryable[start.RequestId] = start
	}
	for _, wf := range result.CompletedCancels {
		canceled[wf.RunId] = true
	}
	for _, wf := range result.CompletedTerminates {
		terminated[wf.RunId] = true
	}

	// Update Invoker state to remove completed items from their buffers.
	i.InvokerInternal.BufferedStarts = util.FilterSlice(i.GetBufferedStarts(), func(start *schedulespb.BufferedStart) bool {
		return !completed[start.RequestId] && !failed[start.RequestId]
	})
	i.InvokerInternal.CancelWorkflows = util.FilterSlice(i.GetCancelWorkflows(), func(we *commonpb.WorkflowExecution) bool {
		return !canceled[we.RunId]
	})
	i.InvokerInternal.TerminateWorkflows = util.FilterSlice(i.GetTerminateWorkflows(), func(we *commonpb.WorkflowExecution) bool {
		return !terminated[we.RunId]
	})

	// Update attempt counts and backoffs for failed/retrying starts.
	for _, start := range i.GetBufferedStarts() {
		if retry, ok := retryable[start.RequestId]; ok {
			start.Attempt++
			start.BackoffTime = retry.GetBackoffTime()
		}
	}
}

// EarliestRetry returns the earliest possible time an attempted BufferedStart
// in the queue can be retried. BufferedStarts that have not yet been attempted are
// ignored. If no BufferedStarts are retrying, the return value will be Time's zero
// value.
func (i Invoker) earliestRetryTime() time.Time {
	var deadline time.Time
	for _, start := range i.GetBufferedStarts() {
		if start.GetAttempt() == 0 {
			continue
		}
		backoff := start.GetBackoffTime().AsTime()
		if deadline.IsZero() || backoff.Before(deadline) {
			deadline = backoff
		}
	}
	return deadline
}

// getEligibleBufferedStarts returns all BufferedStarts that are marked for
// execution (Attempt > 0), and aren't presently backing off, based on last
// processed time.
func (i Invoker) getEligibleBufferedStarts() []*schedulespb.BufferedStart {
	return util.FilterSlice(i.GetBufferedStarts(), func(start *schedulespb.BufferedStart) bool {
		return start.Attempt > 0 && start.BackoffTime.AsTime().Before(i.GetLastProcessedTime().AsTime())
	})
}
