// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package nexusoperations

import (
	"errors"
	"fmt"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/service/history/hsm"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// OperationMachineType is a unique type identifier for the Operation state machine.
	OperationMachineType = "nexusoperations.Operation"

	// CancelationMachineType is a unique type identifier for the Cancelation state machine.
	CancelationMachineType = "nexusoperations.Cancelation"

	// A marker for the first return value from a progress() that indicates the machine is in a terminal state.
	// TODO: Remove this once transition history is fully implemented.
	terminalStage = 3
)

// CancelationMachineKey is a fixed key for the cancelation machine as a child of the operation machine.
var CancelationMachineKey = hsm.Key{Type: CancelationMachineType, ID: ""}

// MachineCollection creates a new typed [statemachines.Collection] for operations.
func MachineCollection(tree *hsm.Node) hsm.Collection[Operation] {
	return hsm.NewCollection[Operation](tree, OperationMachineType)
}

// Operation state machine.
type Operation struct {
	*persistencespb.NexusOperationInfo
}

// AddChild adds a new operation child machine to the given node and transitions it to the SCHEDULED state.
func AddChild(node *hsm.Node, id string, event *historypb.HistoryEvent, eventToken []byte, deleteOnCompletion bool) (*hsm.Node, error) {
	attrs := event.GetNexusOperationScheduledEventAttributes()

	node, err := node.AddChild(hsm.Key{Type: OperationMachineType, ID: id}, Operation{
		&persistencespb.NexusOperationInfo{
			EndpointId:             attrs.EndpointId,
			Endpoint:               attrs.Endpoint,
			Service:                attrs.Service,
			Operation:              attrs.Operation,
			ScheduledTime:          event.EventTime,
			ScheduleToCloseTimeout: attrs.ScheduleToCloseTimeout,
			RequestId:              attrs.RequestId,
			State:                  enumsspb.NEXUS_OPERATION_STATE_UNSPECIFIED,
			// TODO(bergundy): actually delete on completion if this is set.
			DeleteOnCompletion:  deleteOnCompletion,
			ScheduledEventToken: eventToken,
		},
	})

	if err != nil {
		return nil, err
	}

	return node, hsm.MachineTransition(node, func(op Operation) (hsm.TransitionOutput, error) {
		output, err := TransitionScheduled.Apply(op, EventScheduled{Node: node})
		if err != nil {
			return output, err
		}
		creationTasks, err := op.creationTasks(node)
		if err != nil {
			return output, err
		}
		output.Tasks = append(output.Tasks, creationTasks...)
		return output, err
	})
}

func (o Operation) State() enumsspb.NexusOperationState {
	return o.NexusOperationInfo.State
}

func (o Operation) SetState(state enumsspb.NexusOperationState) {
	o.NexusOperationInfo.State = state
}

func (o Operation) recordAttempt(ts time.Time) {
	o.NexusOperationInfo.Attempt++
	o.NexusOperationInfo.LastAttemptCompleteTime = timestamppb.New(ts)
	o.NexusOperationInfo.LastAttemptFailure = nil
}

func (o Operation) cancelRequested(node *hsm.Node) (bool, error) {
	_, err := node.Child([]hsm.Key{CancelationMachineKey})
	if err == nil {
		return true, nil
	}
	if errors.Is(err, hsm.ErrStateMachineNotFound) {
		return false, nil
	}
	return false, err
}

func (o Operation) Cancelation(node *hsm.Node) (*Cancelation, error) {
	child, err := node.Child([]hsm.Key{CancelationMachineKey})
	if errors.Is(err, hsm.ErrStateMachineNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	cancelation, err := hsm.MachineData[Cancelation](child)
	return &cancelation, err
}

func (o Operation) CancelationNode(node *hsm.Node) (*hsm.Node, error) {
	child, err := node.Child([]hsm.Key{CancelationMachineKey})
	if errors.Is(err, hsm.ErrStateMachineNotFound) {
		return nil, nil
	}
	return child, err
}

// transitionTasks returns tasks that are emitted as transition outputs.
func (o Operation) transitionTasks() ([]hsm.Task, error) {
	switch o.State() { // nolint:exhaustive
	case enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF:
		return []hsm.Task{BackoffTask{deadline: o.NextAttemptScheduleTime.AsTime()}}, nil
	case enumsspb.NEXUS_OPERATION_STATE_SCHEDULED:
		return []hsm.Task{InvocationTask{EndpointName: o.Endpoint}}, nil
	default:
		return nil, nil
	}
}

// creationTasks returns tasks that are emitted when the machine is created.
func (o Operation) creationTasks(node *hsm.Node) ([]hsm.Task, error) {
	if canceled, err := o.cancelRequested(node); canceled || err != nil {
		return nil, err
	}

	if o.ScheduleToCloseTimeout.AsDuration() != 0 {
		return []hsm.Task{TimeoutTask{deadline: o.ScheduledTime.AsTime().Add(o.ScheduleToCloseTimeout.AsDuration())}}, nil
	}
	return nil, nil
}

func (o Operation) RegenerateTasks(prevData any, node *hsm.Node) ([]hsm.Task, error) {
	tasks, err := o.transitionTasks()
	if err != nil {
		return nil, err
	}
	if prevData == nil {
		creationTasks, err := o.creationTasks(node)
		if err != nil {
			return nil, err
		}
		tasks = append(tasks, creationTasks...)
	}
	return tasks, nil
}

func (o Operation) output() (hsm.TransitionOutput, error) {
	tasks, err := o.transitionTasks()
	if err != nil {
		return hsm.TransitionOutput{}, err
	}
	return hsm.TransitionOutput{Tasks: tasks}, nil
}

type operationMachineDefinition struct{}

func (operationMachineDefinition) Type() string {
	return OperationMachineType
}

func (operationMachineDefinition) Deserialize(d []byte) (any, error) {
	info := &persistencespb.NexusOperationInfo{}
	return Operation{info}, proto.Unmarshal(d, info)
}

func (operationMachineDefinition) Serialize(state any) ([]byte, error) {
	if state, ok := state.(Operation); ok {
		return proto.Marshal(state.NexusOperationInfo)
	}
	return nil, fmt.Errorf("invalid operation provided: %v", state) // nolint:goerr113
}

// CompareState compares the progress of two Operation state machines to determine whether to sync machine state while
// processing a replication task.
// TODO: Remove this implementation once transition history is fully implemented.
func (operationMachineDefinition) CompareState(state1, state2 any) (int, error) {
	o1, ok := state1.(Operation)
	if !ok {
		return 0, fmt.Errorf("%w: expected state1 to be a Operation instance, got %v", hsm.ErrIncompatibleType, state1)
	}
	o2, ok := state2.(Operation)
	if !ok {
		return 0, fmt.Errorf("%w: expected state2 to be a Operation instance, got %v", hsm.ErrIncompatibleType, state2)
	}

	stage1, attempts1, err := o1.progress()
	if err != nil {
		return 0, fmt.Errorf("failed to get progress for state1: %w", err)
	}
	stage2, attempts2, err := o2.progress()
	if err != nil {
		return 0, fmt.Errorf("failed to get progress for state2: %w", err)
	}
	if stage1 != stage2 {
		return stage1 - stage2, nil
	}
	if stage1 == terminalStage && o1.State() != o2.State() {
		return 0, serviceerror.NewInvalidArgument(fmt.Sprintf("cannot compare two distinct terminal states: %v, %v", o1.State(), o2.State()))
	}
	return int(attempts1 - attempts2), nil
}

// CompletionSource is an enum specifying where an operation completion originated from.
type CompletionSource int

const (
	// CompletionSourceUnspecified indicates that the source is unspecified (e.g. when reapplying a history event that
	// doesn't record this information).
	CompletionSourceUnspecified = CompletionSource(iota)
	// CompletionSourceResponse indicates that a completion came synchronously from a response to a StartOperation
	// request.
	CompletionSourceResponse
	// CompletionSourceResponse indicates that a completion came asynchronously from a callback.
	CompletionSourceCallback
	// CompletionSourceCancelRequested indicates that the operation was canceled due to workflow cancelation request.
	CompletionSourceCancelRequested
)

// EventScheduled is triggered when the operation is meant to be scheduled - immediately after initialization.
type EventScheduled struct {
	Node *hsm.Node
}

var TransitionScheduled = hsm.NewTransition(
	[]enumsspb.NexusOperationState{enumsspb.NEXUS_OPERATION_STATE_UNSPECIFIED},
	enumsspb.NEXUS_OPERATION_STATE_SCHEDULED,
	func(op Operation, event EventScheduled) (hsm.TransitionOutput, error) {
		return op.output()
	},
)

// EventRescheduled is triggered when the operation is meant to be rescheduled after backing off from a previous
// attempt.
type EventRescheduled struct {
	Node *hsm.Node
}

var TransitionRescheduled = hsm.NewTransition(
	[]enumsspb.NexusOperationState{enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF},
	enumsspb.NEXUS_OPERATION_STATE_SCHEDULED,
	func(op Operation, event EventRescheduled) (hsm.TransitionOutput, error) {
		op.NextAttemptScheduleTime = nil
		return op.output()
	},
)

// EventAttemptFailed is triggered when an invocation attempt is failed with a retryable error.
type EventAttemptFailed struct {
	Time        time.Time
	Err         error
	Node        *hsm.Node
	RetryPolicy backoff.RetryPolicy
}

var TransitionAttemptFailed = hsm.NewTransition(
	[]enumsspb.NexusOperationState{enumsspb.NEXUS_OPERATION_STATE_SCHEDULED},
	enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF,
	func(op Operation, event EventAttemptFailed) (hsm.TransitionOutput, error) {
		op.recordAttempt(event.Time)
		// Use 0 for elapsed time as we don't limit the retry by time (for now).
		nextDelay := event.RetryPolicy.ComputeNextDelay(0, int(op.Attempt), event.Err)
		nextAttemptScheduleTime := event.Time.Add(nextDelay)
		op.NextAttemptScheduleTime = timestamppb.New(nextAttemptScheduleTime)
		op.LastAttemptFailure = &failurepb.Failure{
			Message: event.Err.Error(),
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable: false,
				},
			},
		}
		return op.output()
	},
)

// EventFailed is triggered when an invocation attempt is failed with a non retryable error.
type EventFailed struct {
	Time             time.Time
	Node             *hsm.Node
	Attributes       *historypb.NexusOperationFailedEventAttributes
	CompletionSource CompletionSource
}

var TransitionFailed = hsm.NewTransition(
	[]enumsspb.NexusOperationState{
		enumsspb.NEXUS_OPERATION_STATE_SCHEDULED,
		enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF,
		enumsspb.NEXUS_OPERATION_STATE_STARTED,
	},
	enumsspb.NEXUS_OPERATION_STATE_FAILED,
	func(op Operation, event EventFailed) (hsm.TransitionOutput, error) {
		// When reapplying history, assume that if we transition from the SCHEDULED state the completion comes from a
		// response to a StartOpration request.  This may not be the case if a completion comes in before a response to
		// the request but we ignore that detail for simplicity.
		if event.CompletionSource == CompletionSourceResponse ||
			event.CompletionSource == CompletionSourceUnspecified && op.State() == enumsspb.NEXUS_OPERATION_STATE_SCHEDULED {
			op.recordAttempt(event.Time)
			op.LastAttemptFailure = &failurepb.Failure{
				// The top level failure in the event is just a wrapper for the actual cause.
				Message: event.Attributes.GetFailure().GetCause().GetMessage(),
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
						NonRetryable: true,
					},
				},
			}
		}
		// Keep last attempt information as-is for debuggability when completed asynchronously.
		// When used in a workflow, this machine node will be deleted from the tree after this transition.
		return op.output()
	},
)

// EventSucceeded is triggered when an invocation attempt succeeds.
type EventSucceeded struct {
	// Only set if the operation completed synchronously, as a response to a StartOperation RPC.
	Time             time.Time
	Node             *hsm.Node
	CompletionSource CompletionSource
}

var TransitionSucceeded = hsm.NewTransition(
	[]enumsspb.NexusOperationState{
		enumsspb.NEXUS_OPERATION_STATE_SCHEDULED,
		enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF,
		enumsspb.NEXUS_OPERATION_STATE_STARTED,
	},
	enumsspb.NEXUS_OPERATION_STATE_SUCCEEDED,
	func(op Operation, event EventSucceeded) (hsm.TransitionOutput, error) {
		// When reapplying history, assume that if we transition from the SCHEDULED state the completion comes from a
		// response to a StartOpration request.  This may not be the case if a completion comes in before a response to
		// the request but we ignore that detail for simplicity.
		if event.CompletionSource == CompletionSourceResponse ||
			event.CompletionSource == CompletionSourceUnspecified && op.State() == enumsspb.NEXUS_OPERATION_STATE_SCHEDULED {
			op.recordAttempt(event.Time)
		}
		// Keep last attempt information as-is for debuggability when completed asynchronously.
		// When used in a workflow, this machine node will be deleted from the tree after this transition.
		return op.output()
	},
)

// EventCanceled is triggered when an invocation attempt succeeds.
type EventCanceled struct {
	Time             time.Time
	Node             *hsm.Node
	CompletionSource CompletionSource
}

var TransitionCanceled = hsm.NewTransition(
	[]enumsspb.NexusOperationState{
		enumsspb.NEXUS_OPERATION_STATE_SCHEDULED,
		enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF,
		enumsspb.NEXUS_OPERATION_STATE_STARTED,
	},
	enumsspb.NEXUS_OPERATION_STATE_CANCELED,
	func(op Operation, event EventCanceled) (hsm.TransitionOutput, error) {
		// When reapplying history, assume that if we transition from the SCHEDULED state the completion comes from a
		// response to a StartOpration request.  This may not be the case if a completion comes in before a response to
		// the request but we ignore that detail for simplicity.
		if event.CompletionSource == CompletionSourceResponse ||
			// TODO: we'll never be in SCHEDULED state here, the state changes before calling the apply function.
			event.CompletionSource == CompletionSourceUnspecified && op.State() == enumsspb.NEXUS_OPERATION_STATE_SCHEDULED {
			op.recordAttempt(event.Time)
			op.LastAttemptFailure = nil
		}
		// Keep last attempt information as-is for debuggability when completed asynchronously.
		// When used in a workflow, this machine node will be deleted from the tree after this transition.
		return op.output()
	},
)

// EventStarted is triggered when an invocation attempt succeeds and the handler indicates that it started an
// asynchronous operation.
type EventStarted struct {
	Time       time.Time
	Node       *hsm.Node
	Attributes *historypb.NexusOperationStartedEventAttributes
}

var TransitionStarted = hsm.NewTransition(
	[]enumsspb.NexusOperationState{enumsspb.NEXUS_OPERATION_STATE_SCHEDULED, enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF},
	enumsspb.NEXUS_OPERATION_STATE_STARTED,
	func(op Operation, event EventStarted) (hsm.TransitionOutput, error) {
		op.recordAttempt(event.Time)
		op.OperationId = event.Attributes.OperationId

		// If cancelation is requested already, schedule sending the cancelation request.
		child, err := op.CancelationNode(event.Node)
		if err != nil {
			return hsm.TransitionOutput{}, err
		}
		if child != nil {
			return hsm.TransitionOutput{}, hsm.MachineTransition(child, func(c Cancelation) (hsm.TransitionOutput, error) {
				return TransitionCancelationScheduled.Apply(c, EventCancelationScheduled{
					Time: event.Time,
					Node: child,
				})
			})
		}
		return op.output()
	},
)

// EventTimedOut is triggered when the schedule-to-close timeout is triggered for an operation.
type EventTimedOut struct {
	Node *hsm.Node
}

var TransitionTimedOut = hsm.NewTransition(
	[]enumsspb.NexusOperationState{
		enumsspb.NEXUS_OPERATION_STATE_SCHEDULED,
		enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF,
		enumsspb.NEXUS_OPERATION_STATE_STARTED,
	},
	enumsspb.NEXUS_OPERATION_STATE_TIMED_OUT,
	func(op Operation, event EventTimedOut) (hsm.TransitionOutput, error) {
		// Keep attempt information as-is for debuggability.
		// When used in a workflow, this machine node will be deleted from the tree after this transition.
		return op.output()
	},
)

// Cancel marks the Operation machine as canceled by spawning a child Cancelation machine and transitioning the child to
// the SCHEDULED state.
func (o Operation) Cancel(node *hsm.Node, t time.Time) (hsm.TransitionOutput, error) {
	child, err := node.AddChild(CancelationMachineKey, Cancelation{
		NexusOperationCancellationInfo: &persistencespb.NexusOperationCancellationInfo{},
	})
	if err != nil {
		// This function should be called as part of command/event handling and it should not called more than once.
		return hsm.TransitionOutput{}, err
	}
	// Operation wasn't started yet, we don't know how to cancel it ATM.
	// TODO(bergundy): Support cancel-before-started.
	if o.OperationId == "" {
		// Don't schedule the cancelation yet. We may schedule it again once the operation is started.
		return hsm.TransitionOutput{}, nil
	}
	return hsm.TransitionOutput{}, hsm.MachineTransition(child, func(c Cancelation) (hsm.TransitionOutput, error) {
		return TransitionCancelationScheduled.Apply(c, EventCancelationScheduled{
			Time: t,
			Node: child,
		})
	})
}

// TODO: Remove this implementation once transition history is fully implemented.
func (o Operation) progress() (int, int32, error) {
	switch o.State() {
	case enumsspb.NEXUS_OPERATION_STATE_UNSPECIFIED:
		return 0, 0, serviceerror.NewInvalidArgument("uninitialized operation state")
	case enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF:
		return 1, o.GetAttempt() * 2, nil
	case enumsspb.NEXUS_OPERATION_STATE_SCHEDULED:
		// We've made slightly more progress if we transitioned from backing off to scheduled.
		return 1, o.GetAttempt()*2 + 1, nil
	case enumsspb.NEXUS_OPERATION_STATE_STARTED:
		return 2, 0, nil
	case enumsspb.NEXUS_OPERATION_STATE_TIMED_OUT,
		enumsspb.NEXUS_OPERATION_STATE_FAILED,
		enumsspb.NEXUS_OPERATION_STATE_CANCELED,
		enumsspb.NEXUS_OPERATION_STATE_SUCCEEDED:
		// Consider any terminal state as "max progress", we'll rely on last update namespace failover version to break
		// the tie when comparing two states.
		return terminalStage, 0, nil
	default:
		return 0, 0, serviceerror.NewInvalidArgument("unknown operation state")
	}
}

type cancelationMachineDefinition struct{}

func (cancelationMachineDefinition) Deserialize(d []byte) (any, error) {
	info := &persistencespb.NexusOperationCancellationInfo{}
	return Cancelation{info}, proto.Unmarshal(d, info)
}

func (cancelationMachineDefinition) Serialize(state any) ([]byte, error) {
	if state, ok := state.(Cancelation); ok {
		return proto.Marshal(state.NexusOperationCancellationInfo)
	}
	return nil, fmt.Errorf("invalid cancelation provided: %v", state) // nolint:goerr113
}

func (cancelationMachineDefinition) Type() string {
	return CancelationMachineType
}

// CompareState compares the progress of two Cancelation state machines to determine whether to sync machine state while
// processing a replication task.
// TODO: Remove this implementation once transition history is fully implemented.
func (cancelationMachineDefinition) CompareState(state1, state2 any) (int, error) {
	c1, ok := state1.(Cancelation)
	if !ok {
		return 0, fmt.Errorf("%w: expected state1 to be a Cancelation instance, got %v", hsm.ErrIncompatibleType, state1)
	}
	c2, ok := state2.(Cancelation)
	if !ok {
		return 0, fmt.Errorf("%w: expected state2 to be a Cancelation instance, got %v", hsm.ErrIncompatibleType, state2)
	}

	stage1, attempts1, err := c1.progress()
	if err != nil {
		return 0, fmt.Errorf("failed to get progress for state1: %w", err)
	}
	stage2, attempts2, err := c2.progress()
	if err != nil {
		return 0, fmt.Errorf("failed to get progress for state2: %w", err)
	}
	if stage1 != stage2 {
		return stage1 - stage2, nil
	}
	if stage1 == terminalStage && c1.State() != c2.State() {
		return 0, serviceerror.NewInvalidArgument(fmt.Sprintf("cannot compare two distinct terminal states: %v, %v", c1.State(), c2.State()))
	}
	return int(attempts1 - attempts2), nil
}

// Cancelation state machine for canceling an operation.
type Cancelation struct {
	*persistencespb.NexusOperationCancellationInfo
}

func (c Cancelation) State() enumspb.NexusOperationCancellationState {
	return c.NexusOperationCancellationInfo.State
}

func (c Cancelation) SetState(state enumspb.NexusOperationCancellationState) {
	c.NexusOperationCancellationInfo.State = state
}

func (c Cancelation) recordAttempt(ts time.Time) {
	c.NexusOperationCancellationInfo.Attempt++
	c.NexusOperationCancellationInfo.LastAttemptCompleteTime = timestamppb.New(ts)
	c.NexusOperationCancellationInfo.LastAttemptFailure = nil
}

func (c Cancelation) RegenerateTasks(_ any, node *hsm.Node) ([]hsm.Task, error) {
	op, err := hsm.MachineData[Operation](node.Parent)
	if err != nil {
		return nil, err
	}
	switch c.State() { // nolint:exhaustive
	case enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED:
		return []hsm.Task{CancelationTask{EndpointName: op.Endpoint}}, nil
	case enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BACKING_OFF:
		return []hsm.Task{CancelationBackoffTask{deadline: c.NextAttemptScheduleTime.AsTime()}}, nil
	default:
		return nil, nil
	}
}

func (c Cancelation) output(node *hsm.Node) (hsm.TransitionOutput, error) {
	tasks, err := c.RegenerateTasks(nil, node)
	if err != nil {
		return hsm.TransitionOutput{}, err
	}
	return hsm.TransitionOutput{Tasks: tasks}, nil
}

// TODO: Remove this implementation once transition history is fully implemented.
func (c Cancelation) progress() (int, int32, error) {
	switch c.State() {
	case enumspb.NEXUS_OPERATION_CANCELLATION_STATE_UNSPECIFIED:
		// UNSPECIFIED is a valid state since the cancelation may not initially get scheduled if the operation hasn't
		// been started yet.
		return 0, 0, nil
	case enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BACKING_OFF:
		return 1, c.GetAttempt() * 2, nil
	case enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED:
		// We've made slightly more progress if we transitioned from backing off to scheduled.
		return 1, c.GetAttempt()*2 + 1, nil
	case enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SUCCEEDED, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_FAILED:
		// Consider any terminal state as "max progress", we'll rely on last update namespace failover version to break
		// the tie when comparing two states.
		return terminalStage, 0, nil
	default:
		return 0, 0, serviceerror.NewInvalidArgument("unknown cancelation state")
	}
}

// EventCancelationScheduled is triggered when cancelation is meant to be scheduled for the first time - immediately
// after it has been requested.
type EventCancelationScheduled struct {
	Time time.Time
	Node *hsm.Node
}

var TransitionCancelationScheduled = hsm.NewTransition(
	[]enumspb.NexusOperationCancellationState{enumspb.NEXUS_OPERATION_CANCELLATION_STATE_UNSPECIFIED},
	enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED,
	func(op Cancelation, event EventCancelationScheduled) (hsm.TransitionOutput, error) {
		op.RequestedTime = timestamppb.New(event.Time)
		return op.output(event.Node)
	},
)

// EventCancelationRescheduled is triggered when cancelation is meant to be rescheduled after backing off from a
// previous attempt.
type EventCancelationRescheduled struct {
	Node *hsm.Node
}

var TransitionCancelationRescheduled = hsm.NewTransition(
	[]enumspb.NexusOperationCancellationState{enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BACKING_OFF},
	enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED,
	func(c Cancelation, event EventCancelationRescheduled) (hsm.TransitionOutput, error) {
		c.NextAttemptScheduleTime = nil
		return c.output(event.Node)
	},
)

// EventCancelationAttemptFailed is triggered when a cancelation attempt is failed with a retryable error.
type EventCancelationAttemptFailed struct {
	Time        time.Time
	Err         error
	Node        *hsm.Node
	RetryPolicy backoff.RetryPolicy
}

var TransitionCancelationAttemptFailed = hsm.NewTransition(
	[]enumspb.NexusOperationCancellationState{enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED},
	enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BACKING_OFF,
	func(c Cancelation, event EventCancelationAttemptFailed) (hsm.TransitionOutput, error) {
		c.recordAttempt(event.Time)
		// Use 0 for elapsed time as we don't limit the retry by time (for now).
		nextDelay := event.RetryPolicy.ComputeNextDelay(0, int(c.Attempt), event.Err)
		nextAttemptScheduleTime := event.Time.Add(nextDelay)
		c.NextAttemptScheduleTime = timestamppb.New(nextAttemptScheduleTime)
		c.LastAttemptFailure = &failurepb.Failure{
			Message: event.Err.Error(),
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable: false,
				},
			},
		}
		return c.output(event.Node)
	},
)

// EventCancelationFailed is triggered when a cancelation attempt is failed with a non retryable error.
type EventCancelationFailed struct {
	Time time.Time
	Err  error
	Node *hsm.Node
}

var TransitionCancelationFailed = hsm.NewTransition(
	[]enumspb.NexusOperationCancellationState{
		// We can immediately transition to failed to since we don't know how to send a cancelation request for an
		// unstarted operation.
		enumspb.NEXUS_OPERATION_CANCELLATION_STATE_UNSPECIFIED,
		enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED,
	},
	enumspb.NEXUS_OPERATION_CANCELLATION_STATE_FAILED,
	func(c Cancelation, event EventCancelationFailed) (hsm.TransitionOutput, error) {
		c.recordAttempt(event.Time)
		c.LastAttemptFailure = &failurepb.Failure{
			Message: event.Err.Error(),
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable: true,
				},
			},
		}
		return c.output(event.Node)
	},
)

// EventCancelationSucceeded is triggered when a cancelation attempt succeeds.
type EventCancelationSucceeded struct {
	Time time.Time
	Node *hsm.Node
}

var TransitionCancelationSucceeded = hsm.NewTransition(
	[]enumspb.NexusOperationCancellationState{enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED},
	enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SUCCEEDED,
	func(c Cancelation, event EventCancelationSucceeded) (hsm.TransitionOutput, error) {
		c.recordAttempt(event.Time)
		return c.output(event.Node)
	},
)

func RegisterStateMachines(r *hsm.Registry) error {
	if err := r.RegisterMachine(operationMachineDefinition{}); err != nil {
		return err
	}
	return r.RegisterMachine(cancelationMachineDefinition{})
}
