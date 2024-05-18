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

	"github.com/nexus-rpc/sdk-go/nexus"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/service/history/hsm"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var (
	// OperationMachineType is a unique type identifier for the Operation state machine.
	OperationMachineType = hsm.MachineType{
		ID:   3,
		Name: "nexusoperations.Operation",
	}

	// CancelationMachineType is a unique type identifier for the Cancelation state machine.
	CancelationMachineType = hsm.MachineType{
		ID:   4,
		Name: "nexusoperations.Cancelation",
	}

	// CancelationMachineKey is a fixed key for the cancelation machine as a child of the operation machine.
	CancelationMachineKey = hsm.Key{Type: CancelationMachineType.ID, ID: ""}
)

// MachineCollection creates a new typed [statemachines.Collection] for operations.
func MachineCollection(tree *hsm.Node) hsm.Collection[Operation] {
	return hsm.NewCollection[Operation](tree, OperationMachineType.ID)
}

// Operation state machine.
type Operation struct {
	*persistencespb.NexusOperationInfo
}

// AddChild adds a new operation child machine to the given node and transitions it to the SCHEDULED state.
func AddChild(node *hsm.Node, id string, event *historypb.HistoryEvent, eventToken []byte, deleteOnCompletion bool) (*hsm.Node, error) {
	attrs := event.GetNexusOperationScheduledEventAttributes()

	node, err := node.AddChild(hsm.Key{Type: OperationMachineType.ID, ID: id}, Operation{
		&persistencespb.NexusOperationInfo{
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

// transitionTasks returns tasks that are emitted as transition outputs.
func (o Operation) transitionTasks(node *hsm.Node) ([]hsm.Task, error) {
	if canceled, err := o.cancelRequested(node); canceled || err != nil {
		return nil, err
	}
	switch o.State() { // nolint:exhaustive
	case enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF:
		return []hsm.Task{BackoffTask{Deadline: o.NextAttemptScheduleTime.AsTime()}}, nil
	case enumsspb.NEXUS_OPERATION_STATE_SCHEDULED:
		return []hsm.Task{InvocationTask{Destination: o.Endpoint}}, nil
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
		return []hsm.Task{TimeoutTask{Deadline: o.ScheduledTime.AsTime().Add(o.ScheduleToCloseTimeout.AsDuration())}}, nil
	}
	return nil, nil
}

func (o Operation) RegenerateTasks(node *hsm.Node) ([]hsm.Task, error) {
	transitionTasks, err := o.transitionTasks(node)
	if err != nil {
		return nil, err
	}
	creationTasks, err := o.creationTasks(node)
	if err != nil {
		return nil, err
	}
	return append(transitionTasks, creationTasks...), nil
}

func (o Operation) output(node *hsm.Node) (hsm.TransitionOutput, error) {
	tasks, err := o.transitionTasks(node)
	if err != nil {
		return hsm.TransitionOutput{}, err
	}
	return hsm.TransitionOutput{Tasks: tasks}, nil
}

type operationMachineDefinition struct{}

func (operationMachineDefinition) Type() hsm.MachineType {
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

func RegisterStateMachines(r *hsm.Registry) error {
	if err := r.RegisterMachine(operationMachineDefinition{}); err != nil {
		return err
	}
	return r.RegisterMachine(cancelationMachineDefinition{})
}

// AttemptFailure carries failure information of an invocation attempt.
type AttemptFailure struct {
	Time time.Time
	Err  error
}

// EventScheduled is triggered when the operation is meant to be scheduled - immediately after initialization.
type EventScheduled struct {
	Node *hsm.Node
}

var TransitionScheduled = hsm.NewTransition(
	[]enumsspb.NexusOperationState{enumsspb.NEXUS_OPERATION_STATE_UNSPECIFIED},
	enumsspb.NEXUS_OPERATION_STATE_SCHEDULED,
	func(op Operation, event EventScheduled) (hsm.TransitionOutput, error) {
		return op.output(event.Node)
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
		return op.output(event.Node)
	},
)

// EventAttemptFailed is triggered when an invocation attempt is failed with a retryable error.
type EventAttemptFailed struct {
	AttemptFailure
	Node *hsm.Node
}

var TransitionAttemptFailed = hsm.NewTransition(
	[]enumsspb.NexusOperationState{enumsspb.NEXUS_OPERATION_STATE_SCHEDULED},
	enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF,
	func(op Operation, event EventAttemptFailed) (hsm.TransitionOutput, error) {
		op.recordAttempt(event.Time)
		// Use 0 for elapsed time as we don't limit the retry by time (for now).
		// TODO: Make the retry policy initial interval configurable.
		nextDelay := backoff.NewExponentialRetryPolicy(time.Second).ComputeNextDelay(0, int(op.Attempt))
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
		return op.output(event.Node)
	},
)

// EventFailed is triggered when an invocation attempt is failed with a non retryable error.
type EventFailed struct {
	// Only set if the operation completed synchronously, as a response to a StartOperation RPC.
	AttemptFailure *AttemptFailure
	Node           *hsm.Node
}

var TransitionFailed = hsm.NewTransition(
	[]enumsspb.NexusOperationState{
		enumsspb.NEXUS_OPERATION_STATE_SCHEDULED,
		enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF,
		enumsspb.NEXUS_OPERATION_STATE_STARTED,
	},
	enumsspb.NEXUS_OPERATION_STATE_FAILED,
	func(op Operation, event EventFailed) (hsm.TransitionOutput, error) {
		if event.AttemptFailure != nil {
			op.recordAttempt(event.AttemptFailure.Time)
			op.LastAttemptFailure = &failurepb.Failure{
				Message: event.AttemptFailure.Err.Error(),
				FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
					ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
						NonRetryable: true,
					},
				},
			}
		}
		// Keep last attempt information as-is for debuggability when completed asynchronously.
		// When used in a workflow, this machine node will be deleted from the tree after this transition.
		return op.output(event.Node)
	},
)

// EventSucceeded is triggered when an invocation attempt succeeds.
type EventSucceeded struct {
	// Only set if the operation completed synchronously, as a response to a StartOperation RPC.
	AttemptTime *time.Time
	Node        *hsm.Node
}

var TransitionSucceeded = hsm.NewTransition(
	[]enumsspb.NexusOperationState{
		enumsspb.NEXUS_OPERATION_STATE_SCHEDULED,
		enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF,
		enumsspb.NEXUS_OPERATION_STATE_STARTED,
	},
	enumsspb.NEXUS_OPERATION_STATE_SUCCEEDED,
	func(op Operation, event EventSucceeded) (hsm.TransitionOutput, error) {
		if event.AttemptTime != nil {
			op.recordAttempt(*event.AttemptTime)
		}
		// Keep last attempt information as-is for debuggability when completed asynchronously.
		// When used in a workflow, this machine node will be deleted from the tree after this transition.
		return op.output(event.Node)
	},
)

// EventCanceled is triggered when an invocation attempt succeeds.
type EventCanceled struct {
	// Only set if the operation completed synchronously, as a response to a StartOperation RPC.
	AttemptFailure *AttemptFailure
	Node           *hsm.Node
}

var TransitionCanceled = hsm.NewTransition(
	[]enumsspb.NexusOperationState{
		enumsspb.NEXUS_OPERATION_STATE_SCHEDULED,
		enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF,
		enumsspb.NEXUS_OPERATION_STATE_STARTED,
	},
	enumsspb.NEXUS_OPERATION_STATE_CANCELED,
	func(op Operation, event EventCanceled) (hsm.TransitionOutput, error) {
		if event.AttemptFailure != nil {
			op.recordAttempt(event.AttemptFailure.Time)
			op.LastAttemptFailure = &failurepb.Failure{
				Message: event.AttemptFailure.Err.Error(),
				FailureInfo: &failurepb.Failure_CanceledFailureInfo{
					CanceledFailureInfo: &failurepb.CanceledFailureInfo{},
				},
			}
		}
		// Keep last attempt information as-is for debuggability when completed asynchronously.
		// When used in a workflow, this machine node will be deleted from the tree after this transition.
		return op.output(event.Node)
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
	[]enumsspb.NexusOperationState{enumsspb.NEXUS_OPERATION_STATE_SCHEDULED},
	enumsspb.NEXUS_OPERATION_STATE_STARTED,
	func(op Operation, event EventStarted) (hsm.TransitionOutput, error) {
		op.recordAttempt(event.Time)
		op.OperationId = event.Attributes.OperationId
		return op.output(event.Node)
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
		return op.output(event.Node)
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
	// TODO(bergundy): Support cancel before started. We need to transmit this intent to cancel to the handler because
	// we don't know for sure that the operation hasn't been started.
	if o.State() == enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF || o.State() == enumsspb.NEXUS_OPERATION_STATE_SCHEDULED {
		return handleUnsuccessfulOperationError(node, o, &nexus.UnsuccessfulOperationError{
			State:   nexus.OperationStateCanceled,
			Failure: nexus.Failure{Message: "operation canceled before started"},
		}, nil)
	}
	return hsm.TransitionOutput{}, hsm.MachineTransition(child, func(c Cancelation) (hsm.TransitionOutput, error) {
		return TranstionCancelationScheduled.Apply(c, EventCancelationScheduled{
			Time: t,
			Node: child,
		})
	})
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

func (cancelationMachineDefinition) Type() hsm.MachineType {
	return CancelationMachineType
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

func (c Cancelation) RegenerateTasks(node *hsm.Node) ([]hsm.Task, error) {
	op, err := hsm.MachineData[Operation](node.Parent)
	if err != nil {
		return nil, err
	}
	switch c.State() { // nolint:exhaustive
	case enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED:
		return []hsm.Task{CancelationTask{Destination: op.Endpoint}}, nil
	case enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BACKING_OFF:
		return []hsm.Task{CancelationBackoffTask{Deadline: c.NextAttemptScheduleTime.AsTime()}}, nil
	default:
		return nil, nil
	}
}

func (c Cancelation) output(node *hsm.Node) (hsm.TransitionOutput, error) {
	tasks, err := c.RegenerateTasks(node)
	if err != nil {
		return hsm.TransitionOutput{}, err
	}
	return hsm.TransitionOutput{Tasks: tasks}, nil
}

// EventCancelationScheduled is triggered when cancelation is meant to be scheduled for the first time - immediately
// after it has been requested.
type EventCancelationScheduled struct {
	Time time.Time
	Node *hsm.Node
}

var TranstionCancelationScheduled = hsm.NewTransition(
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
	Time time.Time
	Err  error
	Node *hsm.Node
}

var TransitionCancelationAttemptFailed = hsm.NewTransition(
	[]enumspb.NexusOperationCancellationState{enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED},
	enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BACKING_OFF,
	func(c Cancelation, event EventCancelationAttemptFailed) (hsm.TransitionOutput, error) {
		c.recordAttempt(event.Time)
		// Use 0 for elapsed time as we don't limit the retry by time (for now).
		// TODO: Make the retry policy initial interval configurable.
		nextDelay := backoff.NewExponentialRetryPolicy(time.Second).ComputeNextDelay(0, int(c.Attempt))
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
	[]enumspb.NexusOperationCancellationState{enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED},
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
