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

package callbacks

import (
	"fmt"
	"net/url"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/hsm"
)

// StateMachineType is a unique type identifier for this state machine.
var StateMachineType = "callbacks.Callback"

// MachineCollection creates a new typed [statemachines.Collection] for callbacks.
func MachineCollection(tree *hsm.Node) hsm.Collection[Callback] {
	return hsm.NewCollection[Callback](tree, StateMachineType)
}

// Callback state machine.
type Callback struct {
	*persistencespb.CallbackInfo
}

// NewWorkflowClosedTrigger creates a WorkflowClosed trigger variant.
func NewWorkflowClosedTrigger() *persistencespb.CallbackInfo_Trigger {
	return &persistencespb.CallbackInfo_Trigger{
		Variant: &persistencespb.CallbackInfo_Trigger_WorkflowClosed{},
	}
}

// NewCallback creates a new callback in the STANDBY state from given params.
func NewCallback(registrationTime *timestamppb.Timestamp, trigger *persistencespb.CallbackInfo_Trigger, cb *persistencespb.Callback) Callback {
	return Callback{
		&persistencespb.CallbackInfo{
			Trigger:          trigger,
			Callback:         cb,
			State:            enumsspb.CALLBACK_STATE_STANDBY,
			RegistrationTime: registrationTime,
		},
	}
}

func (c Callback) State() enumsspb.CallbackState {
	return c.CallbackInfo.State
}

func (c Callback) SetState(state enumsspb.CallbackState) {
	c.CallbackInfo.State = state
}

func (c Callback) recordAttempt(ts time.Time) {
	c.CallbackInfo.Attempt++
	c.CallbackInfo.LastAttemptCompleteTime = timestamppb.New(ts)
}

func (c Callback) RegenerateTasks(*hsm.Node) ([]hsm.Task, error) {
	switch c.CallbackInfo.State {
	case enumsspb.CALLBACK_STATE_BACKING_OFF:
		return []hsm.Task{BackoffTask{Deadline: c.NextAttemptScheduleTime.AsTime()}}, nil
	case enumsspb.CALLBACK_STATE_SCHEDULED:
		var destination string
		switch v := c.Callback.GetVariant().(type) {
		case *persistencespb.Callback_Nexus_:
			u, err := url.Parse(c.Callback.GetNexus().Url)
			if err != nil {
				return nil, fmt.Errorf("failed to parse URL: %v: %w", &c, err)
			}
			destination = u.Scheme + "://" + u.Host
		default:
			return nil, fmt.Errorf("unsupported callback variant %v", v) // nolint:goerr113
		}

		return []hsm.Task{InvocationTask{Destination: destination}}, nil
	}
	return nil, nil
}

func (c Callback) output() (hsm.TransitionOutput, error) {
	// Task logic is the same when regenerating tasks for a given state and when transitioning to that state.
	// Node is ignored here.
	tasks, err := c.RegenerateTasks(nil)
	return hsm.TransitionOutput{Tasks: tasks}, err
}

type stateMachineDefinition struct{}

func (stateMachineDefinition) Type() string {
	return StateMachineType
}

func (stateMachineDefinition) Deserialize(d []byte) (any, error) {
	info := &persistencespb.CallbackInfo{}
	if err := proto.Unmarshal(d, info); err != nil {
		return nil, serialization.NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}
	return Callback{info}, nil
}

func (stateMachineDefinition) Serialize(state any) ([]byte, error) {
	if state, ok := state.(Callback); ok {
		return proto.Marshal(state.CallbackInfo)
	}
	return nil, fmt.Errorf("invalid callback provided: %v", state) // nolint:goerr113
}

func (stateMachineDefinition) CompareState(state1, state2 any) (int, error) {
	// TODO: remove this implementation once transition history is fully implemented
	return 0, serviceerror.NewUnimplemented("CompareState not implemented for callbacks")
}

func RegisterStateMachine(r *hsm.Registry) error {
	return r.RegisterMachine(stateMachineDefinition{})
}

// EventScheduled is triggered when the callback is meant to be scheduled for the first time - when its Trigger
// condition is met.
type EventScheduled struct{}

var TransitionScheduled = hsm.NewTransition(
	[]enumsspb.CallbackState{enumsspb.CALLBACK_STATE_STANDBY},
	enumsspb.CALLBACK_STATE_SCHEDULED,
	func(cb Callback, event EventScheduled) (hsm.TransitionOutput, error) {
		return cb.output()
	},
)

// EventRescheduled is triggered when the callback is meant to be rescheduled after backing off from a previous attempt.
type EventRescheduled struct{}

var TransitionRescheduled = hsm.NewTransition(
	[]enumsspb.CallbackState{enumsspb.CALLBACK_STATE_BACKING_OFF},
	enumsspb.CALLBACK_STATE_SCHEDULED,
	func(cb Callback, event EventRescheduled) (hsm.TransitionOutput, error) {
		cb.CallbackInfo.NextAttemptScheduleTime = nil
		return cb.output()
	},
)

// EventAttemptFailed is triggered when an attempt is failed with a retryable error.
type EventAttemptFailed struct {
	Time        time.Time
	Err         error
	RetryPolicy backoff.RetryPolicy
}

var TransitionAttemptFailed = hsm.NewTransition(
	[]enumsspb.CallbackState{enumsspb.CALLBACK_STATE_SCHEDULED},
	enumsspb.CALLBACK_STATE_BACKING_OFF,
	func(cb Callback, event EventAttemptFailed) (hsm.TransitionOutput, error) {
		cb.recordAttempt(event.Time)
		// Use 0 for elapsed time as we don't limit the retry by time (for now).
		nextDelay := event.RetryPolicy.ComputeNextDelay(0, int(cb.Attempt), event.Err)
		nextAttemptScheduleTime := event.Time.Add(nextDelay)
		cb.CallbackInfo.NextAttemptScheduleTime = timestamppb.New(nextAttemptScheduleTime)
		cb.CallbackInfo.LastAttemptFailure = &failurepb.Failure{
			Message: event.Err.Error(),
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable: false,
				},
			},
		}
		return cb.output()
	},
)

// EventFailed is triggered when an attempt is failed with a non retryable error.
type EventFailed struct {
	Time time.Time
	Err  error
}

var TransitionFailed = hsm.NewTransition(
	[]enumsspb.CallbackState{enumsspb.CALLBACK_STATE_SCHEDULED},
	enumsspb.CALLBACK_STATE_FAILED,
	func(cb Callback, event EventFailed) (hsm.TransitionOutput, error) {
		cb.recordAttempt(event.Time)
		cb.CallbackInfo.LastAttemptFailure = &failurepb.Failure{
			Message: event.Err.Error(),
			FailureInfo: &failurepb.Failure_ApplicationFailureInfo{
				ApplicationFailureInfo: &failurepb.ApplicationFailureInfo{
					NonRetryable: true,
				},
			},
		}
		return cb.output()
	},
)

// EventSucceeded is triggered when an attempt succeeds.
type EventSucceeded struct {
	Time time.Time
}

var TransitionSucceeded = hsm.NewTransition(
	[]enumsspb.CallbackState{enumsspb.CALLBACK_STATE_SCHEDULED},
	enumsspb.CALLBACK_STATE_SUCCEEDED,
	func(cb Callback, event EventSucceeded) (hsm.TransitionOutput, error) {
		cb.recordAttempt(event.Time)
		cb.CallbackInfo.LastAttemptFailure = nil
		return cb.output()
	},
)
