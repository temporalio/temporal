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

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/hsm"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Unique type identifier for this state machine.
var StateMachineType = hsm.MachineType{
	ID:   2,
	Name: "callbacks.Callback",
}

// MachineCollection creates a new typed [statemachines.Collection] for callbacks.
func MachineCollection(tree *hsm.Node) hsm.Collection[Callback] {
	return hsm.NewCollection[Callback](tree, StateMachineType.ID)
}

// Callback state machine.
type Callback struct {
	*persistencespb.CallbackInfo
}

// NewWorkflowClosedTrigger creates a WorkflowClosed trigger variant.
func NewWorkflowClosedTrigger() *workflowpb.CallbackInfo_Trigger {
	return &workflowpb.CallbackInfo_Trigger{
		Variant: &workflowpb.CallbackInfo_Trigger_WorkflowClosed{},
	}
}

// NewCallback creates a new callback in the STANDBY state from given params.
func NewCallback(registrationTime *timestamppb.Timestamp, trigger *workflowpb.CallbackInfo_Trigger, cb *commonpb.Callback) Callback {
	return Callback{
		&persistencespb.CallbackInfo{
			PublicInfo: &workflowpb.CallbackInfo{
				Trigger:          trigger,
				Callback:         cb,
				State:            enumspb.CALLBACK_STATE_STANDBY,
				RegistrationTime: registrationTime,
			},
		},
	}
}

func (c Callback) State() enumspb.CallbackState {
	return c.PublicInfo.State
}

func (c Callback) SetState(state enumspb.CallbackState) {
	c.PublicInfo.State = state
}

func (c Callback) recordAttempt(ts time.Time) {
	c.PublicInfo.Attempt++
	c.PublicInfo.LastAttemptCompleteTime = timestamppb.New(ts)
}

func (c Callback) RegenerateTasks(*hsm.Node) ([]hsm.Task, error) {
	switch c.PublicInfo.State {
	case enumspb.CALLBACK_STATE_BACKING_OFF:
		return []hsm.Task{BackoffTask{Deadline: c.PublicInfo.NextAttemptScheduleTime.AsTime()}}, nil
	case enumspb.CALLBACK_STATE_SCHEDULED:
		var destination string
		switch v := c.PublicInfo.Callback.GetVariant().(type) {
		case *commonpb.Callback_Nexus_:
			u, err := url.Parse(c.PublicInfo.Callback.GetNexus().Url)
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

func (stateMachineDefinition) Type() hsm.MachineType {
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

func RegisterStateMachine(r *hsm.Registry) error {
	return r.RegisterMachine(stateMachineDefinition{})
}

// EventScheduled is triggered when the callback is meant to be scheduled for the first time - when its Trigger
// condition is met.
type EventScheduled struct{}

var TransitionScheduled = hsm.NewTransition(
	[]enumspb.CallbackState{enumspb.CALLBACK_STATE_STANDBY},
	enumspb.CALLBACK_STATE_SCHEDULED,
	func(cb Callback, event EventScheduled) (hsm.TransitionOutput, error) {
		return cb.output()
	},
)

// EventRescheduled is triggered when the callback is meant to be rescheduled after backing off from a previous attempt.
type EventRescheduled struct{}

var TransitionRescheduled = hsm.NewTransition(
	[]enumspb.CallbackState{enumspb.CALLBACK_STATE_BACKING_OFF},
	enumspb.CALLBACK_STATE_SCHEDULED,
	func(cb Callback, event EventRescheduled) (hsm.TransitionOutput, error) {
		cb.PublicInfo.NextAttemptScheduleTime = nil
		return cb.output()
	},
)

// EventAttemptFailed is triggered when an attempt is failed with a retryable error.
type EventAttemptFailed struct {
	Time time.Time
	Err  error
}

var TransitionAttemptFailed = hsm.NewTransition(
	[]enumspb.CallbackState{enumspb.CALLBACK_STATE_SCHEDULED},
	enumspb.CALLBACK_STATE_BACKING_OFF,
	func(cb Callback, event EventAttemptFailed) (hsm.TransitionOutput, error) {
		cb.recordAttempt(event.Time)
		// Use 0 for elapsed time as we don't limit the retry by time (for now).
		// TODO: Make the retry policy initial interval configurable.
		nextDelay := backoff.NewExponentialRetryPolicy(time.Second).ComputeNextDelay(0, int(cb.PublicInfo.Attempt))
		nextAttemptScheduleTime := event.Time.Add(nextDelay)
		cb.PublicInfo.NextAttemptScheduleTime = timestamppb.New(nextAttemptScheduleTime)
		cb.PublicInfo.LastAttemptFailure = &failurepb.Failure{
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
	[]enumspb.CallbackState{enumspb.CALLBACK_STATE_SCHEDULED},
	enumspb.CALLBACK_STATE_FAILED,
	func(cb Callback, event EventFailed) (hsm.TransitionOutput, error) {
		cb.recordAttempt(event.Time)
		cb.PublicInfo.LastAttemptFailure = &failurepb.Failure{
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
	[]enumspb.CallbackState{enumspb.CALLBACK_STATE_SCHEDULED},
	enumspb.CALLBACK_STATE_SUCCEEDED,
	func(cb Callback, event EventSucceeded) (hsm.TransitionOutput, error) {
		cb.recordAttempt(event.Time)
		cb.PublicInfo.LastAttemptFailure = nil
		return cb.output()
	},
)
