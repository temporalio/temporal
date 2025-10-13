package callbacks

import (
	"fmt"
	"net/url"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/hsm"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// StateMachineType is a unique type identifier for this state machine.
	StateMachineType = "callbacks.Callback"

	// A marker for the first return value from a progress() that indicates the machine is in a terminal state.
	// TODO: Remove this once transition history is fully implemented.
	terminalStage = 3
)

// MachineCollection creates a new typed [statemachines.Collection] for callbacks.
func MachineCollection(tree *hsm.Node) hsm.Collection[Callback] {
	return hsm.NewCollection[Callback](tree, StateMachineType)
}

// Callback state machine.
//
// Deprecated: HSM Callback is no longer supported.
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
func NewCallback(
	requestId string,
	registrationTime *timestamppb.Timestamp,
	trigger *persistencespb.CallbackInfo_Trigger,
	cb *persistencespb.Callback,
) Callback {
	return Callback{
		&persistencespb.CallbackInfo{
			Trigger:          trigger,
			Callback:         cb,
			State:            enumsspb.CALLBACK_STATE_STANDBY,
			RegistrationTime: registrationTime,
			RequestId:        requestId,
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
		return []hsm.Task{BackoffTask{deadline: c.NextAttemptScheduleTime.AsTime()}}, nil
	case enumsspb.CALLBACK_STATE_SCHEDULED:
		switch v := c.Callback.GetVariant().(type) {
		case *persistencespb.Callback_Nexus_:
			u, err := url.Parse(c.Callback.GetNexus().Url)
			if err != nil {
				return nil, fmt.Errorf("failed to parse URL: %v: %w", &c, err)
			}
			return []hsm.Task{InvocationTask{destination: u.Scheme + "://" + u.Host}}, nil
		case *persistencespb.Callback_Hsm:
			// Destination is empty on the internal queue.
			return []hsm.Task{InvocationTask{"TODO(bergundy): make this empty"}}, nil

		default:
			return nil, fmt.Errorf("unsupported callback variant %v", v)
		}
	}
	return nil, nil
}

func (c Callback) output() (hsm.TransitionOutput, error) {
	// Task logic is the same when regenerating tasks for a given state and when transitioning to that state.
	// Node is ignored here.
	tasks, err := c.RegenerateTasks(nil)
	return hsm.TransitionOutput{Tasks: tasks}, err
}

// TODO: Remove this implementation once transition history is fully implemented.
func (c Callback) progress() (int, int32, error) {
	switch c.State() {
	case enumsspb.CALLBACK_STATE_UNSPECIFIED:
		return 0, 0, serviceerror.NewInvalidArgument("uninitialized callback state")
	case enumsspb.CALLBACK_STATE_STANDBY:
		return 1, 0, nil
	case enumsspb.CALLBACK_STATE_BACKING_OFF:
		return 2, c.GetAttempt() * 2, nil
	case enumsspb.CALLBACK_STATE_SCHEDULED:
		// We've made slightly more progress if we transitioned from backing off to scheduled.
		return 2, c.GetAttempt()*2 + 1, nil
	case enumsspb.CALLBACK_STATE_FAILED, enumsspb.CALLBACK_STATE_SUCCEEDED:
		// Consider any terminal state as "max progress", we'll rely on last update namespace failover version to break
		// the tie when comparing two states.
		return terminalStage, 0, nil
	default:
		return 0, 0, serviceerror.NewInvalidArgument("unknown callback state")
	}
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
	return nil, fmt.Errorf("invalid callback provided: %v", state)
}

// CompareState compares the progress of two Callback state machines to determine whether to sync machine state while
// processing a replication task.
// TODO: Remove this implementation once transition history is fully implemented.
func (stateMachineDefinition) CompareState(state1, state2 any) (int, error) {
	cb1, ok := state1.(Callback)
	if !ok {
		return 0, fmt.Errorf("%w: expected state1 to be a Callback instance, got %v", hsm.ErrIncompatibleType, state1)
	}
	cb2, ok := state2.(Callback)
	if !ok {
		return 0, fmt.Errorf("%w: expected state2 to be a Callback instance, got %v", hsm.ErrIncompatibleType, state2)
	}

	stage1, attempts1, err := cb1.progress()
	if err != nil {
		return 0, fmt.Errorf("failed to get progress for state1: %w", err)
	}
	stage2, attempts2, err := cb2.progress()
	if err != nil {
		return 0, fmt.Errorf("failed to get progress for state2: %w", err)
	}
	if stage1 != stage2 {
		return stage1 - stage2, nil
	}
	if stage1 == terminalStage && cb1.State() != cb2.State() {
		return 0, serviceerror.NewInvalidArgumentf("cannot compare two distinct terminal states: %v, %v", cb1.State(), cb2.State())
	}
	return int(attempts1 - attempts2), nil
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
