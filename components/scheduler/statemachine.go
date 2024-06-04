package scheduler

import (
	"fmt"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/service/history/hsm"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
	"time"
)

// Unique type identifier for this state machine.
var StateMachineType = hsm.MachineType{
	ID:   5,
	Name: "scheduler.Scheduler",
}

// MachineCollection creates a new typed [statemachines.Collection] for callbacks.
func MachineCollection(tree *hsm.Node) hsm.Collection[Scheduler] {
	return hsm.NewCollection[Scheduler](tree, StateMachineType.ID)
}

// Callback state machine.
type Scheduler struct {
	*schedspb.HsmSchedulerState
}

// NewCallback creates a new callback in the STANDBY state from given params.
func NewScheduler(args *schedspb.StartScheduleArgs) Scheduler {
	return Scheduler{
		&schedspb.HsmSchedulerState{
			Args:     args,
			HsmState: enumsspb.SCHEDULER_STATE_WAITING,
		},
	}
}

func (s Scheduler) State() enumsspb.SchedulerState {
	return s.HsmState
}

func (s Scheduler) SetState(state enumsspb.SchedulerState) {
	s.HsmState = state
}

func (s Scheduler) RegenerateTasks(*hsm.Node) ([]hsm.Task, error) {
	switch s.HsmState {
	case enumsspb.SCHEDULER_STATE_WAITING:
		s.Args.State.LastProcessedTime = timestamppb.Now()
		// TODO(Tianyu): Replace with actual scheduler work
		fmt.Printf("Scheduler has been invoked")
		// TODO(Tianyu): Replace with actual scheduling logic
		nextInvokeTime := timestamppb.New(s.Args.State.LastProcessedTime.AsTime().Add(10 * time.Second))
		return []hsm.Task{ScheduleTask{Deadline: nextInvokeTime.AsTime()}}, nil
	}
	return nil, nil
}

type stateMachineDefinition struct{}

func (stateMachineDefinition) Type() hsm.MachineType {
	return StateMachineType
}

func (stateMachineDefinition) Deserialize(d []byte) (any, error) {
	state := &schedspb.HsmSchedulerState{}
	if err := proto.Unmarshal(d, state); err != nil {
		return nil, serialization.NewDeserializationError(enumspb.ENCODING_TYPE_PROTO3, err)
	}
	return Scheduler{state}, nil
}

func (stateMachineDefinition) Serialize(state any) ([]byte, error) {
	if state, ok := state.(Scheduler); ok {
		return proto.Marshal(state.HsmSchedulerState)
	}
	return nil, fmt.Errorf("invalid callback provided: %v", state) // nolint:goerr113
}

func RegisterStateMachine(r *hsm.Registry) error {
	return r.RegisterMachine(stateMachineDefinition{})
}

// EventSchedulerActivate is triggered when the scheduler state machine should wake up and perform work
type EventSchedulerActivate struct{}

var TransitionSchedulerActivate = hsm.NewTransition(
	[]enumsspb.SchedulerState{enumsspb.SCHEDULER_STATE_WAITING},
	enumsspb.SCHEDULER_STATE_WAITING,
	func(scheduler Scheduler, event EventSchedulerActivate) (hsm.TransitionOutput, error) {
		tasks, err := scheduler.RegenerateTasks(nil)
		return hsm.TransitionOutput{Tasks: tasks}, err
	})
