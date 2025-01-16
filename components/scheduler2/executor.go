package scheduler2

import (
	"fmt"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/service/history/hsm"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	// The Executor sub state machine is responsible for executing buffered actions.
	Executor struct {
		*schedspb.ExecutorInternal
	}

	// The machine definition provides serialization/deserialization and type information.
	executorMachineDefinition struct{}
)

const (
	// Unique identifier for the Executor sub state machine.
	ExecutorMachineType = "scheduler.Executor"
)

var (
	_ hsm.StateMachine[enumsspb.SchedulerExecutorState] = Executor{}
	_ hsm.StateMachineDefinition                        = &executorMachineDefinition{}

	// Each sub state machine is a singleton of the top-level Scheduler, accessed with
	// a fixed key.
	ExecutorMachineKey = hsm.Key{Type: ExecutorMachineType, ID: ""}
)

// NewExecutor returns an intialized Executor sub state machine, which should
// be parented under a Scheduler root node.
func NewExecutor() *Executor {
	var zero time.Time
	return &Executor{
		ExecutorInternal: &schedspb.ExecutorInternal{
			State:              enumsspb.SCHEDULER_EXECUTOR_STATE_WAITING,
			NextInvocationTime: timestamppb.New(zero),
			BufferedStarts:     []*schedspb.BufferedStart{},
		},
	}
}

func (e Executor) State() enumsspb.SchedulerExecutorState {
	return e.ExecutorInternal.State
}

func (e Executor) SetState(state enumsspb.SchedulerExecutorState) {
	e.ExecutorInternal.State = state
}

func (e Executor) RegenerateTasks(node *hsm.Node) ([]hsm.Task, error) {
	return nil, nil
}

func (executorMachineDefinition) Type() string {
	return ExecutorMachineType
}

func (executorMachineDefinition) Serialize(state any) ([]byte, error) {
	if state, ok := state.(Executor); ok {
		return proto.Marshal(state.ExecutorInternal)
	}
	return nil, fmt.Errorf("invalid executor state provided: %v", state)
}

func (executorMachineDefinition) Deserialize(body []byte) (any, error) {
	state := &schedspb.ExecutorInternal{}
	return Executor{state}, proto.Unmarshal(body, state)
}

func (executorMachineDefinition) CompareState(a any, b any) (int, error) {
	panic("TODO: CompareState not yet implemented for Executor")
}

// Transition to executing state to continue executing pending buffered actions,
// writing additional pending actions into the Executor's persistent state.
var TransitionExecute = hsm.NewTransition(
	[]enumsspb.SchedulerExecutorState{
		enumsspb.SCHEDULER_EXECUTOR_STATE_UNSPECIFIED,
		enumsspb.SCHEDULER_EXECUTOR_STATE_WAITING,
		enumsspb.SCHEDULER_EXECUTOR_STATE_EXECUTING,
	},
	enumsspb.SCHEDULER_EXECUTOR_STATE_EXECUTING,
	func(e Executor, event EventExecute) (hsm.TransitionOutput, error) {
		// We want Executor to immediately wake and attempt to buffer when new starts
		// are added.
		e.NextInvocationTime = nil
		e.BufferedStarts = append(e.BufferedStarts, event.BufferedStarts...)

		return e.output()
	},
)

// Transition to waiting state. No new tasks will be created until the Executor
// is transitioned back to Executing state.
var TransitionWait = hsm.NewTransition(
	[]enumsspb.SchedulerExecutorState{
		enumsspb.SCHEDULER_EXECUTOR_STATE_UNSPECIFIED,
		enumsspb.SCHEDULER_EXECUTOR_STATE_EXECUTING,
	},
	enumsspb.SCHEDULER_EXECUTOR_STATE_WAITING,
	func(e Executor, event EventWait) (hsm.TransitionOutput, error) {
		e.NextInvocationTime = nil
		return e.output()
	},
)
