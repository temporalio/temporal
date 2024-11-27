package generator

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/components/scheduler2/common"
	"go.temporal.io/server/service/history/hsm"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type (
	SleepTask struct {
		deadline time.Time
	}

	BufferTask struct{}

	// Fired when generator has no actions to buffer and can sleep.
	EventSleep struct {
		Node *hsm.Node

		// Sets the wakeup deadline, after which the state machine will move back to BUFFERING
		Deadline time.Time
	}

	// Fired when the generator should buffer more events.
	EventBuffer struct {
		Node *hsm.Node
	}
)

const (
	TaskTypeSleep  = "scheduler.generator.Sleep"
	TaskTypeBuffer = "scheduler.generator.Buffer"
)

var (
	_ hsm.Task = SleepTask{}
	_ hsm.Task = BufferTask{}
)

func (SleepTask) Type() string {
	return TaskTypeSleep
}

func (s SleepTask) Deadline() time.Time {
	return s.deadline
}

func (SleepTask) Destination() string {
	return ""
}

func (SleepTask) Validate(_ *persistencespb.StateMachineRef, node *hsm.Node) error {
	return common.ValidateTask(node, TransitionSleep)
}

func (BufferTask) Type() string {
	return TaskTypeBuffer
}

func (BufferTask) Deadline() time.Time {
	return hsm.Immediate
}

func (BufferTask) Destination() string {
	return ""
}

func (BufferTask) Validate(_ *persistencespb.StateMachineRef, node *hsm.Node) error {
	return common.ValidateTask(node, TransitionBuffer)
}

var TransitionSleep = hsm.NewTransition(
	[]enumsspb.SchedulerGeneratorState{
		enumsspb.SCHEDULER_GENERATOR_STATE_UNSPECIFIED,
		enumsspb.SCHEDULER_GENERATOR_STATE_BUFFERING,
	},
	enumsspb.SCHEDULER_GENERATOR_STATE_WAITING,
	func(g Generator, event EventSleep) (hsm.TransitionOutput, error) {
		g.NextInvocationTime = timestamppb.New(event.Deadline)
		return g.output()
	},
)

var TransitionBuffer = hsm.NewTransition(
	[]enumsspb.SchedulerGeneratorState{
		enumsspb.SCHEDULER_GENERATOR_STATE_UNSPECIFIED,
		enumsspb.SCHEDULER_GENERATOR_STATE_WAITING,
	},
	enumsspb.SCHEDULER_GENERATOR_STATE_BUFFERING,
	func(g Generator, _ EventBuffer) (hsm.TransitionOutput, error) {
		g.NextInvocationTime = nil
		return g.output()
	},
)

func (g Generator) tasks() ([]hsm.Task, error) {
	switch g.State() { // nolint:exhaustive
	case enumsspb.SCHEDULER_GENERATOR_STATE_BUFFERING:
		return []hsm.Task{BufferTask{}}, nil
	case enumsspb.SCHEDULER_GENERATOR_STATE_WAITING:
		return []hsm.Task{SleepTask{deadline: g.NextInvocationTime.AsTime()}}, nil
	default:
		return nil, nil
	}
}

func (g Generator) output() (hsm.TransitionOutput, error) {
	tasks, err := g.tasks()
	if err != nil {
		return hsm.TransitionOutput{}, err
	}
	return hsm.TransitionOutput{Tasks: tasks}, nil
}
