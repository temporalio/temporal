package scheduler2

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/hsm"
)

type (
	BufferTask struct {
		deadline time.Time
	}

	// Fired when the Generator should buffer actions. After buffering, another buffer
	// task is usually created with a later deadline. The Generator will
	// alternate between sleeping and buffering without an explicit state
	// transition.
	EventBuffer struct {
		Node *hsm.Node

		Deadline time.Time
	}
)

const (
	TaskTypeBuffer = "scheduler.generator.Buffer"
)

var (
	_ hsm.Task = BufferTask{}
)

func (BufferTask) Type() string {
	return TaskTypeBuffer
}

func (b BufferTask) Deadline() time.Time {
	return b.deadline
}

func (BufferTask) Destination() string {
	return ""
}

func (BufferTask) Validate(_ *persistencespb.StateMachineRef, node *hsm.Node) error {
	return ValidateTask(node, TransitionBuffer)
}

func (g Generator) tasks() ([]hsm.Task, error) {
	switch g.State() { // nolint:exhaustive
	case enumsspb.SCHEDULER_GENERATOR_STATE_BUFFERING:
		return []hsm.Task{BufferTask{deadline: g.NextInvocationTime.AsTime()}}, nil
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
