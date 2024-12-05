package generator

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/components/scheduler2/common"
	"go.temporal.io/server/service/history/hsm"
)

type (
	BufferTask struct{}

	// Fired when the Generator should enter its run loop. The Generator will
	// alternate between sleeping and buffering without an explicit/visible state
	// transition.
	EventBuffer struct {
		Node *hsm.Node
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

func (BufferTask) Deadline() time.Time {
	return hsm.Immediate
}

func (BufferTask) Destination() string {
	return ""
}

func (BufferTask) Validate(_ *persistencespb.StateMachineRef, node *hsm.Node) error {
	return common.ValidateTask(node, TransitionBuffer)
}

func (g Generator) tasks() ([]hsm.Task, error) {
	switch g.State() { // nolint:exhaustive
	case enumsspb.SCHEDULER_GENERATOR_STATE_BUFFERING:
		return []hsm.Task{BufferTask{}}, nil
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
