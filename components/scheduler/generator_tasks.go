package scheduler

import (
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/hsm"
)

type BufferTask struct {
	deadline time.Time
}

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

func (BufferTask) Validate(_ *persistencespb.StateMachineRef, _ *hsm.Node) error {
	// Generator only has a single task/state, so no validation is done here.
	return nil
}

func (g Generator) tasks() ([]hsm.Task, error) {
	return []hsm.Task{BufferTask{deadline: g.NextInvocationTime.AsTime()}}, nil
}

func (g Generator) output() (hsm.TransitionOutput, error) {
	tasks, err := g.tasks()
	if err != nil {
		return hsm.TransitionOutput{}, err
	}
	return hsm.TransitionOutput{Tasks: tasks}, nil
}
