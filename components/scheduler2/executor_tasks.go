package scheduler2

import (
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	schedpb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/service/history/hsm"
)

type (
	ExecuteTask struct {
		deadline time.Time
	}

	// EventExecute is fired when the executor should wake up and begin processing
	// pending buffered starts, including any that are set on BufferedStarts.
	// BufferedStarts are immediately appended to the Executor's queue as the
	// transition is applied.
	//
	// Execution can be delayed (such as in the event of backing off) by setting
	// Deadline to something other than env.Now().
	EventExecute struct {
		Node *hsm.Node

		Deadline       time.Time
		BufferedStarts []*schedpb.BufferedStart
	}

	// EventWait is fired when the executor should return to a waiting state until
	// more actions are buffered.
	EventWait struct {
		Node *hsm.Node
	}
)

const (
	TaskTypeExecute = "scheduler.executor.Execute"
)

var (
	_ hsm.Task = ExecuteTask{}
)

func (ExecuteTask) Type() string {
	return TaskTypeExecute
}

func (e ExecuteTask) Deadline() time.Time {
	return e.deadline
}

func (ExecuteTask) Destination() string {
	return ""
}

func (ExecuteTask) Validate(_ *persistencespb.StateMachineRef, node *hsm.Node) error {
	return validateTaskTransition(node, TransitionExecute)
}

func (e Executor) tasks() ([]hsm.Task, error) {
	if e.State() == enumsspb.SCHEDULER_EXECUTOR_STATE_EXECUTING {
		return []hsm.Task{ExecuteTask{
			deadline: e.NextInvocationTime.AsTime(),
		}}, nil
	}
	return nil, nil
}

func (e Executor) output() (hsm.TransitionOutput, error) {
	tasks, err := e.tasks()
	if err != nil {
		return hsm.TransitionOutput{}, err
	}
	return hsm.TransitionOutput{Tasks: tasks}, nil
}
