package hsmtest

import (
	"encoding/json"
	"fmt"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/hsm"
)

const TaskType = "test-task-type-name"

var (
	errInvalidTaskType = fmt.Errorf("invalid task type")
)

type Task struct {
	attrs        hsm.TaskAttributes
	IsConcurrent bool
}

func NewTask(
	attrs hsm.TaskAttributes,
	concurrent bool,
) *Task {
	return &Task{
		attrs:        attrs,
		IsConcurrent: concurrent,
	}
}

func (t *Task) Type() string {
	return TaskType
}

func (t *Task) Deadline() time.Time {
	return t.attrs.Deadline
}

func (t *Task) Destination() string {
	return t.attrs.Destination
}

func (t *Task) Validate(ref *persistencespb.StateMachineRef, node *hsm.Node) error {
	if t.IsConcurrent {
		return hsm.ValidateNotTransitioned(ref, node)
	}
	return nil
}

type TaskSerializer struct{}

func (s TaskSerializer) Serialize(t hsm.Task) ([]byte, error) {
	if t.Type() != TaskType {
		return nil, errInvalidTaskType
	}
	return json.Marshal(t)
}

func (s TaskSerializer) Deserialize(b []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	var t Task
	if err := json.Unmarshal(b, &t); err != nil {
		return nil, err
	}
	t.attrs = attrs
	return &t, nil
}
