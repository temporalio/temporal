package dummy

import (
	"fmt"
	"time"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/hsm"
)

const (
	TaskTypeTimer     = "dummy.Timer"
	TaskTypeImmediate = "dummy.Immediate"
)

type ImmediateTask struct {
	destination string
}

var _ hsm.Task = ImmediateTask{}

func (ImmediateTask) Type() string {
	return TaskTypeImmediate
}

func (ImmediateTask) Deadline() time.Time {
	return hsm.Immediate
}

func (t ImmediateTask) Destination() string {
	return t.destination
}

func (ImmediateTask) Validate(ref *persistencespb.StateMachineRef, node *hsm.Node) error {
	return hsm.ValidateNotTransitioned(ref, node)
}

type ImmediateTaskSerializer struct{}

func (ImmediateTaskSerializer) Deserialize(data []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	return ImmediateTask{destination: attrs.Destination}, nil
}

func (ImmediateTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
	return nil, nil
}

type TimerTask struct {
	deadline   time.Time
	concurrent bool
}

var _ hsm.Task = TimerTask{}

func (TimerTask) Type() string {
	return TaskTypeTimer
}

func (t TimerTask) Deadline() time.Time {
	return t.deadline
}

func (TimerTask) Destination() string {
	return ""
}

func (t TimerTask) Validate(ref *persistencespb.StateMachineRef, node *hsm.Node) error {
	if !t.concurrent {
		return hsm.ValidateNotTransitioned(ref, node)
	}
	return nil
}

type TimerTaskSerializer struct{}

func (TimerTaskSerializer) Deserialize(data []byte, attrs hsm.TaskAttributes) (hsm.Task, error) {
	return TimerTask{deadline: attrs.Deadline, concurrent: len(data) > 0}, nil
}

func (s TimerTaskSerializer) Serialize(task hsm.Task) ([]byte, error) {
	if tt, ok := task.(TimerTask); ok {
		if tt.concurrent {
			// Non empty data marks the task as concurrent.
			return []byte{1}, nil
		}
		// No-op.
		return nil, nil
	}
	return nil, fmt.Errorf("incompatible task: %v", task)
}

func RegisterTaskSerializers(reg *hsm.Registry) error {
	if err := reg.RegisterTaskSerializer(
		TaskTypeImmediate,
		ImmediateTaskSerializer{},
	); err != nil {
		return err
	}
	return reg.RegisterTaskSerializer(TaskTypeTimer, TimerTaskSerializer{})
}
