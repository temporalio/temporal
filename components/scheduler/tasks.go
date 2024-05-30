package scheduler

import (
	"fmt"
	"go.temporal.io/server/service/history/hsm"
	"time"
)

var (
	TaskTypeSchedule = hsm.TaskType{
		ID:   8,
		Name: "scheduler.Schedule",
	}
)

type ScheduleTask struct {
	Deadline time.Time
}

var _ hsm.Task = ScheduleTask{}

func (ScheduleTask) Type() hsm.TaskType {
	return TaskTypeSchedule
}

func (t ScheduleTask) Kind() hsm.TaskKind {
	return hsm.TaskKindTimer{Deadline: t.Deadline}
}

func (ScheduleTask) Concurrent() bool {
	return false
}

type ScheduleTaskSerializer struct{}

func (ScheduleTaskSerializer) Deserialize(data []byte, kind hsm.TaskKind) (hsm.Task, error) {
	if kind, ok := kind.(hsm.TaskKindTimer); ok {
		return ScheduleTask{Deadline: kind.Deadline}, nil
	}
	return nil, fmt.Errorf("%w: expected timer", hsm.ErrInvalidTaskKind)
}

func (ScheduleTaskSerializer) Serialize(hsm.Task) ([]byte, error) {
	return nil, nil
}

func RegisterTaskSerializers(reg *hsm.Registry) error {
	if err := reg.RegisterTaskSerializer(TaskTypeSchedule.ID, ScheduleTaskSerializer{}); err != nil {
		return err
	}
	return nil
}
