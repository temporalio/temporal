package action

import (
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
	"google.golang.org/protobuf/types/known/durationpb"
)

type CreateTaskQueue struct {
	stamp.ActionActor[*model.Cluster]
	stamp.ActionTarget[*model.TaskQueue]
	Namespace *model.Namespace `validate:"required"`
	Name      stamp.Gen[stamp.ID]
}

func (t CreateTaskQueue) Next(ctx stamp.GenContext) model.NewTaskQueue {
	return model.NewTaskQueue{
		Namespace: t.Namespace,
		Name:      "tq-" + t.Name.Next(ctx.AllowRandom()),
	}
}

type StickyTaskQueue struct {
	NormalTaskQueue *model.TaskQueue `validate:"required"`
	Timeout         stamp.Gen[time.Duration]
}

func (t StickyTaskQueue) Next(ctx stamp.GenContext) *taskqueue.StickyExecutionAttributes {
	return &taskqueue.StickyExecutionAttributes{
		WorkerTaskQueue: &taskqueue.TaskQueue{
			Kind:       enumspb.TASK_QUEUE_KIND_STICKY,
			Name:       string(t.NormalTaskQueue.GetID() + "--sticky"),
			NormalName: string(t.NormalTaskQueue.GetID()),
		},
		ScheduleToStartTimeout: durationpb.New(t.Timeout.NextOrDefault(ctx, 5*time.Second)),
	}
}
