package action

import (
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
)

type CreateTaskQueue struct {
	stamp.ActionActor[*model.Cluster]
	stamp.ActionTarget[*model.TaskQueue]
	Namespace *model.Namespace
	Name      stamp.Gen[stamp.ID]
}

func (t CreateTaskQueue) Next(ctx stamp.GenContext) model.NewTaskQueue {
	return model.NewTaskQueue{
		NamespaceName: t.Namespace.GetID(),
		TaskQueueName: t.Name.Next(ctx.AllowRandom()),
	}
}
