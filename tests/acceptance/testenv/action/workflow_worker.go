package action

import (
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
)

type CreateWorkflowWorker struct {
	stamp.ActionActor[*model.Cluster]
	stamp.ActionTarget[*model.WorkflowWorker]
	TaskQueue *model.TaskQueue
	Name      stamp.Gen[stamp.ID]
}

func (t CreateWorkflowWorker) Next(ctx stamp.GenContext) model.NewWorkflowWorker {
	return model.NewWorkflowWorker{
		TaskQueue: t.TaskQueue,
		Name:      "wf-worker-" + t.Name.Next(ctx.AllowRandom()),
	}
}
