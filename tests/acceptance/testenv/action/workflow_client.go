package action

import (
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
)

type CreateWorkflowClient struct {
	stamp.ActionActor[*model.Cluster]
	stamp.ActionTarget[*model.WorkflowClient]
	TaskQueue *model.TaskQueue
	Name      stamp.Gen[stamp.ID]
}

func (t CreateWorkflowClient) Next(ctx stamp.GenContext) model.NewWorkflowClient {
	return model.NewWorkflowClient{
		TaskQueue: t.TaskQueue,
		Name:      "wf-client-" + t.Name.Next(ctx.AllowRandom()),
	}
}
