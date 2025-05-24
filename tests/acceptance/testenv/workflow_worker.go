package testenv

import (
	"context"
	"fmt"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
	"go.temporal.io/server/tests/acceptance/testenv/action"
	"google.golang.org/protobuf/proto"
)

type WorkflowWorker struct {
	stamp.ActorModel[*model.WorkflowWorker]
	client workflowservice.WorkflowServiceClient
}

func newWorkflowWorker(
	c *Cluster,
	tq *model.TaskQueue,
) *WorkflowWorker {
	return &WorkflowWorker{
		ActorModel: stamp.NewActorModel(stamp.Act(c, action.CreateWorkflowWorker{TaskQueue: tq})),
		client:     c.tbase.FrontendClient(),
	}
}

func (w *WorkflowWorker) OnAction(
	ctx context.Context,
	params stamp.ActionParams,
) error {
	switch t := params.Payload.(type) {
	case proto.Message:
		_, err := issueWorkflowRPC(ctx, w.client, t, params.ActID)
		return err
	default:
		panic(fmt.Sprintf("unhandled action %T", t))
	}
	return nil
}
