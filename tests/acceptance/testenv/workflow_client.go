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

type WorkflowClient struct {
	stamp.ActorModel[*model.WorkflowClient]
	client workflowservice.WorkflowServiceClient
}

func newWorkflowClient(
	c *Cluster,
	tq *model.TaskQueue,
) *WorkflowClient {
	return &WorkflowClient{
		ActorModel: stamp.NewActorModel(stamp.Act(c, action.CreateWorkflowClient{TaskQueue: tq})),
		client:     c.tbase.FrontendClient(),
	}
}

func (c *WorkflowClient) OnAction(
	ctx context.Context,
	params stamp.ActionParams,
) error {
	switch t := params.Payload.(type) {
	case proto.Message:
		_, err := issueWorkflowRPC(ctx, c.client, t, params.ActID)
		return err
	default:
		panic(fmt.Sprintf("unhandled action %T", t))
	}
	return nil
}
