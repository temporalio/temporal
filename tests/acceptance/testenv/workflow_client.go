package testenv

import (
	"context"
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
	"go.temporal.io/server/tests/acceptance/testenv/action"
	"google.golang.org/protobuf/proto"

	. "go.temporal.io/server/common/testing/stamp"
	. "go.temporal.io/server/tests/acceptance/testenv/action"
)

type WorkflowClient struct {
	stamp.ActorModel[*model.WorkflowClient]
	c *Cluster
}

func newWorkflowClient(
	c *Cluster,
	tq *model.TaskQueue,
) *WorkflowClient {
	return &WorkflowClient{
		c:          c,
		ActorModel: stamp.NewActorModel(stamp.Act(c, action.CreateWorkflowClient{TaskQueue: tq})),
	}
}

func (c *WorkflowClient) OnAction(
	ctx context.Context,
	params stamp.ActionParams,
) error {
	switch t := params.Payload.(type) {
	case proto.Message:
		_, err := issueWorkflowRPC(ctx, c.c, t, params.ActID)
		return err
	default:
		panic(fmt.Sprintf("unhandled action %T", t))
	}
	return nil
}

func (c *WorkflowClient) PollUpdateUntilAdmitted(upd *model.WorkflowUpdate) {
	Act(c, PollWorkflowExecutionUpdate{
		WorkflowUpdate: upd,
		WaitStage:      GenJust(enumspb.UPDATE_WORKFLOW_EXECUTION_LIFECYCLE_STAGE_ADMITTED),
	}, WithRetryUntil(&upd.Admitted))
}
