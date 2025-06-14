package testenv

import (
	"context"
	"fmt"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
	"go.temporal.io/server/tests/acceptance/testenv/action"
	"google.golang.org/protobuf/proto"

	. "go.temporal.io/server/common/testing/stamp"
	. "go.temporal.io/server/tests/acceptance/testenv/action"
)

type WorkflowWorker struct {
	stamp.ActorModel[*model.WorkflowWorker]
	c      *Cluster
	client workflowservice.WorkflowServiceClient
}

func newWorkflowWorker(
	c *Cluster,
	tq *model.TaskQueue,
) *WorkflowWorker {
	return &WorkflowWorker{
		c:          c,
		ActorModel: stamp.NewActorModel(stamp.Act(c, action.CreateWorkflowWorker{TaskQueue: tq})),
	}
}

func (w *WorkflowWorker) OnAction(
	ctx context.Context,
	params stamp.ActionParams,
) error {
	switch t := params.Payload.(type) {
	case proto.Message:
		_, err := issueWorkflowRPC(ctx, w.c, t, params.ActID)
		return err
	default:
		panic(fmt.Sprintf("unhandled action %T", t))
	}
	return nil
}

func (w *WorkflowWorker) DrainWorkflowTask(tq *model.TaskQueue) {
	wft := Act(w, PollWorkflowTaskQueue{TaskQueue: tq})
	Act(w, RespondWorkflowTaskCompleted{Task: wft})
}

func (w *WorkflowWorker) CompleteWorkflow(tq *model.TaskQueue) {
	wft := Act(w, PollWorkflowTaskQueue{TaskQueue: tq})
	Act(w, RespondWorkflowTaskCompleted{Task: wft})
}

func (w *WorkflowWorker) HeartbeatWorkflowTask(tq *model.TaskQueue, wft *model.WorkflowTask) *model.WorkflowTask {
	return Act(w, RespondWorkflowTaskCompleted{
		Task:                       wft,
		ReturnNewWorkflowTask:      GenJust(true),
		ForceCreateNewWorkflowTask: GenJust(true),
	})
}
