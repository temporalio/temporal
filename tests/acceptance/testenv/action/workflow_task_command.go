package action

import (
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
)

type CompleteWorkflowExecutionCommand struct{}

func (w CompleteWorkflowExecutionCommand) Next(ctx stamp.GenContext) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_COMPLETE_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_CompleteWorkflowExecutionCommandAttributes{
			CompleteWorkflowExecutionCommandAttributes: &commandpb.CompleteWorkflowExecutionCommandAttributes{
				Result: Payloads{}.Next(ctx), // TODO: make parameter
			},
		},
	}
}

type ProtocolCommand struct {
	MessageID string `validate:"required"`
}

func (c ProtocolCommand) Next(ctx stamp.GenContext) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_PROTOCOL_MESSAGE,
		Attributes: &commandpb.Command_ProtocolMessageCommandAttributes{
			ProtocolMessageCommandAttributes: &commandpb.ProtocolMessageCommandAttributes{
				MessageId: c.MessageID,
			},
		},
	}
}

type StartChildWorkflowExecutionCommand struct {
	WorkflowId stamp.Gen[stamp.ID] `validate:"required"`
	TaskQueue  *model.TaskQueue    `validate:"required"` // TODO: make optional?
	// TODO ...
}

func (c StartChildWorkflowExecutionCommand) Next(ctx stamp.GenContext) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
			StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
				WorkflowId: string(c.WorkflowId.Next(ctx.AllowRandom())),
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: string(c.TaskQueue.GetID()),
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
			},
		},
	}
}
