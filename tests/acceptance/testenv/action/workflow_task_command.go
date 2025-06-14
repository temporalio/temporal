package action

import (
	"time"

	commandpb "go.temporal.io/api/command/v1"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
	"google.golang.org/protobuf/types/known/durationpb"
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
	WorkflowId   stamp.Gen[stamp.ID] `validate:"required"`
	WorkflowType stamp.Gen[stamp.ID]
	TaskQueue    *model.TaskQueue `validate:"required"` // TODO: make optional?
	// TODO ...
}

func (c StartChildWorkflowExecutionCommand) Next(ctx stamp.GenContext) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_START_CHILD_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_StartChildWorkflowExecutionCommandAttributes{
			StartChildWorkflowExecutionCommandAttributes: &commandpb.StartChildWorkflowExecutionCommandAttributes{
				WorkflowId: "child-wf-" + string(c.WorkflowId.Next(ctx.AllowRandom())),
				WorkflowType: &commonpb.WorkflowType{
					Name: "child-wf-type-" + string(c.WorkflowType.Next(ctx.AllowRandom())),
				},
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: string(c.TaskQueue.GetID()),
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
			},
		},
	}
}

type ScheduleActivityTaskCommand struct {
	ActivityId             stamp.Gen[stamp.ID]
	ActivityType           stamp.Gen[stamp.ID]
	TaskQueue              *model.TaskQueue `validate:"required"`
	StartToClose           stamp.Gen[time.Duration]
	ScheduleToCloseTimeout stamp.Gen[time.Duration]
}

func (c ScheduleActivityTaskCommand) Next(ctx stamp.GenContext) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_SCHEDULE_ACTIVITY_TASK,
		Attributes: &commandpb.Command_ScheduleActivityTaskCommandAttributes{
			ScheduleActivityTaskCommandAttributes: &commandpb.ScheduleActivityTaskCommandAttributes{
				ActivityId:   string(c.ActivityId.Next(ctx.AllowRandom())),
				ActivityType: &commonpb.ActivityType{Name: string(c.ActivityType.Next(ctx.AllowRandom()))},
				TaskQueue: &taskqueuepb.TaskQueue{
					Name: string(c.TaskQueue.GetID()),
					Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
				},
				StartToCloseTimeout:    durationpb.New(c.StartToClose.NextOrDefault(ctx, 5*time.Second)),
				ScheduleToCloseTimeout: durationpb.New(c.ScheduleToCloseTimeout.NextOrDefault(ctx, 5*time.Second)),
			},
		},
	}
}

type ContinueAsNewWorkflowCommand struct{}

func (c ContinueAsNewWorkflowCommand) Next(ctx stamp.GenContext) *commandpb.Command {
	return &commandpb.Command{
		CommandType: enumspb.COMMAND_TYPE_CONTINUE_AS_NEW_WORKFLOW_EXECUTION,
		Attributes: &commandpb.Command_ContinueAsNewWorkflowExecutionCommandAttributes{
			ContinueAsNewWorkflowExecutionCommandAttributes: &commandpb.ContinueAsNewWorkflowExecutionCommandAttributes{},
		},
	}
}
