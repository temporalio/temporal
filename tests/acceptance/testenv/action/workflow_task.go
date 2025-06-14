package action

import (
	"encoding/base64"

	commandpb "go.temporal.io/api/command/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	protocolpb "go.temporal.io/api/protocol/v1"
	"go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/testing/stamp"
	"go.temporal.io/server/tests/acceptance/model"
)

type PollWorkflowTaskQueue struct {
	stamp.ActionActor[*model.WorkflowWorker]
	stamp.ActionTarget[*model.WorkflowTask]
	TaskQueue *model.TaskQueue `validate:"required"`
	Version   *model.WorkerDeploymentVersion
}

func (t PollWorkflowTaskQueue) Next(_ stamp.GenContext) *workflowservice.PollWorkflowTaskQueueRequest {
	var deploymentOptions *deploymentpb.WorkerDeploymentOptions
	if t.Version != nil {
		deploymentOptions = &deploymentpb.WorkerDeploymentOptions{
			DeploymentName:       string(t.Version.GetDeployment().GetID()),
			BuildId:              string(t.Version.GetID()),
			WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED, // TODO?
		}
	}

	return &workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: string(t.TaskQueue.GetNamespace().GetID()),
		TaskQueue: &taskqueue.TaskQueue{
			Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
			Name: string(t.TaskQueue.GetID()),
		},
		Identity:          string(t.GetActor().GetID()),
		DeploymentOptions: deploymentOptions,
	}
}

type WorkflowTaskResponse interface {
	Complete() *RespondWorkflowTaskCompleted
	Fail() *FailWorkflowTask
}

type RespondWorkflowTaskCompleted struct {
	stamp.ActionActor[*model.WorkflowWorker]
	stamp.ActionTarget[*model.WorkflowTask]
	Task                       *model.WorkflowTask `validate:"required"`
	Commands                   stamp.ListGen[*commandpb.Command]
	Messages                   stamp.ListGen[*protocolpb.Message]
	Version                    *model.WorkerDeploymentVersion // TODO: get this from WorkflowTask?
	VersioningBehavior         stamp.Gen[enumspb.VersioningBehavior]
	ReturnNewWorkflowTask      stamp.Gen[bool]
	ForceCreateNewWorkflowTask stamp.Gen[bool]
	StickyTaskQueue            *StickyTaskQueue
	// TODO ...
}

func (t RespondWorkflowTaskCompleted) Next(ctx stamp.GenContext) *workflowservice.RespondWorkflowTaskCompletedRequest {
	if t.Task.Token == "" {
		panic("WorkflowTask Token is empty")
	}
	tokenBytes, err := base64.StdEncoding.DecodeString(t.Task.Token)
	if err != nil {
		panic(err)
	}

	var commands []*commandpb.Command
	for _, cmd := range t.Commands {
		commands = append(commands, cmd.Next(ctx))
	}

	// TODO: randomize order
	var messages []*protocolpb.Message
	var protocolCommands []*commandpb.Command
	for _, msg := range t.Messages {
		m := msg.Next(ctx)
		messages = append(messages, m)

		// auto-generate a ProtocolCommand for each Message
		// TODO: allow to turn this off
		cmd := ProtocolCommand{MessageID: m.Id}.Next(ctx)
		protocolCommands = append(protocolCommands, cmd)
	}
	commands = append(protocolCommands, commands...) // prepending to ensure `CompleteWorkflowExecutionCommand` is always last.

	var deploymentOptions *deploymentpb.WorkerDeploymentOptions
	if t.Version != nil {
		deploymentOptions = &deploymentpb.WorkerDeploymentOptions{
			DeploymentName:       string(t.Version.GetDeployment().GetID()),
			BuildId:              string(t.Version.GetID()),
			WorkerVersioningMode: enumspb.WORKER_VERSIONING_MODE_VERSIONED, // TODO?
		}
	}

	var stickyAttributes *taskqueue.StickyExecutionAttributes
	if t.StickyTaskQueue != nil {
		stickyAttributes = t.StickyTaskQueue.Next(ctx)
	}

	return &workflowservice.RespondWorkflowTaskCompletedRequest{
		Namespace:                  string(t.Task.GetNamespace().GetID()),
		Identity:                   string(t.GetActor().GetID()),
		TaskToken:                  tokenBytes,
		Commands:                   commands,
		Messages:                   messages,
		VersioningBehavior:         t.VersioningBehavior.Next(ctx),
		DeploymentOptions:          deploymentOptions,
		ReturnNewWorkflowTask:      t.ReturnNewWorkflowTask.Next(ctx),
		ForceCreateNewWorkflowTask: t.ForceCreateNewWorkflowTask.Next(ctx),
		StickyAttributes:           stickyAttributes,
	}
}

type FailWorkflowTask struct {
	stamp.ActionActor[*model.WorkflowWorker]
	stamp.ActionTarget[*model.WorkflowTask]
	WorkflowTask *model.WorkflowTask `validate:"required"`
	// TODO
}

func (t FailWorkflowTask) Next(ctx stamp.GenContext) *workflowservice.RespondWorkflowTaskFailedRequest {
	return &workflowservice.RespondWorkflowTaskFailedRequest{
		// TODO
	}
}
