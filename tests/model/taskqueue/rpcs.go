package taskqueue

import (
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	nexuspb "go.temporal.io/api/nexus/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/tests/model"
	. "go.temporal.io/server/tests/testcore/umpire"
)

// rpcs is the per-package RPC catalog. It holds the detailed PollNexusTaskQueue
// spec (which belongs to the task-queue concern even though the
// standalone-nexus-op driver invokes it) plus catalog placeholders for
// every other method that can implicitly bring a queue into existence.
var rpcs = []RPCSpec{
	model.PollNexusTaskQueueRPCSpec(
		model.PollNexusTaskQueueRequestSpec{
			Namespace: model.NamespaceField,
			TaskQueue: model.PollNexusTaskQueueRequestTaskQueueSpec{
				Name:       Field[string](Mutation[string]("wrong-task-queue")),
				Kind:       Field[enumspb.TaskQueueKind](),
				NormalName: Field[string](Ignored[string]()),
			},
			PollerGroupID:             Field[string](Ignored[string]()),
			Identity:                  model.IdentityField,
			WorkerInstanceKey:         Field[string](Ignored[string]()),
			WorkerVersionCapabilities: model.PollNexusTaskQueueRequestWorkerVersionCapabilitiesSpec{Ref: Field[*commonpb.WorkerVersionCapabilities](Ignored[*commonpb.WorkerVersionCapabilities]())},
			DeploymentOptions:         model.PollNexusTaskQueueRequestDeploymentOptionsSpec{Ref: Field[*deploymentpb.WorkerDeploymentOptions](Ignored[*deploymentpb.WorkerDeploymentOptions]())},
			WorkerHeartbeat:           Field[any](Ignored[any]()),
		},
		model.PollNexusTaskQueueResponseSpec{
			TaskToken:             Field[[]byte](Role[[]byte](RPCFieldRoleOpaque)),
			Request:               model.PollNexusTaskQueueResponseRequestSpec{Ref: Field[*nexuspb.Request]()},
			PollerScalingDecision: model.PollNexusTaskQueueResponsePollerScalingDecisionSpec{Ref: Field[*taskqueuepb.PollerScalingDecision](Ignored[*taskqueuepb.PollerScalingDecision]())},
			PollerGroupID:         Field[string](Role[string](RPCFieldRoleNonDeterministic)),
			PollerGroupInfos:      Field[any](Ignored[any]()),
		},
	),
	{Method: workflowservice.WorkflowService_PollWorkflowTaskQueue_FullMethodName},
	{Method: workflowservice.WorkflowService_PollActivityTaskQueue_FullMethodName},
	{Method: workflowservice.WorkflowService_DescribeTaskQueue_FullMethodName},
	{Method: workflowservice.WorkflowService_StartWorkflowExecution_FullMethodName},
}
