package fact

import "go.temporal.io/server/common/testing/umpire"

const (
	WorkflowType       umpire.EntityType = "Workflow"
	WorkflowRunType    umpire.EntityType = "WorkflowRun"
	WorkflowTaskType   umpire.EntityType = "WorkflowTask"
	TaskQueueType      umpire.EntityType = "TaskQueue"
	NamespaceType      umpire.EntityType = "Namespace"
	NexusOperationType umpire.EntityType = "NexusOperation"
)
