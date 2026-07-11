package fact

import "go.temporal.io/server/common/testing/umpire"

const (
	WorkflowType          umpire.EntityType = "Workflow"
	WorkflowTaskType      umpire.EntityType = "WorkflowTask"
	TaskQueueType         umpire.EntityType = "TaskQueue"
	WorkflowUpdateType    umpire.EntityType = "WorkflowUpdate"
	NamespaceType         umpire.EntityType = "Namespace"
)
