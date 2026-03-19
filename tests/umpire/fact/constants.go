package fact

import "go.temporal.io/server/common/testing/umpire"

const (
	WorkflowType          umpire.EntityType = "Workflow"
	WorkflowExecutionType umpire.EntityType = "WorkflowExecution"
	WorkflowTaskType      umpire.EntityType = "WorkflowTask"
	TaskQueueType         umpire.EntityType = "TaskQueue"
	WorkflowUpdateType    umpire.EntityType = "WorkflowUpdate"
	ActivityTaskType      umpire.EntityType = "ActivityTask"
	NamespaceType         umpire.EntityType = "Namespace"
)
