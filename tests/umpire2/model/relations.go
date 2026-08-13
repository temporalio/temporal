package model

import "go.temporal.io/server/common/testing/umpire"

const (
	WorkflowRunsRelation       umpire.RelationType = "workflow-runs"
	CallbackOperationRelation  umpire.RelationType = "callback-operation"
	CallbackHandlerRunRelation umpire.RelationType = "callback-handler-run"
)
