package model

import (
	"go.temporal.io/server/common/testing/stamp"
)

type (
	WorkflowClient struct {
		stamp.Model[*WorkflowClient]
		stamp.Scope[*TaskQueue]
	}
	NewWorkflowClient struct {
		TaskQueue *TaskQueue
		Name      stamp.ID
	}
)

func (c *WorkflowClient) Verify() {}
