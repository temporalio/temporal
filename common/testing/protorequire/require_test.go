package protorequire_test

import (
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/common/testing/protorequire"
)

const myUUID = "deb7b204-b384-4fde-85c6-e5a56c42336a"

func TestProtoEqualIgnoreFields(t *testing.T) {
	a := &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "wf-1",
			RunId:      myUUID,
		},
		Status: 1,
	}
	b := &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "wf-1",
			RunId:      myUUID,
		},
		Status: 2, // different — will be ignored
	}

	// Should pass: "status" is ignored
	protorequire.ProtoEqualIgnoreFields(t, a, b,
		&workflowpb.WorkflowExecutionInfo{},
		"status",
	)
}
