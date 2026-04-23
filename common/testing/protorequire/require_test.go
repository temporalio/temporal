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
		Status:    1,
		TaskQueue: "queue-a",
	}
	b := &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "wf-1",
			RunId:      myUUID,
		},
		Status:    2,
		TaskQueue: "queue-b",
	}

	t.Run("all differing fields ignored", func(t *testing.T) {
		protorequire.ProtoEqual(t, a, b,
			protorequire.IgnoreFields(
				"status",
				"task_queue",
			),
		)
	})

	t.Run("partial ignore still fails", func(t *testing.T) {
		failTest := func(t *testing.T) {
			protorequire.ProtoEqual(t, a, b,
				protorequire.IgnoreFields(
					"status",
				),
			)
		}
		if result := testing.RunTests(func(pat, str string) (bool, error) { return true, nil }, []testing.InternalTest{
			{Name: "PartialIgnoreStillFails", F: failTest},
		}); result {
			t.Fatal("expected comparison to fail when not all differing fields are ignored")
		}
	})
}
