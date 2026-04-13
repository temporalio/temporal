package protoassert_test

import (
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/common/testing/protoassert"
)

const myUUID = "deb7b204-b384-4fde-85c6-e5a56c42336a"

type noProtoNoCry struct {
	A float64
	B string
	C []bool
}

type canHazProto struct {
	A float64
	B *commonpb.WorkflowExecution
}

func TestProtoElementsMatch(t *testing.T) {
	assert := protoassert.New(t)
	for _, tc := range []struct {
		Name string
		A    any
		B    any
	}{{
		Name: "Shallow proto object - in order",
		A: []*commonpb.WorkflowExecution{{
			WorkflowId: "some random workflow ID",
			RunId:      myUUID,
		}, {
			WorkflowId: "second workflow",
			RunId:      myUUID,
		}},
		B: []*commonpb.WorkflowExecution{{
			WorkflowId: "some random workflow ID",
			RunId:      myUUID,
		}, {
			WorkflowId: "second workflow",
			RunId:      myUUID,
		}},
	}, {
		Name: "Shallow proto object - out of order",
		A: []*commonpb.WorkflowExecution{{
			WorkflowId: "some random workflow ID",
			RunId:      myUUID,
		}, {
			WorkflowId: "second workflow",
			RunId:      myUUID,
		}},
		B: []*commonpb.WorkflowExecution{{
			WorkflowId: "second workflow",
			RunId:      myUUID,
		}, {
			WorkflowId: "some random workflow ID",
			RunId:      myUUID,
		}},
	}, {
		Name: "Structs containing proto",
		A: []canHazProto{{
			A: 13,
			B: &commonpb.WorkflowExecution{
				WorkflowId: "some random workflow ID",
				RunId:      myUUID,
			},
		}, {
			A: 12,
			B: &commonpb.WorkflowExecution{
				WorkflowId: "second random workflow ID",
				RunId:      myUUID,
			},
		}},
		B: []canHazProto{{
			A: 12,
			B: &commonpb.WorkflowExecution{
				WorkflowId: "second random workflow ID",
				RunId:      myUUID,
			},
		}, {
			A: 13,
			B: &commonpb.WorkflowExecution{
				WorkflowId: "some random workflow ID",
				RunId:      myUUID,
			},
		}},
	}, {
		Name: "Nested proto struct",
		A: []*workflowpb.WorkflowExecutionInfo{
			{
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "some random workflow ID",
					RunId:      myUUID,
				},
			}, {
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "second random workflow ID",
					RunId:      myUUID,
				},
			}},
		B: []*workflowpb.WorkflowExecutionInfo{
			{
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "second random workflow ID",
					RunId:      myUUID,
				},
			}, {
				Execution: &commonpb.WorkflowExecution{
					WorkflowId: "some random workflow ID",
					RunId:      myUUID,
				},
			}},
	}} {
		if !assert.ProtoElementsMatch(tc.A, tc.B) {
			t.Errorf("%s: expected equality", tc.Name)
		}
	}
}
