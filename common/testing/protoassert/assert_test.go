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
		A: []*commonpb.WorkflowExecution{commonpb.WorkflowExecution_builder{
			WorkflowId: "some random workflow ID",
			RunId:      myUUID,
		}.Build(), commonpb.WorkflowExecution_builder{
			WorkflowId: "second workflow",
			RunId:      myUUID,
		}.Build()},
		B: []*commonpb.WorkflowExecution{commonpb.WorkflowExecution_builder{
			WorkflowId: "some random workflow ID",
			RunId:      myUUID,
		}.Build(), commonpb.WorkflowExecution_builder{
			WorkflowId: "second workflow",
			RunId:      myUUID,
		}.Build()},
	}, {
		Name: "Shallow proto object - out of order",
		A: []*commonpb.WorkflowExecution{commonpb.WorkflowExecution_builder{
			WorkflowId: "some random workflow ID",
			RunId:      myUUID,
		}.Build(), commonpb.WorkflowExecution_builder{
			WorkflowId: "second workflow",
			RunId:      myUUID,
		}.Build()},
		B: []*commonpb.WorkflowExecution{commonpb.WorkflowExecution_builder{
			WorkflowId: "second workflow",
			RunId:      myUUID,
		}.Build(), commonpb.WorkflowExecution_builder{
			WorkflowId: "some random workflow ID",
			RunId:      myUUID,
		}.Build()},
	}, {
		Name: "Structs containing proto",
		A: []canHazProto{{
			A: 13,
			B: commonpb.WorkflowExecution_builder{
				WorkflowId: "some random workflow ID",
				RunId:      myUUID,
			}.Build(),
		}, {
			A: 12,
			B: commonpb.WorkflowExecution_builder{
				WorkflowId: "second random workflow ID",
				RunId:      myUUID,
			}.Build(),
		}},
		B: []canHazProto{{
			A: 12,
			B: commonpb.WorkflowExecution_builder{
				WorkflowId: "second random workflow ID",
				RunId:      myUUID,
			}.Build(),
		}, {
			A: 13,
			B: commonpb.WorkflowExecution_builder{
				WorkflowId: "some random workflow ID",
				RunId:      myUUID,
			}.Build(),
		}},
	}, {
		Name: "Nested proto struct",
		A: []*workflowpb.WorkflowExecutionInfo{
			workflowpb.WorkflowExecutionInfo_builder{
				Execution: commonpb.WorkflowExecution_builder{
					WorkflowId: "some random workflow ID",
					RunId:      myUUID,
				}.Build(),
			}.Build(), workflowpb.WorkflowExecutionInfo_builder{
				Execution: commonpb.WorkflowExecution_builder{
					WorkflowId: "second random workflow ID",
					RunId:      myUUID,
				}.Build(),
			}.Build()},
		B: []*workflowpb.WorkflowExecutionInfo{
			workflowpb.WorkflowExecutionInfo_builder{
				Execution: commonpb.WorkflowExecution_builder{
					WorkflowId: "second random workflow ID",
					RunId:      myUUID,
				}.Build(),
			}.Build(), workflowpb.WorkflowExecutionInfo_builder{
				Execution: commonpb.WorkflowExecution_builder{
					WorkflowId: "some random workflow ID",
					RunId:      myUUID,
				}.Build(),
			}.Build()},
	}} {
		if !assert.ProtoElementsMatch(tc.A, tc.B) {
			t.Errorf("%s: expected equality", tc.Name)
		}
	}
}
