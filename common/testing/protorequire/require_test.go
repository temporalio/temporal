package protorequire_test

import (
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/common/testing/protorequire"
)

const myUUID = "deb7b204-b384-4fde-85c6-e5a56c42336a"

type mockT struct {
	failed bool
}

func (m *mockT) Errorf(string, ...any) {
	m.failed = true
}

func (m *mockT) FailNow() {
	m.failed = true
}

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
		mt := &mockT{}
		protorequire.ProtoEqual(mt, a, b,
			protorequire.IgnoreFields(
				"status",
			),
		)
		if !mt.failed {
			t.Fatal("expected comparison to fail when not all differing fields are ignored")
		}
	})
}

type canHazProto struct {
	A float64
	B *commonpb.WorkflowExecution
}

func TestProtoElementsMatch(t *testing.T) {
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
		t.Run(tc.Name, func(t *testing.T) {
			mt := &mockT{}
			protorequire.ProtoElementsMatch(mt, tc.A, tc.B)
			if mt.failed {
				t.Error("expected equality")
			}
		})
	}

	t.Run("mismatch fails", func(t *testing.T) {
		a := []*commonpb.WorkflowExecution{{WorkflowId: "wf-a", RunId: myUUID}}
		b := []*commonpb.WorkflowExecution{{WorkflowId: "wf-b", RunId: myUUID}}
		mt := &mockT{}
		protorequire.ProtoElementsMatch(mt, a, b)
		if !mt.failed {
			t.Fatal("expected comparison to fail for differing element sets")
		}
	})
}

func TestDeepEqual(t *testing.T) {
	a := &commonpb.WorkflowExecution{WorkflowId: "wf-1", RunId: myUUID}

	t.Run("equal values pass", func(t *testing.T) {
		b := &commonpb.WorkflowExecution{WorkflowId: "wf-1", RunId: myUUID}
		mt := &mockT{}
		protorequire.DeepEqual(mt, a, b)
		if mt.failed {
			t.Fatal("expected equal values to pass")
		}
	})

	t.Run("differing values fail", func(t *testing.T) {
		b := &commonpb.WorkflowExecution{WorkflowId: "wf-2", RunId: myUUID}
		mt := &mockT{}
		protorequire.DeepEqual(mt, a, b)
		if !mt.failed {
			t.Fatal("expected differing values to fail")
		}
	})
}
