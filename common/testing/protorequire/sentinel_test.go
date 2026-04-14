package protorequire_test

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNonZero_String(t *testing.T) {
	expected := &commonpb.WorkflowExecution{
		WorkflowId: "wf-1",
		RunId:      protorequire.NonZero[string](),
	}

	t.Run("passes when set", func(t *testing.T) {
		actual := &commonpb.WorkflowExecution{
			WorkflowId: "wf-1",
			RunId:      "some-run-id",
		}
		protorequire.ProtoEqual(t, expected, actual)
	})

	t.Run("fails when empty", func(t *testing.T) {
		mockT := &mockTestingT{}
		actual := &commonpb.WorkflowExecution{
			WorkflowId: "wf-1",
			RunId:      "",
		}
		protorequire.ProtoEqual(mockT, expected, actual)
		require.True(t, mockT.failed, "expected ProtoEqual to fail for empty string field")
	})
}

func TestNonZero_Timestamp(t *testing.T) {
	expected := &workflowpb.WorkflowExecutionInfo{
		TaskQueue: "queue-a",
		StartTime: protorequire.NonZero[*timestamppb.Timestamp](),
	}

	t.Run("passes when set", func(t *testing.T) {
		actual := &workflowpb.WorkflowExecutionInfo{
			TaskQueue: "queue-a",
			StartTime: timestamppb.Now(),
		}
		protorequire.ProtoEqual(t, expected, actual)
	})

	t.Run("fails when nil", func(t *testing.T) {
		mockT := &mockTestingT{}
		actual := &workflowpb.WorkflowExecutionInfo{
			TaskQueue: "queue-a",
		}
		protorequire.ProtoEqual(mockT, expected, actual)
		require.True(t, mockT.failed, "expected ProtoEqual to fail for nil timestamp field")
	})
}

func TestNonZero_Duration(t *testing.T) {
	expected := &workflowpb.WorkflowExecutionInfo{
		TaskQueue:         "queue-a",
		ExecutionDuration: protorequire.NonZero[*durationpb.Duration](),
	}

	t.Run("passes when set", func(t *testing.T) {
		actual := &workflowpb.WorkflowExecutionInfo{
			TaskQueue:         "queue-a",
			ExecutionDuration: durationpb.New(5000),
		}
		protorequire.ProtoEqual(t, expected, actual)
	})

	t.Run("fails when nil", func(t *testing.T) {
		mockT := &mockTestingT{}
		actual := &workflowpb.WorkflowExecutionInfo{
			TaskQueue: "queue-a",
		}
		protorequire.ProtoEqual(mockT, expected, actual)
		require.True(t, mockT.failed, "expected ProtoEqual to fail for nil duration field")
	})
}

func TestNonZero_Int64(t *testing.T) {
	expected := &workflowpb.WorkflowExecutionInfo{
		TaskQueue:            "queue-a",
		StateTransitionCount: protorequire.NonZero[int64](),
	}

	t.Run("passes when non-zero", func(t *testing.T) {
		actual := &workflowpb.WorkflowExecutionInfo{
			TaskQueue:            "queue-a",
			StateTransitionCount: 42,
		}
		protorequire.ProtoEqual(t, expected, actual)
	})

	t.Run("fails when zero", func(t *testing.T) {
		mockT := &mockTestingT{}
		actual := &workflowpb.WorkflowExecutionInfo{
			TaskQueue: "queue-a",
		}
		protorequire.ProtoEqual(mockT, expected, actual)
		require.True(t, mockT.failed, "expected ProtoEqual to fail for zero int64 field")
	})
}

func TestNonZero_Mixed(t *testing.T) {
	expected := &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "wf-1",
			RunId:      protorequire.NonZero[string](),
		},
		TaskQueue:            protorequire.NonZero[string](),
		StartTime:            protorequire.NonZero[*timestamppb.Timestamp](),
		StateTransitionCount: protorequire.NonZero[int64](),
	}
	actual := &workflowpb.WorkflowExecutionInfo{
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "wf-1",
			RunId:      "run-abc",
		},
		TaskQueue:            "queue-a",
		StartTime:            timestamppb.Now(),
		StateTransitionCount: 3,
	}
	protorequire.ProtoEqual(t, expected, actual)
}

// mockTestingT captures failures without stopping the test.
type mockTestingT struct {
	failed bool
}

func (m *mockTestingT) Errorf(string, ...interface{}) { m.failed = true }
func (m *mockTestingT) FailNow()                      { m.failed = true }
