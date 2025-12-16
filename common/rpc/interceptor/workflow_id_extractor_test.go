package interceptor

import (
	"testing"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/workflowservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/tasktoken"
)

func TestWorkflowIDExtractor_Extract(t *testing.T) {
	extractor := NewWorkflowIDExtractor()

	t.Run("nil request returns EmptyBusinessID", func(t *testing.T) {
		result := extractor.Extract(nil)
		require.Equal(t, namespace.EmptyBusinessID, result)
	})

	t.Run("StartWorkflowExecutionRequest - direct WorkflowId field", func(t *testing.T) {
		req := &workflowservice.StartWorkflowExecutionRequest{
			WorkflowId: "test-workflow-id",
		}
		result := extractor.Extract(req)
		require.Equal(t, "test-workflow-id", result)
	})

	t.Run("StartWorkflowExecutionRequest - empty WorkflowId returns EmptyBusinessID", func(t *testing.T) {
		req := &workflowservice.StartWorkflowExecutionRequest{
			WorkflowId: "",
		}
		result := extractor.Extract(req)
		require.Equal(t, namespace.EmptyBusinessID, result)
	})

	t.Run("TerminateWorkflowExecutionRequest - WorkflowExecution.WorkflowId field", func(t *testing.T) {
		req := &workflowservice.TerminateWorkflowExecutionRequest{
			WorkflowExecution: &commonpb.WorkflowExecution{
				WorkflowId: "test-workflow-id-from-execution",
			},
		}
		result := extractor.Extract(req)
		require.Equal(t, "test-workflow-id-from-execution", result)
	})

	t.Run("TerminateWorkflowExecutionRequest - nil WorkflowExecution returns EmptyBusinessID", func(t *testing.T) {
		req := &workflowservice.TerminateWorkflowExecutionRequest{
			WorkflowExecution: nil,
		}
		result := extractor.Extract(req)
		require.Equal(t, namespace.EmptyBusinessID, result)
	})

	t.Run("DescribeWorkflowExecutionRequest - Execution.WorkflowId field", func(t *testing.T) {
		req := &workflowservice.DescribeWorkflowExecutionRequest{
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "test-workflow-id-from-exec",
			},
		}
		result := extractor.Extract(req)
		require.Equal(t, "test-workflow-id-from-exec", result)
	})

	t.Run("DescribeWorkflowExecutionRequest - nil Execution returns EmptyBusinessID", func(t *testing.T) {
		req := &workflowservice.DescribeWorkflowExecutionRequest{
			Execution: nil,
		}
		result := extractor.Extract(req)
		require.Equal(t, namespace.EmptyBusinessID, result)
	})

	t.Run("RespondActivityTaskCompletedRequest - valid TaskToken extraction", func(t *testing.T) {
		serializer := tasktoken.NewSerializer()
		taskToken := &tokenspb.Task{
			WorkflowId: "task-token-workflow-id",
		}
		tokenBytes, err := serializer.Serialize(taskToken)
		require.NoError(t, err)

		req := &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: tokenBytes,
		}
		result := extractor.Extract(req)
		require.Equal(t, "task-token-workflow-id", result)
	})

	t.Run("RespondActivityTaskCompletedRequest - invalid TaskToken returns EmptyBusinessID", func(t *testing.T) {
		req := &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: []byte("invalid-token-data"),
		}
		result := extractor.Extract(req)
		require.Equal(t, namespace.EmptyBusinessID, result)
	})

	t.Run("RespondActivityTaskCompletedRequest - empty TaskToken returns EmptyBusinessID", func(t *testing.T) {
		req := &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: []byte{},
		}
		result := extractor.Extract(req)
		require.Equal(t, namespace.EmptyBusinessID, result)
	})

	t.Run("RespondActivityTaskCompletedRequest - nil TaskToken returns EmptyBusinessID", func(t *testing.T) {
		req := &workflowservice.RespondActivityTaskCompletedRequest{
			TaskToken: nil,
		}
		result := extractor.Extract(req)
		require.Equal(t, namespace.EmptyBusinessID, result)
	})

	t.Run("unknown request type returns EmptyBusinessID", func(t *testing.T) {
		req := struct{}{}
		result := extractor.Extract(req)
		require.Equal(t, namespace.EmptyBusinessID, result)
	})
}
