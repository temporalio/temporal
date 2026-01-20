package testcore

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
)

// ForceWorkflowTaskTimeout forces a workflow task to timeout immediately without waiting
// for the actual timeout duration. This is useful for testing timeout handling behavior.
//
// This works for both regular (persisted) and speculative workflow tasks.
// The pending timeout timers are cancelled/deleted and the timeout event
// is added directly to the workflow history.
//
// The function retries until it succeeds or times out, making it safe to call
// immediately after an operation that schedules a workflow task without needing
// to add a sleep.
//
// Parameters:
//   - namespaceID: The namespace ID (UUID) of the workflow
//   - workflowID: The workflow ID
//   - runID: The run ID of the workflow execution
//   - timeoutType: The type of timeout to force:
//   - TIMEOUT_TYPE_SCHEDULE_TO_START: For workflow tasks that are scheduled but not yet started
//   - TIMEOUT_TYPE_START_TO_CLOSE: For workflow tasks that are started but not yet completed
func ForceWorkflowTaskTimeout(
	t testing.TB,
	tc *TestCluster,
	namespaceID string,
	workflowID string,
	runID string,
	timeoutType enumspb.TimeoutType,
) {
	t.Helper()

	req := &historyservice.ForceWorkflowTaskTimeoutRequest{
		NamespaceId: namespaceID,
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		TimeoutType: timeoutType,
	}

	// Retry until the workflow task is available and can be timed out.
	// This handles the race between scheduling a workflow task and calling this function.
	require.Eventually(t, func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_, err := tc.HistoryClient().ForceWorkflowTaskTimeout(ctx, req)
		return err == nil
	}, 10*time.Second, 50*time.Millisecond, "failed to force workflow task timeout")
}
