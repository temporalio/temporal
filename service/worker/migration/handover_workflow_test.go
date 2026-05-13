package migration

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

func TestHandoverWorkflow(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	var a *activities

	namespaceID := uuid.NewString()

	env.OnActivity(a.GetMetadata, mock.Anything, MetadataRequest{Namespace: "test-ns"}).Return(&MetadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	env.OnActivity(a.GetMaxReplicationTaskIDs, mock.Anything).Return(
		&ReplicationStatus{map[int32]int64{
			1: 100,
			2: 100,
			3: 100,
			4: 100,
		},
		},
		nil,
	)

	env.OnActivity(a.WaitReplication, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(a.UpdateNamespaceState, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(a.WaitHandover, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(a.UpdateActiveCluster, mock.Anything, mock.Anything).Return(nil)

	env.ExecuteWorkflow(NamespaceHandoverWorkflow, NamespaceHandoverParams{
		Namespace:              "test-ns",
		RemoteCluster:          "test-remote",
		AllowedLaggingSeconds:  10,
		HandoverTimeoutSeconds: 10,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	env.AssertExpectations(t)
}

// TestHandoverWorkflow_CancelAfterHandoverState_ResetsToNormal guards against the
// regression where a cancel after the HANDOVER write skipped the deferred reset.
func TestHandoverWorkflow_CancelAfterHandoverState_ResetsToNormal(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	var a *activities

	namespaceID := uuid.NewString()

	env.OnActivity(a.GetMetadata, mock.Anything, MetadataRequest{Namespace: "test-ns"}).
		Return(&MetadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)
	env.OnActivity(a.GetMaxReplicationTaskIDs, mock.Anything).
		Return(&ReplicationStatus{MaxReplicationTaskIds: map[int32]int64{1: 100}}, nil)
	env.OnActivity(a.WaitReplication, mock.Anything, mock.Anything).Return(nil)

	var stateUpdates []enumspb.ReplicationState
	env.OnActivity(a.UpdateNamespaceState, mock.Anything, mock.Anything).
		Run(func(args mock.Arguments) {
			req := args.Get(1).(updateStateRequest)
			stateUpdates = append(stateUpdates, req.NewState)
			// Simulate a cancel arriving once HANDOVER has been written.
			if req.NewState == enumspb.REPLICATION_STATE_HANDOVER {
				env.CancelWorkflow()
			}
		}).
		Return(nil)
	// WaitHandover should observe the cancel and return an error; the defer must still run.
	env.OnActivity(a.WaitHandover, mock.Anything, mock.Anything).Return(temporal.NewCanceledError())

	env.ExecuteWorkflow(NamespaceHandoverWorkflow, NamespaceHandoverParams{
		Namespace:              "test-ns",
		RemoteCluster:          "test-remote",
		AllowedLaggingSeconds:  10,
		HandoverTimeoutSeconds: 10,
	})

	require.True(t, env.IsWorkflowCompleted())
	require.Equal(
		t,
		[]enumspb.ReplicationState{
			enumspb.REPLICATION_STATE_HANDOVER,
			enumspb.REPLICATION_STATE_NORMAL,
		},
		stateUpdates,
		"defer must reset state to NORMAL after cancel",
	)
}

func TestHandoverWorkflow_SetTimeout(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Set workflow execution timeout to 5 minutes
	env.SetWorkflowRunTimeout(5 * time.Minute)

	env.ExecuteWorkflow(NamespaceHandoverWorkflowV2, NamespaceHandoverParams{
		Namespace:              "test-ns",
		RemoteCluster:          "test-remote",
		AllowedLaggingSeconds:  10,
		HandoverTimeoutSeconds: 10,
	})

	require.True(t, env.IsWorkflowCompleted())

	// Verify that the workflow failed with the expected error
	workflowErr := env.GetWorkflowError()
	require.Error(t, workflowErr)

	var applicationErr *temporal.ApplicationError
	require.True(t, errors.As(workflowErr, &applicationErr))
	assert.Equal(t, "InvalidTimeout", applicationErr.Type())
	assert.True(t, applicationErr.NonRetryable())
}
