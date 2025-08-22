package migration

import (
	"errors"
	"testing"
	"time"

	"github.com/pborman/uuid"
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

	namespaceID := uuid.New()

	env.OnActivity(a.GetMetadata, mock.Anything, metadataRequest{Namespace: "test-ns"}).Return(&metadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	env.OnActivity(a.GetMaxReplicationTaskIDs, mock.Anything).Return(
		&replicationStatus{map[int32]int64{
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

func TestHandoverWorkflow_InsufficientTimeoutInput(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()

	// Set workflow execution timeout to 5 minutes (less than the required 10 minutes)
	env.SetWorkflowRunTimeout(5 * time.Minute)

	env.ExecuteWorkflow(NamespaceHandoverWorkflow, NamespaceHandoverParams{
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
	assert.Equal(t, "InsufficientTimeout", applicationErr.Type())
	assert.True(t, applicationErr.NonRetryable())
}

func TestHandoverWorkflow_InsufficientRunTimeout(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	var a *activities

	namespaceID := uuid.New()

	// Set workflow execution timeout to 11 minutes (larger than the required 10 minutes)
	env.SetWorkflowRunTimeout(11 * time.Minute)

	env.OnActivity(a.GetMetadata, mock.Anything, metadataRequest{Namespace: "test-ns"}).Return(&metadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	env.OnActivity(a.GetMaxReplicationTaskIDs, mock.Anything).Return(
		&replicationStatus{map[int32]int64{
			1: 100,
			2: 100,
			3: 100,
			4: 100,
		},
		},
		nil,
	)
	// Simulate that we've already been running for a while, leaving only 9 minutes remaining
	env.OnActivity(a.WaitReplication, mock.Anything, mock.Anything).Return(nil).After(time.Minute * 2)

	// These activities should NOT be called because the workflow should fail before reaching handover state
	// We don't set up mocks for UpdateNamespaceState, WaitHandover, or UpdateActiveCluster

	env.ExecuteWorkflow(NamespaceHandoverWorkflow, NamespaceHandoverParams{
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
	assert.Equal(t, "InsufficientTime", applicationErr.Type())
	assert.True(t, applicationErr.NonRetryable())
}

func TestHandoverWorkflow_SufficientTimeout(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	var a *activities

	namespaceID := uuid.New()

	// Set workflow execution timeout to 15 minutes (more than the required 10 minutes)
	env.SetWorkflowRunTimeout(15 * time.Minute)

	env.OnActivity(a.GetMetadata, mock.Anything, metadataRequest{Namespace: "test-ns"}).Return(&metadataResponse{ShardCount: 4, NamespaceID: namespaceID}, nil)

	env.OnActivity(a.GetMaxReplicationTaskIDs, mock.Anything).Return(
		&replicationStatus{map[int32]int64{
			1: 100,
			2: 100,
			3: 100,
			4: 100,
		},
		},
		nil,
	)

	env.OnActivity(a.WaitReplication, mock.Anything, mock.Anything).Return(nil)

	// Expect the handover state update to be called
	env.OnActivity(a.UpdateNamespaceState, mock.Anything, updateStateRequest{
		Namespace: "test-ns",
		NewState:  enumspb.REPLICATION_STATE_HANDOVER,
	}).Return(nil)

	// Expect the reset state update to be called (from defer)
	env.OnActivity(a.UpdateNamespaceState, mock.Anything, updateStateRequest{
		Namespace: "test-ns",
		NewState:  enumspb.REPLICATION_STATE_NORMAL,
	}).Return(nil)

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
