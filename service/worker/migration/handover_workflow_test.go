package migration

import (
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

func TestHandoverWorkflow(t *testing.T) {
	testSuite := &testsuite.WorkflowTestSuite{}
	env := testSuite.NewTestWorkflowEnvironment()
	var a *activities

	namespaceID := uuid.NewString()

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
