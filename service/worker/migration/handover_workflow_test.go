package migration

import (
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
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
