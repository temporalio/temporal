package workerdeployment

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/testsuite"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/consts"
	"go.uber.org/mock/gomock"
)

// TestDeleteWorkerDeploymentVersionActivity covers how the activity treats the outcome of the
// delete-version update it sends to the Worker Deployment Version workflow. A version workflow
// that is no longer running answers that update with a NotFound, which the activity must treat
// as "the version is already gone" so the deployment workflow can drop its version summary.
// Errors that do not mean the version is gone must still surface.
func TestDeleteWorkerDeploymentVersionActivity(t *testing.T) {
	t.Parallel()
	version := worker_versioning.WorkerDeploymentVersionToStringV31(&deploymentspb.WorkerDeploymentVersion{
		DeploymentName: testDeployment,
		BuildId:        testBuildID,
	})

	testCases := []struct {
		name        string
		updateErr   error
		expectError bool
	}{
		{
			name:        "version workflow already completed",
			updateErr:   consts.ErrWorkflowCompleted,
			expectError: false,
		},
		{
			name:        "version workflow not found",
			updateErr:   consts.ErrWorkflowExecutionNotFound,
			expectError: false,
		},
		{
			name:        "unrelated error is not swallowed",
			updateErr:   serviceerror.NewUnavailable("history service is unavailable"),
			expectError: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			mockHistoryClient := historyservicemock.NewMockHistoryServiceClient(ctrl)
			mockHistoryClient.EXPECT().
				UpdateWorkflowExecution(gomock.Any(), gomock.Any()).
				Return(nil, tc.updateErr).
				Times(1)

			a := &Activities{
				activityDeps: activityDeps{
					Logger:        log.NewNoopLogger(),
					HistoryClient: mockHistoryClient,
				},
				namespace: namespace.NewLocalNamespaceForTest(
					&persistencespb.NamespaceInfo{Name: testNamespace},
					nil,
					"",
				),
			}

			testSuite := &testsuite.WorkflowTestSuite{}
			env := testSuite.NewTestActivityEnvironment()
			env.RegisterActivity(a.DeleteWorkerDeploymentVersion)

			_, err := env.ExecuteActivity(a.DeleteWorkerDeploymentVersion, &deploymentspb.DeleteVersionActivityArgs{
				DeploymentName: testDeployment,
				Version:        version,
				RequestId:      "delete-version-request-id",
			})

			if tc.expectError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
