package workerdeployment

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/testsuite"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testvars"
	"go.uber.org/mock/gomock"
)

func TestDeleteWorkerDeploymentVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		historyErr error
		wantErr    bool
	}{
		{
			name:       "version workflow not found",
			historyErr: serviceerror.NewNotFound("version workflow not found"),
		},
		{
			name:       "other history error",
			historyErr: serviceerror.NewUnavailable("history unavailable"),
			wantErr:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			controller := gomock.NewController(t)
			historyClient := historyservicemock.NewMockHistoryServiceClient(controller)
			historyClient.EXPECT().
				UpdateWorkflowExecution(gomock.Any(), gomock.Any()).
				Return(nil, tt.historyErr)

			tv := testvars.New(t)
			activity := &Activities{
				activityDeps: activityDeps{HistoryClient: historyClient},
				namespace: namespace.NewLocalNamespaceForTest(
					&persistencespb.NamespaceInfo{Id: tv.NamespaceID().String(), Name: tv.NamespaceName().String()},
					nil,
					"",
				),
			}
			env := (&testsuite.WorkflowTestSuite{}).NewTestActivityEnvironment()
			env.RegisterActivity(activity)

			_, err := env.ExecuteActivity(activity.DeleteWorkerDeploymentVersion, &deploymentspb.DeleteVersionActivityArgs{
				DeploymentName: tv.DeploymentSeries(),
				Version:        tv.DeploymentVersionString(),
				RequestId:      tv.RequestID(),
			})
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}
