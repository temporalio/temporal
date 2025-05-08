package deletenamespace

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.uber.org/mock/gomock"
)

func Test_GenerateDeletedNamespaceNameActivity(t *testing.T) {
	ctrl := gomock.NewController(t)
	metadataManager := persistence.NewMockMetadataManager(ctrl)

	a := &localActivities{
		metadataManager: metadataManager,
		logger:          log.NewTestLogger(),
	}

	metadataManager.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: "namespace-deleted-names",
	}).Return(nil, serviceerror.NewNamespaceNotFound("namespace-deleted-names"))
	deletedName, err := a.GenerateDeletedNamespaceNameActivity(context.Background(), "namespace-id", "namespace")
	require.NoError(t, err)
	require.Equal(t, namespace.Name("namespace-deleted-names"), deletedName)

	deletedName, err = a.GenerateDeletedNamespaceNameActivity(context.Background(), "namespace-id", "namespace-deleted-names")
	require.NoError(t, err)
	require.Equal(t, namespace.Name("namespace-deleted-names"), deletedName)

	metadataManager.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: "namespace-deleted-names",
	}).Return(nil, nil)
	metadataManager.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
		Name: "namespace-deleted-namesp",
	}).Return(nil, serviceerror.NewNamespaceNotFound("namespace-deleted-namesp"))
	deletedName, err = a.GenerateDeletedNamespaceNameActivity(context.Background(), "namespace-id", "namespace")
	require.NoError(t, err)
	require.Equal(t, namespace.Name("namespace-deleted-namesp"), deletedName)

	ctrl.Finish()
}
func Test_ValidateNexusEndpointsActivity(t *testing.T) {
	ctrl := gomock.NewController(t)
	nexusEndpointManager := persistence.NewMockNexusEndpointManager(ctrl)

	a := &localActivities{
		nexusEndpointManager: nexusEndpointManager,
		logger:               log.NewTestLogger(),

		allowDeleteNamespaceIfNexusEndpointTarget: func() bool { return false },
		nexusEndpointListDefaultPageSize:          func() int { return 100 },
	}

	// The "fake" namespace ID is associated with a Nexus endoint.
	nexusEndpointManager.EXPECT().ListNexusEndpoints(gomock.Any(), gomock.Any()).Return(&persistence.ListNexusEndpointsResponse{
		Entries: []*persistencespb.NexusEndpointEntry{
			{
				Endpoint: &persistencespb.NexusEndpoint{
					Spec: &persistencespb.NexusEndpointSpec{
						Name: "test-endpoint",
						Target: &persistencespb.NexusEndpointTarget{
							Variant: &persistencespb.NexusEndpointTarget_Worker_{
								Worker: &persistencespb.NexusEndpointTarget_Worker{
									NamespaceId: "namespace-id",
								},
							},
						},
					},
				},
			},
		},
	}, nil).Times(2)

	err := a.ValidateNexusEndpointsActivity(context.Background(), "namespace-id", "namespace")
	require.Error(t, err)
	var appErr *temporal.ApplicationError
	require.ErrorAs(t, err, &appErr)

	err = a.ValidateNexusEndpointsActivity(context.Background(), "namespace2-id", "namespace2")
	require.NoError(t, err)

	nexusEndpointManager.EXPECT().ListNexusEndpoints(gomock.Any(), gomock.Any()).Return(nil, errors.New("persistence failure"))
	err = a.ValidateNexusEndpointsActivity(context.Background(), "namespace-id", "namespace")
	require.Error(t, err)
	require.Equal(t, err.Error(), "unable to list Nexus endpoints for namespace namespace: persistence failure")

	ctrl.Finish()
}
