package deletenamespace

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.uber.org/mock/gomock"
)

func TestMarkNamespaceDeletedActivity(t *testing.T) {
	for _, state := range []enumspb.NamespaceState{
		enumspb.NAMESPACE_STATE_REGISTERED,
		enumspb.NAMESPACE_STATE_DEPRECATED,
		enumspb.NAMESPACE_STATE_DELETED,
	} {
		t.Run(state.String(), func(t *testing.T) {
			metadataManager := persistence.NewMockMetadataManager(gomock.NewController(t))
			a := &localActivities{metadataManager: metadataManager, logger: log.NewTestLogger()}
			ns := &persistencespb.NamespaceDetail{
				Info: &persistencespb.NamespaceInfo{Id: "namespace-id", Name: "namespace", State: state},
			}
			metadataRead := metadataManager.EXPECT().GetMetadata(gomock.Any()).Return(
				&persistence.GetMetadataResponse{NotificationVersion: 42}, nil,
			)
			namespaceRead := metadataManager.EXPECT().GetNamespace(gomock.Any(), &persistence.GetNamespaceRequest{
				Name: "namespace",
			}).Return(&persistence.GetNamespaceResponse{
				Namespace: ns, IsGlobalNamespace: true,
			}, nil).After(metadataRead)

			if state != enumspb.NAMESPACE_STATE_DELETED {
				metadataManager.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(
					func(_ context.Context, request *persistence.UpdateNamespaceRequest) error {
						require.Equal(t, enumspb.NAMESPACE_STATE_DELETED, request.Namespace.Info.State)
						require.Equal(t, "namespace-id", request.Namespace.Info.Id)
						require.Equal(t, "namespace", request.Namespace.Info.Name)
						require.Equal(t, int64(42), request.NotificationVersion)
						require.True(t, request.IsGlobalNamespace)
						return nil
					},
				).After(namespaceRead)
			}

			require.NoError(t, a.MarkNamespaceDeletedActivity(context.Background(), "namespace"))
		})
	}
}
