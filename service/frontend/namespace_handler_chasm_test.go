package frontend

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
)

type captureNamespaceReplicationClient struct {
	request *namespacereplicationpb.TriggerNamespaceMutationRequest
}

func (c *captureNamespaceReplicationClient) TriggerNamespaceMutation(
	_ context.Context,
	request *namespacereplicationpb.TriggerNamespaceMutationRequest,
	_ ...grpc.CallOption,
) (*namespacereplicationpb.TriggerNamespaceMutationResponse, error) {
	c.request = request
	return &namespacereplicationpb.TriggerNamespaceMutationResponse{}, nil
}

func TestInvokeShadowNamespaceMutation(t *testing.T) {
	controller := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(controller)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("cell-a")
	client := &captureNamespaceReplicationClient{}
	handler := &namespaceHandler{
		logger:            log.NewNoopLogger(),
		clusterMetadata:   clusterMetadata,
		chasmNsReplClient: client,
		config: &Config{
			NamespaceReplicationTransportMode: dynamicconfig.GetStringPropertyFn(dynamicconfig.NamespaceReplicationTransportModeShadow),
		},
	}
	detail := &persistencespb.NamespaceDetail{
		Info:              &persistencespb.NamespaceInfo{Id: "namespace-id", State: enumspb.NAMESPACE_STATE_REGISTERED},
		Config:            &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{Clusters: []string{"cell-a", "cell-b"}},
		ConfigVersion:     3,
		FailoverVersion:   5,
	}

	handler.invokeShadowNamespaceMutation(
		context.Background(),
		enumsspb.NAMESPACE_OPERATION_UPDATE,
		detail,
		detail,
		7,
		[]string{"cell-a", "cell-c"},
		true,
		true,
	)

	require.NotNil(t, client.request)
	require.True(t, client.request.GetMutation().GetShadow())
	require.Equal(t, int64(7), client.request.GetMutation().GetExpectedVersion())
	require.Equal(t, []string{"cell-b", "cell-c"}, client.request.GetMutation().GetPeerCells())
}

func TestTriggerAuthoritativeNamespaceMutation(t *testing.T) {
	controller := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(controller)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("cell-a")
	client := &captureNamespaceReplicationClient{}
	handler := &namespaceHandler{
		clusterMetadata:   clusterMetadata,
		chasmNsReplClient: client,
	}
	detail := &persistencespb.NamespaceDetail{
		Info:              &persistencespb.NamespaceInfo{Id: "namespace-id"},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{Clusters: []string{"cell-a", "cell-b"}},
	}

	response, err := handler.triggerNamespaceMutation(
		context.Background(),
		enumsspb.NAMESPACE_OPERATION_UPDATE,
		detail,
		7,
		nil,
		namespaceMutationModeAuthoritative,
	)
	require.NoError(t, err)
	require.NotNil(t, response)
	require.False(t, client.request.GetMutation().GetShadow())
	require.False(t, client.request.GetMutation().GetReplicateOnly())
	require.Equal(t, int64(7), client.request.GetMutation().GetExpectedVersion())
	require.Equal(t, []string{"cell-b"}, client.request.GetMutation().GetPeerCells())
}

func TestTriggerReplicateOnlyNamespaceMutation(t *testing.T) {
	controller := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(controller)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("cell-a")
	client := &captureNamespaceReplicationClient{}
	handler := &namespaceHandler{
		clusterMetadata:   clusterMetadata,
		chasmNsReplClient: client,
	}
	detail := &persistencespb.NamespaceDetail{
		Info:              &persistencespb.NamespaceInfo{Id: "namespace-id"},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{Clusters: []string{"cell-a", "cell-b"}},
	}

	_, err := handler.triggerNamespaceMutation(
		context.Background(),
		enumsspb.NAMESPACE_OPERATION_UPDATE,
		detail,
		7,
		nil,
		namespaceMutationModeReplicateOnly,
	)
	require.NoError(t, err)
	require.True(t, client.request.GetMutation().GetReplicateOnly())
	require.False(t, client.request.GetMutation().GetShadow())
}

func TestShouldUseCHASMNamespaceReplication(t *testing.T) {
	handler := &namespaceHandler{config: &Config{
		NamespaceReplicationTransportMode: dynamicconfig.GetStringPropertyFn(dynamicconfig.NamespaceReplicationTransportModeCHASM),
	}}

	require.True(t, handler.shouldUseCHASMNamespaceReplication(
		true,
		false,
		enumspb.NAMESPACE_STATE_REGISTERED,
		[]string{"cell-a", "cell-b"},
	))
	require.False(t, handler.shouldUseCHASMNamespaceReplication(
		false,
		false,
		enumspb.NAMESPACE_STATE_REGISTERED,
		[]string{"cell-a", "cell-b"},
	))
	require.False(t, handler.shouldUseCHASMNamespaceReplication(
		true,
		false,
		enumspb.NAMESPACE_STATE_REGISTERED,
		[]string{"cell-a"},
	))
}

func TestPeerCellsFromClusters(t *testing.T) {
	require.Equal(
		t,
		[]string{"cell-b", "cell-c"},
		peerCellsFromClusters("cell-a", []string{"cell-a", "cell-b", "cell-b"}, []string{"cell-c", "cell-a"}),
	)
}
