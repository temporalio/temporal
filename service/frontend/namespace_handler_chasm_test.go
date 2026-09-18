package frontend

import (
	"context"
	"testing"
	"time"

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
	request  *namespacereplicationpb.TriggerNamespaceMutationRequest
	requests chan *namespacereplicationpb.TriggerNamespaceMutationRequest
}

func (c *captureNamespaceReplicationClient) TriggerNamespaceMutation(
	_ context.Context,
	request *namespacereplicationpb.TriggerNamespaceMutationRequest,
	_ ...grpc.CallOption,
) (*namespacereplicationpb.TriggerNamespaceMutationResponse, error) {
	c.request = request
	if c.requests != nil {
		c.requests <- request
	}
	return &namespacereplicationpb.TriggerNamespaceMutationResponse{}, nil
}

type blockingNamespaceReplicationClient struct {
	started   chan struct{}
	release   chan struct{}
	completed chan struct{}
}

func (c *blockingNamespaceReplicationClient) TriggerNamespaceMutation(
	_ context.Context,
	_ *namespacereplicationpb.TriggerNamespaceMutationRequest,
	_ ...grpc.CallOption,
) (*namespacereplicationpb.TriggerNamespaceMutationResponse, error) {
	close(c.started)
	<-c.release
	close(c.completed)
	return &namespacereplicationpb.TriggerNamespaceMutationResponse{}, nil
}

func TestInvokeShadowNamespaceMutation(t *testing.T) {
	controller := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(controller)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("cell-a")
	client := &captureNamespaceReplicationClient{requests: make(chan *namespacereplicationpb.TriggerNamespaceMutationRequest, 1)}
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
		namespaceReplicationTransportShadow,
		enumsspb.NAMESPACE_OPERATION_UPDATE,
		detail,
		detail,
		7,
		[]string{"cell-a", "cell-c"},
		true,
		true,
	)
	select {
	case request := <-client.requests:
		require.True(t, request.GetMutation().GetShadow())
		require.Equal(t, int64(7), request.GetMutation().GetExpectedVersion())
		require.Equal(t, []string{"cell-b", "cell-c"}, request.GetMutation().GetPeerCells())
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shadow namespace mutation")
	}
}

func TestInvokeShadowNamespaceMutationDoesNotBlockCaller(t *testing.T) {
	controller := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(controller)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("cell-a")
	client := &blockingNamespaceReplicationClient{
		started:   make(chan struct{}),
		release:   make(chan struct{}),
		completed: make(chan struct{}),
	}
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
	}

	returned := make(chan struct{})
	go func() {
		handler.invokeShadowNamespaceMutation(
			namespaceReplicationTransportShadow,
			enumsspb.NAMESPACE_OPERATION_UPDATE,
			detail,
			detail,
			7,
			nil,
			true,
			true,
		)
		close(returned)
	}()

	select {
	case <-returned:
	case <-time.After(time.Second):
		t.Fatal("shadow namespace mutation blocked the caller")
	}
	select {
	case <-client.started:
	case <-time.After(time.Second):
		t.Fatal("shadow namespace mutation was not started")
	}
	close(client.release)
	select {
	case <-client.completed:
	case <-time.After(time.Second):
		t.Fatal("shadow namespace mutation did not complete")
	}
}

func TestEffectiveNamespaceReplicationTransportMode(t *testing.T) {
	testCases := []struct {
		name       string
		configured string
		want       namespaceReplicationTransportMode
	}{
		{name: "legacy", configured: dynamicconfig.NamespaceReplicationTransportModeLegacy, want: namespaceReplicationTransportLegacy},
		{name: "shadow", configured: dynamicconfig.NamespaceReplicationTransportModeShadow, want: namespaceReplicationTransportShadow},
		{name: "chasm", configured: dynamicconfig.NamespaceReplicationTransportModeCHASM, want: namespaceReplicationTransportCHASM},
		{name: "unknown falls back to legacy", configured: "unknown", want: namespaceReplicationTransportLegacy},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			handler := &namespaceHandler{
				logger: log.NewNoopLogger(),
				config: &Config{
					NamespaceReplicationTransportMode: dynamicconfig.GetStringPropertyFn(tc.configured),
				},
			}
			require.Equal(t, tc.want, handler.effectiveNamespaceReplicationTransportMode())
		})
	}
}

func TestTriggerNamespaceMutationModes(t *testing.T) {
	controller := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(controller)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("cell-a").Times(3)
	client := &captureNamespaceReplicationClient{requests: make(chan *namespacereplicationpb.TriggerNamespaceMutationRequest, 3)}
	handler := &namespaceHandler{
		clusterMetadata:   clusterMetadata,
		chasmNsReplClient: client,
	}
	detail := &persistencespb.NamespaceDetail{
		Info:              &persistencespb.NamespaceInfo{Id: "namespace-id"},
		Config:            &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{Clusters: []string{"cell-a", "cell-b"}},
	}

	for _, tc := range []struct {
		name              string
		mode              namespaceMutationMode
		wantShadow        bool
		wantReplicateOnly bool
	}{
		{name: "authoritative", mode: namespaceMutationModeAuthoritative},
		{name: "shadow", mode: namespaceMutationModeShadow, wantShadow: true},
		{name: "replicate only", mode: namespaceMutationModeReplicateOnly, wantReplicateOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := handler.triggerNamespaceMutation(
				context.Background(),
				enumsspb.NAMESPACE_OPERATION_UPDATE,
				detail,
				7,
				nil,
				tc.mode,
			)
			require.NoError(t, err)
			request := <-client.requests
			require.Equal(t, tc.wantShadow, request.GetMutation().GetShadow())
			require.Equal(t, tc.wantReplicateOnly, request.GetMutation().GetReplicateOnly())
		})
	}
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
		namespaceReplicationTransportCHASM,
		true,
		false,
		enumspb.NAMESPACE_STATE_REGISTERED,
		[]string{"cell-a", "cell-b"},
	))
	require.False(t, handler.shouldUseCHASMNamespaceReplication(
		namespaceReplicationTransportCHASM,
		false,
		false,
		enumspb.NAMESPACE_STATE_REGISTERED,
		[]string{"cell-a", "cell-b"},
	))
	require.False(t, handler.shouldUseCHASMNamespaceReplication(
		namespaceReplicationTransportCHASM,
		true,
		false,
		enumspb.NAMESPACE_STATE_REGISTERED,
		[]string{"cell-a"},
	))
	require.False(t, handler.shouldUseCHASMNamespaceReplication(
		namespaceReplicationTransportShadow,
		true,
		false,
		enumspb.NAMESPACE_STATE_REGISTERED,
		[]string{"cell-a", "cell-b"},
	))
}

func TestPeerCellsFromClusters(t *testing.T) {
	require.Equal(
		t,
		[]string{"cell-b", "cell-c"},
		peerCellsFromClusters("cell-a", []string{"cell-a", "cell-b", "cell-b"}, []string{"cell-c", "cell-a"}),
	)
}
