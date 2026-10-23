package frontend

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type captureNamespaceReplicationClient struct {
	requests chan *namespacereplicationpb.TriggerNamespaceMutationRequest
}

func (c *captureNamespaceReplicationClient) TriggerNamespaceMutation(
	_ context.Context,
	request *namespacereplicationpb.TriggerNamespaceMutationRequest,
	_ ...grpc.CallOption,
) (*namespacereplicationpb.TriggerNamespaceMutationResponse, error) {
	c.requests <- request
	return &namespacereplicationpb.TriggerNamespaceMutationResponse{}, nil
}

type blockingNamespaceReplicationClient struct {
	started   chan context.Context
	release   chan struct{}
	completed chan struct{}
}

func (c *blockingNamespaceReplicationClient) TriggerNamespaceMutation(
	ctx context.Context,
	_ *namespacereplicationpb.TriggerNamespaceMutationRequest,
	_ ...grpc.CallOption,
) (*namespacereplicationpb.TriggerNamespaceMutationResponse, error) {
	c.started <- ctx
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
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{Clusters: []string{"cell-a"}},
		ConfigVersion:     3,
		FailoverVersion:   5,
	}
	legacyDetail := proto.Clone(detail).(*persistencespb.NamespaceDetail)
	legacyDetail.Info.Description = "legacy frontend snapshot"

	handler.invokeShadowNamespaceMutation(
		t.Context(),
		namespaceReplicationTransportShadow,
		enumsspb.NAMESPACE_OPERATION_UPDATE,
		detail,
		legacyDetail,
		7,
		[]string{"cell-a", "cell-c"},
		true,
		true,
	)
	select {
	case request := <-client.requests:
		require.True(t, request.GetMutation().GetShadow())
		require.True(t, request.GetMutation().GetIsGlobalNamespace())
		require.Equal(t, int64(7), request.GetMutation().GetExpectedVersion())
		require.Equal(t, []string{"cell-c"}, request.GetMutation().GetPeerCells())
		legacyTask := &replicationspb.NamespaceTaskAttributes{}
		require.NoError(t, proto.Unmarshal(request.GetMutation().GetShadowLegacyTaskPayload(), legacyTask))
		require.True(t, proto.Equal(nsreplication.NamespaceDetailToTaskAttributes(enumsspb.NAMESPACE_OPERATION_UPDATE, legacyDetail), legacyTask),
			"the reference must use the legacy frontend snapshot, not the CHASM detail")
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shadow namespace mutation")
	}
}

func TestInvokeShadowNamespaceMutationSkipsWhenLegacyWouldNotReplicate(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		mode    namespaceReplicationTransportMode
		global  bool
		single  bool
		deleted bool
	}{
		{name: "legacy mode", mode: namespaceReplicationTransportLegacy, global: true},
		{name: "local namespace", mode: namespaceReplicationTransportShadow},
		{name: "single cluster without membership change", mode: namespaceReplicationTransportShadow, global: true, single: true},
		{name: "deleted namespace", mode: namespaceReplicationTransportShadow, global: true, deleted: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client := &captureNamespaceReplicationClient{requests: make(chan *namespacereplicationpb.TriggerNamespaceMutationRequest, 1)}
			handler := &namespaceHandler{chasmNsReplClient: client}
			detail := &persistencespb.NamespaceDetail{
				Info:              &persistencespb.NamespaceInfo{Id: "namespace-id", State: enumspb.NAMESPACE_STATE_REGISTERED},
				Config:            &persistencespb.NamespaceConfig{},
				ReplicationConfig: &persistencespb.NamespaceReplicationConfig{Clusters: []string{"cell-a", "cell-b"}},
			}
			if tc.single {
				detail.ReplicationConfig.Clusters = []string{"cell-a"}
			}
			if tc.deleted {
				detail.Info.State = enumspb.NAMESPACE_STATE_DELETED
			}
			handler.invokeShadowNamespaceMutation(t.Context(), tc.mode, enumsspb.NAMESPACE_OPERATION_UPDATE, detail, detail, 0, nil, tc.global, false)
			select {
			case <-client.requests:
				t.Fatal("unexpected shadow dispatch")
			default:
			}
		})
	}
}

func TestInvokeShadowNamespaceMutationDoesNotBlockCaller(t *testing.T) {
	controller := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(controller)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("cell-a")
	client := &blockingNamespaceReplicationClient{
		started:   make(chan context.Context, 1),
		release:   make(chan struct{}),
		completed: make(chan struct{}),
	}
	t.Cleanup(func() {
		select {
		case <-client.release:
		default:
			close(client.release)
		}
	})
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

	spanContext := trace.NewSpanContext(trace.SpanContextConfig{TraceID: trace.TraceID{1}, SpanID: trace.SpanID{2}})
	requestCtx, cancel := context.WithCancel(trace.ContextWithSpanContext(t.Context(), spanContext))
	requestCtx = headers.SetCallerInfo(requestCtx, headers.NewCallerInfo("namespace-id", headers.CallerTypeAPI, ""))
	cancel()
	returned := make(chan struct{})
	go func() {
		handler.invokeShadowNamespaceMutation(
			requestCtx,
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
	case shadowCtx := <-client.started:
		require.NoError(t, shadowCtx.Err(), "request cancellation must not cancel shadow dispatch")
		require.Equal(t, spanContext, trace.SpanContextFromContext(shadowCtx))
		require.Equal(t, headers.SystemBackgroundLowCallerInfo, headers.GetCallerInfo(shadowCtx))
		_, hasDeadline := shadowCtx.Deadline()
		require.True(t, hasDeadline, "detached shadow dispatch must remain timeout-bounded")
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

func TestInvokeShadowNamespaceMutationReferencePreparationFailure(t *testing.T) {
	t.Parallel()
	client := &captureNamespaceReplicationClient{requests: make(chan *namespacereplicationpb.TriggerNamespaceMutationRequest, 1)}
	handler := &namespaceHandler{logger: log.NewNoopLogger(), chasmNsReplClient: client}
	detail := &persistencespb.NamespaceDetail{
		Info:              &persistencespb.NamespaceInfo{Id: "namespace-id", Name: string([]byte{0xff}), State: enumspb.NAMESPACE_STATE_REGISTERED},
		Config:            &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{Clusters: []string{"cell-a", "cell-b"}},
	}
	handler.invokeShadowNamespaceMutation(t.Context(), namespaceReplicationTransportShadow, enumsspb.NAMESPACE_OPERATION_UPDATE,
		detail, detail, 7, nil, true, false)
	select {
	case <-client.requests:
		t.Fatal("shadow dispatch started without a valid legacy reference")
	default:
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

func TestTriggerNamespaceMutationIsShadow(t *testing.T) {
	t.Parallel()
	controller := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(controller)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("cell-a")
	client := &captureNamespaceReplicationClient{requests: make(chan *namespacereplicationpb.TriggerNamespaceMutationRequest, 1)}
	handler := &namespaceHandler{
		clusterMetadata:   clusterMetadata,
		chasmNsReplClient: client,
	}
	detail := &persistencespb.NamespaceDetail{
		Info:              &persistencespb.NamespaceInfo{Id: "namespace-id"},
		Config:            &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{Clusters: []string{"cell-a", "cell-b"}},
	}

	_, err := handler.triggerNamespaceMutation(
		t.Context(),
		enumsspb.NAMESPACE_OPERATION_UPDATE,
		detail,
		7,
		nil,
		true,
		[]byte("legacy-reference"),
	)
	require.NoError(t, err)
	request := <-client.requests
	require.True(t, request.GetMutation().GetShadow())
	require.True(t, request.GetMutation().GetIsGlobalNamespace())
}

func TestPeerCellsFromClusters(t *testing.T) {
	require.Equal(
		t,
		[]string{"cell-b", "cell-c"},
		peerCellsFromClusters("cell-a", []string{"cell-a", "cell-b", "cell-b"}, []string{"cell-c", "cell-a"}),
	)
}
