package frontend

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/api/workflowservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/common/wideevents"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestNamespaceMutationTransportRouting(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{dynamicconfig.NamespaceReplicationTransportModeLegacy, dynamicconfig.NamespaceReplicationTransportModeShadow, "unknown"} {
		for _, tc := range []struct {
			name       string
			create     bool
			noop       bool
			removePeer bool
			promote    bool
			writeErr   error
			publishErr error
		}{
			{name: "create", create: true},
			{name: "update"},
			{name: "no-op update", noop: true},
			{name: "remove last peer", removePeer: true},
			{name: "promote local namespace", promote: true},
			{name: "create write failure", create: true, writeErr: serviceerror.NewUnavailable("write failed")},
			{name: "update write failure", writeErr: serviceerror.NewUnavailable("write failed")},
			{name: "create publish failure", create: true, publishErr: serviceerror.NewUnavailable("publish failed")},
			{name: "update publish failure", publishErr: serviceerror.NewUnavailable("publish failed")},
		} {
			t.Run(mode+"/"+tc.name, func(t *testing.T) {
				t.Parallel()
				ctrl := gomock.NewController(t)
				metadata := persistence.NewMockMetadataManager(ctrl)
				clusters := cluster.NewMockMetadata(ctrl)
				queue := persistence.NewMockNamespaceReplicationQueue(ctrl)
				logger := log.NewNoopLogger()
				dc := dynamicconfig.NewNoopCollection()
				cfg := NewConfig(dc, 32)
				cfg.NamespaceReplicationTransportMode = dynamicconfig.GetStringPropertyFn(mode)
				client := &captureNamespaceReplicationClient{requests: make(chan *namespacereplicationpb.TriggerNamespaceMutationRequest, 1)}
				handler := newNamespaceHandler(logger, wideevents.NoopLogger(), metadata, nil, clusters,
					nsreplication.NewReplicator(queue, logger, nil, dynamicconfig.GetBoolPropertyFn(false), "cell-a"),
					archiver.NewArchivalMetadata(dc, "", false, "", false, &config.ArchivalNamespaceDefaults{}),
					nil, clock.NewEventTimeSource(), cfg, client)
				clusters.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
				clusters.EXPECT().IsMasterCluster().Return(true).AnyTimes()
				clusters.EXPECT().GetCurrentClusterName().Return("cell-a").AnyTimes()
				clusters.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
					"cell-a": {Enabled: true}, "cell-b": {Enabled: true},
				}).AnyTimes()
				clusters.EXPECT().GetNextFailoverVersion("cell-a", int64(0)).Return(int64(1)).AnyTimes()
				var write *gomock.Call
				var err error
				if tc.create {
					metadata.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNamespaceNotFound("ns"))
					write = metadata.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).Return(&persistence.CreateNamespaceResponse{ID: "namespace-id"}, tc.writeErr)
				} else {
					existingClusters := []string{"cell-a", "cell-b"}
					if tc.promote {
						existingClusters = []string{"cell-a"}
					}
					metadata.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: 7}, nil)
					metadata.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
						Namespace: &persistencespb.NamespaceDetail{
							Info:              &persistencespb.NamespaceInfo{Id: "namespace-id", Name: "ns", State: enumspb.NAMESPACE_STATE_REGISTERED},
							Config:            &persistencespb.NamespaceConfig{BadBinaries: &namespacepb.BadBinaries{}},
							ReplicationConfig: &persistencespb.NamespaceReplicationConfig{ActiveClusterName: "cell-a", Clusters: existingClusters},
						}, IsGlobalNamespace: !tc.promote,
					}, nil)
					if !tc.noop {
						write = metadata.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(tc.writeErr)
					}
				}
				var legacyTask *replicationspb.NamespaceTaskAttributes
				if tc.writeErr == nil {
					publish := queue.EXPECT().Publish(gomock.Any(), gomock.Any()).DoAndReturn(
						func(_ context.Context, task *replicationspb.ReplicationTask) error {
							legacyTask = task.GetNamespaceTaskAttributes()
							return tc.publishErr
						})
					if write != nil {
						gomock.InOrder(write, publish)
					}
				}
				if tc.create {
					_, err = handler.RegisterNamespace(t.Context(), &workflowservice.RegisterNamespaceRequest{
						Namespace: "ns", IsGlobalNamespace: true,
						WorkflowExecutionRetentionPeriod: durationpb.New(24 * time.Hour),
						Clusters:                         []*replicationpb.ClusterReplicationConfig{{ClusterName: "cell-a"}, {ClusterName: "cell-b"}},
					})
				} else {
					request := &workflowservice.UpdateNamespaceRequest{Namespace: "ns"}
					if tc.removePeer {
						request.ReplicationConfig = &replicationpb.NamespaceReplicationConfig{Clusters: []*replicationpb.ClusterReplicationConfig{{ClusterName: "cell-a"}}}
					} else if tc.promote {
						request.PromoteNamespace = true
						request.ReplicationConfig = &replicationpb.NamespaceReplicationConfig{Clusters: []*replicationpb.ClusterReplicationConfig{{ClusterName: "cell-a"}, {ClusterName: "cell-b"}}}
					} else if !tc.noop {
						request.UpdateInfo = &namespacepb.UpdateNamespaceInfo{Description: "updated"}
					}
					_, err = handler.UpdateNamespace(t.Context(), request)
				}
				if tc.writeErr != nil {
					require.ErrorIs(t, err, tc.writeErr)
				} else {
					require.ErrorIs(t, err, tc.publishErr)
				}
				if mode == dynamicconfig.NamespaceReplicationTransportModeShadow && err == nil {
					mutation := await.Rcv(t, client.requests).GetMutation()
					require.Equal(t, []string{"cell-b"}, mutation.GetPeerCells())
					if tc.removePeer {
						require.Equal(t, []string{"cell-a"}, mutation.GetNamespaceDetail().GetReplicationConfig().GetClusters())
					}
					var reference replicationspb.NamespaceTaskAttributes
					require.NoError(t, proto.Unmarshal(mutation.GetShadowLegacyTaskPayload(), &reference))
					protorequire.ProtoEqual(t, legacyTask, &reference)
				} else {
					select {
					case <-client.requests:
						t.Fatal("unexpected shadow dispatch")
					default:
					}
				}
			})
		}
	}
}

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
		global  bool
		single  bool
		deleted bool
	}{
		{name: "local namespace"},
		{name: "single cluster without membership change", global: true, single: true},
		{name: "deleted namespace", global: true, deleted: true},
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
			handler.invokeShadowNamespaceMutation(t.Context(), enumsspb.NAMESPACE_OPERATION_UPDATE, detail, detail, 0, nil, tc.global, false)
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
	handler.invokeShadowNamespaceMutation(t.Context(), enumsspb.NAMESPACE_OPERATION_UPDATE,
		detail, detail, 7, nil, true, false)
	select {
	case <-client.requests:
		t.Fatal("shadow dispatch started without a valid legacy reference")
	default:
	}
}

func TestEffectiveNamespaceReplicationTransportMode(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name       string
		configured string
		want       namespaceReplicationTransportMode
	}{
		{name: "legacy", configured: dynamicconfig.NamespaceReplicationTransportModeLegacy, want: namespaceReplicationTransportLegacy},
		{name: "shadow", configured: dynamicconfig.NamespaceReplicationTransportModeShadow, want: namespaceReplicationTransportShadow},
		{name: "unknown falls back to legacy", configured: "unknown", want: namespaceReplicationTransportLegacy},
		{name: "unsupported chasm falls back to legacy", configured: "chasm", want: namespaceReplicationTransportLegacy},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dc := dynamicconfig.NewCollection(dynamicconfig.StaticClient{
				dynamicconfig.NamespaceReplicationTransportMode.Key(): tc.configured,
			}, log.NewNoopLogger())
			handler := &namespaceHandler{
				logger: log.NewNoopLogger(),
				config: &Config{
					NamespaceReplicationTransportMode: dynamicconfig.NamespaceReplicationTransportMode.Get(dc),
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
