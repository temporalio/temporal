package frontend

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	otellog "go.opentelemetry.io/otel/log"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/wideevents"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
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
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("cell-a").Times(2)
	client := &captureNamespaceReplicationClient{requests: make(chan *namespacereplicationpb.TriggerNamespaceMutationRequest, 1)}
	metricsHandler := metricstest.NewCaptureHandler()
	metricsCapture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(metricsCapture)
	eventLogger := &captureNamespaceEventLogger{}
	handler := &namespaceHandler{
		logger:            log.NewNoopLogger(),
		eventLogger:       eventLogger,
		clusterMetadata:   clusterMetadata,
		chasmNsReplClient: client,
		metricsHandler:    metricsHandler,
		config: &Config{
			NamespaceReplicationTransportMode: dynamicconfig.GetStringPropertyFn(dynamicconfig.NamespaceReplicationTransportModeShadow),
			EmitNamespaceLifecycleEvents:      dynamicconfig.GetBoolPropertyFn(true),
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
	requireShadowComparisonMetric(
		t,
		metricsCapture,
		metrics.NamespaceReplicationShadowBuildComparisonOutcomes.Name(),
		metrics.SourceClusterTag("").Key,
		"cell-a",
		"update",
		namespaceReplicationShadowOutcomeMatch,
	)
	details := requireShadowComparisonEvent(
		t,
		eventLogger.records,
		namespaceReplicationComparisonBoundaryBuild,
		namespaceReplicationShadowOutcomeMatch,
		"cell-a",
		"",
	)
	require.Equal(t, details["legacy_task_fingerprint"], details["chasm_task_fingerprint"])
	require.Equal(t, details["task_fingerprint"], details["chasm_task_fingerprint"])
	require.NotEmpty(t, details["component_business_id"])
	require.Equal(t, []any{"cell-b", "cell-c"}, details["target_clusters"])
	require.NotContains(t, details, "legacy_task")
	select {
	case request := <-client.requests:
		require.True(t, request.GetMutation().GetShadow())
		require.Equal(t, int64(7), request.GetMutation().GetExpectedVersion())
		require.Equal(t, []string{"cell-b", "cell-c"}, request.GetMutation().GetPeerCells())
		require.Equal(t, details["component_business_id"], request.GetBusinessId())
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shadow namespace mutation")
	}
}

func TestInvokeShadowNamespaceMutationRecordsBuildMismatch(t *testing.T) {
	controller := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(controller)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("cell-a").Times(2)
	client := &captureNamespaceReplicationClient{requests: make(chan *namespacereplicationpb.TriggerNamespaceMutationRequest, 1)}
	metricsHandler := metricstest.NewCaptureHandler()
	metricsCapture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(metricsCapture)
	eventLogger := &captureNamespaceEventLogger{}
	handler := &namespaceHandler{
		logger:            log.NewNoopLogger(),
		eventLogger:       eventLogger,
		clusterMetadata:   clusterMetadata,
		chasmNsReplClient: client,
		metricsHandler:    metricsHandler,
		config: &Config{
			EmitNamespaceLifecycleEvents: dynamicconfig.GetBoolPropertyFn(true),
		},
	}
	chasmDetail := &persistencespb.NamespaceDetail{
		Info:              &persistencespb.NamespaceInfo{Id: "namespace-id", State: enumspb.NAMESPACE_STATE_REGISTERED},
		Config:            &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{Clusters: []string{"cell-a", "cell-b"}},
		ConfigVersion:     3,
	}
	legacyDetail := proto.Clone(chasmDetail).(*persistencespb.NamespaceDetail)
	legacyDetail.ConfigVersion = 2

	handler.invokeShadowNamespaceMutation(
		namespaceReplicationTransportShadow,
		enumsspb.NAMESPACE_OPERATION_UPDATE,
		chasmDetail,
		legacyDetail,
		7,
		nil,
		true,
		true,
	)
	requireShadowComparisonMetric(
		t,
		metricsCapture,
		metrics.NamespaceReplicationShadowBuildComparisonOutcomes.Name(),
		metrics.SourceClusterTag("").Key,
		"cell-a",
		"update",
		namespaceReplicationShadowOutcomeMismatch,
	)
	details := requireShadowComparisonEvent(
		t,
		eventLogger.records,
		namespaceReplicationComparisonBoundaryBuild,
		namespaceReplicationShadowOutcomeMismatch,
		"cell-a",
		"",
	)
	require.NotEqual(t, details["legacy_task_fingerprint"], details["chasm_task_fingerprint"])
	require.Equal(t, []any{"config_version"}, details["differing_fields"])
	require.Equal(t, "2", details["legacy_task"].(map[string]any)["config_version"])
	select {
	case <-client.requests:
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for shadow namespace mutation")
	}
}

func TestInvokeShadowNamespaceMutationRecordsBuildError(t *testing.T) {
	controller := gomock.NewController(t)
	clusterMetadata := cluster.NewMockMetadata(controller)
	clusterMetadata.EXPECT().GetCurrentClusterName().Return("cell-a").Times(2)
	metricsHandler := metricstest.NewCaptureHandler()
	metricsCapture := metricsHandler.StartCapture()
	defer metricsHandler.StopCapture(metricsCapture)
	eventLogger := &captureNamespaceEventLogger{}
	handler := &namespaceHandler{
		logger:          log.NewNoopLogger(),
		eventLogger:     eventLogger,
		clusterMetadata: clusterMetadata,
		metricsHandler:  metricsHandler,
		config: &Config{
			EmitNamespaceLifecycleEvents: dynamicconfig.GetBoolPropertyFn(true),
		},
	}
	chasmDetail := &persistencespb.NamespaceDetail{
		Info:              &persistencespb.NamespaceInfo{Id: "namespace-id", State: enumspb.NAMESPACE_STATE_REGISTERED},
		Config:            &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{Clusters: []string{"cell-a", "cell-b"}},
	}
	legacyDetail := proto.Clone(chasmDetail).(*persistencespb.NamespaceDetail)
	legacyDetail.Info.Name = string([]byte{0xff})

	handler.invokeShadowNamespaceMutation(
		namespaceReplicationTransportShadow,
		enumsspb.NAMESPACE_OPERATION_CREATE,
		chasmDetail,
		legacyDetail,
		0,
		nil,
		true,
		false,
	)
	requireShadowComparisonMetric(
		t,
		metricsCapture,
		metrics.NamespaceReplicationShadowBuildComparisonOutcomes.Name(),
		metrics.SourceClusterTag("").Key,
		"cell-a",
		"create",
		namespaceReplicationShadowOutcomeError,
	)
	details := requireShadowComparisonEvent(
		t,
		eventLogger.records,
		namespaceReplicationComparisonBoundaryBuild,
		namespaceReplicationShadowOutcomeError,
		"cell-a",
		"",
	)
	require.NotEmpty(t, details["error"])
	require.Equal(t, "incomplete", details["legacy_task_payload_status"])
	require.NotEmpty(t, details["legacy_task_json_error"])
}

func requireShadowComparisonMetric(
	t *testing.T,
	capture *metricstest.Capture,
	metricName string,
	clusterTagKey string,
	clusterName string,
	operation string,
	outcome string,
	sourceCluster ...string,
) {
	t.Helper()
	recordings := capture.SnapshotMetric(metricName)
	require.Len(t, recordings, 1)
	require.Equal(t, clusterName, recordings[0].Tags[clusterTagKey])
	require.Equal(t, operation, recordings[0].Tags[metrics.OperationTag("").Key])
	require.Equal(t, outcome, recordings[0].Tags[metrics.OutcomeTag("").Key])
	if len(sourceCluster) > 0 {
		require.Equal(t, sourceCluster[0], recordings[0].Tags[metrics.SourceClusterTag("").Key])
	}
}

func requireShadowComparisonEvent(
	t *testing.T,
	records []otellog.Record,
	boundary string,
	outcome string,
	sourceCluster string,
	targetCluster string,
) map[string]any {
	t.Helper()
	require.Len(t, records, 1)
	require.Equal(t, wideevents.NamespaceLifecycleEventName, records[0].EventName())
	attributes := make(map[string]string)
	records[0].WalkAttributes(func(kv otellog.KeyValue) bool {
		if kv.Value.Kind() == otellog.KindString {
			attributes[kv.Key] = kv.Value.AsString()
		}
		return true
	})
	require.Equal(t, string(wideevents.NamespaceReplicationCompared), attributes["phase"])
	var details map[string]any
	require.NoError(t, json.Unmarshal([]byte(attributes["details"]), &details))
	require.Equal(t, namespaceReplicationShadowTransport, details["transport"])
	require.Equal(t, namespaceReplicationShadowMode, details["mode"])
	require.Equal(t, boundary, details["comparison_boundary"])
	require.Equal(t, outcome, details["outcome"])
	if sourceCluster == "" {
		require.NotContains(t, details, "source_cluster")
	} else {
		require.Equal(t, sourceCluster, details["source_cluster"])
	}
	if targetCluster == "" {
		require.NotContains(t, details, "target_cluster")
	} else {
		require.Equal(t, targetCluster, details["target_cluster"])
	}
	return details
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
	client := &captureNamespaceReplicationClient{requests: make(chan *namespacereplicationpb.TriggerNamespaceMutationRequest, 3)}
	handler := &namespaceHandler{
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
				[]string{"cell-b"},
				"namespace-id:mutation-id",
				tc.mode,
			)
			require.NoError(t, err)
			request := <-client.requests
			require.Equal(t, "namespace-id:mutation-id", request.GetBusinessId())
			require.Equal(t, []string{"cell-b"}, request.GetMutation().GetPeerCells())
			require.Equal(t, tc.wantShadow, request.GetMutation().GetShadow())
			require.Equal(t, tc.wantReplicateOnly, request.GetMutation().GetReplicateOnly())
		})
	}
}

func TestTriggerAuthoritativeNamespaceMutation(t *testing.T) {
	client := &captureNamespaceReplicationClient{}
	handler := &namespaceHandler{
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
		[]string{"cell-b"},
		"namespace-id:mutation-id",
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
	client := &captureNamespaceReplicationClient{}
	handler := &namespaceHandler{
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
		[]string{"cell-b"},
		"namespace-id:mutation-id",
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
