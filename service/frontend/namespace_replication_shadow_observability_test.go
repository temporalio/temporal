package frontend

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/testing/await"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestShadowReceiverAcceptsMergedPR2WireFormat(t *testing.T) {
	t.Parallel()
	payload, err := nsreplication.MarshalNamespaceTask(shadowObservationTask())
	require.NoError(t, err)
	// These payload tags are the wire contract already merged in PR2.
	wire := protowire.AppendTag(nil, 2, protowire.VarintType)
	wire = protowire.AppendVarint(wire, 1)
	wire = protowire.AppendTag(wire, 8, protowire.BytesType)
	wire = protowire.AppendBytes(wire, payload)
	wire = protowire.AppendTag(wire, 9, protowire.BytesType)
	wire = protowire.AppendBytes(wire, payload)
	var request adminservice.ApplyNamespaceMutationRequest
	require.NoError(t, proto.Unmarshal(wire, &request))
	handler := &AdminHandler{logger: log.NewNoopLogger()}
	response, err := handler.ApplyNamespaceMutation(context.Background(), &request)
	require.NoError(t, err)
	require.Equal(t, adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MATCH, response.GetOutcome())
}

func shadowObservationTask() *replicationspb.NamespaceTaskAttributes {
	return &replicationspb.NamespaceTaskAttributes{Id: "namespace-id", NamespaceOperation: enumsspb.NAMESPACE_OPERATION_UPDATE,
		Info: &namespacepb.NamespaceInfo{Name: "payments", Description: "legacy description"}, ConfigVersion: 17, FailoverVersion: 23}
}

func TestShadowReceiverObservability(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		mutate          func(*adminservice.ApplyNamespaceMutationRequest)
		outcome         string
		reason          string
		differingFields []any
		receivedTask    map[string]any
		disableEvents   bool
	}{
		{name: "match ignores typed diagnostic field", outcome: "match", reason: "none"},
		{name: "legacy payload mismatch", outcome: "mismatch", reason: "legacy_payload_mismatch", differingFields: []any{"info"}, mutate: func(req *adminservice.ApplyNamespaceMutationRequest) {
			task := shadowObservationTask()
			task.Info.Description = "CHASM description"
			req.NamespaceTaskPayload, _ = nsreplication.MarshalNamespaceTask(task)
		}},
		{
			name: "mismatch attributed to legacy metadata", outcome: "mismatch", reason: "legacy_payload_mismatch",
			differingFields: []any{"namespace_operation", "id", "info", "config_version", "failover_version"},
			receivedTask: map[string]any{
				"id": "different-namespace-id", "namespace_operation": "NAMESPACE_OPERATION_CREATE",
				"info":           map[string]any{"name": "orders", "description": "legacy description"},
				"config_version": "99", "failover_version": "101",
			},
			mutate: func(req *adminservice.ApplyNamespaceMutationRequest) {
				task := shadowObservationTask()
				task.Id = "different-namespace-id"
				task.Info.Name = "orders"
				task.NamespaceOperation = enumsspb.NAMESPACE_OPERATION_CREATE
				task.ConfigVersion, task.FailoverVersion = 99, 101
				req.NamespaceTaskPayload, _ = nsreplication.MarshalNamespaceTask(task)
			},
		},
		{name: "missing legacy reference", outcome: "error", reason: "missing_legacy_reference", mutate: func(req *adminservice.ApplyNamespaceMutationRequest) { req.ShadowLegacyTaskPayload = nil }},
		{name: "invalid legacy reference", outcome: "error", reason: "invalid_legacy_reference", mutate: func(req *adminservice.ApplyNamespaceMutationRequest) { req.ShadowLegacyTaskPayload = []byte{0xff} }},
		{name: "missing actual payload", outcome: "error", reason: "missing_payload", mutate: func(req *adminservice.ApplyNamespaceMutationRequest) { req.NamespaceTaskPayload = nil }},
		{name: "invalid actual payload", outcome: "error", reason: "invalid_payload", mutate: func(req *adminservice.ApplyNamespaceMutationRequest) {
			req.NamespaceTaskPayload = []byte{0xff}
		}},
		{name: "events disabled", outcome: "match", reason: "none", disableEvents: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			metadata := cluster.NewMockMetadata(gomock.NewController(t))
			metadata.EXPECT().GetCurrentClusterName().Return("cellB").AnyTimes()
			mh := metricstest.NewCaptureHandler()
			capture := mh.StartCapture()
			defer mh.StopCapture(capture)
			logger := &captureRemoteClusterEventLogger{}
			handler := &AdminHandler{logger: log.NewNoopLogger(), clusterMetadata: metadata, metricsHandler: mh, eventLogger: logger,
				config: &Config{EmitNamespaceLifecycleEvents: func() bool { return !tc.disableEvents }}}
			payload, err := nsreplication.MarshalNamespaceTask(shadowObservationTask())
			require.NoError(t, err)
			// Preserve a noncanonical byte layout to catch event fingerprint re-encoding.
			payload = append(protowire.AppendVarint(protowire.AppendTag(nil, 100, protowire.VarintType), 1), payload...)
			req := &adminservice.ApplyNamespaceMutationRequest{
				Shadow: true, NamespaceTask: &replicationspb.NamespaceTaskAttributes{Id: "untrusted-typed-field"},
				NamespaceTaskPayload: payload, ShadowLegacyTaskPayload: payload,
				SourceCluster: "cellA", ComponentBusinessId: "business-id", ComponentRunId: "run-id", AttemptCount: 2,
			}
			if tc.mutate != nil {
				tc.mutate(req)
			}
			response, err := handler.ApplyNamespaceMutation(t.Context(), req)
			if tc.outcome == "error" {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				want := adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MATCH
				if tc.outcome == "mismatch" {
					want = adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MISMATCH
				}
				require.Equal(t, want, response.GetOutcome())
			}
			values := capture.SnapshotMetric(metrics.NamespaceReplicationShadowReceiveComparisonOutcomes.Name())
			require.Len(t, values, 1)
			require.Equal(t, tc.outcome, values[0].Tags["outcome"])
			require.Equal(t, tc.reason, values[0].Tags["reason"])
			require.Equal(t, "cellA", values[0].Tags["source_cluster"])
			require.Equal(t, "cellB", values[0].Tags["target_cluster"])
			require.Equal(t, "update", values[0].Tags["operation"])
			if tc.disableEvents {
				require.Empty(t, logger.records)
				return
			}
			attrs, details := remoteClusterEventValues(t, logger.records)
			require.Equal(t, "compared", attrs["phase"])
			require.Equal(t, "namespace-id", attrs["namespace_id"])
			require.Equal(t, "payments", attrs["namespace"])
			require.Equal(t, enumsspb.NAMESPACE_OPERATION_UPDATE.String(), details["operation"])
			require.NotContains(t, details, "actual_task_fingerprint")
			require.Equal(t, tc.reason, details["reason"])
			require.Equal(t, "receive", details["comparison_boundary"])
			require.Equal(t, "business-id", details["component_business_id"])
			require.Equal(t, "run-id", details["component_run_id"])
			require.InDelta(t, 2, details["attempt_count"], 0)
			if len(req.NamespaceTaskPayload) > 0 {
				require.Equal(t, hex.EncodeToString(nsreplication.NamespaceTaskFingerprintFromPayload(req.NamespaceTaskPayload)), details["task_fingerprint"])
			}
			if tc.reason == "missing_payload" || tc.reason == "invalid_payload" {
				require.Equal(t, "unavailable", details["task_payload_status"])
				require.NotContains(t, details, "task")
			}
			require.InDelta(t, 17, details["config_version"], 0)
			require.InDelta(t, 23, details["failover_version"], 0)
			if tc.reason != "missing_legacy_reference" && tc.reason != "invalid_legacy_reference" {
				require.Equal(t, "legacy_reference", details["metadata_source"])
			} else {
				require.NotContains(t, details, "metadata_source")
			}
			if tc.reason == "legacy_payload_mismatch" {
				require.Equal(t, tc.differingFields, details["differing_fields"])
				require.Equal(t, "namespace-id", details["legacy_task"].(map[string]any)["id"])
			}
			if tc.receivedTask != nil {
				require.Equal(t, tc.receivedTask, details["task"])
			}
		})
	}
}

func TestShadowReceiverWithoutDecodableMetadata(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		payload []byte
	}{
		{name: "missing payloads"},
		{name: "undecodable payloads", payload: []byte{0xff}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			metadata := cluster.NewMockMetadata(gomock.NewController(t))
			metadata.EXPECT().GetCurrentClusterName().Return("cellB").AnyTimes()
			logger := &captureRemoteClusterEventLogger{}
			handler := &AdminHandler{logger: log.NewNoopLogger(), clusterMetadata: metadata, eventLogger: logger,
				config: &Config{EmitNamespaceLifecycleEvents: func() bool { return true }}}
			_, err := handler.ApplyNamespaceMutation(t.Context(), &adminservice.ApplyNamespaceMutationRequest{
				Shadow: true, NamespaceTaskPayload: tc.payload, ShadowLegacyTaskPayload: tc.payload,
			})
			require.Error(t, err)
			_, details := remoteClusterEventValues(t, logger.records)
			require.Equal(t, "unavailable", details["task_payload_status"])
			require.NotContains(t, details, "task")
			require.NotContains(t, details, "metadata_source")
			require.NotContains(t, details, "config_version")
			require.NotContains(t, details, "failover_version")
		})
	}
}

func TestShadowSourceObservationOutcomes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, stage, outcome string
		err                  error
	}{
		{name: "prepared", stage: "prepare", outcome: "prepared"},
		{name: "preparation failed", stage: "prepare", outcome: "error", err: serviceerror.NewInvalidArgument("invalid reference")},
		{name: "trigger completed", stage: "trigger", outcome: "rpc_completed"},
		{name: "trigger uncertain", stage: "trigger", outcome: "rpc_error", err: serviceerror.NewUnavailable("response lost")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			metadata := cluster.NewMockMetadata(gomock.NewController(t))
			metadata.EXPECT().GetCurrentClusterName().Return("cellA")
			mh := metricstest.NewCaptureHandler()
			capture := mh.StartCapture()
			defer mh.StopCapture(capture)
			logger := &captureRemoteClusterEventLogger{}
			handler := &namespaceHandler{clusterMetadata: metadata, metricsHandler: mh, eventLogger: logger, config: &Config{EmitNamespaceLifecycleEvents: func() bool { return true }}}
			task := shadowObservationTask()
			payload, err := nsreplication.MarshalNamespaceTask(task)
			require.NoError(t, err)
			if tc.outcome == "error" {
				task.Info.Name = string([]byte{0xff})
				payload = nil
			}
			handler.recordShadowSourceOutcome(task, payload, "business-id", []string{"cellB"}, tc.stage, tc.err)
			metric := metrics.NamespaceReplicationShadowPreparationOutcomes
			if tc.stage == "trigger" {
				metric = metrics.NamespaceReplicationShadowTriggerOutcomes
			}
			values := capture.SnapshotMetric(metric.Name())
			require.Len(t, values, 1)
			require.Equal(t, tc.outcome, values[0].Tags["outcome"])
			if tc.stage == "trigger" {
				require.Empty(t, logger.records)
				return
			}
			_, details := remoteClusterEventValues(t, logger.records)
			require.Equal(t, "business-id", details["component_business_id"])
			require.Equal(t, "cellA", details["source_cluster"])
			require.Equal(t, "shadow", details["mode"])
			require.Equal(t, []any{"cellB"}, details["target_clusters"])
			if tc.outcome == "error" {
				require.Equal(t, "incomplete", details["task_payload_status"])
			}
		})
	}
}

func TestShadowReceiverPanicObservability(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name             string
		observerPanic    bool
		eventLoggerPanic bool
		mismatch         bool
		invalidPayload   bool
	}{
		{name: "comparison panic is observed as an error"},
		{name: "observer panic preserves match", observerPanic: true},
		{name: "observer panic preserves mismatch", observerPanic: true, mismatch: true},
		{name: "observer panic preserves validation error", observerPanic: true, invalidPayload: true},
		{name: "event logger panic preserves match", observerPanic: true, eventLoggerPanic: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			metadata := cluster.NewMockMetadata(ctrl)
			metadata.EXPECT().GetCurrentClusterName().Return("cellB").AnyTimes()
			logger := log.NewMockLogger(ctrl)
			logger.EXPECT().Error("Panic is captured", gomock.Any(), gomock.Any())
			mh := metricstest.NewCaptureHandler()
			capture := mh.StartCapture()
			defer mh.StopCapture(capture)
			eventLogger := &captureRemoteClusterEventLogger{}
			handler := &AdminHandler{
				logger: logger, clusterMetadata: metadata, metricsHandler: mh, eventLogger: eventLogger,
				config: &Config{EmitNamespaceLifecycleEvents: func() bool {
					if tc.observerPanic && !tc.eventLoggerPanic {
						panic("observation panic")
					}
					return true
				}},
			}
			if tc.eventLoggerPanic {
				handler.eventLogger = &panicRemoteClusterEventLogger{}
			}
			payload, err := nsreplication.MarshalNamespaceTask(shadowObservationTask())
			require.NoError(t, err)
			request := &adminservice.ApplyNamespaceMutationRequest{
				Shadow: true, NamespaceTaskPayload: payload, ShadowLegacyTaskPayload: payload,
				SourceCluster: "cellA",
			}
			if tc.invalidPayload {
				request.NamespaceTaskPayload = nil
			}
			if tc.mismatch || !tc.observerPanic {
				mismatchedTask := shadowObservationTask()
				mismatchedTask.Info.Description = "CHASM description"
				request.NamespaceTaskPayload, err = nsreplication.MarshalNamespaceTask(mismatchedTask)
				require.NoError(t, err)
				warning := logger.EXPECT().Warn("namespace replication shadow receive mismatch", gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any())
				if !tc.observerPanic {
					warning.Do(func(string, ...tag.Tag) { panic("comparison panic") })
				}
			}
			response, err := handler.ApplyNamespaceMutation(t.Context(), request)
			outcomes := capture.SnapshotMetric(metrics.NamespaceReplicationShadowReceiveComparisonOutcomes.Name())
			require.Len(t, outcomes, 1)
			if tc.observerPanic {
				if tc.invalidPayload {
					var invalid *serviceerror.InvalidArgument
					require.ErrorAs(t, err, &invalid)
					require.ErrorContains(t, err, "namespace_task_payload is required")
					require.Equal(t, "error", outcomes[0].Tags["outcome"])
				} else {
					require.NoError(t, err)
					want := adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MATCH
					outcome := "match"
					if tc.mismatch {
						want, outcome = adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MISMATCH, "mismatch"
					}
					require.Equal(t, want, response.GetOutcome())
					require.Equal(t, outcome, outcomes[0].Tags["outcome"])
				}
				require.Empty(t, eventLogger.records)
			} else {
				var internal *serviceerror.Internal
				require.ErrorAs(t, err, &internal)
				require.ErrorContains(t, err, "comparison panic")
				require.Equal(t, "error", outcomes[0].Tags["outcome"])
				require.Equal(t, "panic", outcomes[0].Tags["reason"])
				_, details := remoteClusterEventValues(t, eventLogger.records)
				require.Equal(t, "error", details["outcome"])
				require.Equal(t, "panic", details["reason"])
				require.Contains(t, details["error"], "comparison panic")
				require.NotContains(t, details, "differing_fields")
			}
		})
	}
}

type failingShadowTriggerClient struct {
	namespacereplicationpb.NamespaceReplicationServiceClient
}

func (*failingShadowTriggerClient) TriggerNamespaceMutation(context.Context, *namespacereplicationpb.TriggerNamespaceMutationRequest, ...grpc.CallOption) (*namespacereplicationpb.TriggerNamespaceMutationResponse, error) {
	return nil, serviceerror.NewUnavailable("start response lost")
}

func TestShadowTriggerErrorMetricWithoutEvents(t *testing.T) {
	t.Parallel()
	metadata := cluster.NewMockMetadata(gomock.NewController(t))
	metadata.EXPECT().GetCurrentClusterName().Return("cellA").AnyTimes()
	mh := metricstest.NewCaptureHandler()
	capture := mh.StartCapture()
	defer mh.StopCapture(capture)
	handler := &namespaceHandler{logger: log.NewNoopLogger(), clusterMetadata: metadata, metricsHandler: mh, chasmNsReplClient: &failingShadowTriggerClient{}}
	detail := &persistencespb.NamespaceDetail{Info: &persistencespb.NamespaceInfo{Id: "namespace-id"}, Config: &persistencespb.NamespaceConfig{}, ReplicationConfig: &persistencespb.NamespaceReplicationConfig{Clusters: []string{"cellA", "cellB"}}}
	handler.invokeShadowNamespaceMutation(t.Context(), enumsspb.NAMESPACE_OPERATION_UPDATE, detail, detail, 1, nil, true, false)
	await.Require(t.Context(), t, func(t *await.T) {
		values := capture.SnapshotMetric(metrics.NamespaceReplicationShadowTriggerOutcomes.Name())
		require.Len(t, values, 1)
		require.Equal(t, "rpc_error", values[0].Tags["outcome"])
	}, 5*time.Second, 10*time.Millisecond)
	require.Len(t, capture.SnapshotMetric(metrics.NamespaceReplicationShadowPreparationOutcomes.Name()), 1)
}

func TestShadowSourceObserverPanicDoesNotPreventTrigger(t *testing.T) {
	t.Parallel()
	ctrl := gomock.NewController(t)
	metadata := cluster.NewMockMetadata(ctrl)
	metadata.EXPECT().GetCurrentClusterName().Return("cellA").AnyTimes()
	panicRecorded := make(chan struct{}, 1)
	logger := log.NewMockLogger(ctrl)
	logger.EXPECT().Error("Panic is captured", gomock.Any(), gomock.Any()).Do(func(string, ...tag.Tag) {
		panicRecorded <- struct{}{}
	})
	mh := metricstest.NewCaptureHandler()
	capture := mh.StartCapture()
	defer mh.StopCapture(capture)
	client := &captureNamespaceReplicationClient{requests: make(chan *namespacereplicationpb.TriggerNamespaceMutationRequest, 1)}
	handler := &namespaceHandler{
		logger: logger, clusterMetadata: metadata, metricsHandler: mh, chasmNsReplClient: client,
		eventLogger: &panicRemoteClusterEventLogger{}, config: &Config{EmitNamespaceLifecycleEvents: func() bool { return true }},
	}
	detail := &persistencespb.NamespaceDetail{Info: &persistencespb.NamespaceInfo{Id: "namespace-id"}, Config: &persistencespb.NamespaceConfig{}, ReplicationConfig: &persistencespb.NamespaceReplicationConfig{Clusters: []string{"cellA", "cellB"}}}
	handler.invokeShadowNamespaceMutation(t.Context(), enumsspb.NAMESPACE_OPERATION_UPDATE, detail, detail, 1, nil, true, false)
	require.Equal(t, "namespace-id", await.Rcv(t, client.requests).GetNamespaceId())
	await.Rcv(t, panicRecorded)
	require.Len(t, capture.SnapshotMetric(metrics.NamespaceReplicationShadowPreparationOutcomes.Name()), 1)
	await.Require(t.Context(), t, func(t *await.T) {
		triggers := capture.SnapshotMetric(metrics.NamespaceReplicationShadowTriggerOutcomes.Name())
		require.Len(t, triggers, 1)
		require.Equal(t, "rpc_completed", triggers[0].Tags["outcome"])
	}, 5*time.Second, 10*time.Millisecond)
}
