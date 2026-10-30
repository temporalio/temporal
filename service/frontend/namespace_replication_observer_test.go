package frontend

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/testing/testhooks"
	"go.uber.org/mock/gomock"
)

func TestReceiverCreateRaceObservesOnlyCompletedOutcome(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		create    bool
		ambiguous bool
	}{
		{name: "UPDATE retries a lost CREATE race"},
		{name: "duplicate CREATE remains observable", create: true},
		{name: "committed UPDATE timeout resolves as stale", ambiguous: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			metadata := persistence.NewMockMetadataManager(gomock.NewController(t))
			task := shadowObservationTask()
			task.Info.State = enumspb.NAMESPACE_STATE_REGISTERED
			task.Config = &namespacepb.NamespaceConfig{}
			task.ReplicationConfig = &replicationpb.NamespaceReplicationConfig{ActiveClusterName: "cellA", Clusters: []*replicationpb.ClusterReplicationConfig{{ClusterName: "cellA"}, {ClusterName: "cellB"}}}
			if tc.create {
				task.NamespaceOperation = enumsspb.NAMESPACE_OPERATION_CREATE
			}
			winner := &persistencespb.NamespaceDetail{Info: &persistencespb.NamespaceInfo{Id: task.GetId(), Name: task.GetInfo().GetName(), State: enumspb.NAMESPACE_STATE_REGISTERED}, Config: &persistencespb.NamespaceConfig{}, ReplicationConfig: &persistencespb.NamespaceReplicationConfig{ActiveClusterName: "cellA", Clusters: []string{"cellA", "cellB"}}, ConfigVersion: task.GetConfigVersion() - 1, FailoverVersion: task.GetFailoverVersion()}
			winnerVisible := tc.ambiguous
			metadata.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, *persistence.GetNamespaceRequest) (*persistence.GetNamespaceResponse, error) {
				if !winnerVisible {
					return nil, serviceerror.NewNamespaceNotFound(task.GetInfo().GetName())
				}
				return &persistence.GetNamespaceResponse{Namespace: winner, IsGlobalNamespace: true}, nil
			}).AnyTimes()
			if !tc.ambiguous {
				metadata.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, *persistence.CreateNamespaceRequest) (*persistence.CreateNamespaceResponse, error) {
					winnerVisible = true
					return nil, serviceerror.NewNamespaceAlreadyExists("concurrent CREATE won")
				})
			}
			if !tc.create {
				metadata.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: 11}, nil).Times(2)
				metadata.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, request *persistence.UpdateNamespaceRequest) error {
					winner = request.Namespace
					if tc.ambiguous {
						return serviceerror.NewUnavailable("commit acknowledgement lost")
					}
					return nil
				})
			}
			mh := metricstest.NewCaptureHandler()
			capture := mh.StartCapture()
			defer mh.StopCapture(capture)
			events := &captureRemoteClusterEventLogger{}
			handler := &AdminHandler{logger: log.NewNoopLogger(), clusterMetadata: enabledNamespaceReplicationMetadata(t), metricsHandler: mh, eventLogger: events, config: &Config{EmitNamespaceLifecycleEvents: dynamicconfig.GetBoolPropertyFn(true)}}
			handler.namespaceMutationExecutor = nsreplication.NewMutationTaskExecutor("cellB", metadata, nsreplication.NewNoopDataMerger(), nsreplication.NewDefaultAdmitter(), handler.logger, testhooks.TestHooks{}, nsreplication.WithNamespaceReplicationMetrics(mh), nsreplication.WithNamespaceReplicationLifecycleEvents(events, handler.config.EmitNamespaceLifecycleEvents))
			payload, err := nsreplication.MarshalNamespaceTask(task)
			require.NoError(t, err)
			response, err := handler.ApplyNamespaceMutation(t.Context(), &adminservice.ApplyNamespaceMutationRequest{NamespaceTaskPayload: payload, SourceCluster: "cellA", AttemptCount: 1})
			require.NoError(t, err)
			wantOutcome := adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED
			wantEvent := "updated"
			wantMetric := "applied"
			wantFinalMetric := "applied"
			if tc.create {
				wantOutcome = adminservice.ApplyNamespaceMutationResponse_OUTCOME_DUPLICATE
				wantEvent = "duplicate"
				wantMetric = "no_change"
				wantFinalMetric = "duplicate"
			}
			if tc.ambiguous {
				wantOutcome = adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE
				wantEvent = "no_change"
				wantMetric = "no_change"
				wantFinalMetric = "no_change"
				require.Equal(t, task.GetConfigVersion(), winner.GetConfigVersion())
			}
			require.Equal(t, wantOutcome, response.GetOutcome())
			_, details := remoteClusterEventValues(t, events.records)
			require.Equal(t, wantEvent, details["outcome"], "a retried UPDATE must not emit an intermediate duplicate event")
			shared := capture.SnapshotMetric(metrics.NamespaceReplicationApplyOutcomes.Name())
			require.Len(t, shared, 1)
			require.Equal(t, wantMetric, shared[0].Tags["outcome"])
			final := capture.SnapshotMetric(metrics.NamespaceReplicationCHASMApplyOutcomes.Name())
			require.Len(t, final, 1)
			require.Equal(t, wantFinalMetric, final[0].Tags["outcome"])
		})
	}
}

type authoritativeObserverPanicMetrics struct{ metrics.Handler }

func (authoritativeObserverPanicMetrics) Counter(string) metrics.CounterIface {
	return metrics.CounterFunc(func(int64, ...metrics.Tag) { panic("metric observer failed") })
}

func TestAuthoritativeObserverPanicsPreserveApplyResult(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name            string
		metricPanic     bool
		applyErr        error
		updateGatePanic bool
	}{
		{name: "metric panic after successful write", metricPanic: true},
		{name: "event panic after successful write"},
		{name: "metric panic preserves write failure", metricPanic: true, applyErr: serviceerror.NewInvalidArgument("write rejected")},
		{name: "event panic preserves write failure", applyErr: serviceerror.NewInvalidArgument("write rejected")},
		{name: "event gate panic does not prevent update", updateGatePanic: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			metadata := persistence.NewMockMetadataManager(gomock.NewController(t))
			if tc.updateGatePanic {
				metadata.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
					Namespace: &persistencespb.NamespaceDetail{
						Info:   &persistencespb.NamespaceInfo{Id: "namespace-id", Name: "payments", State: enumspb.NAMESPACE_STATE_REGISTERED},
						Config: &persistencespb.NamespaceConfig{}, ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
					}, IsGlobalNamespace: true,
				}, nil).AnyTimes()
				metadata.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: 1}, nil)
				metadata.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(nil)
			} else {
				metadata.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewNamespaceNotFound("payments")).AnyTimes()
				metadata.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).Return(&persistence.CreateNamespaceResponse{ID: "namespace-id"}, tc.applyErr)
			}
			logger := log.NewNoopLogger()
			metricsHandler := metrics.NoopMetricsHandler
			if tc.metricPanic {
				metricsHandler = authoritativeObserverPanicMetrics{}
			}
			events := &captureRemoteClusterEventLogger{}
			handler := &AdminHandler{
				logger: logger, clusterMetadata: enabledNamespaceReplicationMetadata(t), metricsHandler: metricsHandler,
				eventLogger: events, config: &Config{EmitNamespaceLifecycleEvents: dynamicconfig.GetBoolPropertyFn(true)},
			}
			if !tc.metricPanic {
				handler.eventLogger = &panicRemoteClusterEventLogger{}
			}
			if tc.updateGatePanic {
				handler.config.EmitNamespaceLifecycleEvents = func() bool { panic("event gate failed") }
			}
			handler.namespaceMutationExecutor = nsreplication.NewMutationTaskExecutor("cellB", metadata,
				nsreplication.NewNoopDataMerger(), nsreplication.NewDefaultAdmitter(), logger, testhooks.TestHooks{},
				nsreplication.WithNamespaceReplicationMetrics(metricsHandler),
				nsreplication.WithNamespaceReplicationLifecycleEvents(handler.eventLogger, handler.config.EmitNamespaceLifecycleEvents))
			task := shadowObservationTask()
			task.NamespaceOperation = enumsspb.NAMESPACE_OPERATION_CREATE
			if tc.updateGatePanic {
				task.NamespaceOperation = enumsspb.NAMESPACE_OPERATION_UPDATE
			}
			task.Info.State = enumspb.NAMESPACE_STATE_REGISTERED
			task.Config = &namespacepb.NamespaceConfig{}
			task.ReplicationConfig = &replicationpb.NamespaceReplicationConfig{
				ActiveClusterName: "cellA", Clusters: []*replicationpb.ClusterReplicationConfig{{ClusterName: "cellA"}, {ClusterName: "cellB"}},
			}
			payload, err := nsreplication.MarshalNamespaceTask(task)
			require.NoError(t, err)
			response, err := handler.ApplyNamespaceMutation(t.Context(), &adminservice.ApplyNamespaceMutationRequest{NamespaceTaskPayload: payload})
			if tc.applyErr != nil {
				require.ErrorIs(t, err, tc.applyErr)
				require.Nil(t, response)
			} else {
				require.NoError(t, err)
				want := adminservice.ApplyNamespaceMutationResponse_OUTCOME_CREATED
				if tc.updateGatePanic {
					want = adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED
				}
				require.Equal(t, want, response.GetOutcome())
				if tc.metricPanic {
					require.Len(t, events.records, 1, "metric failure must not prevent the persistence event")
				}
			}
		})
	}
}
