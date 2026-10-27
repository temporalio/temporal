package namespacereplication

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/embedded"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type peerEventCapture struct {
	embedded.Logger
	records []otellog.Record
}

func (l *peerEventCapture) Emit(_ context.Context, record otellog.Record) {
	l.records = append(l.records, record)
}
func (*peerEventCapture) Enabled(context.Context, otellog.EnabledParameters) bool { return true }

func peerEventDetails(t *testing.T, record otellog.Record) map[string]any {
	t.Helper()
	var details map[string]any
	record.WalkAttributes(func(kv otellog.KeyValue) bool {
		if kv.Key == "details" {
			require.NoError(t, json.Unmarshal([]byte(kv.Value.AsString()), &details))
		}
		return true
	})
	require.NotNil(t, details)
	return details
}

func TestPeerApplyObservability(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		wireOutcome   adminservice.ApplyNamespaceMutationResponse_Outcome
		applyErr      error
		age           time.Duration
		saveFailure   bool
		staleOutcome  bool
		disableEvents bool
		observerPanic bool
		outcome       string
		eventOutcome  string
		wantEvents    int
	}{
		{name: "match", wireOutcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MATCH, outcome: "shadow_match", eventOutcome: "match", wantEvents: 2},
		{name: "mismatch", wireOutcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MISMATCH, outcome: "shadow_mismatch", eventOutcome: "mismatch", wantEvents: 2},
		{name: "match after retry", wireOutcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MATCH, age: 2 * time.Minute, outcome: "shadow_match", eventOutcome: "match", wantEvents: 2},
		{name: "unavailable peer", applyErr: serviceerror.NewUnavailable("peer down"), outcome: "retryable_error"},
		{name: "retry below alert threshold", applyErr: serviceerror.NewUnavailable("peer down"), age: 59 * time.Second, outcome: "retryable_error"},
		{name: "retry at alert threshold", applyErr: serviceerror.NewUnavailable("peer down"), age: time.Minute, outcome: "retryable_error"},
		{name: "retry above alert threshold", applyErr: serviceerror.NewUnavailable("peer down"), age: 61 * time.Second, outcome: "retryable_error"},
		{name: "aged retry", applyErr: serviceerror.NewUnavailable("peer down"), age: 2 * time.Minute, outcome: "retryable_error"},
		{name: "terminal error", applyErr: serviceerror.NewInvalidArgument("invalid reference"), outcome: "terminal_error", eventOutcome: "error", wantEvents: 2},
		{name: "retry exhausted", applyErr: serviceerror.NewUnavailable("peer down"), age: 8 * 24 * time.Hour, outcome: "retry_exhausted", eventOutcome: "error", wantEvents: 2},
		{name: "state save failure", wireOutcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MATCH, saveFailure: true, outcome: "state_transition_error", eventOutcome: "error", wantEvents: 1},
		{name: "retry save failure", applyErr: serviceerror.NewUnavailable("peer down"), saveFailure: true, outcome: "state_transition_error", eventOutcome: "error", wantEvents: 1},
		{name: "stale outcome", wireOutcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MATCH, staleOutcome: true, outcome: "state_update_skipped"},
		{name: "events disabled", wireOutcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MATCH, disableEvents: true, outcome: "shadow_match"},
		{name: "observer panic preserves success", wireOutcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MATCH, observerPanic: true, outcome: "shadow_match"},
		{name: "observer panic preserves save error", wireOutcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MATCH, observerPanic: true, saveFailure: true, outcome: "state_transition_error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			env := newNsreplTestEnv(t)
			mh := metricstest.NewCaptureHandler()
			capture := mh.StartCapture()
			defer mh.StopCapture(capture)
			logger := &peerEventCapture{}
			env.peerHandler.metricsHandler = mh
			env.peerHandler.currentCluster = "cellA"
			env.peerHandler.eventLogger = logger
			env.peerHandler.emitNamespaceLifecycleEvents = func() bool { return !tc.disableEvents }
			if tc.observerPanic {
				panicLogger := log.NewMockLogger(gomock.NewController(t))
				panics := 2
				if tc.saveFailure {
					panics = 1
				}
				panicLogger.EXPECT().Error("Panic is captured", gomock.Any(), gomock.Any()).Times(panics)
				env.peerHandler.logger = panicLogger
				env.peerHandler.emitNamespaceLifecycleEvents = func() bool { panic("observation panic") }
			}
			mutation := env.mutationUpdate("cellB")
			mutation.Shadow = true
			ref := env.start(mutation, func(c *NamespaceMutationComponent) {
				c.LocalApply.Outcome = namespacereplicationpb.LOCAL_APPLY_OUTCOME_SKIPPED_SHADOW
			})
			if tc.age > 0 {
				_, _, err := chasm.UpdateComponent(env.engineCtx, ref, func(c *NamespaceMutationComponent, ctx chasm.MutableContext, _ chasm.NoValue) (chasm.NoValue, error) {
					c.PeerApply["cellB"].FirstAttemptAt = timestamppb.New(ctx.Now(c).Add(-tc.age))
					return nil, nil
				}, nil)
				require.NoError(t, err)
			}
			if tc.saveFailure {
				engine := chasm.NewMockEngine(gomock.NewController(t))
				engine.EXPECT().ReadComponent(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(env.engine.ReadComponent)
				engine.EXPECT().UpdateComponent(gomock.Any(), gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewUnavailable("save failed"))
				env.engineCtx = chasm.NewEngineContext(t.Context(), engine)
			}
			env.clientBean.EXPECT().GetRemoteAdminClient("cellB").Return(env.adminClient, nil)
			env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Cond(func(req *adminservice.ApplyNamespaceMutationRequest) bool {
				return req.GetSourceCluster() == "cellA" && req.GetComponentBusinessId() == ref.BusinessID && req.GetComponentRunId() == ref.RunID && req.GetAttemptCount() == 1
			})).DoAndReturn(func(context.Context, *adminservice.ApplyNamespaceMutationRequest, ...any) (*adminservice.ApplyNamespaceMutationResponse, error) {
				if tc.staleOutcome {
					_, _, err := chasm.UpdateComponent(env.engineCtx, ref, func(c *NamespaceMutationComponent, _ chasm.MutableContext, _ chasm.NoValue) (chasm.NoValue, error) {
						c.PeerApply["cellB"].Outcome = namespacereplicationpb.PEER_APPLY_OUTCOME_SHADOW_MATCH
						return nil, nil
					}, nil)
					require.NoError(t, err)
				}
				return &adminservice.ApplyNamespaceMutationResponse{Outcome: tc.wireOutcome}, tc.applyErr
			})
			err := env.peerHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyPeerTask{TargetCell: "cellB"})
			if tc.saveFailure {
				if down, ok := errors.AsType[*queueserrors.DestinationDownError](err); ok {
					err = down.Unwrap()
				}
				require.ErrorContains(t, err, "save failed")
			} else if tc.applyErr != nil && isPeerDestinationDown(tc.applyErr) {
				var down *queueserrors.DestinationDownError
				require.ErrorAs(t, err, &down)
				require.NoError(t, down.Unwrap())
			} else {
				require.NoError(t, err)
			}
			if tc.observerPanic && !tc.saveFailure {
				require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_SHADOW_MATCH, env.read(ref).GetPeerApply()["cellB"].GetOutcome())
			}
			outcomes := capture.SnapshotMetric(metrics.NamespaceReplicationCHASMApplyOutcomes.Name())
			require.Len(t, outcomes, 1)
			require.Equal(t, map[string]string{"source_cluster": "cellA", "target_cluster": "cellB", "operation": "update", "outcome": tc.outcome, "mode": "shadow", "apply_stage": "peer"}, outcomes[0].Tags)
			require.Len(t, capture.SnapshotMetric(metrics.NamespaceReplicationCHASMApplyLatency.Name()), 1)
			require.Len(t, logger.records, tc.wantEvents)
			completions := capture.SnapshotMetric(metrics.NamespaceReplicationShadowCompletionOutcomes.Name())
			if !tc.saveFailure && !tc.staleOutcome && tc.outcome != "retryable_error" {
				require.Len(t, completions, 1)
			} else {
				require.Empty(t, completions)
			}
			// These fixtures omit the scheduling timestamp. Do not fabricate an
			// end-to-end duration when timing is unavailable.
			require.Empty(t, capture.SnapshotMetric(metrics.NamespaceReplicationCHASMApplyEndToEndLatency.Name()))
			require.Empty(t, capture.SnapshotMetric(metrics.NamespaceReplicationShadowCompletionLatency.Name()))
			if tc.wantEvents > 0 {
				var phase string
				logger.records[0].WalkAttributes(func(kv otellog.KeyValue) bool {
					if kv.Key == "phase" {
						phase = kv.Value.AsString()
					}
					return true
				})
				require.Equal(t, "peer_delivery", phase)
				details := peerEventDetails(t, logger.records[0])
				require.Equal(t, tc.eventOutcome, details["outcome"])
				if tc.eventOutcome == "error" {
					require.Equal(t, tc.outcome, details["reason"])
				}
				if tc.age == 0 || tc.saveFailure {
					require.NotContains(t, details, "pending_duration_ms")
				} else {
					require.InDelta(t, tc.age.Milliseconds(), details["pending_duration_ms"], 0)
				}
				require.Equal(t, "shadow", details["mode"])
				require.Equal(t, "cellA", details["source_cluster"])
				require.Equal(t, "cellB", details["target_cluster"])
				require.Equal(t, ref.BusinessID, details["component_business_id"])
				require.Equal(t, ref.RunID, details["component_run_id"])
				require.InDelta(t, 1, details["attempt_count"], 0)
				if tc.saveFailure {
					require.NotContains(t, details, "persisted_peer_outcome")
					require.Equal(t, false, details["retry_scheduled"])
				}
				if tc.outcome == "retry_exhausted" {
					require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE.String(), details["attempted_peer_outcome"])
					require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL.String(), details["persisted_peer_outcome"])
				}
			}
			if tc.outcome == "retryable_error" {
				age := capture.SnapshotMetric(metrics.NamespaceReplicationCHASMPeerPendingAge.Name())
				require.Len(t, age, 1)
				require.Equal(t, tc.age, age[0].Value)
				threshold := capture.SnapshotMetric(metrics.NamespaceReplicationCHASMPeerPendingThresholdExceeded.Name())
				if tc.age >= time.Minute {
					require.Len(t, threshold, 1)
				} else {
					require.Empty(t, threshold)
				}
			} else {
				require.Empty(t, capture.SnapshotMetric(metrics.NamespaceReplicationCHASMPeerPendingAge.Name()))
				require.Empty(t, capture.SnapshotMetric(metrics.NamespaceReplicationCHASMPeerPendingThresholdExceeded.Name()))
			}
		})
	}
}
