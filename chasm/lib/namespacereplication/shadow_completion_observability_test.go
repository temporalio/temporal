package namespacereplication

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	otellog "go.opentelemetry.io/otel/log"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	queueserrors "go.temporal.io/server/service/history/queues/errors"
	"go.uber.org/mock/gomock"
)

func TestShadowEndToEndAndCompletion(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		wireOutcome adminservice.ApplyNamespaceMutationResponse_Outcome
		applyErr    error
		peerOutcome namespacereplicationpb.PeerApplyOutcome
		outcome     string
	}{
		{name: "all match", wireOutcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MATCH, peerOutcome: namespacereplicationpb.PEER_APPLY_OUTCOME_SHADOW_MATCH, outcome: "match"},
		{name: "mismatch", wireOutcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MISMATCH, peerOutcome: namespacereplicationpb.PEER_APPLY_OUTCOME_SHADOW_MISMATCH, outcome: "mismatch"},
		{name: "terminal error", applyErr: serviceerror.NewInvalidArgument("invalid payload"), peerOutcome: namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL, outcome: "error"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			started := time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
			ts := clock.NewEventTimeSource().Update(started)
			env := newNsreplTestEnvWithOptions(t, chasmtest.WithTimeSource(ts))
			mh := metricstest.NewCaptureHandler()
			capture := mh.StartCapture()
			defer mh.StopCapture(capture)
			events := &peerEventCapture{}
			env.peerHandler.metricsHandler = mh
			env.peerHandler.eventLogger = events
			env.peerHandler.currentCluster = "cellA"
			env.peerHandler.emitNamespaceLifecycleEvents = func() bool { return true }
			mutation := env.mutationUpdate("cellB", "cellC")
			mutation.Shadow = true
			ref := env.start(mutation, nil)
			scheduleShadowLocal(t, env, ref)
			require.Equal(t, started, env.read(ref).GetStartedAt().AsTime())

			ts.Advance(10 * time.Second)
			require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{}))
			env.clientBean.EXPECT().GetRemoteAdminClient(gomock.Any()).Return(env.adminClient, nil).AnyTimes()
			env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
				&adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_SHADOW_MATCH}, nil)
			ts.Advance(20 * time.Second)
			require.NoError(t, env.peerHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyPeerTask{TargetCell: "cellB"}))
			latencies := capture.SnapshotMetric(metrics.NamespaceReplicationCHASMApplyEndToEndLatency.Name())
			require.Len(t, latencies, 1)
			require.Equal(t, 30*time.Second, latencies[0].Value)
			require.Equal(t, "cellB", latencies[0].Tags["target_cluster"])
			require.Equal(t, "shadow", latencies[0].Tags["mode"])
			require.Empty(t, capture.SnapshotMetric(metrics.NamespaceReplicationShadowCompletionOutcomes.Name()))
			require.Len(t, events.records, 1)
			require.InDelta(t, 30_000, peerEventDetails(t, events.records[0])["end_to_end_duration_ms"], 0)

			ts.Advance(10 * time.Second)
			env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(nil, serviceerror.NewUnavailable("peer down"))
			err := env.peerHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyPeerTask{TargetCell: "cellC"})
			var down *queueserrors.DestinationDownError
			require.ErrorAs(t, err, &down)
			require.NoError(t, down.Unwrap())
			require.Len(t, capture.SnapshotMetric(metrics.NamespaceReplicationCHASMApplyEndToEndLatency.Name()), 1)
			require.Empty(t, capture.SnapshotMetric(metrics.NamespaceReplicationShadowCompletionOutcomes.Name()))
			require.Len(t, events.records, 1)

			ts.Advance(30 * time.Second)
			_, _, err = chasm.UpdateComponent(env.engineCtx, ref, func(c *NamespaceMutationComponent, ctx chasm.MutableContext, _ chasm.NoValue) (chasm.NoValue, error) {
				return nil, env.backoffHandler.Execute(ctx, c, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyPeerBackoffTask{TargetCell: "cellC", Attempt: 1})
			}, nil)
			require.NoError(t, err)
			env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
				&adminservice.ApplyNamespaceMutationResponse{Outcome: tc.wireOutcome}, tc.applyErr).Times(2)
			peerTask := &namespacereplicationpb.ApplyPeerTask{TargetCell: "cellC", Attempt: 1}
			require.NoError(t, env.peerHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, peerTask))
			require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_COMPLETED, env.read(ref).GetStatus())
			latencies = capture.SnapshotMetric(metrics.NamespaceReplicationCHASMApplyEndToEndLatency.Name())
			require.Len(t, latencies, 2)
			require.Equal(t, 70*time.Second, latencies[1].Value)
			require.Equal(t, "cellC", latencies[1].Tags["target_cluster"])
			completions := capture.SnapshotMetric(metrics.NamespaceReplicationShadowCompletionOutcomes.Name())
			require.Len(t, completions, 1)
			require.Equal(t, tc.outcome, completions[0].Tags["outcome"])
			completionLatencies := capture.SnapshotMetric(metrics.NamespaceReplicationShadowCompletionLatency.Name())
			require.Len(t, completionLatencies, 1)
			require.Equal(t, 70*time.Second, completionLatencies[0].Value)
			require.Len(t, events.records, 3)
			details := peerEventDetails(t, events.records[2])
			var phase string
			events.records[2].WalkAttributes(func(kv otellog.KeyValue) bool {
				if kv.Key == "phase" {
					phase = kv.Value.AsString()
				}
				return true
			})
			require.Equal(t, "completed", phase)
			require.Equal(t, tc.outcome, details["outcome"])
			require.Equal(t, ref.BusinessID, details["component_business_id"])
			require.Equal(t, ref.RunID, details["component_run_id"])
			require.Equal(t, "cellA", details["source_cluster"])
			require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_COMPLETED.String(), details["component_status"])
			require.InDelta(t, 70_000, details["end_to_end_duration_ms"], 0)
			require.InDelta(t, 2, details["peer_count"], 0)
			require.InDelta(t, 3, details["peer_attempt_count"], 0)
			require.Equal(t, map[string]any{"cellB": namespacereplicationpb.PEER_APPLY_OUTCOME_SHADOW_MATCH.String(), "cellC": tc.peerOutcome.String()}, details["peer_outcomes"])
			require.Equal(t, map[string]any{"cellB": float64(1), "cellC": float64(2)}, details["peer_attempt_counts"])

			// An in-flight duplicate can still return after completion. Its result
			// must not emit another terminal latency sample or completion event.
			require.NoError(t, env.peerHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, peerTask))
			require.Len(t, capture.SnapshotMetric(metrics.NamespaceReplicationCHASMApplyEndToEndLatency.Name()), 2)
			require.Len(t, capture.SnapshotMetric(metrics.NamespaceReplicationShadowCompletionOutcomes.Name()), 1)
			require.Len(t, events.records, 3)
		})
	}
}

func TestShadowCompletionWithoutPeers(t *testing.T) {
	t.Parallel()
	for _, eventsEnabled := range []bool{true, false} {
		name := "events disabled"
		if eventsEnabled {
			name = "events enabled"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ts := clock.NewEventTimeSource().Update(time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC))
			env := newNsreplTestEnvWithOptions(t, chasmtest.WithTimeSource(ts))
			mh := metricstest.NewCaptureHandler()
			capture := mh.StartCapture()
			defer mh.StopCapture(capture)
			events := &peerEventCapture{}
			env.localHandler.metricsHandler = mh
			env.localHandler.eventLogger = events
			env.localHandler.emitNamespaceLifecycleEvents = func() bool { return eventsEnabled }
			mutation := env.mutationUpdate()
			mutation.Shadow = true
			ref := env.start(mutation, nil)
			scheduleShadowLocal(t, env, ref)
			ts.Advance(12 * time.Second)
			for range 2 {
				require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{}))
			}
			completions := capture.SnapshotMetric(metrics.NamespaceReplicationShadowCompletionOutcomes.Name())
			require.Len(t, completions, 1)
			require.Equal(t, "no_peers", completions[0].Tags["outcome"])
			latencies := capture.SnapshotMetric(metrics.NamespaceReplicationShadowCompletionLatency.Name())
			require.Len(t, latencies, 1)
			require.Equal(t, 12*time.Second, latencies[0].Value)
			require.Empty(t, capture.SnapshotMetric(metrics.NamespaceReplicationCHASMApplyEndToEndLatency.Name()))
			if eventsEnabled {
				require.Len(t, events.records, 1)
				details := peerEventDetails(t, events.records[0])
				require.Equal(t, "no_peers", details["outcome"])
				require.InDelta(t, 0, details["peer_count"], 0)
				require.InDelta(t, 12_000, details["end_to_end_duration_ms"], 0)
			} else {
				require.Empty(t, events.records)
			}
		})
	}
}

func scheduleShadowLocal(t *testing.T, env *nsreplTestEnv, ref chasm.ComponentRef) {
	t.Helper()
	_, _, err := chasm.UpdateComponent(env.engineCtx, ref, func(c *NamespaceMutationComponent, ctx chasm.MutableContext, _ chasm.NoValue) (chasm.NoValue, error) {
		return nil, TransitionScheduleLocal.Apply(c, ctx, EventScheduleLocal{})
	}, nil)
	require.NoError(t, err)
}

func TestShadowCompletionSnapshot(t *testing.T) {
	t.Parallel()
	c := NewNamespaceMutationComponent(&namespacereplicationpb.NamespaceMutation{
		Shadow: true, NamespaceDetail: testDetail(), PeerCells: []string{"cellB", "cellC", "cellD"},
	})
	c.Status = namespacereplicationpb.COMPONENT_STATUS_COMPLETED
	c.PeerApply["cellB"].Outcome = namespacereplicationpb.PEER_APPLY_OUTCOME_SHADOW_MATCH
	c.PeerApply["cellC"].Outcome = namespacereplicationpb.PEER_APPLY_OUTCOME_SHADOW_MISMATCH
	c.PeerApply["cellD"].Outcome = namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL
	record := newShadowCompletionRecord(c, time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC))
	require.Equal(t, "error", record.outcome)
	require.Equal(t, map[string]int{
		namespacereplicationpb.PEER_APPLY_OUTCOME_SHADOW_MATCH.String():    1,
		namespacereplicationpb.PEER_APPLY_OUTCOME_SHADOW_MISMATCH.String(): 1,
		namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL.String(): 1,
	}, record.peerOutcomeCounts)
	c.Mutation.NamespaceDetail.Info.Name = "changed"
	c.PeerApply["cellD"].Outcome = namespacereplicationpb.PEER_APPLY_OUTCOME_PENDING
	require.Equal(t, "ns", record.detail.GetInfo().GetName())
	require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_TERMINAL.String(), record.peerOutcomes["cellD"])
}

func TestPeerEndToEndRequiresPersistedTerminalOutcome(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"shadow", "authoritative"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			for _, tc := range []struct {
				name         string
				record       peerOutcomeRecord
				saveErr      error
				missingStart bool
				wantSample   bool
			}{
				{name: "terminal result", record: peerOutcomeRecord{stateUpdated: true}, wantSample: true},
				{name: "persistence failure", record: peerOutcomeRecord{stateUpdated: true}, saveErr: serviceerror.NewUnavailable("save failed")},
				{name: "stale result", record: peerOutcomeRecord{}},
				{name: "scheduled retry", record: peerOutcomeRecord{stateUpdated: true, retryScheduled: true}},
				{name: "missing start", record: peerOutcomeRecord{stateUpdated: true}, missingStart: true},
			} {
				t.Run(tc.name, func(t *testing.T) {
					t.Parallel()
					mh := metricstest.NewCaptureHandler()
					capture := mh.StartCapture()
					defer mh.StopCapture(capture)
					h := &applyPeerTaskHandler{logger: log.NewNoopLogger(), metricsHandler: mh}
					record := tc.record
					record.startedAt = time.Date(2026, 10, 23, 12, 0, 0, 0, time.UTC)
					record.resolvedAt = record.startedAt.Add(time.Minute)
					if tc.missingStart {
						record.startedAt = time.Time{}
					}
					record.outcome = namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED
					if mode == "shadow" {
						record.outcome = namespacereplicationpb.PEER_APPLY_OUTCOME_SHADOW_MATCH
					}
					h.observePeerApply(PeerApplyRequest{Shadow: mode == "shadow"}, record, nil, tc.saveErr, time.Second)
					samples := capture.SnapshotMetric(metrics.NamespaceReplicationCHASMApplyEndToEndLatency.Name())
					if tc.wantSample {
						require.Len(t, samples, 1)
						require.Equal(t, time.Minute, samples[0].Value)
						require.Equal(t, mode, samples[0].Tags["mode"])
						require.Equal(t, "peer", samples[0].Tags["apply_stage"])
						require.NotContains(t, samples[0].Tags, "namespace")
					} else {
						require.Empty(t, samples)
					}
				})
			}
		})
	}
}
