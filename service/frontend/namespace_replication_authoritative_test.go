package frontend

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/wideevents"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"
)

func enabledNamespaceReplicationMetadata(t *testing.T) *cluster.MockMetadata {
	t.Helper()
	metadata := cluster.NewMockMetadata(gomock.NewController(t))
	metadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	metadata.EXPECT().GetCurrentClusterName().Return("cellB").AnyTimes()
	return metadata
}

func TestNamespaceMutationRejectsDisabledGlobalNamespaces(t *testing.T) {
	t.Parallel()
	for _, shadow := range []bool{true, false} {
		name := "authoritative"
		if shadow {
			name = "shadow"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			metadata := cluster.NewMockMetadata(ctrl)
			metadata.EXPECT().IsGlobalNamespaceEnabled().Return(false)
			metadata.EXPECT().GetCurrentClusterName().Return("cellB").AnyTimes()
			mh := metricstest.NewCaptureHandler()
			capture := mh.StartCapture()
			defer mh.StopCapture(capture)
			handler := &AdminHandler{logger: log.NewNoopLogger(), clusterMetadata: metadata, metricsHandler: mh,
				namespaceMutationExecutor: nsreplication.NewMockMutationTaskExecutor(ctrl)}
			payload, err := nsreplication.MarshalNamespaceTask(shadowObservationTask())
			require.NoError(t, err)
			response, err := handler.ApplyNamespaceMutation(t.Context(), &adminservice.ApplyNamespaceMutationRequest{
				Shadow: shadow, NamespaceTaskPayload: payload, ShadowLegacyTaskPayload: payload, SourceCluster: "cellA",
			})
			var disabled *serviceerror.FailedPrecondition
			require.ErrorAs(t, err, &disabled)
			require.Nil(t, response)
			comparisons := capture.SnapshotMetric(metrics.NamespaceReplicationShadowReceiveComparisonOutcomes.Name())
			if shadow {
				require.Len(t, comparisons, 1)
				require.Equal(t, "error", comparisons[0].Tags["outcome"])
				require.Equal(t, "global_namespace_disabled", comparisons[0].Tags["reason"])
			} else {
				require.Empty(t, comparisons)
			}
		})
	}
}

func TestAuthoritativeNamespaceMutationUsesDecodedPayload(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		outcome nsreplication.ApplyOutcome
		want    adminservice.ApplyNamespaceMutationResponse_Outcome
		err     error
	}{
		{name: "created", outcome: nsreplication.ApplyOutcomeCreated, want: adminservice.ApplyNamespaceMutationResponse_OUTCOME_CREATED},
		{name: "applied", outcome: nsreplication.ApplyOutcomeApplied, want: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED},
		{name: "stale", outcome: nsreplication.ApplyOutcomeNoOpStale, want: adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE},
		{name: "duplicate", outcome: nsreplication.ApplyOutcomeDuplicate, want: adminservice.ApplyNamespaceMutationResponse_OUTCOME_DUPLICATE},
		{name: "not admitted", outcome: nsreplication.ApplyOutcomeNotAdmitted, want: adminservice.ApplyNamespaceMutationResponse_OUTCOME_NOT_ADMITTED},
		{name: "executor failure", err: serviceerror.NewUnavailable("store unavailable")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			executor := nsreplication.NewMockMutationTaskExecutor(gomock.NewController(t))
			mh := metricstest.NewCaptureHandler()
			capture := mh.StartCapture()
			defer mh.StopCapture(capture)
			handler := &AdminHandler{metricsHandler: mh, logger: log.NewNoopLogger(), clusterMetadata: enabledNamespaceReplicationMetadata(t), namespaceMutationExecutor: executor}
			task := shadowObservationTask()
			payload, err := nsreplication.MarshalNamespaceTask(task)
			require.NoError(t, err)
			attempts := 1
			if tc.err != nil {
				attempts = namespaceMutationApplyMaxAttempts
			}
			executor.EXPECT().ExecuteWithOutcome(gomock.Any(), gomock.Cond(func(received *replicationspb.NamespaceTaskAttributes) bool {
				return proto.Equal(task, received)
			})).DoAndReturn(func(ctx context.Context, received *replicationspb.NamespaceTaskAttributes) (nsreplication.ApplyOutcome, error) {
				metadata, ok := wideevents.NamespaceReplicationTaskContextFromContext(ctx)
				require.True(t, ok)
				require.Equal(t, "cellA", metadata.SourceCluster)
				require.Equal(t, "cellB", metadata.TargetCluster)
				require.Equal(t, task.GetId(), metadata.EventData.NamespaceID)
				return tc.outcome, tc.err
			}).Times(attempts)
			response, err := handler.ApplyNamespaceMutation(t.Context(), &adminservice.ApplyNamespaceMutationRequest{
				SourceCluster:           "cellA",
				NamespaceTaskPayload:    payload,
				NamespaceTask:           &replicationspb.NamespaceTaskAttributes{Id: "ignored-typed-task"},
				ShadowLegacyTaskPayload: []byte("ignored-shadow-reference"),
			})
			recordings := capture.SnapshotMetric(metrics.NamespaceReplicationCHASMApplyOutcomes.Name())
			require.Len(t, recordings, 1, "receiver retries emit one final RPC outcome")
			require.Equal(t, nsreplication.CHASMApplyStageReceive, recordings[0].Tags["apply_stage"])
			require.Equal(t, "authoritative", recordings[0].Tags["mode"])
			require.Equal(t, "cellA", recordings[0].Tags["source_cluster"])
			if tc.err != nil {
				require.Equal(t, nsreplication.CHASMApplyOutcomeError, recordings[0].Tags["outcome"])
				require.ErrorIs(t, err, tc.err)
			} else {
				require.NoError(t, err)
				require.Equal(t, authoritativeReceiveOutcome(tc.outcome), recordings[0].Tags["outcome"])
				require.Equal(t, tc.want, response.GetOutcome())
			}
		})
	}
}

func TestAuthoritativeNamespaceMutationRejectsMalformedPayload(t *testing.T) {
	t.Parallel()
	executor := nsreplication.NewMockMutationTaskExecutor(gomock.NewController(t))
	handler := &AdminHandler{logger: log.NewNoopLogger(), clusterMetadata: enabledNamespaceReplicationMetadata(t), namespaceMutationExecutor: executor}
	_, err := handler.ApplyNamespaceMutation(t.Context(), &adminservice.ApplyNamespaceMutationRequest{NamespaceTaskPayload: []byte{0xff}})
	var invalid *serviceerror.InvalidArgument
	require.ErrorAs(t, err, &invalid)
}

func TestAuthoritativeNamespaceMutationShortRetry(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		firstErr  error
		cancel    bool
		wantRetry bool
	}{
		{name: "contention clears", firstErr: serviceerror.NewUnavailable("CAS conflict"), wantRetry: true},
		{name: "invalid input", firstErr: serviceerror.NewInvalidArgument("bad task")},
		{name: "throttled", firstErr: &serviceerror.ResourceExhausted{Message: "busy"}},
		{name: "cancelled during apply", firstErr: serviceerror.NewUnavailable("CAS conflict"), cancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			executor := nsreplication.NewMockMutationTaskExecutor(gomock.NewController(t))
			first := executor.EXPECT().ExecuteWithOutcome(gomock.Any(), gomock.Any()).DoAndReturn(
				func(context.Context, *replicationspb.NamespaceTaskAttributes) (nsreplication.ApplyOutcome, error) {
					if tc.cancel {
						cancel()
					}
					return nsreplication.ApplyOutcomeUnspecified, tc.firstErr
				})
			if tc.wantRetry {
				executor.EXPECT().ExecuteWithOutcome(gomock.Any(), gomock.Any()).After(first).Return(nsreplication.ApplyOutcomeNoOpStale, nil)
			}
			payload, err := nsreplication.MarshalNamespaceTask(shadowObservationTask())
			require.NoError(t, err)
			handler := &AdminHandler{logger: log.NewNoopLogger(), clusterMetadata: enabledNamespaceReplicationMetadata(t), namespaceMutationExecutor: executor}
			response, err := handler.ApplyNamespaceMutation(ctx, &adminservice.ApplyNamespaceMutationRequest{NamespaceTaskPayload: payload})
			if tc.wantRetry {
				require.NoError(t, err)
				require.Equal(t, adminservice.ApplyNamespaceMutationResponse_OUTCOME_NO_OP_STALE, response.GetOutcome())
			} else {
				require.ErrorIs(t, err, tc.firstErr)
			}
		})
	}
}

func TestAuthoritativeNamespaceMutationExpiredDeadline(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancel()
	executor := nsreplication.NewMockMutationTaskExecutor(gomock.NewController(t))
	handler := &AdminHandler{logger: log.NewNoopLogger(), clusterMetadata: enabledNamespaceReplicationMetadata(t), namespaceMutationExecutor: executor}
	payload, err := nsreplication.MarshalNamespaceTask(shadowObservationTask())
	require.NoError(t, err)
	_, err = handler.ApplyNamespaceMutation(ctx, &adminservice.ApplyNamespaceMutationRequest{NamespaceTaskPayload: payload})
	require.ErrorIs(t, err, context.DeadlineExceeded)
}
