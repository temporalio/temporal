package namespacereplication

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	nsreplicationcommon "go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type localCommitPollTimeoutEngine struct {
	chasm.Engine
	commitLocal func(chasm.ComponentRef)
}

func (e localCommitPollTimeoutEngine) PollComponent(
	_ context.Context,
	ref chasm.ComponentRef,
	_ func(chasm.Context, chasm.Component) (bool, error),
	_ ...chasm.TransitionOption,
) ([]byte, error) {
	e.commitLocal(ref)
	return nil, context.DeadlineExceeded
}

func TestLocalCommitObservedWhenTriggerPollTimesOut(t *testing.T) {
	t.Parallel()
	for _, operation := range []enumsspb.NamespaceOperation{enumsspb.NAMESPACE_OPERATION_CREATE, enumsspb.NAMESPACE_OPERATION_UPDATE} {
		t.Run(operation.String(), func(t *testing.T) {
			t.Parallel()
			env := newNsreplTestEnv(t)
			capture, events := env.enableObservability()
			request := validTriggerNamespaceMutationRequest(t)
			request.Mutation = env.mutationUpdate()
			request.Mutation.Operation = operation
			request.NamespaceId = request.Mutation.GetNamespaceDetail().GetInfo().GetId()
			request.BusinessId = request.NamespaceId + ":poll-timeout"
			if operation == enumsspb.NAMESPACE_OPERATION_CREATE {
				env.metadataMgr.EXPECT().CreateNamespace(gomock.Any(), gomock.Any()).Return(&persistence.CreateNamespaceResponse{ID: request.NamespaceId}, nil)
			} else {
				env.metadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(nil)
			}
			var committedRef chasm.ComponentRef
			engine := localCommitPollTimeoutEngine{Engine: env.engine, commitLocal: func(ref chasm.ComponentRef) {
				committedRef = ref
				require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{}))
			}}
			response, err := newServiceHandler(log.NewNoopLogger()).TriggerNamespaceMutation(chasm.NewEngineContext(t.Context(), engine), request)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.Nil(t, response)
			require.Equal(t, namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED, env.read(committedRef).GetLocalApply().GetOutcome())
			// The component owns the commit observation independently of the caller's poll result.
			requireAuthoritativeObservation(t, capture, events, nsreplicationcommon.CHASMApplyStageLocal, nsreplicationcommon.CHASMApplyOutcomeApplied)
		})
	}
}

type localObserverPanicMetrics struct{ metrics.Handler }

func TestLocalRetrySaveFailureDoesNotReportExhaustion(t *testing.T) {
	t.Parallel()
	env := newNsreplTestEnv(t)
	capture, events := env.enableObservability()
	env.localHandler.observeLocalRetry(
		chasm.ComponentRef{BusinessID: "mutation", RunID: "run"},
		nsreplicationcommon.NamespaceDetailToTaskAttributes(enumsspb.NAMESPACE_OPERATION_UPDATE, testDetail()),
		localRetryRecord{stateUpdated: true, retryExhausted: true},
		serviceerror.NewUnavailable("write result unknown"),
		serviceerror.NewUnavailable("component save failed"),
		time.Now(),
	)
	details := requireAuthoritativeObservation(t, capture, events, nsreplicationcommon.CHASMApplyStageLocal, nsreplicationcommon.CHASMApplyOutcomeStateTransitionError)
	require.Equal(t, false, details["retry_exhausted"], "a failed component save cannot claim terminal exhaustion")
}

func (localObserverPanicMetrics) Counter(string) metrics.CounterIface {
	return metrics.CounterFunc(func(int64, ...metrics.Tag) { panic("metric observer failed") })
}

func TestLocalObserverPanicsPreserveCompletion(t *testing.T) {
	t.Parallel()
	for _, metricPanic := range []bool{true, false} {
		name := "event observer"
		if metricPanic {
			name = "metric observer"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			env := newNsreplTestEnv(t)
			_, events := env.enableObservability()
			env.localHandler.logger = log.NewNoopLogger()
			if metricPanic {
				env.localHandler.metricsHandler = localObserverPanicMetrics{}
			} else {
				env.localHandler.emitNamespaceLifecycleEvents = func() bool { panic("event observer failed") }
			}
			ref := env.start(env.mutationUpdate(), nil)
			env.metadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(nil)
			require.NotPanics(t, func() {
				require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{}))
			})
			component := env.read(ref)
			require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_COMPLETED, component.GetStatus())
			require.Equal(t, namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED, component.GetLocalApply().GetOutcome())
			if metricPanic {
				requireAuthoritativeEvent(t, events, nsreplicationcommon.CHASMApplyStageComponent, nsreplicationcommon.CHASMApplyOutcomeCompleted)
			}
		})
	}
}

func TestStaleLocalApplyObservesSkippedTransition(t *testing.T) {
	t.Parallel()
	for _, outcome := range []string{"republish", "committed", "failed", "retry", "indeterminate"} {
		t.Run(outcome, func(t *testing.T) {
			t.Parallel()
			env := newNsreplTestEnv(t)
			capture, events := env.enableObservability()
			mutation := env.mutationUpdate("cellB")
			mutation.ReplicateOnly = outcome == "republish"
			ref := env.start(mutation, nil)
			resolve := func() {
				_, _, err := chasm.UpdateComponent(env.engineCtx, ref, func(c *NamespaceMutationComponent, _ chasm.MutableContext, _ chasm.NoValue) (chasm.NoValue, error) {
					c.LocalApply.Outcome = namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED
					return nil, nil
				}, nil)
				require.NoError(t, err)
			}
			if mutation.ReplicateOnly {
				resolve()
			} else {
				env.metadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, *persistence.UpdateNamespaceRequest) error {
					// Another task resolves local apply while this write is in flight.
					resolve()
					switch outcome {
					case "failed":
						return serviceerror.NewInvalidArgument("write rejected")
					case "retry", "indeterminate":
						return serviceerror.NewUnavailable("write result unknown")
					default:
						return nil
					}
				})
				switch outcome {
				case "retry":
					env.metadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: mutation.GetExpectedVersion()}, nil)
				case "indeterminate":
					env.metadataMgr.EXPECT().GetMetadata(gomock.Any()).Return(&persistence.GetMetadataResponse{NotificationVersion: mutation.GetExpectedVersion() + 2}, nil)
					env.metadataMgr.EXPECT().GetNamespace(gomock.Any(), gomock.Any()).Return(&persistence.GetNamespaceResponse{
						Namespace: mutation.GetNamespaceDetail(), IsGlobalNamespace: true, NotificationVersion: mutation.GetExpectedVersion() + 1,
					}, nil)
				default:
					// Definitive write outcomes do not require readback.
				}
			}
			before, err := env.engine.Tasks(ref)
			require.NoError(t, err)
			require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{}))
			after, err := env.engine.Tasks(ref)
			require.NoError(t, err)
			require.Equal(t, before, after, "stale outcomes must not schedule fan-out or retry tasks")
			requireAuthoritativeMetric(t, capture, nsreplicationcommon.CHASMApplyStageLocal, nsreplicationcommon.CHASMApplyOutcomeStateUpdateSkipped)
			require.Empty(t, events.records)
			require.Empty(t, capture.SnapshotMetric(metrics.NamespaceReplicationCHASMLocalPendingAge.Name()))
			require.Empty(t, capture.SnapshotMetric(metrics.NamespaceReplicationCHASMLocalPendingThresholdExceeded.Name()))
		})
	}
}

func TestComponentTerminalEventReportsEndToEndDuration(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		mutation func(*nsreplTestEnv) *namespacereplicationpb.NamespaceMutation
		seed     func(*NamespaceMutationComponent)
		run      func(*testing.T, *nsreplTestEnv, chasm.ComponentRef)
		outcome  string
	}{
		{
			name:     "local commit without peers",
			mutation: func(env *nsreplTestEnv) *namespacereplicationpb.NamespaceMutation { return env.mutationUpdate() },
			run: func(t *testing.T, env *nsreplTestEnv, ref chasm.ComponentRef) {
				env.metadataMgr.EXPECT().UpdateNamespace(gomock.Any(), gomock.Any()).Return(nil)
				require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{}))
			},
			outcome: nsreplicationcommon.CHASMApplyOutcomeCompleted,
		},
		{
			name: "local failure",
			mutation: func(env *nsreplTestEnv) *namespacereplicationpb.NamespaceMutation {
				mutation := env.mutationUpdate("cellB")
				mutation.IsGlobalNamespace = false
				return mutation
			},
			run: func(t *testing.T, env *nsreplTestEnv, ref chasm.ComponentRef) {
				require.NoError(t, env.localHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyLocalTask{}))
			},
			outcome: nsreplicationcommon.CHASMApplyOutcomeFailed,
		},
		{
			name:     "last peer completion",
			mutation: func(env *nsreplTestEnv) *namespacereplicationpb.NamespaceMutation { return env.mutationUpdate("cellB") },
			seed: func(c *NamespaceMutationComponent) {
				c.LocalApply.Outcome = namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED
			},
			run: func(t *testing.T, env *nsreplTestEnv, ref chasm.ComponentRef) {
				env.clientBean.EXPECT().GetRemoteAdminClient("cellB").Return(env.adminClient, nil)
				env.adminClient.EXPECT().ApplyNamespaceMutation(gomock.Any(), gomock.Any()).Return(
					&adminservice.ApplyNamespaceMutationResponse{Outcome: adminservice.ApplyNamespaceMutationResponse_OUTCOME_APPLIED}, nil)
				require.NoError(t, env.peerHandler.Execute(env.engineCtx, ref, chasm.TaskAttributes{}, &namespacereplicationpb.ApplyPeerTask{TargetCell: "cellB"}))
			},
			outcome: nsreplicationcommon.CHASMApplyOutcomeCompleted,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			started := time.Date(2026, 11, 4, 12, 0, 0, 0, time.UTC)
			timeSource := clock.NewEventTimeSource().Update(started)
			env := newNsreplTestEnvWithOptions(t, chasmtest.WithTimeSource(timeSource))
			_, events := env.enableObservability()
			ref := env.start(tc.mutation(env), func(c *NamespaceMutationComponent) {
				c.StartedAt = timestamppb.New(started)
				if tc.seed != nil {
					tc.seed(c)
				}
			})
			timeSource.Advance(45 * time.Second)
			tc.run(t, env, ref)
			details := requireAuthoritativeEvent(t, events, nsreplicationcommon.CHASMApplyStageComponent, tc.outcome)
			require.InDelta(t, 45_000, details["end_to_end_duration_ms"], 0)
		})
	}
}
