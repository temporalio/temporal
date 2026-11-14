package workflow

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commandpb "go.temporal.io/api/command/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/nexusoperation"
	nexusoperationpb "go.temporal.io/server/chasm/lib/nexusoperation/gen/nexusoperationpb/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/service/history/tests"
)

func TestBuildPendingNexusOperationInfos_CircuitBreaker(t *testing.T) {
	for _, endpoint := range []string{"endpoint", commonnexus.SystemEndpoint} {
		for _, open := range []bool{false, true} {
			// System endpoint tasks run on the transfer queue, which has no circuit breaker.
			expectBlocked := open && endpoint != commonnexus.SystemEndpoint
			var calls []string
			// withCircuitBreaker sets up the outbound queue circuit breaker lookup that the history service provides.
			withCircuitBreaker := func(t *testing.T, tcx testContext) {
				tcx.chasmCtx.GoCtx = context.WithValue(context.Background(), nexusoperation.OperationContextKey, &nexusoperation.OperationContext{
					MetricTagConfig: dynamicconfig.GetTypedPropertyFn(nexusoperation.NexusMetricTagConfig{}),
					DestinationBlocked: func(namespaceID, destination string) bool {
						require.Equal(t, tests.GlobalNamespaceEntry.ID().String(), namespaceID)
						calls = append(calls, destination)
						return open
					},
				})
			}
			expectedCalls := []string{endpoint}
			if endpoint == commonnexus.SystemEndpoint {
				expectedCalls = nil
			}
			name := endpoint
			if open {
				name += "/open"
			} else {
				name += "/closed"
			}

			t.Run(name+"/operation", func(t *testing.T) {
				calls = nil
				tcx := newTestContext(t, defaultConfig)
				_, key := scheduleOperation(t, tcx)
				op := tcx.wf.Operations[key].Get(tcx.chasmCtx)
				op.Endpoint = endpoint
				op.SetStateMachineState(nexusoperationpb.OPERATION_STATUS_SCHEDULED)

				withCircuitBreaker(t, tcx)
				infos, err := tcx.wf.BuildPendingNexusOperationInfos(tcx.chasmCtx)
				require.NoError(t, err)
				require.Len(t, infos, 1)
				if expectBlocked {
					require.Equal(t, enumspb.PENDING_NEXUS_OPERATION_STATE_BLOCKED, infos[0].GetState())
					require.Equal(t, "The circuit breaker is open.", infos[0].GetBlockedReason())
				} else {
					require.Equal(t, enumspb.PENDING_NEXUS_OPERATION_STATE_SCHEDULED, infos[0].GetState())
					require.Empty(t, infos[0].GetBlockedReason())
				}
				require.Equal(t, expectedCalls, calls)
			})

			t.Run(name+"/cancellation", func(t *testing.T) {
				calls = nil
				tcx := newTestContext(t, defaultConfig)
				_, key := scheduleOperation(t, tcx)
				applyStartedEvent(t, tcx, key, time.Now())
				err := tcx.cancelHandler(tcx.chasmCtx, tcx.wf, commandValidator{maxPayloadSize: 1}, &commandpb.Command{
					Attributes: &commandpb.Command_RequestCancelNexusOperationCommandAttributes{
						RequestCancelNexusOperationCommandAttributes: &commandpb.RequestCancelNexusOperationCommandAttributes{
							ScheduledEventId: key,
						},
					},
				}, CommandHandlerOptions{WorkflowTaskCompletedEventID: 1})
				require.NoError(t, err)
				op := tcx.wf.Operations[key].Get(tcx.chasmCtx)
				op.Endpoint = endpoint
				cancellation, ok := op.Cancellation.TryGet(tcx.chasmCtx)
				require.True(t, ok)
				cancellation.Status = nexusoperationpb.CANCELLATION_STATUS_SCHEDULED

				withCircuitBreaker(t, tcx)
				infos, err := tcx.wf.BuildPendingNexusOperationInfos(tcx.chasmCtx)
				require.NoError(t, err)
				require.Len(t, infos, 1)
				require.Equal(t, enumspb.PENDING_NEXUS_OPERATION_STATE_STARTED, infos[0].GetState())
				cancelInfo := infos[0].GetCancellationInfo()
				if expectBlocked {
					require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BLOCKED, cancelInfo.GetState())
					require.Equal(t, "The circuit breaker is open.", cancelInfo.GetBlockedReason())
				} else {
					require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED, cancelInfo.GetState())
					require.Empty(t, cancelInfo.GetBlockedReason())
				}
				require.Equal(t, expectedCalls, calls)
			})
		}
	}
}

func TestBuildPendingNexusOperationInfosCallerRPSThrottled(t *testing.T) {
	t.Parallel()
	tcx := newTestContext(t, defaultConfig)
	_, key := scheduleOperation(t, tcx)
	op := tcx.wf.Operations[key].Get(tcx.chasmCtx)
	op.Cancellation = chasm.NewComponentField(tcx.chasmCtx, &nexusoperation.Cancellation{
		CancellationState: &nexusoperationpb.CancellationState{Status: nexusoperationpb.CANCELLATION_STATUS_SCHEDULED},
	})
	tcx.chasmCtx.GoCtx = context.WithValue(t.Context(), nexusoperation.OperationContextKey, &nexusoperation.OperationContext{
		DestinationBlocked: func(string, string) bool { return false },
		DestinationThrottled: func(ns namespace.Name, destination string) bool {
			require.Equal(t, tests.GlobalNamespaceEntry.Name(), ns)
			require.Equal(t, op.Endpoint, destination)
			return true
		},
	})
	infos, err := tcx.wf.BuildPendingNexusOperationInfos(tcx.chasmCtx)
	require.NoError(t, err)
	require.Len(t, infos, 1)
	require.Equal(t, enumspb.PENDING_NEXUS_OPERATION_STATE_BLOCKED, infos[0].State)
	require.Equal(t, "Requests from this namespace to the endpoint are being rate limited.", infos[0].BlockedReason)
	require.Equal(t, enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BLOCKED, infos[0].CancellationInfo.State)
	require.Equal(t, "Requests from this namespace to the endpoint are being rate limited.", infos[0].CancellationInfo.BlockedReason)
}
