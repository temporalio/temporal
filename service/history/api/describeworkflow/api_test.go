package describeworkflow

import (
	"testing"

	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	workflowpb "go.temporal.io/api/workflow/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/circuitbreaker"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/testing/protorequire"
	"go.temporal.io/server/service/history/circuitbreakerpool"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/hsm/hsmtest"
	"go.temporal.io/server/service/history/hsm/nexusoperations"
	"go.temporal.io/server/service/history/tasks"
)

func TestPendingNexusOperationBlockedReason(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		noFailure   bool
		otherCause  bool
		backingOff  bool
		breakerOpen bool
		wantReason  string
	}{
		{name: "caller RPS", wantReason: "Waiting to retry after exceeding the caller RPS limit."},
		{name: "no rejection", noFailure: true},
		{name: "other cause", otherCause: true},
		{name: "backing off", backingOff: true},
		{name: "circuit breaker takes precedence", breakerOpen: true, wantReason: "The circuit breaker is open."},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cause := enumspb.RESOURCE_EXHAUSTED_CAUSE_CALLER_RPS_LIMIT
			if tc.otherCause {
				cause = enumspb.RESOURCE_EXHAUSTED_CAUSE_RPS_LIMIT
			}
			nf, err := nexusrpc.DefaultFailureConverter().ErrorToFailure(commonnexus.ConvertGRPCError(&serviceerror.ResourceExhausted{
				Cause: cause, Scope: enumspb.RESOURCE_EXHAUSTED_SCOPE_NAMESPACE,
			}, true))
			require.NoError(t, err)
			failure, err := commonnexus.NexusFailureToTemporalFailure(nf)
			require.NoError(t, err)
			if tc.noFailure {
				failure = nil
			}
			op := nexusoperations.Operation{NexusOperationInfo: &persistencespb.NexusOperationInfo{
				State:    enumsspb.NEXUS_OPERATION_STATE_SCHEDULED,
				Endpoint: "endpoint", LastAttemptFailure: failure,
			}}
			cancel := nexusoperations.Cancelation{NexusOperationCancellationInfo: &persistencespb.NexusOperationCancellationInfo{
				State: enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED, LastAttemptFailure: failure,
			}}
			wantState := enumspb.PENDING_NEXUS_OPERATION_STATE_SCHEDULED
			wantCancelState := enumspb.NEXUS_OPERATION_CANCELLATION_STATE_SCHEDULED
			if tc.backingOff {
				op.SetState(enumsspb.NEXUS_OPERATION_STATE_BACKING_OFF)
				cancel.SetState(enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BACKING_OFF)
				wantState = enumspb.PENDING_NEXUS_OPERATION_STATE_BACKING_OFF
				wantCancelState = enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BACKING_OFF
			}
			if tc.wantReason != "" {
				wantState = enumspb.PENDING_NEXUS_OPERATION_STATE_BLOCKED
				wantCancelState = enumspb.NEXUS_OPERATION_CANCELLATION_STATE_BLOCKED
			}
			registry := hsm.NewRegistry()
			require.NoError(t, registry.RegisterMachine(hsmtest.NewDefinition("root")))
			require.NoError(t, nexusoperations.RegisterStateMachines(registry))
			root, err := hsm.NewRoot(registry, "root", hsmtest.NewData(hsmtest.State1), nil, &hsmtest.NodeBackend{})
			require.NoError(t, err)
			node, err := root.AddChild(hsm.Key{Type: nexusoperations.OperationMachineType, ID: "1"}, op)
			require.NoError(t, err)
			_, err = node.AddChild(nexusoperations.CancelationMachineKey, cancel)
			require.NoError(t, err)
			pool := &circuitbreakerpool.OutboundQueueCircuitBreakerPool{
				CircuitBreakerPool: circuitbreakerpool.NewCircuitBreakerPool(func(tasks.TaskGroupNamespaceIDAndDestination) circuitbreaker.TwoStepCircuitBreaker {
					cb := gobreaker.NewTwoStepCircuitBreaker(gobreaker.Settings{
						ReadyToTrip: func(gobreaker.Counts) bool { return tc.breakerOpen },
					})
					if tc.breakerOpen {
						done, err := cb.Allow()
						require.NoError(t, err)
						done(false)
					}
					return cb
				}),
			}
			originalOp, originalCancel := common.CloneProto(op.NexusOperationInfo), common.CloneProto(cancel.NexusOperationCancellationInfo)
			info, err := buildPendingNexusOperationInfo(namespace.ID("namespace-id"), node, op, pool)
			require.NoError(t, err)
			protorequire.ProtoEqual(t, &workflowpb.PendingNexusOperationInfo{
				Endpoint: "endpoint", ScheduledEventId: 1,
				State: wantState, BlockedReason: tc.wantReason, LastAttemptFailure: failure,
				CancellationInfo: &workflowpb.NexusOperationCancellationInfo{
					State: wantCancelState, BlockedReason: tc.wantReason, LastAttemptFailure: failure,
				},
			}, info)
			protorequire.ProtoEqual(t, originalOp, op.NexusOperationInfo)
			protorequire.ProtoEqual(t, originalCancel, cancel.NexusOperationCancellationInfo)
		})
	}
}
