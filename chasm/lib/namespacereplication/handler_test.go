package namespacereplication

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	failurepb "go.temporal.io/api/failure/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestTriggerNamespaceMutationRoutingMatchesExecution(t *testing.T) {
	for _, numShards := range []int32{1, 4, 32, 512, 4096} {
		for range 2000 {
			namespaceID := uuid.NewString()
			request := &namespacereplicationpb.TriggerNamespaceMutationRequest{
				NamespaceId:       namespaceID,
				SystemNamespaceId: primitives.SystemNamespaceID,
				BusinessId:        namespaceID + ":" + uuid.NewString(),
			}
			routingShard := common.WorkflowIDToHistoryShard(
				request.GetSystemNamespaceId(), request.GetBusinessId(), numShards)
			key := executionKey(request)
			executionShard := common.WorkflowIDToHistoryShard(key.NamespaceID, key.BusinessID, numShards)
			require.Equal(t, routingShard, executionShard)
		}
	}
}

func TestExecutionKeyUsesRoutingFields(t *testing.T) {
	request := &namespacereplicationpb.TriggerNamespaceMutationRequest{
		NamespaceId:       "target-ns",
		SystemNamespaceId: primitives.SystemNamespaceID,
		BusinessId:        "target-ns:mutation-uuid",
	}
	key := executionKey(request)
	require.Equal(t, primitives.SystemNamespaceID, key.NamespaceID)
	require.Equal(t, "target-ns:mutation-uuid", key.BusinessID)
}

func TestValidateTriggerNamespaceMutationRequest(t *testing.T) {
	testCases := []struct {
		name      string
		request   func() *namespacereplicationpb.TriggerNamespaceMutationRequest
		mutate    func(*namespacereplicationpb.TriggerNamespaceMutationRequest)
		wantValid bool
	}{
		{
			name:    "nil request",
			request: func() *namespacereplicationpb.TriggerNamespaceMutationRequest { return nil },
		},
		{
			name: "missing mutation",
			mutate: func(req *namespacereplicationpb.TriggerNamespaceMutationRequest) {
				req.Mutation = nil
			},
		},
		{
			name: "missing namespace id",
			mutate: func(req *namespacereplicationpb.TriggerNamespaceMutationRequest) {
				req.NamespaceId = ""
			},
		},
		{
			name: "missing system namespace id",
			mutate: func(req *namespacereplicationpb.TriggerNamespaceMutationRequest) {
				req.SystemNamespaceId = ""
			},
		},
		{
			name: "incorrect system namespace id",
			mutate: func(req *namespacereplicationpb.TriggerNamespaceMutationRequest) {
				req.SystemNamespaceId = "other-namespace-id"
			},
		},
		{
			name: "missing business id",
			mutate: func(req *namespacereplicationpb.TriggerNamespaceMutationRequest) {
				req.BusinessId = ""
			},
		},
		{
			name: "missing namespace detail",
			mutate: func(req *namespacereplicationpb.TriggerNamespaceMutationRequest) {
				req.Mutation.NamespaceDetail = nil
			},
		},
		{
			name: "missing info",
			mutate: func(req *namespacereplicationpb.TriggerNamespaceMutationRequest) {
				req.Mutation.NamespaceDetail.Info = nil
			},
		},
		{
			name: "missing config",
			mutate: func(req *namespacereplicationpb.TriggerNamespaceMutationRequest) {
				req.Mutation.NamespaceDetail.Config = nil
			},
		},
		{
			name: "missing replication config",
			mutate: func(req *namespacereplicationpb.TriggerNamespaceMutationRequest) {
				req.Mutation.NamespaceDetail.ReplicationConfig = nil
			},
		},
		{
			name: "missing detail namespace id",
			mutate: func(req *namespacereplicationpb.TriggerNamespaceMutationRequest) {
				req.Mutation.NamespaceDetail.Info.Id = ""
			},
		},
		{
			name: "namespace id mismatch",
			mutate: func(req *namespacereplicationpb.TriggerNamespaceMutationRequest) {
				req.Mutation.NamespaceDetail.Info.Id = "other-id"
			},
		},
		{
			name: "unspecified operation",
			mutate: func(req *namespacereplicationpb.TriggerNamespaceMutationRequest) {
				req.Mutation.Operation = enumsspb.NAMESPACE_OPERATION_UNSPECIFIED
			},
		},
		{
			name: "valid authoritative create",
			mutate: func(req *namespacereplicationpb.TriggerNamespaceMutationRequest) {
				req.Mutation.Operation = enumsspb.NAMESPACE_OPERATION_CREATE
				req.Mutation.Shadow = false
			},
			wantValid: true,
		},
		{
			name: "valid authoritative update",
			mutate: func(req *namespacereplicationpb.TriggerNamespaceMutationRequest) {
				req.Mutation.Shadow = false
			},
			wantValid: true,
		},
		{name: "valid shadow update", wantValid: true},
		{
			name: "valid replicate-only update",
			mutate: func(req *namespacereplicationpb.TriggerNamespaceMutationRequest) {
				req.Mutation.Shadow = false
				req.Mutation.ReplicateOnly = true
			},
			wantValid: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			req := validTriggerNamespaceMutationRequest()
			if tc.request != nil {
				req = tc.request()
			}
			if tc.mutate != nil {
				tc.mutate(req)
			}
			err := validateTriggerNamespaceMutationRequest(req)
			if tc.wantValid {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			var invalidArgument *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgument)
		})
	}
}

func TestTriggerNamespaceMutationRejectsInvalidReplicateOnlyModes(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		operation enumsspb.NamespaceOperation
		shadow    bool
	}{
		{name: "create", operation: enumsspb.NAMESPACE_OPERATION_CREATE},
		{name: "shadow", operation: enumsspb.NAMESPACE_OPERATION_UPDATE, shadow: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			req := validTriggerNamespaceMutationRequest()
			req.Mutation.Operation = testCase.operation
			req.Mutation.Shadow = testCase.shadow
			req.Mutation.ReplicateOnly = true

			_, err := newHandler(log.NewNoopLogger()).TriggerNamespaceMutation(context.Background(), req)
			var invalidArgument *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgument)
		})
	}
}

func TestTriggerNamespaceMutationRetryUsesExistingExecution(t *testing.T) {
	for _, completed := range []bool{false, true} {
		t.Run(map[bool]string{false: "running", true: "completed"}[completed], func(t *testing.T) {
			logger := log.NewTestLogger()
			registry := chasm.NewRegistry(logger)
			require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
			require.NoError(t, registry.Register(NewNilLibrary()))
			engine := chasmtest.NewEngine(t, registry)
			ctx := chasm.NewEngineContext(context.Background(), engine)
			req := validTriggerNamespaceMutationRequest()
			_, err := chasm.StartExecution(
				ctx,
				executionKey(req),
				func(mctx chasm.MutableContext, mutation *namespacereplicationpb.NamespaceMutation) (*NamespaceMutationComponent, error) {
					component := NewNamespaceMutationComponent(mutation)
					component.LocalApply.Outcome = namespacereplicationpb.LOCAL_APPLY_OUTCOME_SKIPPED_SHADOW
					if completed {
						component.Status = namespacereplicationpb.COMPONENT_STATUS_COMPLETED
					}
					return component, nil
				},
				req.GetMutation(),
				chasm.WithRequestID(req.GetBusinessId()),
			)
			require.NoError(t, err)

			response, err := newHandler(logger).TriggerNamespaceMutation(ctx, req)
			require.NoError(t, err)
			require.NotNil(t, response)
		})
	}
}

func TestReconcileNamespaceMutationPreservesAttemptsAndCompletes(t *testing.T) {
	logger := log.NewTestLogger()
	registry := chasm.NewRegistry(logger)
	require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
	require.NoError(t, registry.Register(NewNilLibrary()))
	engine := chasmtest.NewEngine(t, registry)
	ctx := chasm.NewEngineContext(context.Background(), engine)
	key := chasm.ExecutionKey{
		NamespaceID: primitives.SystemNamespaceID,
		BusinessID:  "namespace-id:mutation-id",
		RunID:       "run-id",
	}
	resolvedAt := time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)
	firstAttemptAt := resolvedAt.Add(time.Minute)
	lastAttemptAt := resolvedAt.Add(2 * time.Minute)
	_, err := chasm.StartExecution(
		ctx,
		key,
		func(_ chasm.MutableContext, mutation *namespacereplicationpb.NamespaceMutation) (*NamespaceMutationComponent, error) {
			component := NewNamespaceMutationComponent(mutation)
			component.LocalApply = &namespacereplicationpb.LocalApplyStatus{
				Outcome:    namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED,
				ResolvedAt: timestamppb.New(resolvedAt),
			}
			component.PeerApply["cell-b"] = &namespacereplicationpb.PeerApplyStatus{
				Outcome:        namespacereplicationpb.PEER_APPLY_OUTCOME_FAILED_RETRIABLE,
				AttemptCount:   7,
				FirstAttemptAt: timestamppb.New(firstAttemptAt),
				LastAttemptAt:  timestamppb.New(lastAttemptAt),
				LastFailure:    &failurepb.Failure{Message: "peer unavailable"},
			}
			component.PeerApply["already-applied-cell"].Outcome = namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED
			return component, nil
		},
		&namespacereplicationpb.NamespaceMutation{
			Operation: enumsspb.NAMESPACE_OPERATION_UPDATE,
			NamespaceDetail: &persistencespb.NamespaceDetail{
				Info:              &persistencespb.NamespaceInfo{Id: "namespace-id"},
				Config:            &persistencespb.NamespaceConfig{},
				ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
			},
			PeerCells: []string{"cell-b", "removed-cell", "already-applied-cell"},
		},
	)
	require.NoError(t, err)

	handler := newHandler(logger)
	request := &namespacereplicationpb.ReconcileNamespaceMutationRequest{
		SystemNamespaceId:      primitives.SystemNamespaceID,
		BusinessId:             key.BusinessID,
		RunId:                  key.RunID,
		NamespaceId:            "namespace-id",
		RepairId:               "repair-id",
		Reason:                 "break-glass repair",
		Cutoff:                 timestamppb.New(resolvedAt.Add(time.Hour)),
		AuthoritativePeerCells: []string{"cell-b"},
		VerifiedPeerCells:      []string{"cell-b"},
	}
	response, err := handler.ReconcileNamespaceMutation(ctx, request)
	require.NoError(t, err)
	require.True(t, response.GetComponentEligible())
	require.True(t, response.GetComponentCompleted())
	require.Len(t, response.GetPeers(), 3)

	component, err := chasm.ReadComponent(
		chasm.NewContextWithOperationIntent(ctx, chasm.OperationIntentObserve),
		chasm.NewComponentRef[*NamespaceMutationComponent](key),
		func(component *NamespaceMutationComponent, _ chasm.Context, _ chasm.NoValue) (*NamespaceMutationComponent, error) {
			return component, nil
		},
		nil,
	)
	require.NoError(t, err)
	require.Equal(t, namespacereplicationpb.COMPONENT_STATUS_COMPLETED, component.GetStatus())
	configured := component.GetPeerApply()["cell-b"]
	require.Equal(t, namespacereplicationpb.PEER_APPLY_OUTCOME_MANUALLY_RECONCILED, configured.GetOutcome())
	require.Equal(t, int32(7), configured.GetAttemptCount())
	require.Equal(t, firstAttemptAt, configured.GetFirstAttemptAt().AsTime())
	require.Equal(t, lastAttemptAt, configured.GetLastAttemptAt().AsTime())
	require.Equal(t, "peer unavailable", configured.GetLastFailure().GetMessage())
	require.Equal(t, "repair-id", configured.GetManualReconciliation().GetRepairId())
	require.Equal(t, namespacereplicationpb.MANUAL_RECONCILIATION_BASIS_VERIFIED_TARGET, configured.GetManualReconciliation().GetBasis())
	require.Equal(
		t,
		namespacereplicationpb.MANUAL_RECONCILIATION_BASIS_REMOVED_MEMBERSHIP,
		component.GetPeerApply()["removed-cell"].GetManualReconciliation().GetBasis(),
	)
	require.Equal(
		t,
		namespacereplicationpb.PEER_APPLY_OUTCOME_APPLIED,
		component.GetPeerApply()["already-applied-cell"].GetOutcome(),
	)

	// The exact repair RPC can be retried after its first response is lost. CHASM
	// has already closed the execution at that point, so the retry must return
	// the durable terminal result instead of attempting another update.
	retryResponse, err := handler.ReconcileNamespaceMutation(ctx, request)
	require.NoError(t, err)
	require.True(t, retryResponse.GetComponentEligible())
	require.True(t, retryResponse.GetComponentCompleted())
	require.Len(t, retryResponse.GetPeers(), 3)
	for _, peer := range retryResponse.GetPeers() {
		require.Equal(t, namespacereplicationpb.PeerReconciliation_OUTCOME_ALREADY_TERMINAL, peer.GetOutcome())
	}
}

func TestReconcileNamespaceMutationHonorsCutoffAndVerification(t *testing.T) {
	for _, testCase := range []struct {
		name                 string
		cutoff               time.Time
		verifiedPeers        []string
		wantEligible         bool
		wantIneligibleReason string
		wantPeerOutcome      namespacereplicationpb.PeerReconciliation_Outcome
	}{
		{
			name:                 "local apply after cutoff",
			cutoff:               time.Unix(99, 0),
			verifiedPeers:        []string{"cell-b"},
			wantIneligibleReason: "local apply resolved after repair cutoff",
		},
		{
			name:            "configured peer was not verified",
			cutoff:          time.Unix(101, 0),
			wantEligible:    true,
			wantPeerOutcome: namespacereplicationpb.PeerReconciliation_OUTCOME_NOT_VERIFIED,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			logger := log.NewTestLogger()
			registry := chasm.NewRegistry(logger)
			require.NoError(t, registry.Register(&chasm.CoreLibrary{}))
			require.NoError(t, registry.Register(NewNilLibrary()))
			engine := chasmtest.NewEngine(t, registry)
			ctx := chasm.NewEngineContext(context.Background(), engine)
			key := chasm.ExecutionKey{
				NamespaceID: primitives.SystemNamespaceID,
				BusinessID:  "namespace-id:mutation-id",
				RunID:       "run-id",
			}
			_, err := chasm.StartExecution(
				ctx,
				key,
				func(_ chasm.MutableContext, mutation *namespacereplicationpb.NamespaceMutation) (*NamespaceMutationComponent, error) {
					component := NewNamespaceMutationComponent(mutation)
					component.LocalApply = &namespacereplicationpb.LocalApplyStatus{
						Outcome:    namespacereplicationpb.LOCAL_APPLY_OUTCOME_COMMITTED,
						ResolvedAt: timestamppb.New(time.Unix(100, 0)),
					}
					return component, nil
				},
				&namespacereplicationpb.NamespaceMutation{
					Operation: enumsspb.NAMESPACE_OPERATION_UPDATE,
					NamespaceDetail: &persistencespb.NamespaceDetail{
						Info:              &persistencespb.NamespaceInfo{Id: "namespace-id"},
						Config:            &persistencespb.NamespaceConfig{},
						ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
					},
					PeerCells: []string{"cell-b"},
				},
			)
			require.NoError(t, err)

			response, err := newHandler(logger).ReconcileNamespaceMutation(ctx, &namespacereplicationpb.ReconcileNamespaceMutationRequest{
				SystemNamespaceId:      primitives.SystemNamespaceID,
				BusinessId:             key.BusinessID,
				RunId:                  key.RunID,
				NamespaceId:            "namespace-id",
				RepairId:               "repair-id",
				Reason:                 "break-glass repair",
				Cutoff:                 timestamppb.New(testCase.cutoff),
				AuthoritativePeerCells: []string{"cell-b"},
				VerifiedPeerCells:      testCase.verifiedPeers,
			})
			require.NoError(t, err)
			require.Equal(t, testCase.wantEligible, response.GetComponentEligible())
			require.Equal(t, testCase.wantIneligibleReason, response.GetIneligibleReason())
			require.False(t, response.GetComponentCompleted())
			if testCase.wantPeerOutcome != namespacereplicationpb.PeerReconciliation_OUTCOME_UNSPECIFIED {
				require.Len(t, response.GetPeers(), 1)
				require.Equal(t, testCase.wantPeerOutcome, response.GetPeers()[0].GetOutcome())
			}
		})
	}
}

func validTriggerNamespaceMutationRequest() *namespacereplicationpb.TriggerNamespaceMutationRequest {
	const namespaceID = "namespace-id"
	return &namespacereplicationpb.TriggerNamespaceMutationRequest{
		NamespaceId:       namespaceID,
		SystemNamespaceId: primitives.SystemNamespaceID,
		BusinessId:        namespaceID + ":mutation-id",
		Mutation: &namespacereplicationpb.NamespaceMutation{
			Operation: enumsspb.NAMESPACE_OPERATION_UPDATE,
			NamespaceDetail: &persistencespb.NamespaceDetail{
				Info:              &persistencespb.NamespaceInfo{Id: namespaceID},
				Config:            &persistencespb.NamespaceConfig{},
				ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
			},
			Shadow: true,
		},
	}
}
