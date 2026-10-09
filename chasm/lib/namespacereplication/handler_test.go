package namespacereplication

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/chasmtest"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives"
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
		name       string
		request    func() *namespacereplicationpb.TriggerNamespaceMutationRequest
		mutate     func(*namespacereplicationpb.TriggerNamespaceMutationRequest)
		wantFailed bool
		wantValid  bool
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
			name: "authoritative mutation rejected",
			mutate: func(req *namespacereplicationpb.TriggerNamespaceMutationRequest) {
				req.Mutation.Shadow = false
			},
			wantFailed: true,
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
				req.Mutation.Operation = namespacereplicationpb.NAMESPACE_OPERATION_UNSPECIFIED
			},
		},
		{
			name: "valid create",
			mutate: func(req *namespacereplicationpb.TriggerNamespaceMutationRequest) {
				req.Mutation.Operation = namespacereplicationpb.NAMESPACE_OPERATION_CREATE
			},
			wantValid: true,
		},
		{name: "valid update", wantValid: true},
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
			if tc.wantFailed {
				var failedPrecondition *serviceerror.FailedPrecondition
				require.ErrorAs(t, err, &failedPrecondition)
			} else {
				var invalidArgument *serviceerror.InvalidArgument
				require.ErrorAs(t, err, &invalidArgument)
			}
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

func validTriggerNamespaceMutationRequest() *namespacereplicationpb.TriggerNamespaceMutationRequest {
	const namespaceID = "namespace-id"
	return &namespacereplicationpb.TriggerNamespaceMutationRequest{
		NamespaceId:       namespaceID,
		SystemNamespaceId: primitives.SystemNamespaceID,
		BusinessId:        namespaceID + ":mutation-id",
		Mutation: &namespacereplicationpb.NamespaceMutation{
			Operation: namespacereplicationpb.NAMESPACE_OPERATION_UPDATE,
			NamespaceDetail: &persistencespb.NamespaceDetail{
				Info:              &persistencespb.NamespaceInfo{Id: namespaceID},
				Config:            &persistencespb.NamespaceConfig{},
				ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
			},
			Shadow: true,
		},
	}
}
