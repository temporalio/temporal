package namespacereplication

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
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

func TestTriggerNamespaceMutationRejectsInvalidReplicateOnlyModes(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		operation namespacereplicationpb.NamespaceOperation
		shadow    bool
	}{
		{name: "create", operation: namespacereplicationpb.NAMESPACE_OPERATION_CREATE},
		{name: "shadow", operation: namespacereplicationpb.NAMESPACE_OPERATION_UPDATE, shadow: true},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			h := newHandler(log.NewNoopLogger())
			_, err := h.TriggerNamespaceMutation(context.Background(), &namespacereplicationpb.TriggerNamespaceMutationRequest{
				NamespaceId:       "namespace-id",
				SystemNamespaceId: primitives.SystemNamespaceID,
				BusinessId:        "namespace-id:mutation-id",
				Mutation: &namespacereplicationpb.NamespaceMutation{
					Operation:       testCase.operation,
					NamespaceDetail: &persistencespb.NamespaceDetail{},
					Shadow:          testCase.shadow,
					ReplicateOnly:   true,
				},
			})

			var invalidArgument *serviceerror.InvalidArgument
			require.ErrorAs(t, err, &invalidArgument)
		})
	}
}
