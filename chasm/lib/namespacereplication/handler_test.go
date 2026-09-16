package namespacereplication

import (
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	namespacereplicationpb "go.temporal.io/server/chasm/lib/namespacereplication/gen/namespacereplicationpb/v1"
	"go.temporal.io/server/common"
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
