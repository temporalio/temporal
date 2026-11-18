package worker

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	workerstatepb "go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"go.temporal.io/server/common/log"
)

// TestRecordHeartbeatInvalidGroup verifies that the request identifies a group before updating its lease.
func TestRecordHeartbeatInvalidGroup(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name              string
		namespaceID       string
		workerGroupingKey string
	}{
		{name: "missing namespace", workerGroupingKey: "group"},
		{name: "missing grouping key", namespaceID: "namespace-id"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := &handler{logger: log.NewNoopLogger()}
			_, err := h.RecordHeartbeat(t.Context(), &workerstatepb.RecordHeartbeatRequest{
				NamespaceId:       tc.namespaceID,
				WorkerGroupingKey: tc.workerGroupingKey,
			})
			require.ErrorAs(t, err, new(*serviceerror.InvalidArgument))
		})
	}
}
