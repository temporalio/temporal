package worker

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
)

func TestLeaseExpiryValidation(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		status workerpb.WorkerGroupStatus
		valid  bool
	}{
		{name: "active group", status: workerpb.WORKER_GROUP_STATUS_ACTIVE, valid: true},
		{name: "inactive group", status: workerpb.WORKER_GROUP_STATUS_INACTIVE},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			group := &WorkerGroup{WorkerGroupState: &workerpb.WorkerGroupState{
				Status: tc.status,
			}}
			valid, err := (&leaseExpiryTaskExecutor{}).Validate(&chasm.MockContext{}, group,
				chasm.TaskInvocation{},
				&workerpb.LeaseExpiryTask{})
			require.NoError(t, err)
			require.Equal(t, tc.valid, valid)
		})
	}
}
