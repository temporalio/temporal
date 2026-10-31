package worker

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/chasm/lib/worker/gen/workerpb/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestLeaseExpiryValidation(t *testing.T) {
	t.Parallel()
	deadline := time.Unix(1000, 0).UTC()
	for _, tc := range []struct {
		name          string
		status        workerpb.WorkerGroupStatus
		scheduledTime time.Time
		valid         bool
	}{
		{name: "current lease", status: workerpb.WORKER_GROUP_STATUS_ACTIVE, scheduledTime: deadline, valid: true},
		{name: "renewed lease", status: workerpb.WORKER_GROUP_STATUS_ACTIVE, scheduledTime: deadline.Add(-time.Minute)},
		{name: "later task", status: workerpb.WORKER_GROUP_STATUS_ACTIVE, scheduledTime: deadline.Add(time.Minute)},
		{name: "inactive group", status: workerpb.WORKER_GROUP_STATUS_INACTIVE, scheduledTime: deadline},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			group := &WorkerGroup{WorkerGroupState: &workerpb.WorkerGroupState{
				Status:              tc.status,
				LeaseExpirationTime: timestamppb.New(deadline),
			}}
			valid, err := (&leaseExpiryTaskExecutor{}).Validate(&chasm.MockContext{}, group,
				chasm.TaskInvocation{TaskAttributes: chasm.TaskAttributes{ScheduledTime: tc.scheduledTime}},
				&workerpb.LeaseExpiryTask{})
			require.NoError(t, err)
			require.Equal(t, tc.valid, valid)
		})
	}
}
