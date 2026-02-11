package scheduler

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/log"
	"google.golang.org/grpc"
)

type mockSchedulerClient struct {
	schedulerpb.SchedulerServiceClient
	migrateErr error
}

func (m *mockSchedulerClient) MigrateSchedule(
	_ context.Context,
	_ *schedulerpb.MigrateScheduleRequest,
	_ ...grpc.CallOption,
) (*schedulerpb.MigrateScheduleResponse, error) {
	return &schedulerpb.MigrateScheduleResponse{}, m.migrateErr
}

func newTestActivities(client schedulerpb.SchedulerServiceClient) *activities {
	return &activities{
		activityDeps: activityDeps{
			Logger:          log.NewNoopLogger(),
			SchedulerClient: client,
		},
	}
}

func TestMigrateSchedule_Success(t *testing.T) {
	client := &mockSchedulerClient{}
	a := newTestActivities(client)

	err := a.MigrateSchedule(context.Background(), &schedulerpb.MigrateScheduleRequest{})
	require.NoError(t, err)
}

func TestMigrateSchedule_AlreadyExists(t *testing.T) {
	client := &mockSchedulerClient{
		migrateErr: serviceerror.NewWorkflowExecutionAlreadyStarted("already exists", "", ""),
	}
	a := newTestActivities(client)

	err := a.MigrateSchedule(context.Background(), &schedulerpb.MigrateScheduleRequest{})
	require.NoError(t, err, "already-exists should be treated as success")
}

func TestMigrateSchedule_OtherError(t *testing.T) {
	client := &mockSchedulerClient{
		migrateErr: errors.New("some transient error"),
	}
	a := newTestActivities(client)

	err := a.MigrateSchedule(context.Background(), &schedulerpb.MigrateScheduleRequest{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "MigrateSchedule")
}
