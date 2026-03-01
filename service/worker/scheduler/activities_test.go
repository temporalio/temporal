package scheduler

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"google.golang.org/grpc"
)

type mockSchedulerClient struct {
	schedulerpb.SchedulerServiceClient
	migrateErr error
}

func (m *mockSchedulerClient) CreateFromMigrationState(
	_ context.Context,
	_ *schedulerpb.CreateFromMigrationStateRequest,
	_ ...grpc.CallOption,
) (*schedulerpb.CreateFromMigrationStateResponse, error) {
	return &schedulerpb.CreateFromMigrationStateResponse{}, m.migrateErr
}

func newTestActivities(client schedulerpb.SchedulerServiceClient) *activities {
	return &activities{
		activityDeps: activityDeps{
			Logger:          log.NewNoopLogger(),
			SchedulerClient: client,
			MetricsHandler:  metrics.NoopMetricsHandler,
		},
	}
}

func TestMigrateScheduleToChasm_Success(t *testing.T) {
	client := &mockSchedulerClient{}
	a := newTestActivities(client)

	err := a.MigrateScheduleToChasm(context.Background(), &schedulerpb.CreateFromMigrationStateRequest{})
	require.NoError(t, err)
}

func TestMigrateScheduleToChasm_AlreadyExists(t *testing.T) {
	client := &mockSchedulerClient{
		migrateErr: serviceerror.NewAlreadyExistsf("schedule %q is already registered", "test-schedule"),
	}
	a := newTestActivities(client)

	err := a.MigrateScheduleToChasm(context.Background(), &schedulerpb.CreateFromMigrationStateRequest{})
	require.NoError(t, err, "already-exists should be treated as success")
}

func TestMigrateScheduleToChasm_OtherError(t *testing.T) {
	client := &mockSchedulerClient{
		migrateErr: errors.New("some transient error"),
	}
	a := newTestActivities(client)

	err := a.MigrateScheduleToChasm(context.Background(), &schedulerpb.CreateFromMigrationStateRequest{})
	require.Error(t, err)
	require.Contains(t, err.Error(), "MigrateScheduleToChasm")
}
