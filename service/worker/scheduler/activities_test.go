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
	"go.temporal.io/server/common/namespace"
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

func newTestActivities(client schedulerpb.SchedulerServiceClient, nsID namespace.ID) *activities {
	return &activities{
		activityDeps: activityDeps{
			Logger:          log.NewNoopLogger(),
			SchedulerClient: client,
			MetricsHandler:  metrics.NoopMetricsHandler,
		},
		namespaceID: nsID,
	}
}

const testNamespaceID = "test-namespace-id"

func TestMigrateScheduleToChasm_Success(t *testing.T) {
	client := &mockSchedulerClient{}
	a := newTestActivities(client, testNamespaceID)

	err := a.MigrateScheduleToChasm(context.Background(), &schedulerpb.CreateFromMigrationStateRequest{
		NamespaceId: testNamespaceID,
	})
	require.NoError(t, err)
}

func TestMigrateScheduleToChasm_AlreadyExists(t *testing.T) {
	client := &mockSchedulerClient{
		migrateErr: serviceerror.NewAlreadyExistsf("schedule %q is already registered", "test-schedule"),
	}
	a := newTestActivities(client, testNamespaceID)

	err := a.MigrateScheduleToChasm(context.Background(), &schedulerpb.CreateFromMigrationStateRequest{
		NamespaceId: testNamespaceID,
	})
	require.NoError(t, err, "already-exists should be treated as success")
}

func TestMigrateScheduleToChasm_SentinelBlocked(t *testing.T) {
	client := &mockSchedulerClient{
		migrateErr: serviceerror.NewUnavailable("schedule is a sentinel; please retry after sentinel expires"),
	}
	a := newTestActivities(client, testNamespaceID)

	err := a.MigrateScheduleToChasm(context.Background(), &schedulerpb.CreateFromMigrationStateRequest{
		NamespaceId: testNamespaceID,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "blocked by sentinel")
}

func TestMigrateScheduleToChasm_OtherError(t *testing.T) {
	client := &mockSchedulerClient{
		migrateErr: errors.New("some transient error"),
	}
	a := newTestActivities(client, testNamespaceID)

	err := a.MigrateScheduleToChasm(context.Background(), &schedulerpb.CreateFromMigrationStateRequest{
		NamespaceId: testNamespaceID,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "MigrateScheduleToChasm")
}

func TestMigrateScheduleToChasm_NamespaceMismatch(t *testing.T) {
	client := &mockSchedulerClient{}
	a := newTestActivities(client, testNamespaceID)

	err := a.MigrateScheduleToChasm(context.Background(), &schedulerpb.CreateFromMigrationStateRequest{
		NamespaceId: "different-namespace-id",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "namespace_mismatch")
	require.Contains(t, err.Error(), "different-namespace-id")
	require.Contains(t, err.Error(), testNamespaceID)
}
