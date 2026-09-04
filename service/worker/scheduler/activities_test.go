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
	migrateErr  error
	createCalls int
}

func (m *mockSchedulerClient) CreateFromMigrationState(
	_ context.Context,
	_ *schedulerpb.CreateFromMigrationStateRequest,
	_ ...grpc.CallOption,
) (*schedulerpb.CreateFromMigrationStateResponse, error) {
	m.createCalls++
	return &schedulerpb.CreateFromMigrationStateResponse{}, m.migrateErr
}

func newTestActivities(client schedulerpb.SchedulerServiceClient, nsID namespace.ID) *activities {
	return &activities{
		activityDeps: activityDeps{
			Logger:          log.NewNoopLogger(),
			SchedulerClient: client,
			MetricsHandler:  metrics.NoopMetricsHandler,
		},
		namespaceID:      nsID,
		migrationEnabled: func() bool { return true },
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

// TestMigrateScheduleToChasm_MigrationDisabled verifies the activity performs
// its own live check of EnableCHASMSchedulerMigration -- independent of
// whatever the calling workflow last cached -- and refuses when it's off.
// This is what stops a pending migration from completing after a rollback,
// even if the workflow retrying it has been asleep since before the rollback.
func TestMigrateScheduleToChasm_MigrationDisabled(t *testing.T) {
	client := &mockSchedulerClient{}
	a := newTestActivities(client, testNamespaceID)
	a.migrationEnabled = func() bool { return false }

	err := a.MigrateScheduleToChasm(context.Background(), &schedulerpb.CreateFromMigrationStateRequest{
		NamespaceId: testNamespaceID,
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "migration is currently disabled")
}

// The guard must refuse before touching the scheduler service, not merely translate its
// error -- otherwise it creates the V2 target and then reports failure, leaving both a V1
// workflow and a V2 schedule behind.
func TestMigrateScheduleToChasm_MigrationDisabledShortCircuits(t *testing.T) {
	client := &mockSchedulerClient{}
	a := newTestActivities(client, testNamespaceID)
	a.migrationEnabled = func() bool { return false }

	err := a.MigrateScheduleToChasm(context.Background(), &schedulerpb.CreateFromMigrationStateRequest{
		NamespaceId: testNamespaceID,
	})
	require.Error(t, err)
	require.Zero(t, client.createCalls, "the V2 schedule must not be created while migration is disabled")
}

// The non-retryable namespace mismatch must not be masked by the retryable disabled
// error, which would turn a permanent misroute into an endless retry loop.
func TestMigrateScheduleToChasm_NamespaceMismatchBeatsDisabledCheck(t *testing.T) {
	client := &mockSchedulerClient{}
	a := newTestActivities(client, testNamespaceID)
	a.migrationEnabled = func() bool { return false }

	err := a.MigrateScheduleToChasm(context.Background(), &schedulerpb.CreateFromMigrationStateRequest{
		NamespaceId: "different-namespace-id",
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), "does not match activity namespace ID")
	require.Zero(t, client.createCalls)
}

// Documents the fail-open default of the `migrationEnabled != nil` guard: an unwired
// dependency migrates as if enabled, which is the wrong polarity for a rollback guard.
func TestMigrateScheduleToChasm_MigrationEnabledUnwired(t *testing.T) {
	client := &mockSchedulerClient{}
	a := newTestActivities(client, testNamespaceID)
	a.migrationEnabled = nil

	err := a.MigrateScheduleToChasm(context.Background(), &schedulerpb.CreateFromMigrationStateRequest{
		NamespaceId: testNamespaceID,
	})
	require.NoError(t, err)
	require.Equal(t, 1, client.createCalls, "unwired migrationEnabled currently fails open")
}
