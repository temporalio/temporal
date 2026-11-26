package scheduler

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	schedulepb "go.temporal.io/api/schedule/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/temporal"
	schedulespb "go.temporal.io/server/api/schedule/v1"
	schedulerpb "go.temporal.io/server/chasm/lib/scheduler/gen/schedulerpb/v1"
	"go.temporal.io/server/chasm/lib/scheduler/migration"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
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

func TestMigrateScheduleToChasm_Deferral(t *testing.T) {
	running := &schedulerpb.InvokerState{BufferedStarts: []*schedulespb.BufferedStart{{RunId: "run"}}}
	completedOnly := &schedulerpb.InvokerState{BufferedStarts: []*schedulespb.BufferedStart{
		{RunId: "run", Completed: &schedulespb.CompletedResult{}},
		{}, // pending, not started
	}}
	state := func(backfill bool, invoker *schedulerpb.InvokerState) *schedulerpb.SchedulerMigrationState {
		st := &schedulerpb.SchedulerMigrationState{
			SchedulerState: &schedulerpb.SchedulerState{ScheduleId: "sched"},
			InvokerState:   invoker,
		}
		if backfill {
			st.Backfillers = map[string]*schedulerpb.BackfillerState{"bf": {}}
		}
		return st
	}
	for _, tc := range []struct {
		name            string
		selected        bool
		allowRunning    bool
		state           *schedulerpb.SchedulerMigrationState
		wantDeferredErr string
	}{
		{name: "backfill, selected", selected: true, state: state(true, nil), wantDeferredErr: errTypeMigrationDeferredBackfill},
		// Lowering the rollout must not strip the backfill guard from a pending migration.
		{name: "backfill, not selected", state: state(true, nil), wantDeferredErr: errTypeMigrationDeferredBackfill},
		{name: "running, selected", selected: true, state: state(false, running), wantDeferredErr: errTypeMigrationDeferredRunningWorkflows},
		{name: "running, selected, running allowed", selected: true, allowRunning: true, state: state(false, running)},
		// Operator signal can force a migration past the running-workflow setting.
		{name: "running, not selected", state: state(false, running)},
		{name: "completed and pending starts only, selected", selected: true, state: state(false, completedOnly)},
		{name: "nothing in flight, selected", selected: true, state: state(false, nil)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &mockSchedulerClient{}
			a := newTestActivities(client, testNamespaceID)
			a.autoMigrationSelected = func(id string) bool {
				require.Equal(t, "sched", id)
				return tc.selected
			}
			a.migrateWithRunningWorkflows = func() bool { return tc.allowRunning }

			err := a.MigrateScheduleToChasm(context.Background(), &schedulerpb.CreateFromMigrationStateRequest{
				NamespaceId: testNamespaceID,
				State:       tc.state,
			})
			if tc.wantDeferredErr == "" {
				require.NoError(t, err)
				require.Equal(t, 1, client.createCalls)
				return
			}
			var appErr *temporal.ApplicationError
			require.ErrorAs(t, err, &appErr)
			require.Equal(t, tc.wantDeferredErr, appErr.Type())
			require.True(t, appErr.NonRetryable())
			require.Zero(t, client.createCalls, "must not create the V2 schedule while deferred")
		})
	}
}

// hasRunningWorkflows infers V1 RunningWorkflows from the converted request shape; pin that
// coupling to the real request builder.
func TestHasRunningWorkflows_MatchesMigrationRequestBuilder(t *testing.T) {
	build := func(info *schedulepb.ScheduleInfo) *schedulerpb.SchedulerMigrationState {
		return migration.LegacyToCreateFromMigrationStateRequest(
			&schedulepb.Schedule{},
			info,
			&schedulespb.InternalState{NamespaceId: testNamespaceID, ScheduleId: "sched"},
			nil, nil, time.Now(),
		).GetState()
	}
	wf := &commonpb.WorkflowExecution{WorkflowId: "wf", RunId: "run"}
	require.True(t, hasRunningWorkflows(build(&schedulepb.ScheduleInfo{RunningWorkflows: []*commonpb.WorkflowExecution{wf}})))
	require.False(t, hasRunningWorkflows(build(&schedulepb.ScheduleInfo{
		RecentActions: []*schedulepb.ScheduleActionResult{{
			ScheduleTime:        timestamppb.Now(),
			ActualTime:          timestamppb.Now(),
			StartWorkflowResult: wf,
			StartWorkflowStatus: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		}},
	})))
}
