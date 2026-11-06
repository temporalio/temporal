package queues_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	sqlstore "go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/queues/queuestest"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

// timeoutOutageHistoryDB injects driver errors at the production SQL history store.
type timeoutOutageHistoryDB struct {
	sqlplugin.DB
	writes int
}

func (d *timeoutOutageHistoryDB) InsertIntoHistoryNode(context.Context, *sqlplugin.HistoryNodeRow) (sql.Result, error) {
	d.writes++
	if d.writes == 1 {
		return nil, &pgconn.PgError{Code: "53100", Message: "synthetic disk full"}
	}
	if d.writes <= 71 {
		return nil, &pgconn.PgError{Code: "08P01", Message: "server login has been failing, try again later (server_login_retry)"}
	}
	return nil, nil
}

func (*timeoutOutageHistoryDB) IsDupEntryError(error) bool { return false }

func (s *executableSuite) newTaskRecoveryExecutable(task tasks.Task, writer *queuestest.FakeQueueWriter, maxAttempts int, options ...queues.ExecutableOption) queues.Executable {
	defaults := []queues.ExecutableOption{func(p *queues.ExecutableParams) {
		p.DLQEnabled = func() bool { return true }
		p.DLQWriter = queues.NewDLQWriter(writer, metrics.NoopMetricsHandler, log.NewTestLogger(), s.mockNamespaceRegistry, s.chasmRegistry)
		p.MaxUnexpectedErrorAttempts = func() int { return maxAttempts }
	}}
	return queues.NewExecutable(
		queues.DefaultReaderId, task, s.mockExecutor, s.mockScheduler, s.mockRescheduler,
		queues.NewNoopPriorityAssigner(), s.timeSource, s.mockNamespaceRegistry,
		s.mockClusterMetadata, s.chasmRegistry, queues.GetTaskTypeTagValue,
		log.NewTestLogger(), s.metricsHandler, telemetry.NoopTracer,
		append(defaults, options...)...,
	)
}

func newRecoveryFixture(t *testing.T) *executableSuite {
	t.Helper()
	s := new(executableSuite)
	s.SetT(t)
	s.SetupTest()
	return s
}

func TestWorkflowTaskTimeoutSurvivesPersistenceOutage(t *testing.T) {
	t.Parallel()
	s := newRecoveryFixture(t)
	writer := &queuestest.FakeQueueWriter{}
	executable := s.newTaskRecoveryExecutable(&tasks.WorkflowTaskTimeoutTask{
		WorkflowKey: definition.NewWorkflowKey(tests.NamespaceID.String(), tests.WorkflowID, tests.RunID),
		TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE, EventID: 10,
	}, writer, 70)
	db := &timeoutOutageHistoryDB{}
	store, err := sqlstore.NewSQLExecutionStore(db, log.NewNoopLogger(), nil)
	require.NoError(t, err)
	request := &persistence.InternalAppendHistoryNodesRequest{
		BranchInfo: &persistencespb.HistoryBranch{TreeId: "00000000-0000-4000-8000-000000000001", BranchId: "00000000-0000-4000-8000-000000000002"},
		Node:       persistence.InternalHistoryNode{NodeID: 11, Events: &commonpb.DataBlob{}},
	}
	s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).DoAndReturn(func(ctx context.Context, _ queues.Executable) queues.ExecuteResponse {
		err := store.AppendHistoryNodes(ctx, request)
		if err != nil && db.writes%2 == 0 {
			err = fmt.Errorf("synthetic execution context: %w", err)
		}
		return queues.ExecuteResponse{ExecutionErr: err}
	}).Times(72)
	capture := s.metricsHandler.StartCapture()
	for range 71 {
		err = executable.Execute()
		require.Error(t, err)
		require.Same(t, err, executable.HandleErr(err))
		require.Empty(t, writer.EnqueueTaskRequests)
	}
	require.NoError(t, executable.HandleErr(executable.Execute()))
	require.Equal(t, 72, db.writes)
	require.Empty(t, writer.EnqueueTaskRequests)
	snapshot := capture.Snapshot()
	require.Len(t, snapshot[metrics.TaskFailures.Name()], 71)
	require.Empty(t, snapshot[metrics.TaskTerminalFailures.Name()])
}

type terminalUnavailable struct{ error }

func (terminalUnavailable) IsTerminalTaskError() bool { return true }
func (e terminalUnavailable) Unwrap() error           { return e.error }

func TestWorkflowTaskTimeoutRecoveryPreservesDLQPolicy(t *testing.T) {
	t.Parallel()
	key := definition.NewWorkflowKey(tests.NamespaceID.String(), tests.WorkflowID, tests.RunID)
	timeout := func(inMemory bool) tasks.Task {
		return &tasks.WorkflowTaskTimeoutTask{WorkflowKey: key, TimeoutType: enumspb.TIMEOUT_TYPE_START_TO_CLOSE, InMemory: inMemory}
	}
	for _, tc := range []struct {
		name     string
		task     tasks.Task
		err      error
		attempts int
		pattern  string
	}{
		{"other_timer", &tasks.UserTimerTask{WorkflowKey: key}, serviceerror.NewUnavailable("backend unavailable"), 2, ""},
		{"transfer", &tasks.WorkflowTask{WorkflowKey: key}, serviceerror.NewUnavailable("backend unavailable"), 2, ""},
		{"in_memory_timeout", timeout(true), serviceerror.NewUnavailable("backend unavailable"), 2, ""},
		{"data_loss", timeout(false), serviceerror.NewDataLoss("synthetic corruption"), 1, ""},
		{"internal", timeout(false), serviceerror.NewInternal("synthetic internal error"), 1, ""},
		{"unexpected", timeout(false), errors.New("synthetic programming error"), 2, ""},
		{"explicit_terminal", timeout(false), terminalUnavailable{serviceerror.NewUnavailable("backend unavailable")}, 1, ""},
		{"explicit_dlq_pattern", timeout(false), serviceerror.NewUnavailable("backend unavailable"), 1, "backend unavailable"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newRecoveryFixture(t)
			writer := &queuestest.FakeQueueWriter{}
			executable := s.newTaskRecoveryExecutable(tc.task, writer, 2, func(p *queues.ExecutableParams) {
				p.DLQInternalErrors = func() bool { return true }
				p.DLQErrorPattern = func() string { return tc.pattern }
			})
			s.mockExecutor.EXPECT().Execute(gomock.Any(), executable).Return(queues.ExecuteResponse{ExecutionErr: tc.err}).Times(tc.attempts)
			var err error
			for range tc.attempts {
				err = executable.HandleErr(executable.Execute())
			}
			require.ErrorIs(t, err, queues.ErrTerminalTaskFailure)
		})
	}
}
