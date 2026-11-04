package tests

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/persistence/sql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/shuffle"
	"go.temporal.io/server/common/testing/await"
)

const (
	divisionByZeroCode  = "22012"
	uniqueViolationCode = "23505"
)

type (
	pipelineSuite struct {
		suite.Suite
		*require.Assertions

		store sqlplugin.DB
		admin sqlplugin.AdminDB
	}
)

// NewPipelineSuite tests the pipelined transactions of a Postgres store. admin
// runs statements on its own connection.
func NewPipelineSuite(
	t *testing.T,
	store sqlplugin.DB,
	admin sqlplugin.AdminDB,
) *pipelineSuite {
	return &pipelineSuite{
		Assertions: require.New(t),
		store:      store,
		admin:      admin,
	}
}

func (s *pipelineSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *pipelineSuite) TestReadLockShardsWithRangeID() {
	shard := s.newShardRow()
	_, err := s.store.InsertIntoShards(newExecutionContext(), &shard)
	s.NoError(err)
	filter := sqlplugin.ShardsFilter{ShardID: shard.ShardID}

	written, err := s.commit(shard.ShardID, func(tx sqlplugin.PipelinedTx) {
		s.NoError(tx.ReadLockShardsWithRangeID(newExecutionContext(), filter, shard.RangeID))
	})
	s.NoError(err)
	s.True(written)

	written, err = s.commit(shard.ShardID, func(tx sqlplugin.PipelinedTx) {
		s.NoError(tx.ReadLockShardsWithRangeID(newExecutionContext(), filter, shard.RangeID+1))
	})
	_ = s.requireRejected(err, divisionByZeroCode)
	s.False(written)
}

func (s *pipelineSuite) TestLockCurrentExecutionsWithRunID() {
	for _, archetypeID := range []chasm.ArchetypeID{chasm.WorkflowArchetypeID, math.MaxUint32} {
		current, _ := s.insertExecution(archetypeID)
		filter := sqlplugin.CurrentExecutionsFilter{
			ShardID:     current.ShardID,
			NamespaceID: current.NamespaceID,
			WorkflowID:  current.WorkflowID,
			RunID:       current.RunID,
			ArchetypeID: archetypeID,
		}

		written, err := s.commit(current.ShardID, func(tx sqlplugin.PipelinedTx) {
			s.NoError(tx.LockCurrentExecutionsWithRunID(newExecutionContext(), filter))
		})
		s.NoError(err)
		s.True(written)

		filter.RunID = primitives.NewUUID()
		written, err = s.commit(current.ShardID, func(tx sqlplugin.PipelinedTx) {
			s.NoError(tx.LockCurrentExecutionsWithRunID(newExecutionContext(), filter))
		})
		_ = s.requireRejected(err, divisionByZeroCode)
		s.False(written)
	}
}

func (s *pipelineSuite) TestUpdateExecutionsWithCondition() {
	_, execution := s.insertExecution(chasm.WorkflowArchetypeID)
	filter := sqlplugin.ExecutionsFilter{
		ShardID:     execution.ShardID,
		NamespaceID: execution.NamespaceID,
		WorkflowID:  execution.WorkflowID,
		RunID:       execution.RunID,
	}
	update := sqlplugin.ExecutionsUpdate{ExecutionsRow: execution, Condition: execution.NextEventID}

	update.DBRecordVersion = execution.DBRecordVersion + 2
	written, err := s.commit(execution.ShardID, func(tx sqlplugin.PipelinedTx) {
		_, err := tx.UpdateExecutionsWithCondition(newExecutionContext(), &update)
		s.NoError(err)
	})
	_ = s.requireRejected(err, divisionByZeroCode)
	s.False(written)
	stored, err := s.store.SelectFromExecutions(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal(execution.DBRecordVersion, stored.DBRecordVersion)

	update.DBRecordVersion = execution.DBRecordVersion + 1
	written, err = s.commit(execution.ShardID, func(tx sqlplugin.PipelinedTx) {
		_, err := tx.UpdateExecutionsWithCondition(newExecutionContext(), &update)
		s.NoError(err)
	})
	s.NoError(err)
	s.True(written)
	stored, err = s.store.SelectFromExecutions(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal(update.DBRecordVersion, stored.DBRecordVersion)
}

func (s *pipelineSuite) TestUpdateCurrentExecutions() {
	current, _ := s.insertExecution(chasm.WorkflowArchetypeID)
	changed := current
	changed.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	changed.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED

	written, err := s.commit(current.ShardID, func(tx sqlplugin.PipelinedTx) {
		for _, row := range []*sqlplugin.CurrentExecutionsRow{&current, &changed} {
			_, err := tx.UpdateCurrentExecutions(newExecutionContext(), row)
			s.NoError(err)
		}
	})
	s.NoError(err)
	s.True(written)

	stored, err := s.store.SelectFromCurrentExecutions(newExecutionContext(), sqlplugin.CurrentExecutionsFilter{
		ShardID:     current.ShardID,
		NamespaceID: current.NamespaceID,
		WorkflowID:  current.WorkflowID,
		ArchetypeID: current.ArchetypeID,
	})
	s.NoError(err)
	s.Equal(changed.State, stored.State)
	s.Equal(changed.Status, stored.Status)

	// A row that would not change is not updated at all.
	s.NoError(s.admin.Exec(`CREATE FUNCTION pipeline_test_fail_update() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'pipeline test update'; END $$`))
	defer func() { s.NoError(s.admin.Exec(`DROP FUNCTION pipeline_test_fail_update()`)) }()
	s.NoError(s.admin.Exec(fmt.Sprintf(`CREATE TRIGGER pipeline_test_fail_update
AFTER UPDATE ON current_executions
FOR EACH ROW WHEN (NEW.shard_id = %d) EXECUTE FUNCTION pipeline_test_fail_update()`, current.ShardID)))
	defer func() { s.NoError(s.admin.Exec(`DROP TRIGGER pipeline_test_fail_update ON current_executions`)) }()

	written, err = s.commit(current.ShardID, func(tx sqlplugin.PipelinedTx) {
		_, err := tx.UpdateCurrentExecutions(newExecutionContext(), &changed)
		s.NoError(err)
	})
	s.NoError(err)
	s.True(written)
}

func (s *pipelineSuite) TestDuplicateInsert() {
	shardID := rand.Int31()
	task := []sqlplugin.TransferTasksRow{{
		ShardID:      shardID,
		TaskID:       rand.Int63(),
		Data:         shuffle.Bytes(testHistoryTransferTaskData),
		DataEncoding: testHistoryTransferTaskEncoding,
	}}

	written, err := s.commit(shardID, func(tx sqlplugin.PipelinedTx) {
		for range 2 {
			_, err := tx.InsertIntoTransferTasks(newExecutionContext(), task)
			s.NoError(err)
		}
	})
	pgErr := s.requireRejected(err, uniqueViolationCode)
	s.True(s.store.IsDupEntryError(pgErr))
	s.False(written)

	// A rejected batch does not leave a pooled connection mid-batch.
	written, err = s.commit(shardID, func(tx sqlplugin.PipelinedTx) {
		_, err := tx.InsertIntoTransferTasks(newExecutionContext(), task)
		s.NoError(err)
	})
	s.NoError(err)
	s.True(written)
}

// A canceled batch is not a rejection, since Postgres may still run and commit
// the statements it received.
func (s *pipelineSuite) TestCanceledContext() {
	shard := s.newShardRow()
	_, err := s.store.InsertIntoShards(newExecutionContext(), &shard)
	s.NoError(err)
	filter := sqlplugin.ShardsFilter{ShardID: shard.ShardID}
	lockTx, err := s.store.BeginTx(newExecutionContext())
	s.NoError(err)
	_, err = lockTx.WriteLockShards(newExecutionContext(), filter)
	s.NoError(err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tx := s.beginPipelinedTx(ctx)
	s.NoError(tx.ReadLockShardsWithRangeID(ctx, filter, shard.RangeID))
	commitErr := make(chan error, 1)
	go func() { commitErr <- tx.Commit() }()

	// Cancel once Postgres has the batch, which waits on lockTx's shard lock.
	await.RequireTrue(s.T(), func() bool {
		return s.admin.Exec(`DO $$ BEGIN
IF NOT EXISTS (SELECT FROM pg_stat_activity
  WHERE datname = current_database() AND wait_event_type = 'Lock' AND query LIKE '%shards%') THEN
  RAISE EXCEPTION 'no statement waits on a shard lock';
END IF;
END $$`) == nil
	}, 10*time.Second, 10*time.Millisecond)
	cancel()
	err = <-commitErr
	s.Error(err)
	s.NotErrorIs(err, sqlplugin.ErrPipelineNotWritten)
	s.NoError(lockTx.Rollback())
}

// A Commit that cannot check out a connection sends nothing.
func (s *pipelineSuite) TestCommitWithoutConnection() {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := s.beginPipelinedTx(ctx).Commit()
	s.ErrorIs(err, sqlplugin.ErrPipelineNotWritten)
	s.ErrorIs(err, context.Canceled)
}

// An error at the implicit commit is not a rejection, since it can follow a
// commit that happened.
func (s *pipelineSuite) TestCommitError() {
	shardID := rand.Int31()
	s.NoError(s.admin.Exec(`CREATE FUNCTION pipeline_test_fail_commit() RETURNS trigger
LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'pipeline test commit failure'; END $$`))
	defer func() { s.NoError(s.admin.Exec(`DROP FUNCTION pipeline_test_fail_commit()`)) }()
	s.NoError(s.admin.Exec(fmt.Sprintf(`CREATE CONSTRAINT TRIGGER pipeline_test_fail_commit
AFTER INSERT ON history_node DEFERRABLE INITIALLY DEFERRED
FOR EACH ROW WHEN (NEW.shard_id = %d) EXECUTE FUNCTION pipeline_test_fail_commit()`, shardID)))
	defer func() { s.NoError(s.admin.Exec(`DROP TRIGGER pipeline_test_fail_commit ON history_node`)) }()

	written, err := s.commit(shardID, func(sqlplugin.PipelinedTx) {})
	s.Error(err)
	s.NotErrorIs(err, sqlplugin.ErrPipelineNotWritten)
	s.False(written)
}

// commit queues a history node, then the statements queue adds, and commits.
// It returns whether the history node was written and the Commit error.
func (s *pipelineSuite) commit(shardID int32, queue func(tx sqlplugin.PipelinedTx)) (bool, error) {
	node := sqlplugin.HistoryNodeRow{
		ShardID:      shardID,
		TreeID:       primitives.NewUUID(),
		BranchID:     primitives.NewUUID(),
		NodeID:       rand.Int63(),
		PrevTxnID:    rand.Int63(),
		TxnID:        rand.Int63(),
		Data:         shuffle.Bytes(testHistoryNodeData),
		DataEncoding: testHistoryNodeEncoding,
	}

	tx := s.beginPipelinedTx(newExecutionContext())
	_, err := tx.InsertIntoHistoryNode(newExecutionContext(), &node)
	s.NoError(err)
	queue(tx)
	commitErr := tx.Commit()

	rows, err := s.store.RangeSelectFromHistoryNode(newExecutionContext(), sqlplugin.HistoryNodeSelectFilter{
		ShardID:   shardID,
		TreeID:    node.TreeID,
		BranchID:  node.BranchID,
		MinNodeID: node.NodeID,
		MinTxnID:  sql.MinTxnID,
		MaxNodeID: math.MaxInt64,
		PageSize:  1,
	})
	s.NoError(err)
	return len(rows) > 0, commitErr
}

// requireRejected requires a rejection by the Postgres error with code, and
// returns that error.
func (s *pipelineSuite) requireRejected(err error, code string) *pgconn.PgError {
	s.ErrorIs(err, sqlplugin.ErrPipelineNotWritten)
	var pgErr *pgconn.PgError
	s.ErrorAs(err, &pgErr)
	s.Equal(code, pgErr.Code)
	return pgErr
}

func (s *pipelineSuite) beginPipelinedTx(ctx context.Context) sqlplugin.PipelinedTx {
	pipeliner, ok := s.store.(sqlplugin.TxPipeliner)
	s.True(ok)
	tx, err := pipeliner.BeginPipelinedTx(ctx)
	s.NoError(err)
	return tx
}

func (s *pipelineSuite) newShardRow() sqlplugin.ShardsRow {
	return sqlplugin.ShardsRow{
		ShardID:      rand.Int31(),
		RangeID:      rand.Int63(),
		Data:         shuffle.Bytes(testHistoryShardData),
		DataEncoding: testHistoryShardEncoding,
	}
}

// insertExecution inserts a current execution and its execution, and returns
// their rows.
func (s *pipelineSuite) insertExecution(archetypeID chasm.ArchetypeID) (
	sqlplugin.CurrentExecutionsRow,
	sqlplugin.ExecutionsRow,
) {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	startTime := time.Now().UTC()

	current := sqlplugin.CurrentExecutionsRow{
		ShardID:          shardID,
		NamespaceID:      namespaceID,
		WorkflowID:       workflowID,
		RunID:            runID,
		ArchetypeID:      archetypeID,
		CreateRequestID:  primitives.NewUUID().String(),
		StartTime:        &startTime,
		LastWriteVersion: rand.Int63(),
		State:            enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		Data:             shuffle.Bytes(testHistoryExecutionStateData),
		DataEncoding:     testHistoryExecutionStateEncoding,
	}
	_, err := s.store.InsertIntoCurrentExecutions(newExecutionContext(), &current)
	s.NoError(err)
	execution := sqlplugin.ExecutionsRow{
		ShardID:          shardID,
		NamespaceID:      namespaceID,
		WorkflowID:       workflowID,
		RunID:            runID,
		NextEventID:      rand.Int63(),
		LastWriteVersion: rand.Int63(),
		Data:             shuffle.Bytes(testHistoryExecutionData),
		DataEncoding:     testHistoryExecutionEncoding,
		State:            shuffle.Bytes(testHistoryExecutionStateData),
		StateEncoding:    testHistoryExecutionStateEncoding,
		DBRecordVersion:  rand.Int63(),
	}
	_, err = s.store.InsertIntoExecutions(newExecutionContext(), &execution)
	s.NoError(err)

	return current, execution
}
