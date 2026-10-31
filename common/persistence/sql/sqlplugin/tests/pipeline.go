package tests

import (
	"database/sql"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/shuffle"
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

func (s *pipelineSuite) TestReads() {
	shardFilter, currentFilter, executionFilter := s.insertExecution()

	tx := s.beginPipelinedTx()
	_, err := tx.ReadLockShards(newExecutionContext(), shardFilter)
	s.ErrorIs(err, sqlplugin.ErrNotFlushed)
	_, err = tx.LockCurrentExecutions(newExecutionContext(), currentFilter)
	s.ErrorIs(err, sqlplugin.ErrNotFlushed)
	_, _, err = tx.WriteLockExecutions(newExecutionContext(), executionFilter)
	s.ErrorIs(err, sqlplugin.ErrNotFlushed)
	s.NoError(tx.Flush())
	rangeID, err := tx.ReadLockShards(newExecutionContext(), shardFilter)
	s.NoError(err)
	current, err := tx.LockCurrentExecutions(newExecutionContext(), currentFilter)
	s.NoError(err)
	dbRecordVersion, nextEventID, err := tx.WriteLockExecutions(newExecutionContext(), executionFilter)
	s.NoError(err)
	s.NoError(tx.Commit())

	sequentialTx, err := s.store.BeginTx(newExecutionContext())
	s.NoError(err)
	expectedRangeID, err := sequentialTx.ReadLockShards(newExecutionContext(), shardFilter)
	s.NoError(err)
	expectedCurrent, err := sequentialTx.LockCurrentExecutions(newExecutionContext(), currentFilter)
	s.NoError(err)
	expectedDBRecordVersion, expectedNextEventID, err := sequentialTx.WriteLockExecutions(newExecutionContext(), executionFilter)
	s.NoError(err)
	s.NoError(sequentialTx.Rollback())

	s.Equal(expectedRangeID, rangeID)
	s.Equal(expectedCurrent, current)
	s.Equal(expectedDBRecordVersion, dbRecordVersion)
	s.Equal(expectedNextEventID, nextEventID)
}

func (s *pipelineSuite) TestFirstErrorWins() {
	shard := s.newShardRow()
	other := s.newShardRow()

	tx := s.beginPipelinedTx()
	insert := func() []error {
		var errs []error
		for _, row := range []*sqlplugin.ShardsRow{&shard, &shard, &other} {
			_, err := tx.InsertIntoShards(newExecutionContext(), row)
			errs = append(errs, err)
		}
		return errs
	}
	s.Equal([]error{nil, nil, nil}, insert())
	s.NoError(tx.Flush())
	errs := insert()
	s.NoError(errs[0])
	s.True(s.store.IsDupEntryError(errs[1]))
	s.Error(errs[2])
	s.NoError(tx.Rollback())

	for _, row := range []sqlplugin.ShardsRow{shard, other} {
		_, err := s.store.SelectFromShards(newExecutionContext(), sqlplugin.ShardsFilter{ShardID: row.ShardID})
		s.ErrorIs(err, sql.ErrNoRows)
	}
}

func (s *pipelineSuite) TestCommitFailure() {
	shard := s.newShardRow()

	tx := s.beginPipelinedTx()
	_, err := tx.InsertIntoShards(newExecutionContext(), &shard)
	s.NoError(err)
	s.NoError(tx.Flush())
	_, err = tx.InsertIntoShards(newExecutionContext(), &shard)
	s.NoError(err)

	// The timeout makes pg_terminate_backend wait until the backend has exited.
	s.NoError(s.admin.Exec(`SELECT pg_terminate_backend(pid, 5000) FROM pg_stat_activity
WHERE datname = current_database() AND state = 'idle in transaction' AND pid <> pg_backend_pid()`))
	s.Error(tx.Commit())
	s.False(tx.Diverged())

	_, err = s.store.SelectFromShards(newExecutionContext(), sqlplugin.ShardsFilter{ShardID: shard.ShardID})
	s.ErrorIs(err, sql.ErrNoRows)
}

func (s *pipelineSuite) beginPipelinedTx() sqlplugin.PipelinedTx {
	pipeliner, ok := s.store.(sqlplugin.TxPipeliner)
	s.True(ok)
	tx, err := pipeliner.BeginPipelinedTx(newExecutionContext())
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

// insertExecution inserts a shard, a current execution and its execution, and
// returns the filters that lock them.
func (s *pipelineSuite) insertExecution() (
	sqlplugin.ShardsFilter,
	sqlplugin.CurrentExecutionsFilter,
	sqlplugin.ExecutionsFilter,
) {
	shard := s.newShardRow()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	startTime := time.Now().UTC()

	_, err := s.store.InsertIntoShards(newExecutionContext(), &shard)
	s.NoError(err)
	_, err = s.store.InsertIntoCurrentExecutions(newExecutionContext(), &sqlplugin.CurrentExecutionsRow{
		ShardID:          shard.ShardID,
		NamespaceID:      namespaceID,
		WorkflowID:       workflowID,
		RunID:            runID,
		ArchetypeID:      chasm.WorkflowArchetypeID,
		CreateRequestID:  primitives.NewUUID().String(),
		StartTime:        &startTime,
		LastWriteVersion: rand.Int63(),
		State:            enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		Status:           enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		Data:             shuffle.Bytes(testHistoryExecutionStateData),
		DataEncoding:     testHistoryExecutionStateEncoding,
	})
	s.NoError(err)
	_, err = s.store.InsertIntoExecutions(newExecutionContext(), &sqlplugin.ExecutionsRow{
		ShardID:          shard.ShardID,
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
	})
	s.NoError(err)

	return sqlplugin.ShardsFilter{ShardID: shard.ShardID},
		sqlplugin.CurrentExecutionsFilter{
			ShardID:     shard.ShardID,
			NamespaceID: namespaceID,
			WorkflowID:  workflowID,
			ArchetypeID: chasm.WorkflowArchetypeID,
		},
		sqlplugin.ExecutionsFilter{
			ShardID:     shard.ShardID,
			NamespaceID: namespaceID,
			WorkflowID:  workflowID,
			RunID:       runID,
		}
}
