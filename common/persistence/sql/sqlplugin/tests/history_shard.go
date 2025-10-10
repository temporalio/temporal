package tests

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/shuffle"
)

type (
	historyShardSuite struct {
		suite.Suite

		store sqlplugin.DB
	}
)

const (
	testHistoryShardEncoding = "random encoding"
)

var (
	testHistoryShardData = []byte("random history shard data")
)

func NewHistoryShardSuite(
	t *testing.T,
	store sqlplugin.DB,
) *historyShardSuite {
	return &historyShardSuite{

		store: store,
	}
}

func (s *historyShardSuite) TearDownSuite() {

}

func (s *historyShardSuite) TearDownTest() {

}

func (s *historyShardSuite) TestInsert_Success() {
	shardID := rand.Int31()
	rangeID := int64(1)

	shard := s.newRandomShardRow(shardID, rangeID)
	result, err := s.store.InsertIntoShards(newExecutionContext(), &shard)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))
}

func (s *historyShardSuite) TestInsert_Fail_Duplicate() {
	shardID := rand.Int31()
	rangeID := int64(1)

	shard := s.newRandomShardRow(shardID, rangeID)
	result, err := s.store.InsertIntoShards(newExecutionContext(), &shard)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	shard = s.newRandomShardRow(shardID, rangeID)
	_, err = s.store.InsertIntoShards(newExecutionContext(), &shard)
	require.Error(s.T(), err) // TODO persistence layer should do proper error translation
}

func (s *historyShardSuite) TestInsertSelect() {
	shardID := rand.Int31()
	rangeID := int64(1)

	shard := s.newRandomShardRow(shardID, rangeID)
	result, err := s.store.InsertIntoShards(newExecutionContext(), &shard)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	filter := sqlplugin.ShardsFilter{
		ShardID: shardID,
	}
	row, err := s.store.SelectFromShards(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), &shard, row)
}

func (s *historyShardSuite) TestInsertUpdate_Success() {
	shardID := rand.Int31()
	rangeID := int64(1)

	shard := s.newRandomShardRow(shardID, rangeID)
	rangeID += 100
	result, err := s.store.InsertIntoShards(newExecutionContext(), &shard)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	shard = s.newRandomShardRow(shardID, rangeID)
	result, err = s.store.UpdateShards(newExecutionContext(), &shard)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))
}

func (s *historyShardSuite) TestUpdate_Fail() {
	shardID := rand.Int31()
	rangeID := int64(1)

	shard := s.newRandomShardRow(shardID, rangeID)
	result, err := s.store.UpdateShards(newExecutionContext(), &shard)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))
}

func (s *historyShardSuite) TestInsertUpdateSelect() {
	shardID := rand.Int31()
	rangeID := int64(1)

	shard := s.newRandomShardRow(shardID, rangeID)
	rangeID += 100
	result, err := s.store.InsertIntoShards(newExecutionContext(), &shard)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	shard = s.newRandomShardRow(shardID, rangeID)
	result, err = s.store.UpdateShards(newExecutionContext(), &shard)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	filter := sqlplugin.ShardsFilter{
		ShardID: shardID,
	}
	row, err := s.store.SelectFromShards(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), &shard, row)
}

func (s *historyShardSuite) TestInsertReadLock() {
	shardID := rand.Int31()
	rangeID := int64(rand.Int31())

	shard := s.newRandomShardRow(shardID, rangeID)
	result, err := s.store.InsertIntoShards(newExecutionContext(), &shard)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	tx, err := s.store.BeginTx(newExecutionContext())
	require.NoError(s.T(), err)
	filter := sqlplugin.ShardsFilter{
		ShardID: shardID,
	}
	shardRange, err := tx.ReadLockShards(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), rangeID, shardRange)
	require.NoError(s.T(), tx.Commit())
}

func (s *historyShardSuite) TestInsertWriteLock() {
	shardID := rand.Int31()
	rangeID := int64(rand.Int31())

	shard := s.newRandomShardRow(shardID, rangeID)
	result, err := s.store.InsertIntoShards(newExecutionContext(), &shard)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	tx, err := s.store.BeginTx(newExecutionContext())
	require.NoError(s.T(), err)
	filter := sqlplugin.ShardsFilter{
		ShardID: shardID,
	}
	shardRange, err := tx.WriteLockShards(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), rangeID, shardRange)
	require.NoError(s.T(), tx.Commit())
}

func (s *historyShardSuite) newRandomShardRow(
	shardID int32,
	rangeID int64,
) sqlplugin.ShardsRow {
	return sqlplugin.ShardsRow{
		ShardID:      shardID,
		RangeID:      rangeID,
		Data:         shuffle.Bytes(testHistoryShardData),
		DataEncoding: testHistoryShardEncoding,
	}
}
