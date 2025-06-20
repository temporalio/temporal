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
		*require.Assertions

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
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *historyShardSuite) SetupSuite() {

}

func (s *historyShardSuite) TearDownSuite() {

}

func (s *historyShardSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *historyShardSuite) TearDownTest() {

}

func (s *historyShardSuite) TestInsert_Success() {
	shardID := rand.Int31()
	rangeID := int64(1)

	shard := s.newRandomShardRow(shardID, rangeID)
	result, err := s.store.InsertIntoShards(newExecutionContext(), &shard)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *historyShardSuite) TestInsert_Fail_Duplicate() {
	shardID := rand.Int31()
	rangeID := int64(1)

	shard := s.newRandomShardRow(shardID, rangeID)
	result, err := s.store.InsertIntoShards(newExecutionContext(), &shard)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	shard = s.newRandomShardRow(shardID, rangeID)
	_, err = s.store.InsertIntoShards(newExecutionContext(), &shard)
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *historyShardSuite) TestInsertSelect() {
	shardID := rand.Int31()
	rangeID := int64(1)

	shard := s.newRandomShardRow(shardID, rangeID)
	result, err := s.store.InsertIntoShards(newExecutionContext(), &shard)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.ShardsFilter{
		ShardID: shardID,
	}
	row, err := s.store.SelectFromShards(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal(&shard, row)
}

func (s *historyShardSuite) TestInsertUpdate_Success() {
	shardID := rand.Int31()
	rangeID := int64(1)

	shard := s.newRandomShardRow(shardID, rangeID)
	rangeID += 100
	result, err := s.store.InsertIntoShards(newExecutionContext(), &shard)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	shard = s.newRandomShardRow(shardID, rangeID)
	result, err = s.store.UpdateShards(newExecutionContext(), &shard)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *historyShardSuite) TestUpdate_Fail() {
	shardID := rand.Int31()
	rangeID := int64(1)

	shard := s.newRandomShardRow(shardID, rangeID)
	result, err := s.store.UpdateShards(newExecutionContext(), &shard)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))
}

func (s *historyShardSuite) TestInsertUpdateSelect() {
	shardID := rand.Int31()
	rangeID := int64(1)

	shard := s.newRandomShardRow(shardID, rangeID)
	rangeID += 100
	result, err := s.store.InsertIntoShards(newExecutionContext(), &shard)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	shard = s.newRandomShardRow(shardID, rangeID)
	result, err = s.store.UpdateShards(newExecutionContext(), &shard)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.ShardsFilter{
		ShardID: shardID,
	}
	row, err := s.store.SelectFromShards(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal(&shard, row)
}

func (s *historyShardSuite) TestInsertReadLock() {
	shardID := rand.Int31()
	rangeID := int64(rand.Int31())

	shard := s.newRandomShardRow(shardID, rangeID)
	result, err := s.store.InsertIntoShards(newExecutionContext(), &shard)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	tx, err := s.store.BeginTx(newExecutionContext())
	s.NoError(err)
	filter := sqlplugin.ShardsFilter{
		ShardID: shardID,
	}
	shardRange, err := tx.ReadLockShards(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal(rangeID, shardRange)
	s.NoError(tx.Commit())
}

func (s *historyShardSuite) TestInsertWriteLock() {
	shardID := rand.Int31()
	rangeID := int64(rand.Int31())

	shard := s.newRandomShardRow(shardID, rangeID)
	result, err := s.store.InsertIntoShards(newExecutionContext(), &shard)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	tx, err := s.store.BeginTx(newExecutionContext())
	s.NoError(err)
	filter := sqlplugin.ShardsFilter{
		ShardID: shardID,
	}
	shardRange, err := tx.WriteLockShards(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal(rangeID, shardRange)
	s.NoError(tx.Commit())
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
