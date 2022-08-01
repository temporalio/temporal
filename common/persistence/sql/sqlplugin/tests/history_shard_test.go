// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

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

		store sqlplugin.HistoryShard
	}
)

const (
	testHistoryShardEncoding = "random encoding"
)

var (
	testHistoryShardData = []byte("random history shard data")
)

func newHistoryShardSuite(
	t *testing.T,
	store sqlplugin.HistoryShard,
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

	// NOTE: lock without transaction is equivalent to select
	//  this test only test the select functionality
	filter := sqlplugin.ShardsFilter{
		ShardID: shardID,
	}
	shardRange, err := s.store.ReadLockShards(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal(rangeID, shardRange)
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

	// NOTE: lock without transaction is equivalent to select
	//  this test only test the select functionality
	filter := sqlplugin.ShardsFilter{
		ShardID: shardID,
	}
	shardRange, err := s.store.WriteLockShards(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal(rangeID, shardRange)
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
