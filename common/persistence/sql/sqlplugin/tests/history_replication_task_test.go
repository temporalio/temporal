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
	historyHistoryReplicationTaskSuite struct {
		suite.Suite
		*require.Assertions

		store sqlplugin.HistoryReplicationTask
	}
)

const (
	testHistoryReplicationTaskEncoding = "random encoding"
)

var (
	testHistoryReplicationTaskData = []byte("random history replication task data")
)

func newHistoryReplicationTaskSuite(
	t *testing.T,
	store sqlplugin.HistoryReplicationTask,
) *historyHistoryReplicationTaskSuite {
	return &historyHistoryReplicationTaskSuite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *historyHistoryReplicationTaskSuite) SetupSuite() {

}

func (s *historyHistoryReplicationTaskSuite) TearDownSuite() {

}

func (s *historyHistoryReplicationTaskSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *historyHistoryReplicationTaskSuite) TearDownTest() {

}

func (s *historyHistoryReplicationTaskSuite) TestInsert_Single_Success() {
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomReplicationTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoReplicationTasks(newExecutionContext(), []sqlplugin.ReplicationTasksRow{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *historyHistoryReplicationTaskSuite) TestInsert_Multiple_Success() {
	shardID := rand.Int31()
	taskID := int64(1)

	task1 := s.newRandomReplicationTaskRow(shardID, taskID)
	taskID++
	task2 := s.newRandomReplicationTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoReplicationTasks(newExecutionContext(), []sqlplugin.ReplicationTasksRow{task1, task2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))
}

func (s *historyHistoryReplicationTaskSuite) TestInsert_Single_Fail_Duplicate() {
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomReplicationTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoReplicationTasks(newExecutionContext(), []sqlplugin.ReplicationTasksRow{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	task = s.newRandomReplicationTaskRow(shardID, taskID)
	_, err = s.store.InsertIntoReplicationTasks(newExecutionContext(), []sqlplugin.ReplicationTasksRow{task})
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *historyHistoryReplicationTaskSuite) TestInsert_Multiple_Fail_Duplicate() {
	shardID := rand.Int31()
	taskID := int64(1)

	task1 := s.newRandomReplicationTaskRow(shardID, taskID)
	taskID++
	task2 := s.newRandomReplicationTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoReplicationTasks(newExecutionContext(), []sqlplugin.ReplicationTasksRow{task1, task2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))

	task2 = s.newRandomReplicationTaskRow(shardID, taskID)
	taskID++
	task3 := s.newRandomReplicationTaskRow(shardID, taskID)
	_, err = s.store.InsertIntoReplicationTasks(newExecutionContext(), []sqlplugin.ReplicationTasksRow{task2, task3})
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *historyHistoryReplicationTaskSuite) TestInsertSelect_Single() {
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomReplicationTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoReplicationTasks(newExecutionContext(), []sqlplugin.ReplicationTasksRow{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.ReplicationTasksFilter{
		ShardID: shardID,
		TaskID:  taskID,
	}
	rows, err := s.store.SelectFromReplicationTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	s.Equal([]sqlplugin.ReplicationTasksRow{task}, rows)
}

func (s *historyHistoryReplicationTaskSuite) TestInsertSelect_Multiple() {
	numTasks := 20
	pageSize := numTasks * 2

	shardID := rand.Int31()
	minTaskID := int64(1)
	taskID := minTaskID
	maxTaskID := taskID + int64(numTasks)

	var tasks []sqlplugin.ReplicationTasksRow
	for i := 0; i < numTasks; i++ {
		task := s.newRandomReplicationTaskRow(shardID, taskID)
		taskID++
		tasks = append(tasks, task)
	}
	result, err := s.store.InsertIntoReplicationTasks(newExecutionContext(), tasks)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numTasks, int(rowsAffected))

	filter := sqlplugin.ReplicationTasksRangeFilter{
		ShardID:            shardID,
		InclusiveMinTaskID: minTaskID,
		ExclusiveMaxTaskID: maxTaskID,
		PageSize:           pageSize,
	}
	rows, err := s.store.RangeSelectFromReplicationTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	s.Equal(tasks, rows)
}

func (s *historyHistoryReplicationTaskSuite) TestDeleteSelect_Single() {
	shardID := rand.Int31()
	taskID := int64(1)

	filter := sqlplugin.ReplicationTasksFilter{
		ShardID: shardID,
		TaskID:  taskID,
	}
	result, err := s.store.DeleteFromReplicationTasks(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	rows, err := s.store.SelectFromReplicationTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	s.Equal([]sqlplugin.ReplicationTasksRow(nil), rows)
}

func (s *historyHistoryReplicationTaskSuite) TestDeleteSelect_Multiple() {
	shardID := rand.Int31()
	minTaskID := int64(1)
	maxTaskID := int64(101)

	filter := sqlplugin.ReplicationTasksRangeFilter{
		ShardID:            shardID,
		InclusiveMinTaskID: minTaskID,
		ExclusiveMaxTaskID: maxTaskID,
		PageSize:           0,
	}
	result, err := s.store.RangeDeleteFromReplicationTasks(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	rows, err := s.store.RangeSelectFromReplicationTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	s.Equal([]sqlplugin.ReplicationTasksRow(nil), rows)
}

func (s *historyHistoryReplicationTaskSuite) TestInsertDeleteSelect_Single() {
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomReplicationTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoReplicationTasks(newExecutionContext(), []sqlplugin.ReplicationTasksRow{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.ReplicationTasksFilter{
		ShardID: shardID,
		TaskID:  taskID,
	}
	result, err = s.store.DeleteFromReplicationTasks(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	rows, err := s.store.SelectFromReplicationTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	s.Equal([]sqlplugin.ReplicationTasksRow(nil), rows)
}

func (s *historyHistoryReplicationTaskSuite) TestInsertDeleteSelect_Multiple() {
	numTasks := 20
	pageSize := numTasks * 2

	shardID := rand.Int31()
	minTaskID := int64(1)
	taskID := minTaskID
	maxTaskID := taskID + int64(numTasks)

	var tasks []sqlplugin.ReplicationTasksRow
	for i := 0; i < numTasks; i++ {
		task := s.newRandomReplicationTaskRow(shardID, taskID)
		taskID++
		tasks = append(tasks, task)
	}
	result, err := s.store.InsertIntoReplicationTasks(newExecutionContext(), tasks)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numTasks, int(rowsAffected))

	filter := sqlplugin.ReplicationTasksRangeFilter{
		ShardID:            shardID,
		InclusiveMinTaskID: minTaskID,
		ExclusiveMaxTaskID: maxTaskID,
		PageSize:           pageSize,
	}
	result, err = s.store.RangeDeleteFromReplicationTasks(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(numTasks, int(rowsAffected))

	rows, err := s.store.RangeSelectFromReplicationTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	s.Equal([]sqlplugin.ReplicationTasksRow(nil), rows)
}

func (s *historyHistoryReplicationTaskSuite) newRandomReplicationTaskRow(
	shardID int32,
	taskID int64,
) sqlplugin.ReplicationTasksRow {
	return sqlplugin.ReplicationTasksRow{
		ShardID:      shardID,
		TaskID:       taskID,
		Data:         shuffle.Bytes(testHistoryReplicationTaskData),
		DataEncoding: testHistoryReplicationTaskEncoding,
	}
}
