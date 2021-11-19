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
	historyTieredStorageTaskSuite struct {
		suite.Suite
		*require.Assertions

		store sqlplugin.HistoryTieredStorageTask
	}
)

const (
	testTieredStorageTaskEncoding = "random encoding"
)

var (
	testTieredStorageTaskData = []byte("random history TieredStorage task data")
)

func newHistoryTieredStorageTaskSuite(
	t *testing.T,
	store sqlplugin.HistoryTieredStorageTask,
) *historyTieredStorageTaskSuite {
	return &historyTieredStorageTaskSuite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *historyTieredStorageTaskSuite) SetupSuite() {

}

func (s *historyTieredStorageTaskSuite) TearDownSuite() {

}

func (s *historyTieredStorageTaskSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *historyTieredStorageTaskSuite) TearDownTest() {

}

func (s *historyTieredStorageTaskSuite) TestInsert_Single_Success() {
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomTieredStorageTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoTieredStorageTasks(newExecutionContext(), []sqlplugin.TieredStorageTasksRow{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *historyTieredStorageTaskSuite) TestInsert_Multiple_Success() {
	shardID := rand.Int31()
	taskID := int64(1)

	task1 := s.newRandomTieredStorageTaskRow(shardID, taskID)
	taskID++
	task2 := s.newRandomTieredStorageTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoTieredStorageTasks(newExecutionContext(), []sqlplugin.TieredStorageTasksRow{task1, task2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))
}

func (s *historyTieredStorageTaskSuite) TestInsert_Single_Fail_Duplicate() {
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomTieredStorageTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoTieredStorageTasks(newExecutionContext(), []sqlplugin.TieredStorageTasksRow{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	task = s.newRandomTieredStorageTaskRow(shardID, taskID)
	_, err = s.store.InsertIntoTieredStorageTasks(newExecutionContext(), []sqlplugin.TieredStorageTasksRow{task})
	s.Error(err)
}

func (s *historyTieredStorageTaskSuite) TestInsert_Multiple_Fail_Duplicate() {
	shardID := rand.Int31()
	taskID := int64(1)

	task1 := s.newRandomTieredStorageTaskRow(shardID, taskID)
	taskID++
	task2 := s.newRandomTieredStorageTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoTieredStorageTasks(newExecutionContext(), []sqlplugin.TieredStorageTasksRow{task1, task2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))

	task2 = s.newRandomTieredStorageTaskRow(shardID, taskID)
	taskID++
	task3 := s.newRandomTieredStorageTaskRow(shardID, taskID)
	_, err = s.store.InsertIntoTieredStorageTasks(newExecutionContext(), []sqlplugin.TieredStorageTasksRow{task2, task3})
	s.Error(err)
}

func (s *historyTieredStorageTaskSuite) TestInsertSelect_Single() {
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomTieredStorageTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoTieredStorageTasks(newExecutionContext(), []sqlplugin.TieredStorageTasksRow{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.TieredStorageTasksFilter{
		ShardID: shardID,
		TaskID:  taskID,
	}
	rows, err := s.store.SelectFromTieredStorageTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	s.Equal([]sqlplugin.TieredStorageTasksRow{task}, rows)
}

func (s *historyTieredStorageTaskSuite) TestInsertSelect_Multiple() {
	numTasks := 20

	shardID := rand.Int31()
	minTaskID := int64(0)
	taskID := minTaskID + 1
	maxTaskID := taskID + int64(numTasks)

	var tasks []sqlplugin.TieredStorageTasksRow
	for i := 0; i < numTasks; i++ {
		task := s.newRandomTieredStorageTaskRow(shardID, taskID)
		taskID++
		tasks = append(tasks, task)
	}
	result, err := s.store.InsertIntoTieredStorageTasks(newExecutionContext(), tasks)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numTasks, int(rowsAffected))

	filter := sqlplugin.TieredStorageTasksRangeFilter{
		ShardID:   shardID,
		MinTaskID: minTaskID,
		MaxTaskID: maxTaskID,
	}
	rows, err := s.store.RangeSelectFromTieredStorageTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	s.Equal(tasks, rows)
}

func (s *historyTieredStorageTaskSuite) TestDeleteSelect_Single() {
	shardID := rand.Int31()
	taskID := int64(1)

	filter := sqlplugin.TieredStorageTasksFilter{
		ShardID: shardID,
		TaskID:  taskID,
	}
	result, err := s.store.DeleteFromTieredStorageTasks(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	rows, err := s.store.SelectFromTieredStorageTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	s.Equal([]sqlplugin.TieredStorageTasksRow(nil), rows)
}

func (s *historyTieredStorageTaskSuite) TestDeleteSelect_Multiple() {
	shardID := rand.Int31()
	minTaskID := int64(1)
	maxTaskID := int64(100)

	filter := sqlplugin.TieredStorageTasksRangeFilter{
		ShardID:   shardID,
		MinTaskID: minTaskID,
		MaxTaskID: maxTaskID,
	}
	result, err := s.store.RangeDeleteFromTieredStorageTasks(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	rows, err := s.store.RangeSelectFromTieredStorageTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	s.Equal([]sqlplugin.TieredStorageTasksRow(nil), rows)
}

func (s *historyTieredStorageTaskSuite) TestInsertDeleteSelect_Single() {
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomTieredStorageTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoTieredStorageTasks(newExecutionContext(), []sqlplugin.TieredStorageTasksRow{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.TieredStorageTasksFilter{
		ShardID: shardID,
		TaskID:  taskID,
	}
	result, err = s.store.DeleteFromTieredStorageTasks(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	rows, err := s.store.SelectFromTieredStorageTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	s.Equal([]sqlplugin.TieredStorageTasksRow(nil), rows)
}

func (s *historyTieredStorageTaskSuite) TestInsertDeleteSelect_Multiple() {
	numTasks := 20

	shardID := rand.Int31()
	minTaskID := int64(0)
	taskID := minTaskID + 1
	maxTaskID := taskID + int64(numTasks)

	var tasks []sqlplugin.TieredStorageTasksRow
	for i := 0; i < numTasks; i++ {
		task := s.newRandomTieredStorageTaskRow(shardID, taskID)
		taskID++
		tasks = append(tasks, task)
	}
	result, err := s.store.InsertIntoTieredStorageTasks(newExecutionContext(), tasks)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numTasks, int(rowsAffected))

	filter := sqlplugin.TieredStorageTasksRangeFilter{
		ShardID:   shardID,
		MinTaskID: minTaskID,
		MaxTaskID: maxTaskID,
	}
	result, err = s.store.RangeDeleteFromTieredStorageTasks(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(numTasks, int(rowsAffected))

	rows, err := s.store.RangeSelectFromTieredStorageTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	s.Equal([]sqlplugin.TieredStorageTasksRow(nil), rows)
}

func (s *historyTieredStorageTaskSuite) newRandomTieredStorageTaskRow(
	shardID int32,
	taskID int64,
) sqlplugin.TieredStorageTasksRow {
	return sqlplugin.TieredStorageTasksRow{
		ShardID:      shardID,
		TaskID:       taskID,
		Data:         shuffle.Bytes(testTieredStorageTaskData),
		DataEncoding: testTieredStorageTaskEncoding,
	}
}
