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
	"go.temporal.io/server/common/util"
)

type (
	historyHistoryVisibilityTaskSuite struct {
		suite.Suite
		*require.Assertions

		store sqlplugin.HistoryVisibilityTask
	}
)

const (
	testHistoryVisibilityTaskEncoding = "random encoding"
)

var (
	testHistoryVisibilityTaskData = []byte("random history visibility task data")
)

func newHistoryVisibilityTaskSuite(
	t *testing.T,
	store sqlplugin.HistoryVisibilityTask,
) *historyHistoryVisibilityTaskSuite {
	return &historyHistoryVisibilityTaskSuite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *historyHistoryVisibilityTaskSuite) SetupSuite() {

}

func (s *historyHistoryVisibilityTaskSuite) TearDownSuite() {

}

func (s *historyHistoryVisibilityTaskSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *historyHistoryVisibilityTaskSuite) TearDownTest() {

}

func (s *historyHistoryVisibilityTaskSuite) TestInsert_Single_Success() {
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomVisibilityTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoVisibilityTasks(newExecutionContext(), []sqlplugin.VisibilityTasksRow{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *historyHistoryVisibilityTaskSuite) TestInsert_Multiple_Success() {
	shardID := rand.Int31()
	taskID := int64(1)

	task1 := s.newRandomVisibilityTaskRow(shardID, taskID)
	taskID++
	task2 := s.newRandomVisibilityTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoVisibilityTasks(newExecutionContext(), []sqlplugin.VisibilityTasksRow{task1, task2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))
}

func (s *historyHistoryVisibilityTaskSuite) TestInsert_Single_Fail_Duplicate() {
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomVisibilityTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoVisibilityTasks(newExecutionContext(), []sqlplugin.VisibilityTasksRow{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	task = s.newRandomVisibilityTaskRow(shardID, taskID)
	_, err = s.store.InsertIntoVisibilityTasks(newExecutionContext(), []sqlplugin.VisibilityTasksRow{task})
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *historyHistoryVisibilityTaskSuite) TestInsert_Multiple_Fail_Duplicate() {
	shardID := rand.Int31()
	taskID := int64(1)

	task1 := s.newRandomVisibilityTaskRow(shardID, taskID)
	taskID++
	task2 := s.newRandomVisibilityTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoVisibilityTasks(newExecutionContext(), []sqlplugin.VisibilityTasksRow{task1, task2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))

	task2 = s.newRandomVisibilityTaskRow(shardID, taskID)
	taskID++
	task3 := s.newRandomVisibilityTaskRow(shardID, taskID)
	_, err = s.store.InsertIntoVisibilityTasks(newExecutionContext(), []sqlplugin.VisibilityTasksRow{task2, task3})
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *historyHistoryVisibilityTaskSuite) TestInsertSelect_Single() {
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomVisibilityTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoVisibilityTasks(newExecutionContext(), []sqlplugin.VisibilityTasksRow{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.VisibilityTasksFilter{
		ShardID: shardID,
		TaskID:  taskID,
	}
	rows, err := s.store.SelectFromVisibilityTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	s.Equal([]sqlplugin.VisibilityTasksRow{task}, rows)
}

func (s *historyHistoryVisibilityTaskSuite) TestInsertSelect_Multiple() {
	numTasks := 20

	shardID := rand.Int31()
	minTaskID := int64(1)
	taskID := minTaskID
	maxTaskID := taskID + int64(numTasks)

	var tasks []sqlplugin.VisibilityTasksRow
	for i := 0; i < numTasks; i++ {
		task := s.newRandomVisibilityTaskRow(shardID, taskID)
		taskID++
		tasks = append(tasks, task)
	}
	result, err := s.store.InsertIntoVisibilityTasks(newExecutionContext(), tasks)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numTasks, int(rowsAffected))

	for _, pageSize := range []int{numTasks / 2, numTasks * 2} {
		filter := sqlplugin.VisibilityTasksRangeFilter{
			ShardID:            shardID,
			InclusiveMinTaskID: minTaskID,
			ExclusiveMaxTaskID: maxTaskID,
			PageSize:           pageSize,
		}
		rows, err := s.store.RangeSelectFromVisibilityTasks(newExecutionContext(), filter)
		s.NoError(err)
		s.NotEmpty(rows)
		s.True(len(rows) <= filter.PageSize)
		for index := range rows {
			rows[index].ShardID = shardID
		}
		s.Equal(tasks[:util.Min(numTasks, pageSize)], rows)
	}
}

func (s *historyHistoryVisibilityTaskSuite) TestDeleteSelect_Single() {
	shardID := rand.Int31()
	taskID := int64(1)

	filter := sqlplugin.VisibilityTasksFilter{
		ShardID: shardID,
		TaskID:  taskID,
	}
	result, err := s.store.DeleteFromVisibilityTasks(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	rows, err := s.store.SelectFromVisibilityTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	s.Equal([]sqlplugin.VisibilityTasksRow(nil), rows)
}

func (s *historyHistoryVisibilityTaskSuite) TestDeleteSelect_Multiple() {
	shardID := rand.Int31()
	minTaskID := int64(1)
	maxTaskID := int64(101)

	filter := sqlplugin.VisibilityTasksRangeFilter{
		ShardID:            shardID,
		InclusiveMinTaskID: minTaskID,
		ExclusiveMaxTaskID: maxTaskID,
		PageSize:           int(maxTaskID - minTaskID),
	}
	result, err := s.store.RangeDeleteFromVisibilityTasks(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	rows, err := s.store.RangeSelectFromVisibilityTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	s.Equal([]sqlplugin.VisibilityTasksRow(nil), rows)
}

func (s *historyHistoryVisibilityTaskSuite) TestInsertDeleteSelect_Single() {
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomVisibilityTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoVisibilityTasks(newExecutionContext(), []sqlplugin.VisibilityTasksRow{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.VisibilityTasksFilter{
		ShardID: shardID,
		TaskID:  taskID,
	}
	result, err = s.store.DeleteFromVisibilityTasks(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	rows, err := s.store.SelectFromVisibilityTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	s.Equal([]sqlplugin.VisibilityTasksRow(nil), rows)
}

func (s *historyHistoryVisibilityTaskSuite) TestInsertDeleteSelect_Multiple() {
	numTasks := 20

	shardID := rand.Int31()
	minTaskID := int64(1)
	taskID := minTaskID
	maxTaskID := taskID + int64(numTasks)

	var tasks []sqlplugin.VisibilityTasksRow
	for i := 0; i < numTasks; i++ {
		task := s.newRandomVisibilityTaskRow(shardID, taskID)
		taskID++
		tasks = append(tasks, task)
	}
	result, err := s.store.InsertIntoVisibilityTasks(newExecutionContext(), tasks)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numTasks, int(rowsAffected))

	filter := sqlplugin.VisibilityTasksRangeFilter{
		ShardID:            shardID,
		InclusiveMinTaskID: minTaskID,
		ExclusiveMaxTaskID: maxTaskID,
		PageSize:           int(maxTaskID - minTaskID),
	}
	result, err = s.store.RangeDeleteFromVisibilityTasks(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(numTasks, int(rowsAffected))

	rows, err := s.store.RangeSelectFromVisibilityTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	s.Equal([]sqlplugin.VisibilityTasksRow(nil), rows)
}

func (s *historyHistoryVisibilityTaskSuite) newRandomVisibilityTaskRow(
	shardID int32,
	taskID int64,
) sqlplugin.VisibilityTasksRow {
	return sqlplugin.VisibilityTasksRow{
		ShardID:      shardID,
		TaskID:       taskID,
		Data:         shuffle.Bytes(testHistoryVisibilityTaskData),
		DataEncoding: testHistoryVisibilityTaskEncoding,
	}
}
