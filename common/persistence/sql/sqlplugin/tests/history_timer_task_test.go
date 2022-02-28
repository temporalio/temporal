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
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/shuffle"
)

type (
	historyHistoryTimerTaskSuite struct {
		suite.Suite
		*require.Assertions

		store sqlplugin.HistoryTimerTask
	}
)

const (
	testHistoryTimerTaskEncoding = "random encoding"
)

var (
	testHistoryTimerTaskData = []byte("random history timer task data")
)

func newHistoryTimerTaskSuite(
	t *testing.T,
	store sqlplugin.HistoryTimerTask,
) *historyHistoryTimerTaskSuite {
	return &historyHistoryTimerTaskSuite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *historyHistoryTimerTaskSuite) SetupSuite() {

}

func (s *historyHistoryTimerTaskSuite) TearDownSuite() {

}

func (s *historyHistoryTimerTaskSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *historyHistoryTimerTaskSuite) TearDownTest() {

}

func (s *historyHistoryTimerTaskSuite) TestInsert_Single_Success() {
	shardID := rand.Int31()
	timestamp := s.now()
	taskID := int64(1)

	task := s.newRandomTimerTaskRow(shardID, timestamp, taskID)
	result, err := s.store.InsertIntoTimerTasks(newExecutionContext(), []sqlplugin.TimerTasksRow{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *historyHistoryTimerTaskSuite) TestInsert_Multiple_Success() {
	shardID := rand.Int31()
	timestamp := s.now()
	taskID := int64(1)

	task1 := s.newRandomTimerTaskRow(shardID, timestamp, taskID)
	timestamp = timestamp.Add(time.Millisecond)
	taskID++
	task2 := s.newRandomTimerTaskRow(shardID, timestamp, taskID)
	result, err := s.store.InsertIntoTimerTasks(newExecutionContext(), []sqlplugin.TimerTasksRow{task1, task2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))
}

func (s *historyHistoryTimerTaskSuite) TestInsert_Single_Fail_Duplicate() {
	shardID := rand.Int31()
	timestamp := s.now()
	taskID := int64(1)

	task := s.newRandomTimerTaskRow(shardID, timestamp, taskID)
	result, err := s.store.InsertIntoTimerTasks(newExecutionContext(), []sqlplugin.TimerTasksRow{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	task = s.newRandomTimerTaskRow(shardID, timestamp, taskID)
	_, err = s.store.InsertIntoTimerTasks(newExecutionContext(), []sqlplugin.TimerTasksRow{task})
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *historyHistoryTimerTaskSuite) TestInsert_Multiple_Fail_Duplicate() {
	shardID := rand.Int31()
	timestamp := s.now()
	taskID := int64(1)

	task1 := s.newRandomTimerTaskRow(shardID, timestamp, taskID)
	timestamp = timestamp.Add(time.Millisecond)
	taskID++
	task2 := s.newRandomTimerTaskRow(shardID, timestamp, taskID)
	result, err := s.store.InsertIntoTimerTasks(newExecutionContext(), []sqlplugin.TimerTasksRow{task1, task2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))

	task2 = s.newRandomTimerTaskRow(shardID, timestamp, taskID)
	timestamp = timestamp.Add(time.Millisecond)
	taskID++
	task3 := s.newRandomTimerTaskRow(shardID, timestamp, taskID)
	_, err = s.store.InsertIntoTimerTasks(newExecutionContext(), []sqlplugin.TimerTasksRow{task2, task3})
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *historyHistoryTimerTaskSuite) TestInsertSelect_Single() {
	shardID := rand.Int31()
	timestamp := s.now()
	taskID := int64(1)

	task := s.newRandomTimerTaskRow(shardID, timestamp, taskID)
	result, err := s.store.InsertIntoTimerTasks(newExecutionContext(), []sqlplugin.TimerTasksRow{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.TimerTasksFilter{
		ShardID:             shardID,
		VisibilityTimestamp: timestamp,
		TaskID:              taskID,
	}
	rows, err := s.store.SelectFromTimerTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	s.Equal([]sqlplugin.TimerTasksRow{task}, rows)
}

func (s *historyHistoryTimerTaskSuite) TestInsertSelect_Multiple() {
	numTasks := 20

	shardID := rand.Int31()
	timestamp := s.now()
	minTimestamp := timestamp
	taskID := int64(1)
	maxTimestamp := timestamp.Add(time.Duration(numTasks) * time.Millisecond)

	var tasks []sqlplugin.TimerTasksRow
	for i := 0; i < numTasks; i++ {
		task := s.newRandomTimerTaskRow(shardID, timestamp, taskID)
		timestamp = timestamp.Add(time.Millisecond)
		taskID++
		tasks = append(tasks, task)
	}
	result, err := s.store.InsertIntoTimerTasks(newExecutionContext(), tasks)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numTasks, int(rowsAffected))

	filter := sqlplugin.TimerTasksRangeFilter{
		ShardID:                         shardID,
		InclusiveMinVisibilityTimestamp: minTimestamp,
		ExclusiveMaxVisibilityTimestamp: maxTimestamp,
		PageSize:                        numTasks,
	}
	rows, err := s.store.RangeSelectFromTimerTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	s.Equal(tasks, rows)
}

func (s *historyHistoryTimerTaskSuite) TestDeleteSelect_Single() {
	shardID := rand.Int31()
	timestamp := s.now()
	taskID := int64(1)

	filter := sqlplugin.TimerTasksFilter{
		ShardID:             shardID,
		VisibilityTimestamp: timestamp,
		TaskID:              taskID,
	}
	result, err := s.store.DeleteFromTimerTasks(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	rows, err := s.store.SelectFromTimerTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	s.Equal([]sqlplugin.TimerTasksRow(nil), rows)
}

func (s *historyHistoryTimerTaskSuite) TestDeleteSelect_Multiple() {
	pageSize := 100

	shardID := rand.Int31()
	minTimestamp := s.now()
	maxTimestamp := minTimestamp.Add(time.Minute)

	filter := sqlplugin.TimerTasksRangeFilter{
		ShardID:                         shardID,
		InclusiveMinVisibilityTimestamp: minTimestamp,
		ExclusiveMaxVisibilityTimestamp: maxTimestamp,
		PageSize:                        0,
	}
	result, err := s.store.RangeDeleteFromTimerTasks(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	filter.PageSize = pageSize
	rows, err := s.store.RangeSelectFromTimerTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	s.Equal([]sqlplugin.TimerTasksRow(nil), rows)
}

func (s *historyHistoryTimerTaskSuite) TestInsertDeleteSelect_Single() {
	shardID := rand.Int31()
	timestamp := s.now()
	taskID := int64(1)

	task := s.newRandomTimerTaskRow(shardID, timestamp, taskID)
	result, err := s.store.InsertIntoTimerTasks(newExecutionContext(), []sqlplugin.TimerTasksRow{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.TimerTasksFilter{
		ShardID:             shardID,
		VisibilityTimestamp: timestamp,
		TaskID:              taskID,
	}
	result, err = s.store.DeleteFromTimerTasks(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	rows, err := s.store.SelectFromTimerTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	s.Equal([]sqlplugin.TimerTasksRow(nil), rows)
}

func (s *historyHistoryTimerTaskSuite) TestInsertDeleteSelect_Multiple() {
	numTasks := 20
	pageSize := numTasks

	shardID := rand.Int31()
	timestamp := s.now()
	minTimestamp := timestamp
	taskID := int64(1)
	maxTimestamp := timestamp.Add(time.Duration(numTasks) * time.Millisecond)

	var tasks []sqlplugin.TimerTasksRow
	for i := 0; i < numTasks; i++ {
		task := s.newRandomTimerTaskRow(shardID, timestamp, taskID)
		timestamp = timestamp.Add(time.Millisecond)
		taskID++
		tasks = append(tasks, task)
	}
	result, err := s.store.InsertIntoTimerTasks(newExecutionContext(), tasks)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numTasks, int(rowsAffected))

	filter := sqlplugin.TimerTasksRangeFilter{
		ShardID:                         shardID,
		InclusiveMinVisibilityTimestamp: minTimestamp,
		ExclusiveMaxVisibilityTimestamp: maxTimestamp,
		PageSize:                        0,
	}
	result, err = s.store.RangeDeleteFromTimerTasks(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(numTasks, int(rowsAffected))

	filter.PageSize = pageSize
	rows, err := s.store.RangeSelectFromTimerTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	s.Equal([]sqlplugin.TimerTasksRow(nil), rows)
}

func (s *historyHistoryTimerTaskSuite) now() time.Time {
	return time.Now().UTC().Truncate(time.Millisecond)
}

func (s *historyHistoryTimerTaskSuite) newRandomTimerTaskRow(
	shardID int32,
	timestamp time.Time,
	taskID int64,
) sqlplugin.TimerTasksRow {
	return sqlplugin.TimerTasksRow{
		ShardID:             shardID,
		VisibilityTimestamp: timestamp,
		TaskID:              taskID,
		Data:                shuffle.Bytes(testHistoryTimerTaskData),
		DataEncoding:        testHistoryTimerTaskEncoding,
	}
}
