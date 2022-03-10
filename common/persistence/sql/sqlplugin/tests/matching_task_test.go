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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/shuffle"
)

const (
	testMatchingTaskRangeHash = 42
	testMatchingTaskEncoding  = "random encoding"
)

var (
	testMatchingTaskTaskData = []byte("random matching task data")
)

type (
	matchingTaskSuite struct {
		suite.Suite
		*require.Assertions

		store sqlplugin.MatchingTask
	}
)

func newMatchingTaskSuite(
	t *testing.T,
	store sqlplugin.MatchingTask,
) *matchingTaskSuite {
	return &matchingTaskSuite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *matchingTaskSuite) SetupSuite() {

}

func (s *matchingTaskSuite) TearDownSuite() {

}

func (s *matchingTaskSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *matchingTaskSuite) TearDownTest() {

}

func (s *matchingTaskSuite) TestInsert_Single_Success() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	taskID := int64(1)

	task := s.newRandomTasksRow(queueID, taskID)
	result, err := s.store.InsertIntoTasks(newExecutionContext(), []sqlplugin.TasksRow{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *matchingTaskSuite) TestInsert_Multiple_Success() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	taskID := int64(1)

	task1 := s.newRandomTasksRow(queueID, taskID)
	taskID++
	task2 := s.newRandomTasksRow(queueID, taskID)
	taskID++
	result, err := s.store.InsertIntoTasks(newExecutionContext(), []sqlplugin.TasksRow{task1, task2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))
}

func (s *matchingTaskSuite) TestInsert_Single_Fail_Duplicate() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	taskID := int64(1)

	task := s.newRandomTasksRow(queueID, taskID)
	result, err := s.store.InsertIntoTasks(newExecutionContext(), []sqlplugin.TasksRow{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	task = s.newRandomTasksRow(queueID, taskID)
	_, err = s.store.InsertIntoTasks(newExecutionContext(), []sqlplugin.TasksRow{task})
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *matchingTaskSuite) TestInsert_Multiple_Fail_Duplicate() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	taskID := int64(1)

	task1 := s.newRandomTasksRow(queueID, taskID)
	taskID++
	task2 := s.newRandomTasksRow(queueID, taskID)
	result, err := s.store.InsertIntoTasks(newExecutionContext(), []sqlplugin.TasksRow{task1, task2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))

	task2 = s.newRandomTasksRow(queueID, taskID)
	taskID++
	task3 := s.newRandomTasksRow(queueID, taskID)
	_, err = s.store.InsertIntoTasks(newExecutionContext(), []sqlplugin.TasksRow{task2, task3})
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *matchingTaskSuite) TestInsertSelect_Single() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	taskID := int64(100)

	task := s.newRandomTasksRow(queueID, taskID)
	result, err := s.store.InsertIntoTasks(newExecutionContext(), []sqlplugin.TasksRow{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	inclusiveMinTaskID := convert.Int64Ptr(taskID)
	exclusiveMaxTaskID := convert.Int64Ptr(taskID + 1)
	pageSize := convert.IntPtr(1)
	filter := sqlplugin.TasksFilter{
		RangeHash:          testMatchingTaskRangeHash,
		TaskQueueID:        queueID,
		InclusiveMinTaskID: inclusiveMinTaskID,
		ExclusiveMaxTaskID: exclusiveMaxTaskID,
		PageSize:           pageSize,
	}
	rows, err := s.store.SelectFromTasks(newExecutionContext(), filter)
	s.NoError(err)
	// fill in some omitted info
	for index := range rows {
		rows[index].RangeHash = testMatchingTaskRangeHash
		rows[index].TaskQueueID = queueID
	}
	s.Equal([]sqlplugin.TasksRow{task}, rows)
}

func (s *matchingTaskSuite) TestInsertSelect_Multiple() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	taskID := int64(100)

	task1 := s.newRandomTasksRow(queueID, taskID)
	taskID++
	task2 := s.newRandomTasksRow(queueID, taskID)
	result, err := s.store.InsertIntoTasks(newExecutionContext(), []sqlplugin.TasksRow{task1, task2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))

	inclusiveMinTaskID := convert.Int64Ptr(taskID - 1)
	exclusiveMaxTaskID := convert.Int64Ptr(taskID + 1)
	pageSize := convert.IntPtr(2)
	filter := sqlplugin.TasksFilter{
		RangeHash:          testMatchingTaskRangeHash,
		TaskQueueID:        queueID,
		InclusiveMinTaskID: inclusiveMinTaskID,
		ExclusiveMaxTaskID: exclusiveMaxTaskID,
		PageSize:           pageSize,
	}
	rows, err := s.store.SelectFromTasks(newExecutionContext(), filter)
	s.NoError(err)
	// fill in some omitted info
	for index := range rows {
		rows[index].RangeHash = testMatchingTaskRangeHash
		rows[index].TaskQueueID = queueID
	}
	s.Equal([]sqlplugin.TasksRow{task1, task2}, rows)
}

func (s *matchingTaskSuite) TestDeleteSelect() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	taskID := int64(100)

	filter := sqlplugin.TasksFilter{
		RangeHash:   testMatchingTaskRangeHash,
		TaskQueueID: queueID,
		TaskID:      convert.Int64Ptr(taskID),
	}
	result, err := s.store.DeleteFromTasks(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))
}

func (s *matchingTaskSuite) TestInsertDeleteSelect_Single() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	taskID := int64(100)

	task := s.newRandomTasksRow(queueID, taskID)
	result, err := s.store.InsertIntoTasks(newExecutionContext(), []sqlplugin.TasksRow{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.TasksFilter{
		RangeHash:   testMatchingTaskRangeHash,
		TaskQueueID: queueID,
		TaskID:      convert.Int64Ptr(taskID),
	}
	result, err = s.store.DeleteFromTasks(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	inclusiveMinTaskID := convert.Int64Ptr(taskID)
	exclusiveMaxTaskID := convert.Int64Ptr(taskID + 1)
	pageSize := convert.IntPtr(1)
	filter = sqlplugin.TasksFilter{
		RangeHash:          testMatchingTaskRangeHash,
		TaskQueueID:        queueID,
		InclusiveMinTaskID: inclusiveMinTaskID,
		ExclusiveMaxTaskID: exclusiveMaxTaskID,
		PageSize:           pageSize,
	}
	rows, err := s.store.SelectFromTasks(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal([]sqlplugin.TasksRow(nil), rows)
}

func (s *matchingTaskSuite) TestInsertDeleteSelect_Multiple() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	taskID := int64(100)

	task1 := s.newRandomTasksRow(queueID, taskID)
	taskID++
	task2 := s.newRandomTasksRow(queueID, taskID)
	result, err := s.store.InsertIntoTasks(newExecutionContext(), []sqlplugin.TasksRow{task1, task2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))

	filter := sqlplugin.TasksFilter{
		RangeHash:          testMatchingTaskRangeHash,
		TaskQueueID:        queueID,
		ExclusiveMaxTaskID: convert.Int64Ptr(taskID + 1),
		Limit:              convert.IntPtr(2),
	}
	result, err = s.store.DeleteFromTasks(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))

	inclusiveMinTaskID := convert.Int64Ptr(taskID - 1)
	exclusiveMaxTaskID := convert.Int64Ptr(taskID + 1)
	pageSize := convert.IntPtr(2)
	filter = sqlplugin.TasksFilter{
		RangeHash:          testMatchingTaskRangeHash,
		TaskQueueID:        queueID,
		InclusiveMinTaskID: inclusiveMinTaskID,
		ExclusiveMaxTaskID: exclusiveMaxTaskID,
		PageSize:           pageSize,
	}
	rows, err := s.store.SelectFromTasks(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal([]sqlplugin.TasksRow(nil), rows)
}

func (s *matchingTaskSuite) newRandomTasksRow(
	queueID []byte,
	taskID int64,
) sqlplugin.TasksRow {
	return sqlplugin.TasksRow{
		RangeHash:    testMatchingTaskRangeHash,
		TaskQueueID:  queueID,
		TaskID:       taskID,
		Data:         shuffle.Bytes(testMatchingTaskTaskData),
		DataEncoding: testMatchingTaskEncoding,
	}
}
