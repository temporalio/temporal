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
	testMatchingTaskQueueRangeHash = 42
	testMatchingTaskQueueEncoding  = "random encoding"
)

var (
	testMatchingTaskTaskQueueID   = []byte("random matching task queue")
	testMatchingTaskTaskQueueData = []byte("random matching task data")
)

type (
	matchingTaskQueueSuite struct {
		suite.Suite
		*require.Assertions

		store sqlplugin.MatchingTaskQueue
	}
)

// TODO SelectFromTaskQueues with RangeHashGreaterThanEqualTo / RangeHashLessThanEqualTo / TaskQueueIDGreaterThan looks weird
//  need to go over the logic in matching engine

func newMatchingTaskQueueSuite(
	t *testing.T,
	store sqlplugin.MatchingTaskQueue,
) *matchingTaskQueueSuite {
	return &matchingTaskQueueSuite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *matchingTaskQueueSuite) SetupSuite() {

}

func (s *matchingTaskQueueSuite) TearDownSuite() {

}

func (s *matchingTaskQueueSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *matchingTaskQueueSuite) TearDownTest() {

}

func (s *matchingTaskQueueSuite) TestInsert_Success() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *matchingTaskQueueSuite) TestInsert_Fail_Duplicate() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	taskQueue = s.newRandomTasksQueueRow(queueID, rangeID)
	_, err = s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue)
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *matchingTaskQueueSuite) TestInsertSelect() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
	}
	rows, err := s.store.SelectFromTaskQueues(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal([]sqlplugin.TaskQueuesRow{taskQueue}, rows)
}

func (s *matchingTaskQueueSuite) TestInsertUpdate_Success() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	rangeID++
	result, err := s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	taskQueue = s.newRandomTasksQueueRow(queueID, rangeID)
	result, err = s.store.UpdateTaskQueues(newExecutionContext(), &taskQueue)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *matchingTaskQueueSuite) TestUpdate_Fail() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.UpdateTaskQueues(newExecutionContext(), &taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))
}

func (s *matchingTaskQueueSuite) TestInsertUpdateSelect() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	rangeID++
	result, err := s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	taskQueue = s.newRandomTasksQueueRow(queueID, rangeID)
	result, err = s.store.UpdateTaskQueues(newExecutionContext(), &taskQueue)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
	}
	rows, err := s.store.SelectFromTaskQueues(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal([]sqlplugin.TaskQueuesRow{taskQueue}, rows)
}

func (s *matchingTaskQueueSuite) TestDeleteSelect() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	filter := sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
		RangeID:     convert.Int64Ptr(rangeID),
	}
	result, err := s.store.DeleteFromTaskQueues(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	filter = sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
	}
	// TODO the behavior is weird
	_, err = s.store.SelectFromTaskQueues(newExecutionContext(), filter)
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *matchingTaskQueueSuite) TestInsertDeleteSelect_Success() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
		RangeID:     convert.Int64Ptr(rangeID),
	}
	result, err = s.store.DeleteFromTaskQueues(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter = sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
	}
	rows, err := s.store.SelectFromTaskQueues(newExecutionContext(), filter)
	s.Error(err) // TODO persistence layer should do proper error translation
	s.Nil(rows)
}

func (s *matchingTaskQueueSuite) TestInsertDeleteSelect_Fail() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
		RangeID:     convert.Int64Ptr(rangeID + 1),
	}
	result, err = s.store.DeleteFromTaskQueues(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	filter = sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
	}
	rows, err := s.store.SelectFromTaskQueues(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal([]sqlplugin.TaskQueuesRow{taskQueue}, rows)
}

func (s *matchingTaskQueueSuite) TestInsertLock() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(2)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	// NOTE: lock without transaction is equivalent to select
	//  this test only test the select functionality
	filter := sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
	}
	rangeIDInDB, err := s.store.LockTaskQueues(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal(rangeID, rangeIDInDB)
}

func (s *matchingTaskQueueSuite) newRandomTasksQueueRow(
	queueID []byte,
	rangeID int64,
) sqlplugin.TaskQueuesRow {
	return sqlplugin.TaskQueuesRow{
		RangeHash:    testMatchingTaskQueueRangeHash,
		TaskQueueID:  queueID,
		RangeID:      rangeID,
		Data:         shuffle.Bytes(testMatchingTaskTaskQueueData),
		DataEncoding: testMatchingTaskQueueEncoding,
	}
}
