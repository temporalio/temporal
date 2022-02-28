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
	historyHistoryReplicationDLQTaskSuite struct {
		suite.Suite
		*require.Assertions

		store sqlplugin.HistoryReplicationDLQTask
	}
)

const (
	testHistoryReplicationTaskDLQSourceCluster = "random history replication task DLQ source cluster"

	testHistoryReplicationTaskDLQEncoding = "random encoding"
)

var (
	testHistoryReplicationTaskDLQData = []byte("random history replication task data")
)

func newHistoryReplicationDLQTaskSuite(
	t *testing.T,
	store sqlplugin.HistoryReplicationDLQTask,
) *historyHistoryReplicationDLQTaskSuite {
	return &historyHistoryReplicationDLQTaskSuite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *historyHistoryReplicationDLQTaskSuite) SetupSuite() {

}

func (s *historyHistoryReplicationDLQTaskSuite) TearDownSuite() {

}

func (s *historyHistoryReplicationDLQTaskSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *historyHistoryReplicationDLQTaskSuite) TearDownTest() {

}

func (s *historyHistoryReplicationDLQTaskSuite) TestInsert_Single_Success() {
	sourceCluster := shuffle.String(testHistoryReplicationTaskDLQSourceCluster)
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
	result, err := s.store.InsertIntoReplicationDLQTasks(newExecutionContext(), []sqlplugin.ReplicationDLQTasksRow{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *historyHistoryReplicationDLQTaskSuite) TestInsert_Multiple_Success() {
	sourceCluster := shuffle.String(testHistoryReplicationTaskDLQSourceCluster)
	shardID := rand.Int31()
	taskID := int64(1)

	task1 := s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
	taskID++
	task2 := s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
	result, err := s.store.InsertIntoReplicationDLQTasks(newExecutionContext(), []sqlplugin.ReplicationDLQTasksRow{task1, task2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))
}

func (s *historyHistoryReplicationDLQTaskSuite) TestInsert_Single_Fail_Duplicate() {
	sourceCluster := shuffle.String(testHistoryReplicationTaskDLQSourceCluster)
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
	result, err := s.store.InsertIntoReplicationDLQTasks(newExecutionContext(), []sqlplugin.ReplicationDLQTasksRow{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	task = s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
	_, err = s.store.InsertIntoReplicationDLQTasks(newExecutionContext(), []sqlplugin.ReplicationDLQTasksRow{task})
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *historyHistoryReplicationDLQTaskSuite) TestInsert_Multiple_Fail_Duplicate() {
	sourceCluster := shuffle.String(testHistoryReplicationTaskDLQSourceCluster)
	shardID := rand.Int31()
	taskID := int64(1)

	task1 := s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
	taskID++
	task2 := s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
	result, err := s.store.InsertIntoReplicationDLQTasks(newExecutionContext(), []sqlplugin.ReplicationDLQTasksRow{task1, task2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))

	task2 = s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
	taskID++
	task3 := s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
	_, err = s.store.InsertIntoReplicationDLQTasks(newExecutionContext(), []sqlplugin.ReplicationDLQTasksRow{task2, task3})
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *historyHistoryReplicationDLQTaskSuite) TestInsertSelect_Single() {
	sourceCluster := shuffle.String(testHistoryReplicationTaskDLQSourceCluster)
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
	result, err := s.store.InsertIntoReplicationDLQTasks(newExecutionContext(), []sqlplugin.ReplicationDLQTasksRow{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.ReplicationDLQTasksFilter{
		ShardID:           shardID,
		SourceClusterName: sourceCluster,
		TaskID:            taskID,
	}
	rows, err := s.store.SelectFromReplicationDLQTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
		rows[index].SourceClusterName = sourceCluster
	}
	s.Equal([]sqlplugin.ReplicationDLQTasksRow{task}, rows)
}

func (s *historyHistoryReplicationDLQTaskSuite) TestInsertSelect_Multiple() {
	numTasks := 20
	pageSize := numTasks * 2

	sourceCluster := shuffle.String(testHistoryReplicationTaskDLQSourceCluster)
	shardID := rand.Int31()
	minTaskID := int64(1)
	taskID := minTaskID
	maxTaskID := taskID + int64(numTasks)

	var tasks []sqlplugin.ReplicationDLQTasksRow
	for i := 0; i < numTasks; i++ {
		task := s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
		taskID++
		tasks = append(tasks, task)
	}
	result, err := s.store.InsertIntoReplicationDLQTasks(newExecutionContext(), tasks)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numTasks, int(rowsAffected))

	filter := sqlplugin.ReplicationDLQTasksRangeFilter{
		ShardID:            shardID,
		SourceClusterName:  sourceCluster,
		InclusiveMinTaskID: minTaskID,
		ExclusiveMaxTaskID: maxTaskID,
		PageSize:           pageSize,
	}
	rows, err := s.store.RangeSelectFromReplicationDLQTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
		rows[index].SourceClusterName = sourceCluster
	}
	s.Equal(tasks, rows)
}

func (s *historyHistoryReplicationDLQTaskSuite) TestDeleteSelect_Single() {
	sourceCluster := shuffle.String(testHistoryReplicationTaskDLQSourceCluster)
	shardID := rand.Int31()
	taskID := int64(1)

	filter := sqlplugin.ReplicationDLQTasksFilter{
		ShardID:           shardID,
		SourceClusterName: sourceCluster,
		TaskID:            taskID,
	}
	result, err := s.store.DeleteFromReplicationDLQTasks(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	rows, err := s.store.SelectFromReplicationDLQTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
		rows[index].SourceClusterName = sourceCluster
	}
	s.Equal([]sqlplugin.ReplicationDLQTasksRow(nil), rows)
}

func (s *historyHistoryReplicationDLQTaskSuite) TestDeleteSelect_Multiple() {
	sourceCluster := shuffle.String(testHistoryReplicationTaskDLQSourceCluster)
	shardID := rand.Int31()
	minTaskID := int64(1)
	maxTaskID := int64(101)

	filter := sqlplugin.ReplicationDLQTasksRangeFilter{
		ShardID:            shardID,
		SourceClusterName:  sourceCluster,
		InclusiveMinTaskID: minTaskID,
		ExclusiveMaxTaskID: maxTaskID,
		PageSize:           0,
	}
	result, err := s.store.RangeDeleteFromReplicationDLQTasks(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	rows, err := s.store.RangeSelectFromReplicationDLQTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
		rows[index].SourceClusterName = sourceCluster
	}
	s.Equal([]sqlplugin.ReplicationDLQTasksRow(nil), rows)
}

func (s *historyHistoryReplicationDLQTaskSuite) TestInsertDeleteSelect_Single() {
	sourceCluster := shuffle.String(testHistoryReplicationTaskDLQSourceCluster)
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
	result, err := s.store.InsertIntoReplicationDLQTasks(newExecutionContext(), []sqlplugin.ReplicationDLQTasksRow{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.ReplicationDLQTasksFilter{
		ShardID:           shardID,
		SourceClusterName: sourceCluster,
		TaskID:            taskID,
	}
	result, err = s.store.DeleteFromReplicationDLQTasks(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	rows, err := s.store.SelectFromReplicationDLQTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
		rows[index].SourceClusterName = sourceCluster
	}
	s.Equal([]sqlplugin.ReplicationDLQTasksRow(nil), rows)
}

func (s *historyHistoryReplicationDLQTaskSuite) TestInsertDeleteSelect_Multiple() {
	numTasks := 20
	pageSize := numTasks * 2

	sourceCluster := shuffle.String(testHistoryReplicationTaskDLQSourceCluster)
	shardID := rand.Int31()
	minTaskID := int64(1)
	taskID := minTaskID
	maxTaskID := taskID + int64(numTasks)

	var tasks []sqlplugin.ReplicationDLQTasksRow
	for i := 0; i < numTasks; i++ {
		task := s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
		taskID++
		tasks = append(tasks, task)
	}
	result, err := s.store.InsertIntoReplicationDLQTasks(newExecutionContext(), tasks)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numTasks, int(rowsAffected))

	filter := sqlplugin.ReplicationDLQTasksRangeFilter{
		ShardID:            shardID,
		SourceClusterName:  sourceCluster,
		InclusiveMinTaskID: minTaskID,
		ExclusiveMaxTaskID: maxTaskID,
		PageSize:           pageSize,
	}
	result, err = s.store.RangeDeleteFromReplicationDLQTasks(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(numTasks, int(rowsAffected))

	rows, err := s.store.RangeSelectFromReplicationDLQTasks(newExecutionContext(), filter)
	s.NoError(err)
	for index := range rows {
		rows[index].ShardID = shardID
		rows[index].SourceClusterName = sourceCluster
	}
	s.Equal([]sqlplugin.ReplicationDLQTasksRow(nil), rows)
}

func (s *historyHistoryReplicationDLQTaskSuite) newRandomReplicationTasksDLQRow(
	sourceClusterName string,
	shardID int32,
	taskID int64,
) sqlplugin.ReplicationDLQTasksRow {
	return sqlplugin.ReplicationDLQTasksRow{
		SourceClusterName: sourceClusterName,
		ShardID:           shardID,
		TaskID:            taskID,
		Data:              shuffle.Bytes(testHistoryReplicationTaskDLQData),
		DataEncoding:      testHistoryReplicationTaskDLQEncoding,
	}
}
