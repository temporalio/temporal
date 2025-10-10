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

		store sqlplugin.HistoryReplicationTask
	}
)

const (
	testHistoryReplicationTaskEncoding = "random encoding"
)

var (
	testHistoryReplicationTaskData = []byte("random history replication task data")
)

func NewHistoryReplicationTaskSuite(
	t *testing.T,
	store sqlplugin.HistoryReplicationTask,
) *historyHistoryReplicationTaskSuite {
	return &historyHistoryReplicationTaskSuite{

		store: store,
	}
}



func (s *historyHistoryReplicationTaskSuite) TearDownSuite() {

}



func (s *historyHistoryReplicationTaskSuite) TearDownTest() {

}

func (s *historyHistoryReplicationTaskSuite) TestInsert_Single_Success() {
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomReplicationTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoReplicationTasks(newExecutionContext(), []sqlplugin.ReplicationTasksRow{task})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))
}

func (s *historyHistoryReplicationTaskSuite) TestInsert_Multiple_Success() {
	shardID := rand.Int31()
	taskID := int64(1)

	task1 := s.newRandomReplicationTaskRow(shardID, taskID)
	taskID++
	task2 := s.newRandomReplicationTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoReplicationTasks(newExecutionContext(), []sqlplugin.ReplicationTasksRow{task1, task2})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, int(rowsAffected))
}

func (s *historyHistoryReplicationTaskSuite) TestInsert_Single_Fail_Duplicate() {
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomReplicationTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoReplicationTasks(newExecutionContext(), []sqlplugin.ReplicationTasksRow{task})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	task = s.newRandomReplicationTaskRow(shardID, taskID)
	_, err = s.store.InsertIntoReplicationTasks(newExecutionContext(), []sqlplugin.ReplicationTasksRow{task})
	require.Error(s.T(), err) // TODO persistence layer should do proper error translation
}

func (s *historyHistoryReplicationTaskSuite) TestInsert_Multiple_Fail_Duplicate() {
	shardID := rand.Int31()
	taskID := int64(1)

	task1 := s.newRandomReplicationTaskRow(shardID, taskID)
	taskID++
	task2 := s.newRandomReplicationTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoReplicationTasks(newExecutionContext(), []sqlplugin.ReplicationTasksRow{task1, task2})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, int(rowsAffected))

	task2 = s.newRandomReplicationTaskRow(shardID, taskID)
	taskID++
	task3 := s.newRandomReplicationTaskRow(shardID, taskID)
	_, err = s.store.InsertIntoReplicationTasks(newExecutionContext(), []sqlplugin.ReplicationTasksRow{task2, task3})
	require.Error(s.T(), err) // TODO persistence layer should do proper error translation
}

func (s *historyHistoryReplicationTaskSuite) TestInsertSelect_Single() {
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomReplicationTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoReplicationTasks(newExecutionContext(), []sqlplugin.ReplicationTasksRow{task})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	rangeFilter := sqlplugin.ReplicationTasksRangeFilter{
		ShardID:            shardID,
		InclusiveMinTaskID: taskID,
		ExclusiveMaxTaskID: taskID + 1,
		PageSize:           1,
	}
	rows, err := s.store.RangeSelectFromReplicationTasks(newExecutionContext(), rangeFilter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	require.Equal(s.T(), []sqlplugin.ReplicationTasksRow{task}, rows)
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
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numTasks, int(rowsAffected))

	filter := sqlplugin.ReplicationTasksRangeFilter{
		ShardID:            shardID,
		InclusiveMinTaskID: minTaskID,
		ExclusiveMaxTaskID: maxTaskID,
		PageSize:           pageSize,
	}
	rows, err := s.store.RangeSelectFromReplicationTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	require.Equal(s.T(), tasks, rows)
}

func (s *historyHistoryReplicationTaskSuite) TestDeleteSelect_Single() {
	shardID := rand.Int31()
	taskID := int64(1)

	filter := sqlplugin.ReplicationTasksFilter{
		ShardID: shardID,
		TaskID:  taskID,
	}
	result, err := s.store.DeleteFromReplicationTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	rangeFilter := sqlplugin.ReplicationTasksRangeFilter{
		ShardID:            shardID,
		InclusiveMinTaskID: taskID,
		ExclusiveMaxTaskID: taskID + 1,
		PageSize:           1,
	}
	rows, err := s.store.RangeSelectFromReplicationTasks(newExecutionContext(), rangeFilter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	require.Equal(s.T(), []sqlplugin.ReplicationTasksRow(nil), rows)
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
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	rows, err := s.store.RangeSelectFromReplicationTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	require.Equal(s.T(), []sqlplugin.ReplicationTasksRow(nil), rows)
}

func (s *historyHistoryReplicationTaskSuite) TestInsertDeleteSelect_Single() {
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomReplicationTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoReplicationTasks(newExecutionContext(), []sqlplugin.ReplicationTasksRow{task})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	filter := sqlplugin.ReplicationTasksFilter{
		ShardID: shardID,
		TaskID:  taskID,
	}
	result, err = s.store.DeleteFromReplicationTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	rangeFilter := sqlplugin.ReplicationTasksRangeFilter{
		ShardID:            shardID,
		InclusiveMinTaskID: taskID,
		ExclusiveMaxTaskID: taskID + 1,
		PageSize:           1,
	}
	rows, err := s.store.RangeSelectFromReplicationTasks(newExecutionContext(), rangeFilter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	require.Equal(s.T(), []sqlplugin.ReplicationTasksRow(nil), rows)
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
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numTasks, int(rowsAffected))

	filter := sqlplugin.ReplicationTasksRangeFilter{
		ShardID:            shardID,
		InclusiveMinTaskID: minTaskID,
		ExclusiveMaxTaskID: maxTaskID,
		PageSize:           pageSize,
	}
	result, err = s.store.RangeDeleteFromReplicationTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numTasks, int(rowsAffected))

	rows, err := s.store.RangeSelectFromReplicationTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	require.Equal(s.T(), []sqlplugin.ReplicationTasksRow(nil), rows)
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
