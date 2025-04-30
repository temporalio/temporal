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

func NewHistoryReplicationTaskSuite(
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

	rangeFilter := sqlplugin.ReplicationTasksRangeFilter{
		ShardID:            shardID,
		InclusiveMinTaskID: taskID,
		ExclusiveMaxTaskID: taskID + 1,
		PageSize:           1,
	}
	rows, err := s.store.RangeSelectFromReplicationTasks(newExecutionContext(), rangeFilter)
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

	rangeFilter := sqlplugin.ReplicationTasksRangeFilter{
		ShardID:            shardID,
		InclusiveMinTaskID: taskID,
		ExclusiveMaxTaskID: taskID + 1,
		PageSize:           1,
	}
	rows, err := s.store.RangeSelectFromReplicationTasks(newExecutionContext(), rangeFilter)
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

	rangeFilter := sqlplugin.ReplicationTasksRangeFilter{
		ShardID:            shardID,
		InclusiveMinTaskID: taskID,
		ExclusiveMaxTaskID: taskID + 1,
		PageSize:           1,
	}
	rows, err := s.store.RangeSelectFromReplicationTasks(newExecutionContext(), rangeFilter)
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
