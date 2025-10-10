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
	historyHistoryTransferTaskSuite struct {
		suite.Suite

		store sqlplugin.HistoryTransferTask
	}
)

const (
	testHistoryTransferTaskEncoding = "random encoding"
)

var (
	testHistoryTransferTaskData = []byte("random history transfer task data")
)

func NewHistoryTransferTaskSuite(
	t *testing.T,
	store sqlplugin.HistoryTransferTask,
) *historyHistoryTransferTaskSuite {
	return &historyHistoryTransferTaskSuite{

		store: store,
	}
}



func (s *historyHistoryTransferTaskSuite) TearDownSuite() {

}



func (s *historyHistoryTransferTaskSuite) TearDownTest() {

}

func (s *historyHistoryTransferTaskSuite) TestInsert_Single_Success() {
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomTransferTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoTransferTasks(newExecutionContext(), []sqlplugin.TransferTasksRow{task})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))
}

func (s *historyHistoryTransferTaskSuite) TestInsert_Multiple_Success() {
	shardID := rand.Int31()
	taskID := int64(1)

	task1 := s.newRandomTransferTaskRow(shardID, taskID)
	taskID++
	task2 := s.newRandomTransferTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoTransferTasks(newExecutionContext(), []sqlplugin.TransferTasksRow{task1, task2})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, int(rowsAffected))
}

func (s *historyHistoryTransferTaskSuite) TestInsert_Single_Fail_Duplicate() {
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomTransferTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoTransferTasks(newExecutionContext(), []sqlplugin.TransferTasksRow{task})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	task = s.newRandomTransferTaskRow(shardID, taskID)
	_, err = s.store.InsertIntoTransferTasks(newExecutionContext(), []sqlplugin.TransferTasksRow{task})
	require.Error(s.T(), err) // TODO persistence layer should do proper error translation
}

func (s *historyHistoryTransferTaskSuite) TestInsert_Multiple_Fail_Duplicate() {
	shardID := rand.Int31()
	taskID := int64(1)

	task1 := s.newRandomTransferTaskRow(shardID, taskID)
	taskID++
	task2 := s.newRandomTransferTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoTransferTasks(newExecutionContext(), []sqlplugin.TransferTasksRow{task1, task2})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, int(rowsAffected))

	task2 = s.newRandomTransferTaskRow(shardID, taskID)
	taskID++
	task3 := s.newRandomTransferTaskRow(shardID, taskID)
	_, err = s.store.InsertIntoTransferTasks(newExecutionContext(), []sqlplugin.TransferTasksRow{task2, task3})
	require.Error(s.T(), err) // TODO persistence layer should do proper error translation
}

func (s *historyHistoryTransferTaskSuite) TestInsertSelect_Single() {
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomTransferTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoTransferTasks(newExecutionContext(), []sqlplugin.TransferTasksRow{task})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	rangeFilter := sqlplugin.TransferTasksRangeFilter{
		ShardID:            shardID,
		InclusiveMinTaskID: taskID,
		ExclusiveMaxTaskID: taskID + 1,
		PageSize:           1,
	}
	rows, err := s.store.RangeSelectFromTransferTasks(newExecutionContext(), rangeFilter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	require.Equal(s.T(), []sqlplugin.TransferTasksRow{task}, rows)
}

func (s *historyHistoryTransferTaskSuite) TestInsertSelect_Multiple() {
	numTasks := 20

	shardID := rand.Int31()
	minTaskID := int64(1)
	taskID := minTaskID
	maxTaskID := taskID + int64(numTasks)

	var tasks []sqlplugin.TransferTasksRow
	for i := 0; i < numTasks; i++ {
		task := s.newRandomTransferTaskRow(shardID, taskID)
		taskID++
		tasks = append(tasks, task)
	}
	result, err := s.store.InsertIntoTransferTasks(newExecutionContext(), tasks)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numTasks, int(rowsAffected))

	for _, pageSize := range []int{numTasks / 2, numTasks * 2} {
		filter := sqlplugin.TransferTasksRangeFilter{
			ShardID:            shardID,
			InclusiveMinTaskID: minTaskID,
			ExclusiveMaxTaskID: maxTaskID,
			PageSize:           pageSize,
		}
		rows, err := s.store.RangeSelectFromTransferTasks(newExecutionContext(), filter)
		require.NoError(s.T(), err)
		require.NotEmpty(s.T(), rows)
		require.True(s.T(), len(rows) <= filter.PageSize)
		for index := range rows {
			rows[index].ShardID = shardID
		}
		require.Equal(s.T(), tasks[:min(numTasks, pageSize)], rows)
	}
}

func (s *historyHistoryTransferTaskSuite) TestDeleteSelect_Single() {
	shardID := rand.Int31()
	taskID := int64(1)

	filter := sqlplugin.TransferTasksFilter{
		ShardID: shardID,
		TaskID:  taskID,
	}
	result, err := s.store.DeleteFromTransferTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	rangeFilter := sqlplugin.TransferTasksRangeFilter{
		ShardID:            shardID,
		InclusiveMinTaskID: taskID,
		ExclusiveMaxTaskID: taskID + 1,
		PageSize:           1,
	}
	rows, err := s.store.RangeSelectFromTransferTasks(newExecutionContext(), rangeFilter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	require.Equal(s.T(), []sqlplugin.TransferTasksRow(nil), rows)
}

func (s *historyHistoryTransferTaskSuite) TestDeleteSelect_Multiple() {
	shardID := rand.Int31()
	minTaskID := int64(1)
	maxTaskID := int64(101)

	filter := sqlplugin.TransferTasksRangeFilter{
		ShardID:            shardID,
		InclusiveMinTaskID: minTaskID,
		ExclusiveMaxTaskID: maxTaskID,
		PageSize:           int(maxTaskID - minTaskID),
	}
	result, err := s.store.RangeDeleteFromTransferTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	rows, err := s.store.RangeSelectFromTransferTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	require.Equal(s.T(), []sqlplugin.TransferTasksRow(nil), rows)
}

func (s *historyHistoryTransferTaskSuite) TestInsertDeleteSelect_Single() {
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomTransferTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoTransferTasks(newExecutionContext(), []sqlplugin.TransferTasksRow{task})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	filter := sqlplugin.TransferTasksFilter{
		ShardID: shardID,
		TaskID:  taskID,
	}
	result, err = s.store.DeleteFromTransferTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	rangeFilter := sqlplugin.TransferTasksRangeFilter{
		ShardID:            shardID,
		InclusiveMinTaskID: taskID,
		ExclusiveMaxTaskID: taskID + 1,
		PageSize:           1,
	}
	rows, err := s.store.RangeSelectFromTransferTasks(newExecutionContext(), rangeFilter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	require.Equal(s.T(), []sqlplugin.TransferTasksRow(nil), rows)
}

func (s *historyHistoryTransferTaskSuite) TestInsertDeleteSelect_Multiple() {
	numTasks := 20

	shardID := rand.Int31()
	minTaskID := int64(1)
	taskID := minTaskID
	maxTaskID := taskID + int64(numTasks)

	var tasks []sqlplugin.TransferTasksRow
	for i := 0; i < numTasks; i++ {
		task := s.newRandomTransferTaskRow(shardID, taskID)
		taskID++
		tasks = append(tasks, task)
	}
	result, err := s.store.InsertIntoTransferTasks(newExecutionContext(), tasks)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numTasks, int(rowsAffected))

	filter := sqlplugin.TransferTasksRangeFilter{
		ShardID:            shardID,
		InclusiveMinTaskID: minTaskID,
		ExclusiveMaxTaskID: maxTaskID,
		PageSize:           int(maxTaskID - minTaskID),
	}
	result, err = s.store.RangeDeleteFromTransferTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numTasks, int(rowsAffected))

	rows, err := s.store.RangeSelectFromTransferTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	require.Equal(s.T(), []sqlplugin.TransferTasksRow(nil), rows)
}

func (s *historyHistoryTransferTaskSuite) newRandomTransferTaskRow(
	shardID int32,
	taskID int64,
) sqlplugin.TransferTasksRow {
	return sqlplugin.TransferTasksRow{
		ShardID:      shardID,
		TaskID:       taskID,
		Data:         shuffle.Bytes(testHistoryTransferTaskData),
		DataEncoding: testHistoryTransferTaskEncoding,
	}
}
