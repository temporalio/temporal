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
	historyHistoryVisibilityTaskSuite struct {
		suite.Suite

		store sqlplugin.HistoryVisibilityTask
	}
)

const (
	testHistoryVisibilityTaskEncoding = "random encoding"
)

var (
	testHistoryVisibilityTaskData = []byte("random history visibility task data")
)

func NewHistoryVisibilityTaskSuite(
	t *testing.T,
	store sqlplugin.HistoryVisibilityTask,
) *historyHistoryVisibilityTaskSuite {
	return &historyHistoryVisibilityTaskSuite{

		store: store,
	}
}



func (s *historyHistoryVisibilityTaskSuite) TearDownSuite() {

}



func (s *historyHistoryVisibilityTaskSuite) TearDownTest() {

}

func (s *historyHistoryVisibilityTaskSuite) TestInsert_Single_Success() {
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomVisibilityTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoVisibilityTasks(newExecutionContext(), []sqlplugin.VisibilityTasksRow{task})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))
}

func (s *historyHistoryVisibilityTaskSuite) TestInsert_Multiple_Success() {
	shardID := rand.Int31()
	taskID := int64(1)

	task1 := s.newRandomVisibilityTaskRow(shardID, taskID)
	taskID++
	task2 := s.newRandomVisibilityTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoVisibilityTasks(newExecutionContext(), []sqlplugin.VisibilityTasksRow{task1, task2})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, int(rowsAffected))
}

func (s *historyHistoryVisibilityTaskSuite) TestInsert_Single_Fail_Duplicate() {
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomVisibilityTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoVisibilityTasks(newExecutionContext(), []sqlplugin.VisibilityTasksRow{task})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	task = s.newRandomVisibilityTaskRow(shardID, taskID)
	_, err = s.store.InsertIntoVisibilityTasks(newExecutionContext(), []sqlplugin.VisibilityTasksRow{task})
	require.Error(s.T(), err) // TODO persistence layer should do proper error translation
}

func (s *historyHistoryVisibilityTaskSuite) TestInsert_Multiple_Fail_Duplicate() {
	shardID := rand.Int31()
	taskID := int64(1)

	task1 := s.newRandomVisibilityTaskRow(shardID, taskID)
	taskID++
	task2 := s.newRandomVisibilityTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoVisibilityTasks(newExecutionContext(), []sqlplugin.VisibilityTasksRow{task1, task2})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, int(rowsAffected))

	task2 = s.newRandomVisibilityTaskRow(shardID, taskID)
	taskID++
	task3 := s.newRandomVisibilityTaskRow(shardID, taskID)
	_, err = s.store.InsertIntoVisibilityTasks(newExecutionContext(), []sqlplugin.VisibilityTasksRow{task2, task3})
	require.Error(s.T(), err) // TODO persistence layer should do proper error translation
}

func (s *historyHistoryVisibilityTaskSuite) TestInsertSelect_Single() {
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomVisibilityTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoVisibilityTasks(newExecutionContext(), []sqlplugin.VisibilityTasksRow{task})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	rangeFilter := sqlplugin.VisibilityTasksRangeFilter{
		ShardID:            shardID,
		InclusiveMinTaskID: taskID,
		ExclusiveMaxTaskID: taskID + 1,
		PageSize:           1,
	}
	rows, err := s.store.RangeSelectFromVisibilityTasks(newExecutionContext(), rangeFilter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	require.Equal(s.T(), []sqlplugin.VisibilityTasksRow{task}, rows)
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
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numTasks, int(rowsAffected))

	for _, pageSize := range []int{numTasks / 2, numTasks * 2} {
		filter := sqlplugin.VisibilityTasksRangeFilter{
			ShardID:            shardID,
			InclusiveMinTaskID: minTaskID,
			ExclusiveMaxTaskID: maxTaskID,
			PageSize:           pageSize,
		}
		rows, err := s.store.RangeSelectFromVisibilityTasks(newExecutionContext(), filter)
		require.NoError(s.T(), err)
		require.NotEmpty(s.T(), rows)
		require.True(s.T(), len(rows) <= filter.PageSize)
		for index := range rows {
			rows[index].ShardID = shardID
		}
		require.Equal(s.T(), tasks[:min(numTasks, pageSize)], rows)
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
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	rangeFilter := sqlplugin.VisibilityTasksRangeFilter{
		ShardID:            shardID,
		InclusiveMinTaskID: taskID,
		ExclusiveMaxTaskID: taskID + 1,
		PageSize:           1,
	}
	rows, err := s.store.RangeSelectFromVisibilityTasks(newExecutionContext(), rangeFilter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	require.Equal(s.T(), []sqlplugin.VisibilityTasksRow(nil), rows)
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
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	rows, err := s.store.RangeSelectFromVisibilityTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	require.Equal(s.T(), []sqlplugin.VisibilityTasksRow(nil), rows)
}

func (s *historyHistoryVisibilityTaskSuite) TestInsertDeleteSelect_Single() {
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomVisibilityTaskRow(shardID, taskID)
	result, err := s.store.InsertIntoVisibilityTasks(newExecutionContext(), []sqlplugin.VisibilityTasksRow{task})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	filter := sqlplugin.VisibilityTasksFilter{
		ShardID: shardID,
		TaskID:  taskID,
	}
	result, err = s.store.DeleteFromVisibilityTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	rangeFilter := sqlplugin.VisibilityTasksRangeFilter{
		ShardID:            shardID,
		InclusiveMinTaskID: taskID,
		ExclusiveMaxTaskID: taskID + 1,
		PageSize:           1,
	}
	rows, err := s.store.RangeSelectFromVisibilityTasks(newExecutionContext(), rangeFilter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	require.Equal(s.T(), []sqlplugin.VisibilityTasksRow(nil), rows)
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
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numTasks, int(rowsAffected))

	filter := sqlplugin.VisibilityTasksRangeFilter{
		ShardID:            shardID,
		InclusiveMinTaskID: minTaskID,
		ExclusiveMaxTaskID: maxTaskID,
		PageSize:           int(maxTaskID - minTaskID),
	}
	result, err = s.store.RangeDeleteFromVisibilityTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numTasks, int(rowsAffected))

	rows, err := s.store.RangeSelectFromVisibilityTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	require.Equal(s.T(), []sqlplugin.VisibilityTasksRow(nil), rows)
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
