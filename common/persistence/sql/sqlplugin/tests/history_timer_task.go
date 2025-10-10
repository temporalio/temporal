package tests

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/shuffle"
)

type (
	historyHistoryTimerTaskSuite struct {
		suite.Suite

		store sqlplugin.HistoryTimerTask
	}
)

const (
	testHistoryTimerTaskEncoding = "random encoding"
)

var (
	testHistoryTimerTaskData = []byte("random history timer task data")
)

func NewHistoryTimerTaskSuite(
	t *testing.T,
	store sqlplugin.HistoryTimerTask,
) *historyHistoryTimerTaskSuite {
	return &historyHistoryTimerTaskSuite{

		store: store,
	}
}



func (s *historyHistoryTimerTaskSuite) TearDownSuite() {

}



func (s *historyHistoryTimerTaskSuite) TearDownTest() {

}

func (s *historyHistoryTimerTaskSuite) TestInsert_Single_Success() {
	shardID := rand.Int31()
	timestamp := s.now()
	taskID := int64(1)

	task := s.newRandomTimerTaskRow(shardID, timestamp, taskID)
	result, err := s.store.InsertIntoTimerTasks(newExecutionContext(), []sqlplugin.TimerTasksRow{task})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))
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
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, int(rowsAffected))
}

func (s *historyHistoryTimerTaskSuite) TestInsert_Single_Fail_Duplicate() {
	shardID := rand.Int31()
	timestamp := s.now()
	taskID := int64(1)

	task := s.newRandomTimerTaskRow(shardID, timestamp, taskID)
	result, err := s.store.InsertIntoTimerTasks(newExecutionContext(), []sqlplugin.TimerTasksRow{task})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	task = s.newRandomTimerTaskRow(shardID, timestamp, taskID)
	_, err = s.store.InsertIntoTimerTasks(newExecutionContext(), []sqlplugin.TimerTasksRow{task})
	require.Error(s.T(), err) // TODO persistence layer should do proper error translation
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
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, int(rowsAffected))

	task2 = s.newRandomTimerTaskRow(shardID, timestamp, taskID)
	timestamp = timestamp.Add(time.Millisecond)
	taskID++
	task3 := s.newRandomTimerTaskRow(shardID, timestamp, taskID)
	_, err = s.store.InsertIntoTimerTasks(newExecutionContext(), []sqlplugin.TimerTasksRow{task2, task3})
	require.Error(s.T(), err) // TODO persistence layer should do proper error translation
}

func (s *historyHistoryTimerTaskSuite) TestInsertSelect_Single() {
	shardID := rand.Int31()
	timestamp := s.now()
	taskID := int64(1)

	task := s.newRandomTimerTaskRow(shardID, timestamp, taskID)
	result, err := s.store.InsertIntoTimerTasks(newExecutionContext(), []sqlplugin.TimerTasksRow{task})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	rangeFilter := sqlplugin.TimerTasksRangeFilter{
		ShardID:                         shardID,
		InclusiveMinTaskID:              taskID,
		InclusiveMinVisibilityTimestamp: timestamp,
		ExclusiveMaxVisibilityTimestamp: timestamp.Add(persistence.ScheduledTaskMinPrecision),
		PageSize:                        1,
	}
	rows, err := s.store.RangeSelectFromTimerTasks(newExecutionContext(), rangeFilter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	require.Equal(s.T(), []sqlplugin.TimerTasksRow{task}, rows)
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
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numTasks, int(rowsAffected))

	filter := sqlplugin.TimerTasksRangeFilter{
		ShardID:                         shardID,
		InclusiveMinVisibilityTimestamp: minTimestamp,
		ExclusiveMaxVisibilityTimestamp: maxTimestamp,
		PageSize:                        numTasks,
	}
	rows, err := s.store.RangeSelectFromTimerTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	require.Equal(s.T(), tasks, rows)
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
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	rangeFilter := sqlplugin.TimerTasksRangeFilter{
		ShardID:                         shardID,
		InclusiveMinTaskID:              taskID,
		InclusiveMinVisibilityTimestamp: timestamp,
		ExclusiveMaxVisibilityTimestamp: timestamp.Add(persistence.ScheduledTaskMinPrecision),
		PageSize:                        1,
	}
	rows, err := s.store.RangeSelectFromTimerTasks(newExecutionContext(), rangeFilter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	require.Equal(s.T(), []sqlplugin.TimerTasksRow(nil), rows)
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
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	filter.PageSize = pageSize
	rows, err := s.store.RangeSelectFromTimerTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	require.Equal(s.T(), []sqlplugin.TimerTasksRow(nil), rows)
}

func (s *historyHistoryTimerTaskSuite) TestInsertDeleteSelect_Single() {
	shardID := rand.Int31()
	timestamp := s.now()
	taskID := int64(1)

	task := s.newRandomTimerTaskRow(shardID, timestamp, taskID)
	result, err := s.store.InsertIntoTimerTasks(newExecutionContext(), []sqlplugin.TimerTasksRow{task})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	filter := sqlplugin.TimerTasksFilter{
		ShardID:             shardID,
		VisibilityTimestamp: timestamp,
		TaskID:              taskID,
	}
	result, err = s.store.DeleteFromTimerTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	rangeFilter := sqlplugin.TimerTasksRangeFilter{
		ShardID:                         shardID,
		InclusiveMinTaskID:              taskID,
		InclusiveMinVisibilityTimestamp: timestamp,
		ExclusiveMaxVisibilityTimestamp: timestamp.Add(persistence.ScheduledTaskMinPrecision),
		PageSize:                        1,
	}
	rows, err := s.store.RangeSelectFromTimerTasks(newExecutionContext(), rangeFilter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	require.Equal(s.T(), []sqlplugin.TimerTasksRow(nil), rows)
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
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numTasks, int(rowsAffected))

	filter := sqlplugin.TimerTasksRangeFilter{
		ShardID:                         shardID,
		InclusiveMinVisibilityTimestamp: minTimestamp,
		ExclusiveMaxVisibilityTimestamp: maxTimestamp,
		PageSize:                        0,
	}
	result, err = s.store.RangeDeleteFromTimerTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numTasks, int(rowsAffected))

	filter.PageSize = pageSize
	rows, err := s.store.RangeSelectFromTimerTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
	}
	require.Equal(s.T(), []sqlplugin.TimerTasksRow(nil), rows)
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
