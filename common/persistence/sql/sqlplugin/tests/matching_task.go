package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/shuffle"
	"go.temporal.io/server/common/util"
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

func NewMatchingTaskSuite(
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

	inclusiveMinTaskID := util.Ptr(taskID)
	exclusiveMaxTaskID := util.Ptr(taskID + 1)
	pageSize := util.Ptr(1)
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

	inclusiveMinTaskID := util.Ptr(taskID - 1)
	exclusiveMaxTaskID := util.Ptr(taskID + 1)
	pageSize := util.Ptr(2)
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

func (s *matchingTaskSuite) TestDeleteSingle_Fail() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)

	filter := sqlplugin.TasksFilter{
		RangeHash:   testMatchingTaskRangeHash,
		TaskQueueID: queueID,
	}
	_, err := s.store.DeleteFromTasks(newExecutionContext(), filter)
	s.Error(err)
}

func (s *matchingTaskSuite) TestInsertDeleteSingle_Fail() {
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
	}
	result, err = s.store.DeleteFromTasks(newExecutionContext(), filter)
	s.Error(err)
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
		ExclusiveMaxTaskID: util.Ptr(taskID + 1),
		Limit:              util.Ptr(2),
	}
	result, err = s.store.DeleteFromTasks(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))

	inclusiveMinTaskID := util.Ptr(taskID - 1)
	exclusiveMaxTaskID := util.Ptr(taskID + 1)
	pageSize := util.Ptr(2)
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
