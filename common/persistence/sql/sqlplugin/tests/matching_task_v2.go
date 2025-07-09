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
	testMatchingTaskRangeHashV2 = 42
	testMatchingTaskEncodingV2  = "random encoding"
)

var (
	testMatchingTaskTaskDataV2 = []byte("random matching task data")
)

type (
	matchingTaskSuiteV2 struct {
		suite.Suite
		*require.Assertions

		store sqlplugin.MatchingTaskV2
	}
)

func NewMatchingTaskSuiteV2(
	t *testing.T,
	store sqlplugin.MatchingTaskV2,
) *matchingTaskSuiteV2 {
	return &matchingTaskSuiteV2{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *matchingTaskSuiteV2) SetupSuiteV2() {

}

func (s *matchingTaskSuiteV2) TearDownSuiteV2() {

}

func (s *matchingTaskSuiteV2) SetupTestV2() {
	s.Assertions = require.New(s.T())
}

func (s *matchingTaskSuiteV2) TearDownTestV2() {

}

func (s *matchingTaskSuiteV2) TestInsert_Single_SuccessV2() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	taskID := int64(1)

	task := s.newRandomTasksRowV2(queueID, taskID)
	result, err := s.store.InsertIntoTasksV2(newExecutionContext(), []sqlplugin.TasksRowV2{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *matchingTaskSuiteV2) TestInsert_Multiple_Successv2() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	taskID := int64(1)

	task1 := s.newRandomTasksRowV2(queueID, taskID)
	taskID++
	task2 := s.newRandomTasksRowV2(queueID, taskID)
	taskID++
	result, err := s.store.InsertIntoTasksV2(newExecutionContext(), []sqlplugin.TasksRowV2{task1, task2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))
}

func (s *matchingTaskSuiteV2) TestInsert_Single_Fail_DuplicateV2() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	taskID := int64(1)

	task := s.newRandomTasksRowV2(queueID, taskID)
	result, err := s.store.InsertIntoTasksV2(newExecutionContext(), []sqlplugin.TasksRowV2{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	task = s.newRandomTasksRowV2(queueID, taskID)
	_, err = s.store.InsertIntoTasksV2(newExecutionContext(), []sqlplugin.TasksRowV2{task})
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *matchingTaskSuiteV2) TestInsert_Multiple_Fail_DuplicateV2() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	taskID := int64(1)

	task1 := s.newRandomTasksRowV2(queueID, taskID)
	taskID++
	task2 := s.newRandomTasksRowV2(queueID, taskID)
	result, err := s.store.InsertIntoTasksV2(newExecutionContext(), []sqlplugin.TasksRowV2{task1, task2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))

	task2 = s.newRandomTasksRowV2(queueID, taskID)
	taskID++
	task3 := s.newRandomTasksRowV2(queueID, taskID)
	_, err = s.store.InsertIntoTasksV2(newExecutionContext(), []sqlplugin.TasksRowV2{task2, task3})
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *matchingTaskSuiteV2) TestInsertSelect_SingleV2() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	taskID := int64(100)

	task := s.newRandomTasksRowV2(queueID, taskID)
	result, err := s.store.InsertIntoTasksV2(newExecutionContext(), []sqlplugin.TasksRowV2{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	inclusiveMinTaskID := util.Ptr(taskID)
	exclusiveMaxTaskID := util.Ptr(taskID + 1)
	pageSize := util.Ptr(1)
	filter := sqlplugin.TasksFilterV2{
		RangeHash:          testMatchingTaskRangeHashV2,
		TaskQueueID:        queueID,
		Pass:               0, // pass for tasks (see stride scheduling algorithm for fairness)
		InclusiveMinTaskID: inclusiveMinTaskID,
		ExclusiveMaxTaskID: exclusiveMaxTaskID,
		PageSize:           pageSize,
	}
	rows, err := s.store.SelectFromTasksV2(newExecutionContext(), filter)
	s.NoError(err)
	// fill in some omitted info
	for index := range rows {
		rows[index].RangeHash = testMatchingTaskRangeHashV2
		rows[index].TaskQueueID = queueID
	}
	s.Equal([]sqlplugin.TasksRowV2{task}, rows)
}

func (s *matchingTaskSuiteV2) TestInsertSelect_MultipleV2() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	taskID := int64(100)

	task1 := s.newRandomTasksRowV2(queueID, taskID)
	taskID++
	task2 := s.newRandomTasksRowV2(queueID, taskID)
	result, err := s.store.InsertIntoTasksV2(newExecutionContext(), []sqlplugin.TasksRowV2{task1, task2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))

	inclusiveMinTaskID := util.Ptr(taskID - 1)
	exclusiveMaxTaskID := util.Ptr(taskID + 1)
	pageSize := util.Ptr(2)
	filter := sqlplugin.TasksFilterV2{
		RangeHash:          testMatchingTaskRangeHashV2,
		TaskQueueID:        queueID,
		Pass:               0, // pass for tasks (see stride scheduling algorithm for fairness)
		InclusiveMinTaskID: inclusiveMinTaskID,
		ExclusiveMaxTaskID: exclusiveMaxTaskID,
		PageSize:           pageSize,
	}
	rows, err := s.store.SelectFromTasksV2(newExecutionContext(), filter)
	s.NoError(err)
	// fill in some omitted info
	for index := range rows {
		rows[index].RangeHash = testMatchingTaskRangeHashV2
		rows[index].TaskQueueID = queueID
	}
	s.Equal([]sqlplugin.TasksRowV2{task1, task2}, rows)
}

func (s *matchingTaskSuiteV2) TestDeleteSingle_FailV2() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)

	filter := sqlplugin.TasksFilterV2{
		RangeHash:   testMatchingTaskRangeHashV2,
		TaskQueueID: queueID,
	}
	_, err := s.store.DeleteFromTasksV2(newExecutionContext(), filter)
	s.Error(err)
}

func (s *matchingTaskSuiteV2) TestInsertDeleteSingle_FailV2() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	taskID := int64(100)

	task := s.newRandomTasksRowV2(queueID, taskID)
	result, err := s.store.InsertIntoTasksV2(newExecutionContext(), []sqlplugin.TasksRowV2{task})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.TasksFilterV2{
		RangeHash:   testMatchingTaskRangeHashV2,
		TaskQueueID: queueID,
	}
	_, err = s.store.DeleteFromTasksV2(newExecutionContext(), filter)
	s.Error(err)
}

func (s *matchingTaskSuiteV2) TestInsertDeleteSelect_MultipleV2() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	taskID := int64(100)

	task1 := s.newRandomTasksRowV2(queueID, taskID)
	taskID++
	task2 := s.newRandomTasksRowV2(queueID, taskID)
	result, err := s.store.InsertIntoTasksV2(newExecutionContext(), []sqlplugin.TasksRowV2{task1, task2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))

	filter := sqlplugin.TasksFilterV2{
		RangeHash:          testMatchingTaskRangeHashV2,
		TaskQueueID:        queueID,
		ExclusiveMaxTaskID: util.Ptr(taskID + 1),
		Limit:              util.Ptr(2),
	}
	result, err = s.store.DeleteFromTasksV2(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))

	inclusiveMinTaskID := util.Ptr(taskID - 1)
	exclusiveMaxTaskID := util.Ptr(taskID + 1)
	pageSize := util.Ptr(2)
	filter = sqlplugin.TasksFilterV2{
		RangeHash:          testMatchingTaskRangeHashV2,
		TaskQueueID:        queueID,
		InclusiveMinTaskID: inclusiveMinTaskID,
		ExclusiveMaxTaskID: exclusiveMaxTaskID,
		PageSize:           pageSize,
	}
	rows, err := s.store.SelectFromTasksV2(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal([]sqlplugin.TasksRowV2(nil), rows)
}

func (s *matchingTaskSuiteV2) newRandomTasksRowV2(
	queueID []byte,
	taskID int64,
) sqlplugin.TasksRowV2 {
	return sqlplugin.TasksRowV2{
		RangeHash:    testMatchingTaskRangeHashV2,
		TaskQueueID:  queueID,
		TaskID:       taskID,
		Pass:         0,
		Data:         shuffle.Bytes(testMatchingTaskTaskDataV2),
		DataEncoding: testMatchingTaskEncodingV2,
	}
}
