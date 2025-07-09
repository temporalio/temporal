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
	testMatchingTaskQueueRangeHashV2 = 42
	testMatchingTaskQueueEncodingV2  = "random encoding"
)

var (
	testMatchingTaskTaskQueueIDV2   = []byte("random matching task queue")
	testMatchingTaskTaskQueueDataV2 = []byte("random matching task data")
)

type (
	matchingTaskQueueSuiteV2 struct {
		suite.Suite
		*require.Assertions

		store sqlplugin.MatchingTaskQueueV2
	}
)

// TODO SelectFromTaskQueues with RangeHashGreaterThanEqualTo / RangeHashLessThanEqualTo / TaskQueueIDGreaterThan looks weird
//  need to go over the logic in matching engine

func NewMatchingTaskQueueSuiteV2(
	t *testing.T,
	store sqlplugin.MatchingTaskQueue,
) *matchingTaskQueueSuite {
	return &matchingTaskQueueSuite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *matchingTaskQueueSuiteV2) SetupSuiteV2() {

}

func (s *matchingTaskQueueSuiteV2) TearDownSuiteV2() {

}

func (s *matchingTaskQueueSuiteV2) SetupTestV2() {
	s.Assertions = require.New(s.T())
}

func (s *matchingTaskQueueSuiteV2) TearDownTestV2() {

}

func (s *matchingTaskQueueSuiteV2) TestInsert_SuccessV2() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueIDV2)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRowV2(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueuesV2(newExecutionContext(), &taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *matchingTaskQueueSuiteV2) TestInsert_Fail_DuplicateV2() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueIDV2)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRowV2(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueuesV2(newExecutionContext(), &taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	taskQueue = s.newRandomTasksQueueRowV2(queueID, rangeID)
	_, err = s.store.InsertIntoTaskQueuesV2(newExecutionContext(), &taskQueue)
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *matchingTaskQueueSuiteV2) TestInsertSelectV2() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueIDV2)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRowV2(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueuesV2(newExecutionContext(), &taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.TaskQueuesFilterV2{
		RangeHash:   testMatchingTaskQueueRangeHashV2,
		TaskQueueID: queueID,
	}
	rows, err := s.store.SelectFromTaskQueuesV2(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal([]sqlplugin.TaskQueuesRowV2{taskQueue}, rows)
}

func (s *matchingTaskQueueSuiteV2) TestInsertUpdate_SuccessV2() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueIDV2)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRowV2(queueID, rangeID)
	rangeID++
	result, err := s.store.InsertIntoTaskQueuesV2(newExecutionContext(), &taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	taskQueue = s.newRandomTasksQueueRowV2(queueID, rangeID)
	result, err = s.store.UpdateTaskQueuesV2(newExecutionContext(), &taskQueue)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *matchingTaskQueueSuiteV2) TestUpdate_FailV2() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueIDV2)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRowV2(queueID, rangeID)
	result, err := s.store.UpdateTaskQueuesV2(newExecutionContext(), &taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))
}

func (s *matchingTaskQueueSuiteV2) TestInsertUpdateSelectV2() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueIDV2)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRowV2(queueID, rangeID)
	rangeID++
	result, err := s.store.InsertIntoTaskQueuesV2(newExecutionContext(), &taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	taskQueue = s.newRandomTasksQueueRowV2(queueID, rangeID)
	result, err = s.store.UpdateTaskQueuesV2(newExecutionContext(), &taskQueue)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.TaskQueuesFilterV2{
		RangeHash:   testMatchingTaskQueueRangeHashV2,
		TaskQueueID: queueID,
	}
	rows, err := s.store.SelectFromTaskQueuesV2(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal([]sqlplugin.TaskQueuesRowV2{taskQueue}, rows)
}

func (s *matchingTaskQueueSuiteV2) TestDeleteSelectV2() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueIDV2)
	rangeID := int64(1)

	filter := sqlplugin.TaskQueuesFilterV2{
		RangeHash:   testMatchingTaskQueueRangeHashV2,
		TaskQueueID: queueID,
		RangeID:     util.Ptr(rangeID),
	}
	result, err := s.store.DeleteFromTaskQueuesV2(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	filter = sqlplugin.TaskQueuesFilterV2{
		RangeHash:   testMatchingTaskQueueRangeHashV2,
		TaskQueueID: queueID,
	}
	// TODO the behavior is weird
	_, err = s.store.SelectFromTaskQueuesV2(newExecutionContext(), filter)
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *matchingTaskQueueSuiteV2) TestInsertDeleteSelect_SuccessV2() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueIDV2)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRowV2(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueuesV2(newExecutionContext(), &taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.TaskQueuesFilterV2{
		RangeHash:   testMatchingTaskQueueRangeHashV2,
		TaskQueueID: queueID,
		RangeID:     util.Ptr(rangeID),
	}
	result, err = s.store.DeleteFromTaskQueuesV2(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter = sqlplugin.TaskQueuesFilterV2{
		RangeHash:   testMatchingTaskQueueRangeHashV2,
		TaskQueueID: queueID,
	}
	rows, err := s.store.SelectFromTaskQueuesV2(newExecutionContext(), filter)
	s.Error(err) // TODO persistence layer should do proper error translation
	s.Nil(rows)
}

func (s *matchingTaskQueueSuiteV2) TestInsertDeleteSelect_FailV2() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueIDV2)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRowV2(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueuesV2(newExecutionContext(), &taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.TaskQueuesFilterV2{
		RangeHash:   testMatchingTaskQueueRangeHashV2,
		TaskQueueID: queueID,
		RangeID:     util.Ptr(rangeID + 1),
	}
	result, err = s.store.DeleteFromTaskQueuesV2(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	filter = sqlplugin.TaskQueuesFilterV2{
		RangeHash:   testMatchingTaskQueueRangeHashV2,
		TaskQueueID: queueID,
	}
	rows, err := s.store.SelectFromTaskQueuesV2(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal([]sqlplugin.TaskQueuesRowV2{taskQueue}, rows)
}

func (s *matchingTaskQueueSuiteV2) TestInsertLockV2() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueIDV2)
	rangeID := int64(2)

	taskQueue := s.newRandomTasksQueueRowV2(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueuesV2(newExecutionContext(), &taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	// NOTE: lock without transaction is equivalent to select
	//  this test only test the select functionality
	filter := sqlplugin.TaskQueuesFilterV2{
		RangeHash:   testMatchingTaskQueueRangeHashV2,
		TaskQueueID: queueID,
	}
	rangeIDInDB, err := s.store.LockTaskQueuesV2(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal(rangeID, rangeIDInDB)
}

func (s *matchingTaskQueueSuiteV2) newRandomTasksQueueRowV2(
	queueID []byte,
	rangeID int64,
) sqlplugin.TaskQueuesRowV2 {
	return sqlplugin.TaskQueuesRowV2{
		RangeHash:    testMatchingTaskQueueRangeHashV2,
		TaskQueueID:  queueID,
		RangeID:      rangeID,
		Data:         shuffle.Bytes(testMatchingTaskTaskQueueDataV2),
		DataEncoding: testMatchingTaskQueueEncodingV2,
	}
}
