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

		store   sqlplugin.MatchingTaskQueue
		version sqlplugin.MatchingTaskVersion
	}
)

// TODO SelectFromTaskQueues with RangeHashGreaterThanEqualTo / RangeHashLessThanEqualTo / TaskQueueIDGreaterThan looks weird
//  need to go over the logic in matching engine

func NewMatchingTaskQueueSuite(
	t *testing.T,
	store sqlplugin.MatchingTaskQueue,
	version sqlplugin.MatchingTaskVersion,
) *matchingTaskQueueSuite {
	return &matchingTaskQueueSuite{

		store:   store,
		version: version,
	}
}

func (s *matchingTaskQueueSuite) TearDownSuite() {

}

func (s *matchingTaskQueueSuite) TearDownTest() {

}

func (s *matchingTaskQueueSuite) TestInsert_Success() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue, s.version)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))
}

func (s *matchingTaskQueueSuite) TestInsert_Fail_Duplicate() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue, s.version)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	taskQueue = s.newRandomTasksQueueRow(queueID, rangeID)
	_, err = s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue, s.version)
	require.Error(s.T(), err) // TODO persistence layer should do proper error translation
}

func (s *matchingTaskQueueSuite) TestInsertSelect() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue, s.version)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	filter := sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
	}
	rows, err := s.store.SelectFromTaskQueues(newExecutionContext(), filter, s.version)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.TaskQueuesRow{taskQueue}, rows)
}

func (s *matchingTaskQueueSuite) TestInsertUpdate_Success() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	rangeID++
	result, err := s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue, s.version)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	taskQueue = s.newRandomTasksQueueRow(queueID, rangeID)
	result, err = s.store.UpdateTaskQueues(newExecutionContext(), &taskQueue, s.version)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))
}

func (s *matchingTaskQueueSuite) TestUpdate_Fail() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.UpdateTaskQueues(newExecutionContext(), &taskQueue, s.version)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))
}

func (s *matchingTaskQueueSuite) TestInsertUpdateSelect() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	rangeID++
	result, err := s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue, s.version)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	taskQueue = s.newRandomTasksQueueRow(queueID, rangeID)
	result, err = s.store.UpdateTaskQueues(newExecutionContext(), &taskQueue, s.version)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	filter := sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
	}
	rows, err := s.store.SelectFromTaskQueues(newExecutionContext(), filter, s.version)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.TaskQueuesRow{taskQueue}, rows)
}

func (s *matchingTaskQueueSuite) TestDeleteSelect() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	filter := sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
		RangeID:     util.Ptr(rangeID),
	}
	result, err := s.store.DeleteFromTaskQueues(newExecutionContext(), filter, s.version)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	filter = sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
	}
	// TODO the behavior is weird
	_, err = s.store.SelectFromTaskQueues(newExecutionContext(), filter, s.version)
	require.Error(s.T(), err) // TODO persistence layer should do proper error translation
}

func (s *matchingTaskQueueSuite) TestInsertDeleteSelect_Success() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue, s.version)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	filter := sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
		RangeID:     util.Ptr(rangeID),
	}
	result, err = s.store.DeleteFromTaskQueues(newExecutionContext(), filter, s.version)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	filter = sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
	}
	rows, err := s.store.SelectFromTaskQueues(newExecutionContext(), filter, s.version)
	require.Error(s.T(), err) // TODO persistence layer should do proper error translation
	require.Nil(s.T(), rows)
}

func (s *matchingTaskQueueSuite) TestInsertDeleteSelect_Fail() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue, s.version)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	filter := sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
		RangeID:     util.Ptr(rangeID + 1),
	}
	result, err = s.store.DeleteFromTaskQueues(newExecutionContext(), filter, s.version)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	filter = sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
	}
	rows, err := s.store.SelectFromTaskQueues(newExecutionContext(), filter, s.version)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.TaskQueuesRow{taskQueue}, rows)
}

func (s *matchingTaskQueueSuite) TestInsertLock() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(2)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue, s.version)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	// NOTE: lock without transaction is equivalent to select
	//  this test only test the select functionality
	filter := sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
	}
	rangeIDInDB, err := s.store.LockTaskQueues(newExecutionContext(), filter, s.version)
	require.NoError(s.T(), err)
	require.Equal(s.T(), rangeID, rangeIDInDB)
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
