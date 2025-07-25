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
		*require.Assertions

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
		Assertions: require.New(t),
		store:      store,
		version:    version,
	}
}

func (s *matchingTaskQueueSuite) SetupSuite() {

}

func (s *matchingTaskQueueSuite) TearDownSuite() {

}

func (s *matchingTaskQueueSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *matchingTaskQueueSuite) TearDownTest() {

}

func (s *matchingTaskQueueSuite) TestInsert_Success() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue, s.version)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *matchingTaskQueueSuite) TestInsert_Fail_Duplicate() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue, s.version)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	taskQueue = s.newRandomTasksQueueRow(queueID, rangeID)
	_, err = s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue, s.version)
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *matchingTaskQueueSuite) TestInsertSelect() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue, s.version)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
	}
	rows, err := s.store.SelectFromTaskQueues(newExecutionContext(), filter, s.version)
	s.NoError(err)
	s.Equal([]sqlplugin.TaskQueuesRow{taskQueue}, rows)
}

func (s *matchingTaskQueueSuite) TestInsertUpdate_Success() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	rangeID++
	result, err := s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue, s.version)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	taskQueue = s.newRandomTasksQueueRow(queueID, rangeID)
	result, err = s.store.UpdateTaskQueues(newExecutionContext(), &taskQueue, s.version)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *matchingTaskQueueSuite) TestUpdate_Fail() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.UpdateTaskQueues(newExecutionContext(), &taskQueue, s.version)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))
}

func (s *matchingTaskQueueSuite) TestInsertUpdateSelect() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	rangeID++
	result, err := s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue, s.version)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	taskQueue = s.newRandomTasksQueueRow(queueID, rangeID)
	result, err = s.store.UpdateTaskQueues(newExecutionContext(), &taskQueue, s.version)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
	}
	rows, err := s.store.SelectFromTaskQueues(newExecutionContext(), filter, s.version)
	s.NoError(err)
	s.Equal([]sqlplugin.TaskQueuesRow{taskQueue}, rows)
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
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	filter = sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
	}
	// TODO the behavior is weird
	_, err = s.store.SelectFromTaskQueues(newExecutionContext(), filter, s.version)
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *matchingTaskQueueSuite) TestInsertDeleteSelect_Success() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue, s.version)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
		RangeID:     util.Ptr(rangeID),
	}
	result, err = s.store.DeleteFromTaskQueues(newExecutionContext(), filter, s.version)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter = sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
	}
	rows, err := s.store.SelectFromTaskQueues(newExecutionContext(), filter, s.version)
	s.Error(err) // TODO persistence layer should do proper error translation
	s.Nil(rows)
}

func (s *matchingTaskQueueSuite) TestInsertDeleteSelect_Fail() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue, s.version)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
		RangeID:     util.Ptr(rangeID + 1),
	}
	result, err = s.store.DeleteFromTaskQueues(newExecutionContext(), filter, s.version)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	filter = sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
	}
	rows, err := s.store.SelectFromTaskQueues(newExecutionContext(), filter, s.version)
	s.NoError(err)
	s.Equal([]sqlplugin.TaskQueuesRow{taskQueue}, rows)
}

func (s *matchingTaskQueueSuite) TestInsertLock() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(2)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueues(newExecutionContext(), &taskQueue, s.version)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	// NOTE: lock without transaction is equivalent to select
	//  this test only test the select functionality
	filter := sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
	}
	rangeIDInDB, err := s.store.LockTaskQueues(newExecutionContext(), filter, s.version)
	s.NoError(err)
	s.Equal(rangeID, rangeIDInDB)
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
