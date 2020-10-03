package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/shuffle"
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

		store sqlplugin.MatchingTaskQueue
	}
)

// TODO SelectFromTaskQueues with RangeHashGreaterThanEqualTo / RangeHashLessThanEqualTo / TaskQueueIDGreaterThan looks weird
//  need to go over the logic in matching engine

func newMatchingTaskQueueSuite(
	t *testing.T,
	store sqlplugin.MatchingTaskQueue,
) *matchingTaskQueueSuite {
	return &matchingTaskQueueSuite{
		Assertions: require.New(t),
		store:      store,
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
	result, err := s.store.InsertIntoTaskQueues(&taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *matchingTaskQueueSuite) TestInsert_Fail() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueues(&taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	taskQueue = s.newRandomTasksQueueRow(queueID, rangeID)
	_, err = s.store.InsertIntoTaskQueues(&taskQueue)
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *matchingTaskQueueSuite) TestInsertSelect() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueues(&taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := &sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
	}
	rows, err := s.store.SelectFromTaskQueues(filter)
	s.NoError(err)
	s.Equal([]sqlplugin.TaskQueuesRow{taskQueue}, rows)
}

func (s *matchingTaskQueueSuite) TestReplace_Exists() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	rangeID++
	result, err := s.store.InsertIntoTaskQueues(&taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	taskQueue = s.newRandomTasksQueueRow(queueID, rangeID)
	result, err = s.store.ReplaceIntoTaskQueues(&taskQueue)
	s.NoError(err)
	// NOTE: cannot do assertion on affected rows
	//  PostgreSQL will return 1
	//  MySQL will return 2: ref https://dev.mysql.com/doc/c-api/5.7/en/mysql-affected-rows.html
}

func (s *matchingTaskQueueSuite) TestReplace_NonExists() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.ReplaceIntoTaskQueues(&taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *matchingTaskQueueSuite) TestReplaceSelect() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.ReplaceIntoTaskQueues(&taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := &sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
	}
	rows, err := s.store.SelectFromTaskQueues(filter)
	s.NoError(err)
	s.Equal([]sqlplugin.TaskQueuesRow{taskQueue}, rows)
}

func (s *matchingTaskQueueSuite) TestUpdate_Success() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	rangeID++
	result, err := s.store.InsertIntoTaskQueues(&taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	taskQueue = s.newRandomTasksQueueRow(queueID, rangeID)
	result, err = s.store.UpdateTaskQueues(&taskQueue)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.Equal(1, int(rowsAffected))
}

func (s *matchingTaskQueueSuite) TestUpdate_Fail() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	_, err := s.store.UpdateTaskQueues(&taskQueue)
	s.NoError(err)
}

func (s *matchingTaskQueueSuite) TestUpdateSelect() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	rangeID++
	result, err := s.store.InsertIntoTaskQueues(&taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	taskQueue = s.newRandomTasksQueueRow(queueID, rangeID)
	result, err = s.store.UpdateTaskQueues(&taskQueue)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.Equal(1, int(rowsAffected))

	filter := &sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
	}
	rows, err := s.store.SelectFromTaskQueues(filter)
	s.NoError(err)
	s.Equal([]sqlplugin.TaskQueuesRow{taskQueue}, rows)
}

func (s *matchingTaskQueueSuite) TestInsertDeleteSelect_Success() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueues(&taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := &sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
		RangeID:     convert.Int64Ptr(rangeID),
	}
	result, err = s.store.DeleteFromTaskQueues(filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.Equal(1, int(rowsAffected))

	filter = &sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
	}
	rows, err := s.store.SelectFromTaskQueues(filter)
	s.Error(err) // TODO persistence layer should do proper error translation
	s.Nil(rows)
}

func (s *matchingTaskQueueSuite) TestInsertDeleteSelect_Fail() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(1)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueues(&taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := &sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
		RangeID:     convert.Int64Ptr(rangeID + 1),
	}
	result, err = s.store.DeleteFromTaskQueues(filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.Equal(0, int(rowsAffected))

	filter = &sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
	}
	rows, err := s.store.SelectFromTaskQueues(filter)
	s.NoError(err)
	s.Equal([]sqlplugin.TaskQueuesRow{taskQueue}, rows)
}

func (s *matchingTaskQueueSuite) TestInsertLock() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)
	rangeID := int64(2)

	taskQueue := s.newRandomTasksQueueRow(queueID, rangeID)
	result, err := s.store.InsertIntoTaskQueues(&taskQueue)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := &sqlplugin.TaskQueuesFilter{
		RangeHash:   testMatchingTaskQueueRangeHash,
		TaskQueueID: queueID,
	}
	// NOTE: lock without transaction is equivalent to select
	//  this test only test the select functionality
	rangeIDInDB, err := s.store.LockTaskQueues(filter)
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
		Data:         testMatchingTaskTaskQueueData,
		DataEncoding: testMatchingTaskQueueEncoding,
	}
}
