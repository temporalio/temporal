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
	historyHistoryReplicationDLQTaskSuite struct {
		suite.Suite

		store sqlplugin.HistoryReplicationDLQTask
	}
)

const (
	testHistoryReplicationTaskDLQSourceCluster = "random history replication task DLQ source cluster"

	testHistoryReplicationTaskDLQEncoding = "random encoding"
)

var (
	testHistoryReplicationTaskDLQData = []byte("random history replication task data")
)

func NewHistoryReplicationDLQTaskSuite(
	t *testing.T,
	store sqlplugin.HistoryReplicationDLQTask,
) *historyHistoryReplicationDLQTaskSuite {
	return &historyHistoryReplicationDLQTaskSuite{

		store: store,
	}
}

func (s *historyHistoryReplicationDLQTaskSuite) TearDownSuite() {

}

func (s *historyHistoryReplicationDLQTaskSuite) TearDownTest() {

}

func (s *historyHistoryReplicationDLQTaskSuite) TestInsert_Single_Success() {
	sourceCluster := shuffle.String(testHistoryReplicationTaskDLQSourceCluster)
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
	result, err := s.store.InsertIntoReplicationDLQTasks(newExecutionContext(), []sqlplugin.ReplicationDLQTasksRow{task})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))
}

func (s *historyHistoryReplicationDLQTaskSuite) TestInsert_Multiple_Success() {
	sourceCluster := shuffle.String(testHistoryReplicationTaskDLQSourceCluster)
	shardID := rand.Int31()
	taskID := int64(1)

	task1 := s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
	taskID++
	task2 := s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
	result, err := s.store.InsertIntoReplicationDLQTasks(newExecutionContext(), []sqlplugin.ReplicationDLQTasksRow{task1, task2})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, int(rowsAffected))
}

func (s *historyHistoryReplicationDLQTaskSuite) TestInsert_Single_Fail_Duplicate() {
	sourceCluster := shuffle.String(testHistoryReplicationTaskDLQSourceCluster)
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
	result, err := s.store.InsertIntoReplicationDLQTasks(newExecutionContext(), []sqlplugin.ReplicationDLQTasksRow{task})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	task = s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
	_, err = s.store.InsertIntoReplicationDLQTasks(newExecutionContext(), []sqlplugin.ReplicationDLQTasksRow{task})
	require.Error(s.T(), err) // TODO persistence layer should do proper error translation
}

func (s *historyHistoryReplicationDLQTaskSuite) TestInsert_Multiple_Fail_Duplicate() {
	sourceCluster := shuffle.String(testHistoryReplicationTaskDLQSourceCluster)
	shardID := rand.Int31()
	taskID := int64(1)

	task1 := s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
	taskID++
	task2 := s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
	result, err := s.store.InsertIntoReplicationDLQTasks(newExecutionContext(), []sqlplugin.ReplicationDLQTasksRow{task1, task2})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, int(rowsAffected))

	task2 = s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
	taskID++
	task3 := s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
	_, err = s.store.InsertIntoReplicationDLQTasks(newExecutionContext(), []sqlplugin.ReplicationDLQTasksRow{task2, task3})
	require.Error(s.T(), err) // TODO persistence layer should do proper error translation
}

func (s *historyHistoryReplicationDLQTaskSuite) TestInsertSelect_Single() {
	sourceCluster := shuffle.String(testHistoryReplicationTaskDLQSourceCluster)
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
	result, err := s.store.InsertIntoReplicationDLQTasks(newExecutionContext(), []sqlplugin.ReplicationDLQTasksRow{task})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	rangeFilter := sqlplugin.ReplicationDLQTasksRangeFilter{
		ShardID:            shardID,
		SourceClusterName:  sourceCluster,
		InclusiveMinTaskID: taskID,
		ExclusiveMaxTaskID: taskID + 1,
		PageSize:           1,
	}
	rows, err := s.store.RangeSelectFromReplicationDLQTasks(newExecutionContext(), rangeFilter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
		rows[index].SourceClusterName = sourceCluster
	}
	require.Equal(s.T(), []sqlplugin.ReplicationDLQTasksRow{task}, rows)
}

func (s *historyHistoryReplicationDLQTaskSuite) TestInsertSelect_Multiple() {
	numTasks := 20
	pageSize := numTasks * 2

	sourceCluster := shuffle.String(testHistoryReplicationTaskDLQSourceCluster)
	shardID := rand.Int31()
	minTaskID := int64(1)
	taskID := minTaskID
	maxTaskID := taskID + int64(numTasks)

	var tasks []sqlplugin.ReplicationDLQTasksRow
	for i := 0; i < numTasks; i++ {
		task := s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
		taskID++
		tasks = append(tasks, task)
	}
	result, err := s.store.InsertIntoReplicationDLQTasks(newExecutionContext(), tasks)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numTasks, int(rowsAffected))

	filter := sqlplugin.ReplicationDLQTasksRangeFilter{
		ShardID:            shardID,
		SourceClusterName:  sourceCluster,
		InclusiveMinTaskID: minTaskID,
		ExclusiveMaxTaskID: maxTaskID,
		PageSize:           pageSize,
	}
	rows, err := s.store.RangeSelectFromReplicationDLQTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
		rows[index].SourceClusterName = sourceCluster
	}
	require.Equal(s.T(), tasks, rows)
}

func (s *historyHistoryReplicationDLQTaskSuite) TestDeleteSelect_Single() {
	sourceCluster := shuffle.String(testHistoryReplicationTaskDLQSourceCluster)
	shardID := rand.Int31()
	taskID := int64(1)

	filter := sqlplugin.ReplicationDLQTasksFilter{
		ShardID:           shardID,
		SourceClusterName: sourceCluster,
		TaskID:            taskID,
	}
	result, err := s.store.DeleteFromReplicationDLQTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	rangeFilter := sqlplugin.ReplicationDLQTasksRangeFilter{
		ShardID:            shardID,
		SourceClusterName:  sourceCluster,
		InclusiveMinTaskID: taskID,
		ExclusiveMaxTaskID: taskID + 1,
		PageSize:           1,
	}
	rows, err := s.store.RangeSelectFromReplicationDLQTasks(newExecutionContext(), rangeFilter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
		rows[index].SourceClusterName = sourceCluster
	}
	require.Equal(s.T(), []sqlplugin.ReplicationDLQTasksRow(nil), rows)
}

func (s *historyHistoryReplicationDLQTaskSuite) TestDeleteSelect_Multiple() {
	sourceCluster := shuffle.String(testHistoryReplicationTaskDLQSourceCluster)
	shardID := rand.Int31()
	minTaskID := int64(1)
	maxTaskID := int64(101)

	filter := sqlplugin.ReplicationDLQTasksRangeFilter{
		ShardID:            shardID,
		SourceClusterName:  sourceCluster,
		InclusiveMinTaskID: minTaskID,
		ExclusiveMaxTaskID: maxTaskID,
		PageSize:           0,
	}
	result, err := s.store.RangeDeleteFromReplicationDLQTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	rows, err := s.store.RangeSelectFromReplicationDLQTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
		rows[index].SourceClusterName = sourceCluster
	}
	require.Equal(s.T(), []sqlplugin.ReplicationDLQTasksRow(nil), rows)
}

func (s *historyHistoryReplicationDLQTaskSuite) TestInsertDeleteSelect_Single() {
	sourceCluster := shuffle.String(testHistoryReplicationTaskDLQSourceCluster)
	shardID := rand.Int31()
	taskID := int64(1)

	task := s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
	result, err := s.store.InsertIntoReplicationDLQTasks(newExecutionContext(), []sqlplugin.ReplicationDLQTasksRow{task})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	filter := sqlplugin.ReplicationDLQTasksFilter{
		ShardID:           shardID,
		SourceClusterName: sourceCluster,
		TaskID:            taskID,
	}
	result, err = s.store.DeleteFromReplicationDLQTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	rangeFilter := sqlplugin.ReplicationDLQTasksRangeFilter{
		ShardID:            shardID,
		SourceClusterName:  sourceCluster,
		InclusiveMinTaskID: taskID,
		ExclusiveMaxTaskID: taskID + 1,
		PageSize:           1,
	}
	rows, err := s.store.RangeSelectFromReplicationDLQTasks(newExecutionContext(), rangeFilter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
		rows[index].SourceClusterName = sourceCluster
	}
	require.Equal(s.T(), []sqlplugin.ReplicationDLQTasksRow(nil), rows)
}

func (s *historyHistoryReplicationDLQTaskSuite) TestInsertDeleteSelect_Multiple() {
	numTasks := 20
	pageSize := numTasks * 2

	sourceCluster := shuffle.String(testHistoryReplicationTaskDLQSourceCluster)
	shardID := rand.Int31()
	minTaskID := int64(1)
	taskID := minTaskID
	maxTaskID := taskID + int64(numTasks)

	var tasks []sqlplugin.ReplicationDLQTasksRow
	for i := 0; i < numTasks; i++ {
		task := s.newRandomReplicationTasksDLQRow(sourceCluster, shardID, taskID)
		taskID++
		tasks = append(tasks, task)
	}
	result, err := s.store.InsertIntoReplicationDLQTasks(newExecutionContext(), tasks)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numTasks, int(rowsAffected))

	filter := sqlplugin.ReplicationDLQTasksRangeFilter{
		ShardID:            shardID,
		SourceClusterName:  sourceCluster,
		InclusiveMinTaskID: minTaskID,
		ExclusiveMaxTaskID: maxTaskID,
		PageSize:           pageSize,
	}
	result, err = s.store.RangeDeleteFromReplicationDLQTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numTasks, int(rowsAffected))

	rows, err := s.store.RangeSelectFromReplicationDLQTasks(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	for index := range rows {
		rows[index].ShardID = shardID
		rows[index].SourceClusterName = sourceCluster
	}
	require.Equal(s.T(), []sqlplugin.ReplicationDLQTasksRow(nil), rows)
}

func (s *historyHistoryReplicationDLQTaskSuite) newRandomReplicationTasksDLQRow(
	sourceClusterName string,
	shardID int32,
	taskID int64,
) sqlplugin.ReplicationDLQTasksRow {
	return sqlplugin.ReplicationDLQTasksRow{
		SourceClusterName: sourceClusterName,
		ShardID:           shardID,
		TaskID:            taskID,
		Data:              shuffle.Bytes(testHistoryReplicationTaskDLQData),
		DataEncoding:      testHistoryReplicationTaskDLQEncoding,
	}
}
