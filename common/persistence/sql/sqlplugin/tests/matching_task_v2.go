package tests

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/shuffle"
	"go.temporal.io/server/common/util"
)

type (
	matchingTaskV2Suite struct {
		suite.Suite

		store sqlplugin.MatchingTaskV2
	}
)

func NewMatchingTaskV2Suite(
	t *testing.T,
	store sqlplugin.MatchingTaskV2,
) *matchingTaskV2Suite {
	return &matchingTaskV2Suite{

		store: store,
	}
}

func (s *matchingTaskV2Suite) TearDownSuite() {
}

func (s *matchingTaskV2Suite) TearDownTest() {
}

func (s *matchingTaskV2Suite) TestInsertSelect_Order() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)

	tasks := []sqlplugin.TasksRowV2{
		s.newRandomTasksRow(queueID, 2, 10),
		s.newRandomTasksRow(queueID, 1, 20),
		s.newRandomTasksRow(queueID, 1, 5),
		s.newRandomTasksRow(queueID, 2, 15),
	}
	result, err := s.store.InsertIntoTasksV2(newExecutionContext(), tasks)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), len(tasks), int(rowsAffected))

	filter := sqlplugin.TasksFilterV2{
		RangeHash:         testMatchingTaskRangeHash,
		TaskQueueID:       queueID,
		InclusiveMinLevel: &sqlplugin.FairLevel{TaskPass: 1, TaskID: 0},
		PageSize:          util.Ptr(len(tasks)),
	}
	rows, err := s.store.SelectFromTasksV2(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	ids := make([]int64, len(rows))
	for i, r := range rows {
		ids[i] = r.TaskID
	}
	expectedIDs := []int64{5, 20, 10, 15}
	require.Equal(s.T(), expectedIDs, ids)
}

func (s *matchingTaskV2Suite) TestInsertDeleteRange() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)

	tasks := []sqlplugin.TasksRowV2{
		s.newRandomTasksRow(queueID, 1, 1),
		s.newRandomTasksRow(queueID, 2, 2),
		s.newRandomTasksRow(queueID, 3, 3),
	}
	result, err := s.store.InsertIntoTasksV2(newExecutionContext(), tasks)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 3, int(rowsAffected))

	filter := sqlplugin.TasksFilterV2{
		RangeHash:         testMatchingTaskRangeHash,
		TaskQueueID:       queueID,
		ExclusiveMaxLevel: &sqlplugin.FairLevel{TaskPass: 3, TaskID: 0},
		Limit:             util.Ptr(10),
	}
	result, err = s.store.DeleteFromTasksV2(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, int(rowsAffected))

	filter = sqlplugin.TasksFilterV2{
		RangeHash:         testMatchingTaskRangeHash,
		TaskQueueID:       queueID,
		InclusiveMinLevel: &sqlplugin.FairLevel{TaskPass: 1, TaskID: 0},
		PageSize:          util.Ptr(10),
	}
	rows, err := s.store.SelectFromTasksV2(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	ids := make([]int64, len(rows))
	for i, r := range rows {
		ids[i] = r.TaskID
	}
	require.Equal(s.T(), []int64{3}, ids)
}

func (s *matchingTaskV2Suite) newRandomTasksRow(queueID []byte, pass, id int64) sqlplugin.TasksRowV2 {
	return sqlplugin.TasksRowV2{
		RangeHash:   testMatchingTaskRangeHash,
		TaskQueueID: queueID,
		FairLevel: sqlplugin.FairLevel{
			TaskPass: pass,
			TaskID:   id,
		},
		Data:         shuffle.Bytes(testMatchingTaskTaskData),
		DataEncoding: testMatchingTaskEncoding,
	}
}
