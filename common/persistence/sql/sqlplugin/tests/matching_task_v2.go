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
		*require.Assertions

		store sqlplugin.MatchingTaskV2
	}
)

func NewMatchingTaskV2Suite(
	t *testing.T,
	store sqlplugin.MatchingTaskV2,
) *matchingTaskV2Suite {
	return &matchingTaskV2Suite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *matchingTaskV2Suite) SetupSuite() {
}

func (s *matchingTaskV2Suite) TearDownSuite() {
}

func (s *matchingTaskV2Suite) SetupTest() {
	s.Assertions = require.New(s.T())
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
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(len(tasks), int(rowsAffected))

	filter := sqlplugin.TasksFilterV2{
		RangeHash:         testMatchingTaskRangeHash,
		TaskQueueID:       queueID,
		InclusiveMinLevel: &sqlplugin.FairLevel{TaskPass: 1, TaskID: 0},
		PageSize:          util.Ptr(len(tasks)),
	}
	rows, err := s.store.SelectFromTasksV2(newExecutionContext(), filter)
	s.NoError(err)
	ids := make([]int64, len(rows))
	for i, r := range rows {
		ids[i] = r.TaskID
	}
	expectedIDs := []int64{5, 20, 10, 15}
	s.Equal(expectedIDs, ids)
}

func (s *matchingTaskV2Suite) TestInsertDeleteRange() {
	queueID := shuffle.Bytes(testMatchingTaskTaskQueueID)

	tasks := []sqlplugin.TasksRowV2{
		s.newRandomTasksRow(queueID, 1, 1),
		s.newRandomTasksRow(queueID, 2, 2),
		s.newRandomTasksRow(queueID, 3, 3),
	}
	result, err := s.store.InsertIntoTasksV2(newExecutionContext(), tasks)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(3, int(rowsAffected))

	filter := sqlplugin.TasksFilterV2{
		RangeHash:         testMatchingTaskRangeHash,
		TaskQueueID:       queueID,
		ExclusiveMaxLevel: &sqlplugin.FairLevel{TaskPass: 3, TaskID: 0},
		Limit:             util.Ptr(10),
	}
	result, err = s.store.DeleteFromTasksV2(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))

	filter = sqlplugin.TasksFilterV2{
		RangeHash:         testMatchingTaskRangeHash,
		TaskQueueID:       queueID,
		InclusiveMinLevel: &sqlplugin.FairLevel{TaskPass: 1, TaskID: 0},
		PageSize:          util.Ptr(10),
	}
	rows, err := s.store.SelectFromTasksV2(newExecutionContext(), filter)
	s.NoError(err)
	ids := make([]int64, len(rows))
	for i, r := range rows {
		ids[i] = r.TaskID
	}
	s.Equal([]int64{3}, ids)
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
