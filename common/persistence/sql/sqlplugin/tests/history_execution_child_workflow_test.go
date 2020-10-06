package tests

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/shuffle"
)

type (
	historyExecutionChildWorkflowSuite struct {
		suite.Suite
		*require.Assertions

		store sqlplugin.HistoryExecutionChildWorkflow
	}
)

const (
	testHistoryExecutionChildWorkflowEncoding = "random encoding"
)

var (
	testHistoryExecutionChildWorkflowData = []byte("random history execution child workflow data")
)

func newHistoryExecutionChildWorkflowSuite(
	t *testing.T,
	store sqlplugin.HistoryExecutionChildWorkflow,
) *historyExecutionChildWorkflowSuite {
	return &historyExecutionChildWorkflowSuite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *historyExecutionChildWorkflowSuite) SetupSuite() {

}

func (s *historyExecutionChildWorkflowSuite) TearDownSuite() {

}

func (s *historyExecutionChildWorkflowSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *historyExecutionChildWorkflowSuite) TearDownTest() {

}

func (s *historyExecutionChildWorkflowSuite) TestReplace_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	initiatedID := rand.Int63()

	childWorkflow := s.newRandomExecutionChildWorkflowRow(shardID, namespaceID, workflowID, runID, initiatedID)
	result, err := s.store.ReplaceIntoChildExecutionInfoMaps([]sqlplugin.ChildExecutionInfoMapsRow{childWorkflow})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *historyExecutionChildWorkflowSuite) TestReplace_Multiple() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	childWorkflow1 := s.newRandomExecutionChildWorkflowRow(shardID, namespaceID, workflowID, runID, rand.Int63())
	childWorkflow2 := s.newRandomExecutionChildWorkflowRow(shardID, namespaceID, workflowID, runID, rand.Int63())
	result, err := s.store.ReplaceIntoChildExecutionInfoMaps([]sqlplugin.ChildExecutionInfoMapsRow{childWorkflow1, childWorkflow2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))
}

func (s *historyExecutionChildWorkflowSuite) TestReplaceSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	childWorkflow := s.newRandomExecutionChildWorkflowRow(shardID, namespaceID, workflowID, runID, rand.Int63())
	result, err := s.store.ReplaceIntoChildExecutionInfoMaps([]sqlplugin.ChildExecutionInfoMapsRow{childWorkflow})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := &sqlplugin.ChildExecutionInfoMapsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectFromChildExecutionInfoMaps(filter)
	s.NoError(err)
	rowMap := map[int64]sqlplugin.ChildExecutionInfoMapsRow{}
	for _, childWorkflow := range rows {
		rowMap[childWorkflow.InitiatedID] = childWorkflow
	}
	s.Equal(map[int64]sqlplugin.ChildExecutionInfoMapsRow{
		childWorkflow.InitiatedID: childWorkflow,
	}, rowMap)
}

func (s *historyExecutionChildWorkflowSuite) TestReplaceSelect_Multiple() {
	numChildWorkflows := 20

	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	var childWorkflows []sqlplugin.ChildExecutionInfoMapsRow
	for i := 0; i < numChildWorkflows; i++ {
		childWorkflow := s.newRandomExecutionChildWorkflowRow(shardID, namespaceID, workflowID, runID, rand.Int63())
		childWorkflows = append(childWorkflows, childWorkflow)
	}
	result, err := s.store.ReplaceIntoChildExecutionInfoMaps(childWorkflows)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numChildWorkflows, int(rowsAffected))

	filter := &sqlplugin.ChildExecutionInfoMapsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectFromChildExecutionInfoMaps(filter)
	s.NoError(err)
	childWorkflowMap := map[int64]sqlplugin.ChildExecutionInfoMapsRow{}
	for _, childWorkflow := range childWorkflows {
		childWorkflowMap[childWorkflow.InitiatedID] = childWorkflow
	}
	rowMap := map[int64]sqlplugin.ChildExecutionInfoMapsRow{}
	for _, childWorkflow := range rows {
		rowMap[childWorkflow.InitiatedID] = childWorkflow
	}
	s.Equal(childWorkflowMap, rowMap)
}

func (s *historyExecutionChildWorkflowSuite) TestDeleteSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	initiatedID := rand.Int63()

	filter := &sqlplugin.ChildExecutionInfoMapsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		InitiatedID: convert.Int64Ptr(initiatedID),
	}
	result, err := s.store.DeleteFromChildExecutionInfoMaps(filter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	rows, err := s.store.SelectFromChildExecutionInfoMaps(filter)
	s.NoError(err)
	s.Equal([]sqlplugin.ChildExecutionInfoMapsRow(nil), rows)
}

func (s *historyExecutionChildWorkflowSuite) TestDeleteSelect_Multiple() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	filter := &sqlplugin.ChildExecutionInfoMapsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		InitiatedID: nil,
	}
	result, err := s.store.DeleteFromChildExecutionInfoMaps(filter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	rows, err := s.store.SelectFromChildExecutionInfoMaps(filter)
	s.NoError(err)
	s.Equal([]sqlplugin.ChildExecutionInfoMapsRow(nil), rows)
}

func (s *historyExecutionChildWorkflowSuite) TestReplaceDeleteSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	initiatedID := rand.Int63()

	childWorkflow := s.newRandomExecutionChildWorkflowRow(shardID, namespaceID, workflowID, runID, initiatedID)
	result, err := s.store.ReplaceIntoChildExecutionInfoMaps([]sqlplugin.ChildExecutionInfoMapsRow{childWorkflow})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := &sqlplugin.ChildExecutionInfoMapsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		InitiatedID: convert.Int64Ptr(initiatedID),
	}
	result, err = s.store.DeleteFromChildExecutionInfoMaps(filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	rows, err := s.store.SelectFromChildExecutionInfoMaps(filter)
	s.NoError(err)
	s.Equal([]sqlplugin.ChildExecutionInfoMapsRow(nil), rows)
}

func (s *historyExecutionChildWorkflowSuite) TestReplaceDeleteSelect_Multiple() {
	numChildWorkflows := 20

	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	var childWorkflows []sqlplugin.ChildExecutionInfoMapsRow
	for i := 0; i < numChildWorkflows; i++ {
		childWorkflow := s.newRandomExecutionChildWorkflowRow(shardID, namespaceID, workflowID, runID, rand.Int63())
		childWorkflows = append(childWorkflows, childWorkflow)
	}
	result, err := s.store.ReplaceIntoChildExecutionInfoMaps(childWorkflows)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numChildWorkflows, int(rowsAffected))

	filter := &sqlplugin.ChildExecutionInfoMapsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	result, err = s.store.DeleteFromChildExecutionInfoMaps(filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(numChildWorkflows, int(rowsAffected))

	rows, err := s.store.SelectFromChildExecutionInfoMaps(filter)
	s.NoError(err)
	s.Equal([]sqlplugin.ChildExecutionInfoMapsRow(nil), rows)
}

func (s *historyExecutionChildWorkflowSuite) newRandomExecutionChildWorkflowRow(
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
	initiatedID int64,
) sqlplugin.ChildExecutionInfoMapsRow {
	return sqlplugin.ChildExecutionInfoMapsRow{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		InitiatedID:  initiatedID,
		Data:         shuffle.Bytes(testHistoryExecutionChildWorkflowData),
		DataEncoding: testHistoryExecutionChildWorkflowEncoding,
	}
}
