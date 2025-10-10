package tests

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/shuffle"
)

type (
	historyExecutionChildWorkflowSuite struct {
		suite.Suite

		store sqlplugin.HistoryExecutionChildWorkflow
	}
)

const (
	testHistoryExecutionChildWorkflowEncoding = "random encoding"
)

var (
	testHistoryExecutionChildWorkflowData = []byte("random history execution child workflow data")
)

func NewHistoryExecutionChildWorkflowSuite(
	t *testing.T,
	store sqlplugin.HistoryExecutionChildWorkflow,
) *historyExecutionChildWorkflowSuite {
	return &historyExecutionChildWorkflowSuite{

		store: store,
	}
}

func (s *historyExecutionChildWorkflowSuite) TearDownSuite() {

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
	result, err := s.store.ReplaceIntoChildExecutionInfoMaps(newExecutionContext(), []sqlplugin.ChildExecutionInfoMapsRow{childWorkflow})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))
}

func (s *historyExecutionChildWorkflowSuite) TestReplace_Multiple() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	childWorkflow1 := s.newRandomExecutionChildWorkflowRow(shardID, namespaceID, workflowID, runID, rand.Int63())
	childWorkflow2 := s.newRandomExecutionChildWorkflowRow(shardID, namespaceID, workflowID, runID, rand.Int63())
	result, err := s.store.ReplaceIntoChildExecutionInfoMaps(newExecutionContext(), []sqlplugin.ChildExecutionInfoMapsRow{childWorkflow1, childWorkflow2})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, int(rowsAffected))
}

func (s *historyExecutionChildWorkflowSuite) TestReplaceSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	initiatedID := rand.Int63()

	childWorkflow := s.newRandomExecutionChildWorkflowRow(shardID, namespaceID, workflowID, runID, initiatedID)
	result, err := s.store.ReplaceIntoChildExecutionInfoMaps(newExecutionContext(), []sqlplugin.ChildExecutionInfoMapsRow{childWorkflow})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	selectFilter := sqlplugin.ChildExecutionInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromChildExecutionInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	rowMap := map[int64]sqlplugin.ChildExecutionInfoMapsRow{}
	for _, childWorkflow := range rows {
		rowMap[childWorkflow.InitiatedID] = childWorkflow
	}
	require.Equal(s.T(), map[int64]sqlplugin.ChildExecutionInfoMapsRow{
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
	result, err := s.store.ReplaceIntoChildExecutionInfoMaps(newExecutionContext(), childWorkflows)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numChildWorkflows, int(rowsAffected))

	selectFilter := sqlplugin.ChildExecutionInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromChildExecutionInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	childWorkflowMap := map[int64]sqlplugin.ChildExecutionInfoMapsRow{}
	for _, childWorkflow := range childWorkflows {
		childWorkflowMap[childWorkflow.InitiatedID] = childWorkflow
	}
	rowMap := map[int64]sqlplugin.ChildExecutionInfoMapsRow{}
	for _, childWorkflow := range rows {
		rowMap[childWorkflow.InitiatedID] = childWorkflow
	}
	require.Equal(s.T(), childWorkflowMap, rowMap)
}

func (s *historyExecutionChildWorkflowSuite) TestDeleteSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	initiatedID := rand.Int63()

	deleteFilter := sqlplugin.ChildExecutionInfoMapsFilter{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		InitiatedIDs: []int64{initiatedID},
	}
	result, err := s.store.DeleteFromChildExecutionInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	selectFilter := sqlplugin.ChildExecutionInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromChildExecutionInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.ChildExecutionInfoMapsRow(nil), rows)
}

func (s *historyExecutionChildWorkflowSuite) TestDeleteSelect_Multiple() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	deleteFilter := sqlplugin.ChildExecutionInfoMapsFilter{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		InitiatedIDs: []int64{rand.Int63(), rand.Int63()},
	}
	result, err := s.store.DeleteFromChildExecutionInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	selectFilter := sqlplugin.ChildExecutionInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromChildExecutionInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.ChildExecutionInfoMapsRow(nil), rows)
}

func (s *historyExecutionChildWorkflowSuite) TestDeleteSelect_All() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	deleteFilter := sqlplugin.ChildExecutionInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	result, err := s.store.DeleteAllFromChildExecutionInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	selectFilter := sqlplugin.ChildExecutionInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromChildExecutionInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.ChildExecutionInfoMapsRow(nil), rows)
}

func (s *historyExecutionChildWorkflowSuite) TestReplaceDeleteSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	initiatedID := rand.Int63()

	childWorkflow := s.newRandomExecutionChildWorkflowRow(shardID, namespaceID, workflowID, runID, initiatedID)
	result, err := s.store.ReplaceIntoChildExecutionInfoMaps(newExecutionContext(), []sqlplugin.ChildExecutionInfoMapsRow{childWorkflow})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	deleteFilter := sqlplugin.ChildExecutionInfoMapsFilter{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		InitiatedIDs: []int64{initiatedID},
	}
	result, err = s.store.DeleteFromChildExecutionInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	selectFilter := sqlplugin.ChildExecutionInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromChildExecutionInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.ChildExecutionInfoMapsRow(nil), rows)
}

func (s *historyExecutionChildWorkflowSuite) TestReplaceDeleteSelect_Multiple() {
	numChildWorkflows := 20

	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	var childWorkflows []sqlplugin.ChildExecutionInfoMapsRow
	var childWorkflowInitiatedIDs []int64
	for i := 0; i < numChildWorkflows; i++ {
		childWorkflowInitiatedID := rand.Int63()
		childWorkflow := s.newRandomExecutionChildWorkflowRow(shardID, namespaceID, workflowID, runID, childWorkflowInitiatedID)
		childWorkflowInitiatedIDs = append(childWorkflowInitiatedIDs, childWorkflowInitiatedID)
		childWorkflows = append(childWorkflows, childWorkflow)
	}
	result, err := s.store.ReplaceIntoChildExecutionInfoMaps(newExecutionContext(), childWorkflows)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numChildWorkflows, int(rowsAffected))

	deleteFilter := sqlplugin.ChildExecutionInfoMapsFilter{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		InitiatedIDs: childWorkflowInitiatedIDs,
	}
	result, err = s.store.DeleteFromChildExecutionInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numChildWorkflows, int(rowsAffected))

	selectFilter := sqlplugin.ChildExecutionInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromChildExecutionInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.ChildExecutionInfoMapsRow(nil), rows)
}

func (s *historyExecutionChildWorkflowSuite) TestReplaceDeleteSelect_All() {
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
	result, err := s.store.ReplaceIntoChildExecutionInfoMaps(newExecutionContext(), childWorkflows)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numChildWorkflows, int(rowsAffected))

	deleteFilter := sqlplugin.ChildExecutionInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	result, err = s.store.DeleteAllFromChildExecutionInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numChildWorkflows, int(rowsAffected))

	selectFilter := sqlplugin.ChildExecutionInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromChildExecutionInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.ChildExecutionInfoMapsRow(nil), rows)
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
