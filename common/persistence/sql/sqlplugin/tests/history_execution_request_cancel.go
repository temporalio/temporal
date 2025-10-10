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
	historyExecutionRequestCancelSuite struct {
		suite.Suite

		store sqlplugin.HistoryExecutionRequestCancel
	}
)

const (
	testHistoryExecutionRequestCancelEncoding = "random encoding"
)

var (
	testHistoryExecutionRequestCancelData = []byte("random history execution request cancel data")
)

func NewHistoryExecutionRequestCancelSuite(
	t *testing.T,
	store sqlplugin.HistoryExecutionRequestCancel,
) *historyExecutionRequestCancelSuite {
	return &historyExecutionRequestCancelSuite{

		store: store,
	}
}

func (s *historyExecutionRequestCancelSuite) TearDownSuite() {

}

func (s *historyExecutionRequestCancelSuite) TearDownTest() {

}

func (s *historyExecutionRequestCancelSuite) TestReplace_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	initiatedID := rand.Int63()

	requestCancel := s.newRandomExecutionRequestCancelRow(shardID, namespaceID, workflowID, runID, initiatedID)
	result, err := s.store.ReplaceIntoRequestCancelInfoMaps(newExecutionContext(), []sqlplugin.RequestCancelInfoMapsRow{requestCancel})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))
}

func (s *historyExecutionRequestCancelSuite) TestReplace_Multiple() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	requestCancel1 := s.newRandomExecutionRequestCancelRow(shardID, namespaceID, workflowID, runID, rand.Int63())
	requestCancel2 := s.newRandomExecutionRequestCancelRow(shardID, namespaceID, workflowID, runID, rand.Int63())
	result, err := s.store.ReplaceIntoRequestCancelInfoMaps(newExecutionContext(), []sqlplugin.RequestCancelInfoMapsRow{requestCancel1, requestCancel2})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, int(rowsAffected))
}

func (s *historyExecutionRequestCancelSuite) TestReplaceSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	initiatedID := rand.Int63()

	requestCancel := s.newRandomExecutionRequestCancelRow(shardID, namespaceID, workflowID, runID, initiatedID)
	result, err := s.store.ReplaceIntoRequestCancelInfoMaps(newExecutionContext(), []sqlplugin.RequestCancelInfoMapsRow{requestCancel})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	selectFilter := sqlplugin.RequestCancelInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromRequestCancelInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	rowMap := map[int64]sqlplugin.RequestCancelInfoMapsRow{}
	for _, requestCancel := range rows {
		rowMap[requestCancel.InitiatedID] = requestCancel
	}
	require.Equal(s.T(), map[int64]sqlplugin.RequestCancelInfoMapsRow{
		requestCancel.InitiatedID: requestCancel,
	}, rowMap)
}

func (s *historyExecutionRequestCancelSuite) TestReplaceSelect_Multiple() {
	numRequestCancels := 20

	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	var requestCancels []sqlplugin.RequestCancelInfoMapsRow
	for i := 0; i < numRequestCancels; i++ {
		requestCancel := s.newRandomExecutionRequestCancelRow(shardID, namespaceID, workflowID, runID, rand.Int63())
		requestCancels = append(requestCancels, requestCancel)
	}
	result, err := s.store.ReplaceIntoRequestCancelInfoMaps(newExecutionContext(), requestCancels)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numRequestCancels, int(rowsAffected))

	selectFilter := sqlplugin.RequestCancelInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromRequestCancelInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	requestCancelMap := map[int64]sqlplugin.RequestCancelInfoMapsRow{}
	for _, requestCancel := range requestCancels {
		requestCancelMap[requestCancel.InitiatedID] = requestCancel
	}
	rowMap := map[int64]sqlplugin.RequestCancelInfoMapsRow{}
	for _, requestCancel := range rows {
		rowMap[requestCancel.InitiatedID] = requestCancel
	}
	require.Equal(s.T(), requestCancelMap, rowMap)
}

func (s *historyExecutionRequestCancelSuite) TestDeleteSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	initiatedID := rand.Int63()

	deleteFilter := sqlplugin.RequestCancelInfoMapsFilter{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		InitiatedIDs: []int64{initiatedID},
	}
	result, err := s.store.DeleteFromRequestCancelInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	selectFilter := sqlplugin.RequestCancelInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromRequestCancelInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.RequestCancelInfoMapsRow(nil), rows)
}

func (s *historyExecutionRequestCancelSuite) TestDeleteSelect_Multiple() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	deleteFilter := sqlplugin.RequestCancelInfoMapsFilter{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		InitiatedIDs: []int64{rand.Int63(), rand.Int63()},
	}
	result, err := s.store.DeleteFromRequestCancelInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	selectFilter := sqlplugin.RequestCancelInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromRequestCancelInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.RequestCancelInfoMapsRow(nil), rows)
}

func (s *historyExecutionRequestCancelSuite) TestDeleteSelect_All() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	deleteFilter := sqlplugin.RequestCancelInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	result, err := s.store.DeleteAllFromRequestCancelInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	selectFilter := sqlplugin.RequestCancelInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromRequestCancelInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.RequestCancelInfoMapsRow(nil), rows)
}

func (s *historyExecutionRequestCancelSuite) TestReplaceDeleteSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	initiatedID := rand.Int63()

	requestCancel := s.newRandomExecutionRequestCancelRow(shardID, namespaceID, workflowID, runID, initiatedID)
	result, err := s.store.ReplaceIntoRequestCancelInfoMaps(newExecutionContext(), []sqlplugin.RequestCancelInfoMapsRow{requestCancel})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	deleteFilter := sqlplugin.RequestCancelInfoMapsFilter{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		InitiatedIDs: []int64{initiatedID},
	}
	result, err = s.store.DeleteFromRequestCancelInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	selectFilter := sqlplugin.RequestCancelInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromRequestCancelInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.RequestCancelInfoMapsRow(nil), rows)
}

func (s *historyExecutionRequestCancelSuite) TestReplaceDeleteSelect_Multiple() {
	numRequestCancels := 20

	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	var requestCancels []sqlplugin.RequestCancelInfoMapsRow
	var requestCancelInitiatedIDs []int64
	for i := 0; i < numRequestCancels; i++ {
		requestCancelInitiatedID := rand.Int63()
		requestCancel := s.newRandomExecutionRequestCancelRow(shardID, namespaceID, workflowID, runID, requestCancelInitiatedID)
		requestCancelInitiatedIDs = append(requestCancelInitiatedIDs, requestCancelInitiatedID)
		requestCancels = append(requestCancels, requestCancel)
	}
	result, err := s.store.ReplaceIntoRequestCancelInfoMaps(newExecutionContext(), requestCancels)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numRequestCancels, int(rowsAffected))

	deleteFilter := sqlplugin.RequestCancelInfoMapsFilter{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		InitiatedIDs: requestCancelInitiatedIDs,
	}
	result, err = s.store.DeleteFromRequestCancelInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numRequestCancels, int(rowsAffected))

	selectFilter := sqlplugin.RequestCancelInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromRequestCancelInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.RequestCancelInfoMapsRow(nil), rows)
}

func (s *historyExecutionRequestCancelSuite) TestReplaceDeleteSelect_All() {
	numRequestCancels := 20

	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	var requestCancels []sqlplugin.RequestCancelInfoMapsRow
	for i := 0; i < numRequestCancels; i++ {
		requestCancel := s.newRandomExecutionRequestCancelRow(shardID, namespaceID, workflowID, runID, rand.Int63())
		requestCancels = append(requestCancels, requestCancel)
	}
	result, err := s.store.ReplaceIntoRequestCancelInfoMaps(newExecutionContext(), requestCancels)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numRequestCancels, int(rowsAffected))

	deleteFilter := sqlplugin.RequestCancelInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	result, err = s.store.DeleteAllFromRequestCancelInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numRequestCancels, int(rowsAffected))

	selectFilter := sqlplugin.RequestCancelInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromRequestCancelInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.RequestCancelInfoMapsRow(nil), rows)
}

func (s *historyExecutionRequestCancelSuite) newRandomExecutionRequestCancelRow(
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
	initiatedID int64,
) sqlplugin.RequestCancelInfoMapsRow {
	return sqlplugin.RequestCancelInfoMapsRow{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		InitiatedID:  initiatedID,
		Data:         shuffle.Bytes(testHistoryExecutionRequestCancelData),
		DataEncoding: testHistoryExecutionRequestCancelEncoding,
	}
}
