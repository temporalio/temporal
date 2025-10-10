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
	historyExecutionSignalSuite struct {
		suite.Suite

		store sqlplugin.HistoryExecutionSignal
	}
)

const (
	testHistoryExecutionSignalEncoding = "random encoding"
)

var (
	testHistoryExecutionSignalData = []byte("random history execution signal data")
)

func NewHistoryExecutionSignalSuite(
	t *testing.T,
	store sqlplugin.HistoryExecutionSignal,
) *historyExecutionSignalSuite {
	return &historyExecutionSignalSuite{

		store: store,
	}
}

func (s *historyExecutionSignalSuite) TearDownSuite() {

}

func (s *historyExecutionSignalSuite) TearDownTest() {

}

func (s *historyExecutionSignalSuite) TestReplace_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	initiatedID := rand.Int63()

	signal := s.newRandomExecutionSignalRow(shardID, namespaceID, workflowID, runID, initiatedID)
	result, err := s.store.ReplaceIntoSignalInfoMaps(newExecutionContext(), []sqlplugin.SignalInfoMapsRow{signal})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))
}

func (s *historyExecutionSignalSuite) TestReplace_Multiple() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	signal1 := s.newRandomExecutionSignalRow(shardID, namespaceID, workflowID, runID, rand.Int63())
	signal2 := s.newRandomExecutionSignalRow(shardID, namespaceID, workflowID, runID, rand.Int63())
	result, err := s.store.ReplaceIntoSignalInfoMaps(newExecutionContext(), []sqlplugin.SignalInfoMapsRow{signal1, signal2})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, int(rowsAffected))
}

func (s *historyExecutionSignalSuite) TestReplaceSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	initiatedID := rand.Int63()

	signal := s.newRandomExecutionSignalRow(shardID, namespaceID, workflowID, runID, initiatedID)
	result, err := s.store.ReplaceIntoSignalInfoMaps(newExecutionContext(), []sqlplugin.SignalInfoMapsRow{signal})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	selectFilter := sqlplugin.SignalInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromSignalInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	rowMap := map[int64]sqlplugin.SignalInfoMapsRow{}
	for _, signal := range rows {
		rowMap[signal.InitiatedID] = signal
	}
	require.Equal(s.T(), map[int64]sqlplugin.SignalInfoMapsRow{
		signal.InitiatedID: signal,
	}, rowMap)
}

func (s *historyExecutionSignalSuite) TestReplaceSelect_Multiple() {
	numSignals := 20

	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	var signals []sqlplugin.SignalInfoMapsRow
	for i := 0; i < numSignals; i++ {
		signal := s.newRandomExecutionSignalRow(shardID, namespaceID, workflowID, runID, rand.Int63())
		signals = append(signals, signal)
	}
	result, err := s.store.ReplaceIntoSignalInfoMaps(newExecutionContext(), signals)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numSignals, int(rowsAffected))

	selectFilter := sqlplugin.SignalInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromSignalInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	signalMap := map[int64]sqlplugin.SignalInfoMapsRow{}
	for _, signal := range signals {
		signalMap[signal.InitiatedID] = signal
	}
	rowMap := map[int64]sqlplugin.SignalInfoMapsRow{}
	for _, signal := range rows {
		rowMap[signal.InitiatedID] = signal
	}
	require.Equal(s.T(), signalMap, rowMap)
}

func (s *historyExecutionSignalSuite) TestDeleteSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	initiatedID := rand.Int63()

	deleteFilter := sqlplugin.SignalInfoMapsFilter{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		InitiatedIDs: []int64{initiatedID},
	}
	result, err := s.store.DeleteFromSignalInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	selectFilter := sqlplugin.SignalInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromSignalInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.SignalInfoMapsRow(nil), rows)
}

func (s *historyExecutionSignalSuite) TestDeleteSelect_Multiple() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	deleteFilter := sqlplugin.SignalInfoMapsFilter{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		InitiatedIDs: []int64{rand.Int63(), rand.Int63()},
	}
	result, err := s.store.DeleteFromSignalInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	selectFilter := sqlplugin.SignalInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromSignalInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.SignalInfoMapsRow(nil), rows)
}

func (s *historyExecutionSignalSuite) TestDeleteSelect_All() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	deleteFilter := sqlplugin.SignalInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	result, err := s.store.DeleteAllFromSignalInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	selectFilter := sqlplugin.SignalInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromSignalInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.SignalInfoMapsRow(nil), rows)
}

func (s *historyExecutionSignalSuite) TestReplaceDeleteSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	initiatedID := rand.Int63()

	signal := s.newRandomExecutionSignalRow(shardID, namespaceID, workflowID, runID, initiatedID)
	result, err := s.store.ReplaceIntoSignalInfoMaps(newExecutionContext(), []sqlplugin.SignalInfoMapsRow{signal})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	deleteFilter := sqlplugin.SignalInfoMapsFilter{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		InitiatedIDs: []int64{initiatedID},
	}
	result, err = s.store.DeleteFromSignalInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	selectFilter := sqlplugin.SignalInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromSignalInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.SignalInfoMapsRow(nil), rows)
}

func (s *historyExecutionSignalSuite) TestReplaceDeleteSelect_Multiple() {
	numSignals := 20

	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	var signals []sqlplugin.SignalInfoMapsRow
	var signalInitiatedIDs []int64
	for i := 0; i < numSignals; i++ {
		signalInitiatedID := rand.Int63()
		signal := s.newRandomExecutionSignalRow(shardID, namespaceID, workflowID, runID, signalInitiatedID)
		signalInitiatedIDs = append(signalInitiatedIDs, signalInitiatedID)
		signals = append(signals, signal)
	}
	result, err := s.store.ReplaceIntoSignalInfoMaps(newExecutionContext(), signals)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numSignals, int(rowsAffected))

	deleteFilter := sqlplugin.SignalInfoMapsFilter{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		InitiatedIDs: signalInitiatedIDs,
	}
	result, err = s.store.DeleteFromSignalInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numSignals, int(rowsAffected))

	selectFilter := sqlplugin.SignalInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromSignalInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.SignalInfoMapsRow(nil), rows)
}

func (s *historyExecutionSignalSuite) TestReplaceDeleteSelect_All() {
	numSignals := 20

	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	var signals []sqlplugin.SignalInfoMapsRow
	for i := 0; i < numSignals; i++ {
		signal := s.newRandomExecutionSignalRow(shardID, namespaceID, workflowID, runID, rand.Int63())
		signals = append(signals, signal)
	}
	result, err := s.store.ReplaceIntoSignalInfoMaps(newExecutionContext(), signals)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numSignals, int(rowsAffected))

	deleteFilter := sqlplugin.SignalInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	result, err = s.store.DeleteAllFromSignalInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numSignals, int(rowsAffected))

	selectFilter := sqlplugin.SignalInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromSignalInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.SignalInfoMapsRow(nil), rows)
}

func (s *historyExecutionSignalSuite) newRandomExecutionSignalRow(
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
	initiatedID int64,
) sqlplugin.SignalInfoMapsRow {
	return sqlplugin.SignalInfoMapsRow{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		InitiatedID:  initiatedID,
		Data:         shuffle.Bytes(testHistoryExecutionSignalData),
		DataEncoding: testHistoryExecutionSignalEncoding,
	}
}
