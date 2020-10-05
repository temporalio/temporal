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
	historyExecutionSignalSuite struct {
		suite.Suite
		*require.Assertions

		store sqlplugin.HistoryExecutionSignal
	}
)

const (
	testHistoryExecutionSignalEncoding = "random encoding"
)

var (
	testHistoryExecutionSignalData = []byte("random history execution signal data")
)

func newHistoryExecutionSignalSuite(
	t *testing.T,
	store sqlplugin.HistoryExecutionSignal,
) *historyExecutionSignalSuite {
	return &historyExecutionSignalSuite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *historyExecutionSignalSuite) SetupSuite() {

}

func (s *historyExecutionSignalSuite) TearDownSuite() {

}

func (s *historyExecutionSignalSuite) SetupTest() {
	s.Assertions = require.New(s.T())
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
	result, err := s.store.ReplaceIntoSignalInfoMaps([]sqlplugin.SignalInfoMapsRow{signal})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *historyExecutionSignalSuite) TestReplace_Multiple() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	signal1 := s.newRandomExecutionSignalRow(shardID, namespaceID, workflowID, runID, rand.Int63())
	signal2 := s.newRandomExecutionSignalRow(shardID, namespaceID, workflowID, runID, rand.Int63())
	result, err := s.store.ReplaceIntoSignalInfoMaps([]sqlplugin.SignalInfoMapsRow{signal1, signal2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))
}

func (s *historyExecutionSignalSuite) TestReplaceSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	initiatedID := rand.Int63()

	signal := s.newRandomExecutionSignalRow(shardID, namespaceID, workflowID, runID, initiatedID)
	result, err := s.store.ReplaceIntoSignalInfoMaps([]sqlplugin.SignalInfoMapsRow{signal})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := &sqlplugin.SignalInfoMapsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		InitiatedID: convert.Int64Ptr(initiatedID),
	}
	rows, err := s.store.SelectFromSignalInfoMaps(filter)
	s.NoError(err)
	rowMap := map[int64]sqlplugin.SignalInfoMapsRow{}
	for _, signal := range rows {
		rowMap[signal.InitiatedID] = signal
	}
	s.Equal(map[int64]sqlplugin.SignalInfoMapsRow{
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
	result, err := s.store.ReplaceIntoSignalInfoMaps(signals)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numSignals, int(rowsAffected))

	filter := &sqlplugin.SignalInfoMapsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		InitiatedID: nil,
	}
	rows, err := s.store.SelectFromSignalInfoMaps(filter)
	s.NoError(err)
	signalMap := map[int64]sqlplugin.SignalInfoMapsRow{}
	for _, signal := range signals {
		signalMap[signal.InitiatedID] = signal
	}
	rowMap := map[int64]sqlplugin.SignalInfoMapsRow{}
	for _, signal := range rows {
		rowMap[signal.InitiatedID] = signal
	}
	s.Equal(signalMap, rowMap)
}

func (s *historyExecutionSignalSuite) TestDeleteSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	initiatedID := rand.Int63()

	filter := &sqlplugin.SignalInfoMapsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		InitiatedID: convert.Int64Ptr(initiatedID),
	}
	result, err := s.store.DeleteFromSignalInfoMaps(filter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	rows, err := s.store.SelectFromSignalInfoMaps(filter)
	s.NoError(err)
	s.Equal([]sqlplugin.SignalInfoMapsRow(nil), rows)
}

func (s *historyExecutionSignalSuite) TestDeleteSelect_Multiple() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	filter := &sqlplugin.SignalInfoMapsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		InitiatedID: nil,
	}
	result, err := s.store.DeleteFromSignalInfoMaps(filter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	rows, err := s.store.SelectFromSignalInfoMaps(filter)
	s.NoError(err)
	s.Equal([]sqlplugin.SignalInfoMapsRow(nil), rows)
}

func (s *historyExecutionSignalSuite) TestReplaceDeleteSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	initiatedID := rand.Int63()

	signal := s.newRandomExecutionSignalRow(shardID, namespaceID, workflowID, runID, initiatedID)
	result, err := s.store.ReplaceIntoSignalInfoMaps([]sqlplugin.SignalInfoMapsRow{signal})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := &sqlplugin.SignalInfoMapsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		InitiatedID: convert.Int64Ptr(initiatedID),
	}
	result, err = s.store.DeleteFromSignalInfoMaps(filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	rows, err := s.store.SelectFromSignalInfoMaps(filter)
	s.NoError(err)
	s.Equal([]sqlplugin.SignalInfoMapsRow(nil), rows)
}

func (s *historyExecutionSignalSuite) TestReplaceDeleteSelect_Multiple() {
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
	result, err := s.store.ReplaceIntoSignalInfoMaps(signals)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numSignals, int(rowsAffected))

	filter := &sqlplugin.SignalInfoMapsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		InitiatedID: nil,
	}
	result, err = s.store.DeleteFromSignalInfoMaps(filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(numSignals, int(rowsAffected))

	rows, err := s.store.SelectFromSignalInfoMaps(filter)
	s.NoError(err)
	s.Equal([]sqlplugin.SignalInfoMapsRow(nil), rows)
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
