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
	historyExecutionTimerSuite struct {
		suite.Suite

		store sqlplugin.HistoryExecutionTimer
	}
)

const (
	testHistoryExecutionTimerID       = "random history timer ID"
	testHistoryExecutionTimerEncoding = "random encoding"
)

var (
	testHistoryExecutionTimerData = []byte("random history execution timer data")
)

func NewHistoryExecutionTimerSuite(
	t *testing.T,
	store sqlplugin.HistoryExecutionTimer,
) *historyExecutionTimerSuite {
	return &historyExecutionTimerSuite{

		store: store,
	}
}

func (s *historyExecutionTimerSuite) TearDownSuite() {

}

func (s *historyExecutionTimerSuite) TearDownTest() {

}

func (s *historyExecutionTimerSuite) TestReplace_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	timerID := shuffle.String(testHistoryExecutionTimerID)

	timer := s.newRandomExecutionTimerRow(shardID, namespaceID, workflowID, runID, timerID)
	result, err := s.store.ReplaceIntoTimerInfoMaps(newExecutionContext(), []sqlplugin.TimerInfoMapsRow{timer})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))
}

func (s *historyExecutionTimerSuite) TestReplace_Multiple() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	timer1 := s.newRandomExecutionTimerRow(shardID, namespaceID, workflowID, runID, shuffle.String(testHistoryExecutionTimerID))
	timer2 := s.newRandomExecutionTimerRow(shardID, namespaceID, workflowID, runID, shuffle.String(testHistoryExecutionTimerID))
	result, err := s.store.ReplaceIntoTimerInfoMaps(newExecutionContext(), []sqlplugin.TimerInfoMapsRow{timer1, timer2})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, int(rowsAffected))
}

func (s *historyExecutionTimerSuite) TestReplaceSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	timerID := shuffle.String(testHistoryExecutionTimerID)

	timer := s.newRandomExecutionTimerRow(shardID, namespaceID, workflowID, runID, timerID)
	result, err := s.store.ReplaceIntoTimerInfoMaps(newExecutionContext(), []sqlplugin.TimerInfoMapsRow{timer})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	selectFilter := sqlplugin.TimerInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromTimerInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	rowMap := map[string]sqlplugin.TimerInfoMapsRow{}
	for _, timer := range rows {
		rowMap[timer.TimerID] = timer
	}
	require.Equal(s.T(), map[string]sqlplugin.TimerInfoMapsRow{
		timer.TimerID: timer,
	}, rowMap)
}

func (s *historyExecutionTimerSuite) TestReplaceSelect_Multiple() {
	numTimers := 20

	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	var timers []sqlplugin.TimerInfoMapsRow
	for i := 0; i < numTimers; i++ {
		timer := s.newRandomExecutionTimerRow(shardID, namespaceID, workflowID, runID, shuffle.String(testHistoryExecutionTimerID))
		timers = append(timers, timer)
	}
	result, err := s.store.ReplaceIntoTimerInfoMaps(newExecutionContext(), timers)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numTimers, int(rowsAffected))

	selectFilter := sqlplugin.TimerInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromTimerInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	timerMap := map[string]sqlplugin.TimerInfoMapsRow{}
	for _, timer := range timers {
		timerMap[timer.TimerID] = timer
	}
	rowMap := map[string]sqlplugin.TimerInfoMapsRow{}
	for _, timer := range rows {
		rowMap[timer.TimerID] = timer
	}
	require.Equal(s.T(), timerMap, rowMap)
}

func (s *historyExecutionTimerSuite) TestDeleteSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	timerID := shuffle.String(testHistoryExecutionTimerID)

	deletFilter := sqlplugin.TimerInfoMapsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		TimerIDs:    []string{timerID},
	}
	result, err := s.store.DeleteFromTimerInfoMaps(newExecutionContext(), deletFilter)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	selectFilter := sqlplugin.TimerInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromTimerInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.TimerInfoMapsRow(nil), rows)
}

func (s *historyExecutionTimerSuite) TestDeleteSelect_Multiple() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	deleteFilter := sqlplugin.TimerInfoMapsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		TimerIDs:    []string{shuffle.String(testHistoryExecutionTimerID), shuffle.String(testHistoryExecutionTimerID)},
	}
	result, err := s.store.DeleteFromTimerInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	selectFilter := sqlplugin.TimerInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromTimerInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.TimerInfoMapsRow(nil), rows)
}

func (s *historyExecutionTimerSuite) TestDeleteSelect_All() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	deleteFilter := sqlplugin.TimerInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	result, err := s.store.DeleteAllFromTimerInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	selectFilter := sqlplugin.TimerInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromTimerInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.TimerInfoMapsRow(nil), rows)
}

func (s *historyExecutionTimerSuite) TestReplaceDeleteSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	timerID := shuffle.String(testHistoryExecutionTimerID)

	timer := s.newRandomExecutionTimerRow(shardID, namespaceID, workflowID, runID, timerID)
	result, err := s.store.ReplaceIntoTimerInfoMaps(newExecutionContext(), []sqlplugin.TimerInfoMapsRow{timer})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	deleteFilter := sqlplugin.TimerInfoMapsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		TimerIDs:    []string{timerID},
	}
	result, err = s.store.DeleteFromTimerInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))

	selectFilter := sqlplugin.TimerInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromTimerInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.TimerInfoMapsRow(nil), rows)
}

func (s *historyExecutionTimerSuite) TestReplaceDeleteSelect_Multiple() {
	numTimers := 20

	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	var timers []sqlplugin.TimerInfoMapsRow
	var timerIDs []string
	for i := 0; i < numTimers; i++ {
		timerID := shuffle.String(testHistoryExecutionTimerID)
		timer := s.newRandomExecutionTimerRow(shardID, namespaceID, workflowID, runID, timerID)
		timerIDs = append(timerIDs, timerID)
		timers = append(timers, timer)
	}
	result, err := s.store.ReplaceIntoTimerInfoMaps(newExecutionContext(), timers)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numTimers, int(rowsAffected))

	deleteFilter := sqlplugin.TimerInfoMapsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		TimerIDs:    timerIDs,
	}
	result, err = s.store.DeleteFromTimerInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numTimers, int(rowsAffected))

	selectFilter := sqlplugin.TimerInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromTimerInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.TimerInfoMapsRow(nil), rows)
}

func (s *historyExecutionTimerSuite) TestReplaceDeleteSelect_All() {
	numTimers := 20

	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	var timers []sqlplugin.TimerInfoMapsRow
	for i := 0; i < numTimers; i++ {
		timer := s.newRandomExecutionTimerRow(shardID, namespaceID, workflowID, runID, shuffle.String(testHistoryExecutionTimerID))
		timers = append(timers, timer)
	}
	result, err := s.store.ReplaceIntoTimerInfoMaps(newExecutionContext(), timers)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numTimers, int(rowsAffected))

	deleteFilter := sqlplugin.TimerInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	result, err = s.store.DeleteAllFromTimerInfoMaps(newExecutionContext(), deleteFilter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numTimers, int(rowsAffected))

	selectFilter := sqlplugin.TimerInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromTimerInfoMaps(newExecutionContext(), selectFilter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.TimerInfoMapsRow(nil), rows)
}

func (s *historyExecutionTimerSuite) newRandomExecutionTimerRow(
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
	timerID string,
) sqlplugin.TimerInfoMapsRow {
	return sqlplugin.TimerInfoMapsRow{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		TimerID:      timerID,
		Data:         shuffle.Bytes(testHistoryExecutionTimerData),
		DataEncoding: testHistoryExecutionTimerEncoding,
	}
}
