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
	historyExecutionBufferSuite struct {
		suite.Suite

		store sqlplugin.HistoryExecutionBuffer
	}
)

const (
	testHistoryExecutionBufferEncoding = "random encoding"
)

var (
	testHistoryExecutionBufferData = []byte("random history execution buffer data")
)

func NewHistoryExecutionBufferSuite(
	t *testing.T,
	store sqlplugin.HistoryExecutionBuffer,
) *historyExecutionBufferSuite {
	return &historyExecutionBufferSuite{

		store: store,
	}
}

func (s *historyExecutionBufferSuite) TearDownSuite() {

}

func (s *historyExecutionBufferSuite) TearDownTest() {

}

func (s *historyExecutionBufferSuite) TestInsert_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	buffer := s.newRandomExecutionBufferRow(shardID, namespaceID, workflowID, runID)
	result, err := s.store.InsertIntoBufferedEvents(newExecutionContext(), []sqlplugin.BufferedEventsRow{buffer})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 1, int(rowsAffected))
}

func (s *historyExecutionBufferSuite) TestInsert_Multiple() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	buffer1 := s.newRandomExecutionBufferRow(shardID, namespaceID, workflowID, runID)
	buffer2 := s.newRandomExecutionBufferRow(shardID, namespaceID, workflowID, runID)
	result, err := s.store.InsertIntoBufferedEvents(newExecutionContext(), []sqlplugin.BufferedEventsRow{buffer1, buffer2})
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 2, int(rowsAffected))
}

func (s *historyExecutionBufferSuite) TestInsertSelect() {
	numBufferedEvents := 20

	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	var buffers []sqlplugin.BufferedEventsRow
	for i := 0; i < numBufferedEvents; i++ {
		buffer := s.newRandomExecutionBufferRow(shardID, namespaceID, workflowID, runID)
		buffers = append(buffers, buffer)
	}
	result, err := s.store.InsertIntoBufferedEvents(newExecutionContext(), buffers)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numBufferedEvents, int(rowsAffected))

	filter := sqlplugin.BufferedEventsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectFromBufferedEvents(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), buffers, rows)
}

func (s *historyExecutionBufferSuite) TestDeleteSelect() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	filter := sqlplugin.BufferedEventsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	result, err := s.store.DeleteFromBufferedEvents(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), 0, int(rowsAffected))

	rows, err := s.store.SelectFromBufferedEvents(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.BufferedEventsRow(nil), rows)
}

func (s *historyExecutionBufferSuite) TestInsertDelete() {
	numBufferedEvents := 20

	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	var buffers []sqlplugin.BufferedEventsRow
	for i := 0; i < numBufferedEvents; i++ {
		buffer := s.newRandomExecutionBufferRow(shardID, namespaceID, workflowID, runID)
		buffers = append(buffers, buffer)
	}
	result, err := s.store.InsertIntoBufferedEvents(newExecutionContext(), buffers)
	require.NoError(s.T(), err)
	rowsAffected, err := result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numBufferedEvents, int(rowsAffected))

	filter := sqlplugin.BufferedEventsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	result, err = s.store.DeleteFromBufferedEvents(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	rowsAffected, err = result.RowsAffected()
	require.NoError(s.T(), err)
	require.Equal(s.T(), numBufferedEvents, int(rowsAffected))

	rows, err := s.store.SelectFromBufferedEvents(newExecutionContext(), filter)
	require.NoError(s.T(), err)
	require.Equal(s.T(), []sqlplugin.BufferedEventsRow(nil), rows)
}

func (s *historyExecutionBufferSuite) newRandomExecutionBufferRow(
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
) sqlplugin.BufferedEventsRow {
	return sqlplugin.BufferedEventsRow{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		Data:         shuffle.Bytes(testHistoryExecutionBufferData),
		DataEncoding: testHistoryExecutionBufferEncoding,
	}
}
