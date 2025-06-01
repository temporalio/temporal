package tests

import (
	"math/rand"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/shuffle"
)

type (
	historyCurrentExecutionSuite struct {
		suite.Suite
		*require.Assertions

		store sqlplugin.HistoryExecution
	}
)

var (
	testHistoryExecutionStates = []enumsspb.WorkflowExecutionState{
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED,
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE,
	}
	testHistoryExecutionStatus = map[enumsspb.WorkflowExecutionState][]enumspb.WorkflowExecutionStatus{
		enumsspb.WORKFLOW_EXECUTION_STATE_CREATED: {enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING: {enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED: {
			enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
			enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
			enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED,
			enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED,
			enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW,
			enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT,
		},
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE: {enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
	}
)

func NewHistoryCurrentExecutionSuite(
	t *testing.T,
	store sqlplugin.HistoryExecution,
) *historyCurrentExecutionSuite {
	return &historyCurrentExecutionSuite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *historyCurrentExecutionSuite) SetupSuite() {

}

func (s *historyCurrentExecutionSuite) TearDownSuite() {

}

func (s *historyCurrentExecutionSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *historyCurrentExecutionSuite) TearDownTest() {

}

func (s *historyCurrentExecutionSuite) TestInsert_Success() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	requestID := primitives.NewUUID().String()
	lastWriteVersion := rand.Int63()

	currentExecution := s.newRandomCurrentExecutionRow(shardID, namespaceID, workflowID, runID, requestID, lastWriteVersion)
	result, err := s.store.InsertIntoCurrentExecutions(newExecutionContext(), &currentExecution)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *historyCurrentExecutionSuite) TestInsert_Fail_Duplicate() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	requestID := primitives.NewUUID().String()
	lastWriteVersion := rand.Int63()

	currentExecution := s.newRandomCurrentExecutionRow(shardID, namespaceID, workflowID, runID, requestID, lastWriteVersion)
	result, err := s.store.InsertIntoCurrentExecutions(newExecutionContext(), &currentExecution)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	currentExecution = s.newRandomCurrentExecutionRow(shardID, namespaceID, workflowID, runID, requestID, lastWriteVersion)
	_, err = s.store.InsertIntoCurrentExecutions(newExecutionContext(), &currentExecution)
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *historyCurrentExecutionSuite) TestInsertSelect() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	requestID := primitives.NewUUID().String()
	lastWriteVersion := rand.Int63()

	currentExecution := s.newRandomCurrentExecutionRow(shardID, namespaceID, workflowID, runID, requestID, lastWriteVersion)
	result, err := s.store.InsertIntoCurrentExecutions(newExecutionContext(), &currentExecution)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.CurrentExecutionsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       nil,
	}
	row, err := s.store.SelectFromCurrentExecutions(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal(&currentExecution, row)
}

func (s *historyCurrentExecutionSuite) TestInsertUpdate_Success() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	requestID := primitives.NewUUID().String()
	lastWriteVersion := rand.Int63()

	currentExecution := s.newRandomCurrentExecutionRow(shardID, namespaceID, workflowID, runID, requestID, lastWriteVersion)
	result, err := s.store.InsertIntoCurrentExecutions(newExecutionContext(), &currentExecution)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	currentExecution = s.newRandomCurrentExecutionRow(shardID, namespaceID, workflowID, runID, primitives.NewUUID().String(), rand.Int63())
	result, err = s.store.UpdateCurrentExecutions(newExecutionContext(), &currentExecution)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *historyCurrentExecutionSuite) TestUpdate_Fail() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	requestID := primitives.NewUUID().String()
	lastWriteVersion := rand.Int63()

	currentExecution := s.newRandomCurrentExecutionRow(shardID, namespaceID, workflowID, runID, requestID, lastWriteVersion)
	result, err := s.store.UpdateCurrentExecutions(newExecutionContext(), &currentExecution)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))
}

func (s *historyCurrentExecutionSuite) TestInsertUpdateSelect() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	requestID := primitives.NewUUID().String()
	lastWriteVersion := rand.Int63()

	currentExecution := s.newRandomCurrentExecutionRow(shardID, namespaceID, workflowID, runID, requestID, lastWriteVersion)
	result, err := s.store.InsertIntoCurrentExecutions(newExecutionContext(), &currentExecution)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	currentExecution = s.newRandomCurrentExecutionRow(shardID, namespaceID, workflowID, runID, primitives.NewUUID().String(), rand.Int63())
	result, err = s.store.UpdateCurrentExecutions(newExecutionContext(), &currentExecution)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.CurrentExecutionsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       nil,
	}
	row, err := s.store.SelectFromCurrentExecutions(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal(&currentExecution, row)
}

func (s *historyCurrentExecutionSuite) TestInsertDeleteSelect_Success() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	requestID := primitives.NewUUID().String()
	lastWriteVersion := rand.Int63()

	currentExecution := s.newRandomCurrentExecutionRow(shardID, namespaceID, workflowID, runID, requestID, lastWriteVersion)
	result, err := s.store.InsertIntoCurrentExecutions(newExecutionContext(), &currentExecution)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.CurrentExecutionsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	result, err = s.store.DeleteFromCurrentExecutions(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter.RunID = nil
	_, err = s.store.SelectFromCurrentExecutions(newExecutionContext(), filter)
	s.Error(err) // TODO persistence layer should do proper error translation
}

func (s *historyCurrentExecutionSuite) TestInsertDeleteSelect_Fail() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	requestID := primitives.NewUUID().String()
	lastWriteVersion := rand.Int63()

	currentExecution := s.newRandomCurrentExecutionRow(shardID, namespaceID, workflowID, runID, requestID, lastWriteVersion)
	result, err := s.store.InsertIntoCurrentExecutions(newExecutionContext(), &currentExecution)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	filter := sqlplugin.CurrentExecutionsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       primitives.NewUUID(),
	}
	result, err = s.store.DeleteFromCurrentExecutions(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	filter.RunID = nil
	row, err := s.store.SelectFromCurrentExecutions(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal(&currentExecution, row)
}

func (s *historyCurrentExecutionSuite) TestLock() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	requestID := primitives.NewUUID().String()
	lastWriteVersion := rand.Int63()

	currentExecution := s.newRandomCurrentExecutionRow(shardID, namespaceID, workflowID, runID, requestID, lastWriteVersion)
	result, err := s.store.InsertIntoCurrentExecutions(newExecutionContext(), &currentExecution)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	// NOTE: lock without transaction is equivalent to select
	//  this test only test the select functionality
	filter := sqlplugin.CurrentExecutionsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       nil,
	}
	row, err := s.store.LockCurrentExecutions(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal(&currentExecution, row)
}

func (s *historyCurrentExecutionSuite) newRandomCurrentExecutionRow(
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
	requestID string,
	lastWriteVersion int64,
) sqlplugin.CurrentExecutionsRow {
	state := testHistoryExecutionStates[rand.Intn(len(testHistoryExecutionStates))]
	status := testHistoryExecutionStatus[state][rand.Intn(len(testHistoryExecutionStatus[state]))]
	executionState := &persistencespb.WorkflowExecutionState{
		CreateRequestId: requestID,
		RunId:           runID.String(),
		State:           state,
		Status:          status,
		RequestIds: map[string]*persistencespb.RequestIDInfo{
			uuid.NewString(): {
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_OPTIONS_UPDATED,
				EventId:   common.BufferedEventID,
			},
		},
	}
	executionStateBlob, _ := serialization.WorkflowExecutionStateToBlob(executionState)
	return sqlplugin.CurrentExecutionsRow{
		ShardID:          shardID,
		NamespaceID:      namespaceID,
		WorkflowID:       workflowID,
		RunID:            runID,
		CreateRequestID:  requestID,
		LastWriteVersion: lastWriteVersion,
		State:            state,
		Status:           status,
		Data:             executionStateBlob.Data,
		DataEncoding:     executionStateBlob.EncodingType.String(),
	}
}
