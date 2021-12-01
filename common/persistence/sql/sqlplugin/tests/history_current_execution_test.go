// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tests

import (
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
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

func newHistoryCurrentExecutionSuite(
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
	return sqlplugin.CurrentExecutionsRow{
		ShardID:          shardID,
		NamespaceID:      namespaceID,
		WorkflowID:       workflowID,
		RunID:            runID,
		CreateRequestID:  requestID,
		LastWriteVersion: lastWriteVersion,
		State:            state,
		Status:           status,
	}
}
