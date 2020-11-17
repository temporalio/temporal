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

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/shuffle"
)

type (
	historyExecutionSignalRequestSuite struct {
		suite.Suite
		*require.Assertions

		store sqlplugin.HistoryExecutionSignalRequest
	}
)

const (
	testHistoryExecutionSignalID = "random signal ID"
)

func newHistoryExecutionSignalRequestSuite(
	t *testing.T,
	store sqlplugin.HistoryExecutionSignalRequest,
) *historyExecutionSignalRequestSuite {
	return &historyExecutionSignalRequestSuite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *historyExecutionSignalRequestSuite) SetupSuite() {

}

func (s *historyExecutionSignalRequestSuite) TearDownSuite() {

}

func (s *historyExecutionSignalRequestSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *historyExecutionSignalRequestSuite) TearDownTest() {

}

func (s *historyExecutionSignalRequestSuite) TestReplace_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	signalID := shuffle.String(testHistoryExecutionSignalID)

	signalRequest := s.newRandomExecutionSignalRequestRow(shardID, namespaceID, workflowID, runID, signalID)
	result, err := s.store.ReplaceIntoSignalsRequestedSets(newExecutionContext(), []sqlplugin.SignalsRequestedSetsRow{signalRequest})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *historyExecutionSignalRequestSuite) TestReplace_Multiple() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	signalRequest1 := s.newRandomExecutionSignalRequestRow(shardID, namespaceID, workflowID, runID, shuffle.String(testHistoryExecutionSignalID))
	signalRequest2 := s.newRandomExecutionSignalRequestRow(shardID, namespaceID, workflowID, runID, shuffle.String(testHistoryExecutionSignalID))
	result, err := s.store.ReplaceIntoSignalsRequestedSets(newExecutionContext(), []sqlplugin.SignalsRequestedSetsRow{signalRequest1, signalRequest2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))
}

func (s *historyExecutionSignalRequestSuite) TestReplaceSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	signalID := shuffle.String(testHistoryExecutionSignalID)

	signalRequest := s.newRandomExecutionSignalRequestRow(shardID, namespaceID, workflowID, runID, signalID)
	result, err := s.store.ReplaceIntoSignalsRequestedSets(newExecutionContext(), []sqlplugin.SignalsRequestedSetsRow{signalRequest})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	selectFilter := sqlplugin.SignalsRequestedSetsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromSignalsRequestedSets(newExecutionContext(), selectFilter)
	s.NoError(err)
	rowMap := map[string]sqlplugin.SignalsRequestedSetsRow{}
	for _, signalRequest := range rows {
		rowMap[signalRequest.SignalID] = signalRequest
	}
	s.Equal(map[string]sqlplugin.SignalsRequestedSetsRow{
		signalRequest.SignalID: signalRequest,
	}, rowMap)
}

func (s *historyExecutionSignalRequestSuite) TestReplaceSelect_Multiple() {
	numSignalRequests := 20

	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	var signalRequests []sqlplugin.SignalsRequestedSetsRow
	for i := 0; i < numSignalRequests; i++ {
		signalRequest := s.newRandomExecutionSignalRequestRow(shardID, namespaceID, workflowID, runID, shuffle.String(testHistoryExecutionSignalID))
		signalRequests = append(signalRequests, signalRequest)
	}
	result, err := s.store.ReplaceIntoSignalsRequestedSets(newExecutionContext(), signalRequests)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numSignalRequests, int(rowsAffected))

	selectFilter := sqlplugin.SignalsRequestedSetsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromSignalsRequestedSets(newExecutionContext(), selectFilter)
	s.NoError(err)
	signalRequestMap := map[string]sqlplugin.SignalsRequestedSetsRow{}
	for _, signalRequest := range signalRequests {
		signalRequestMap[signalRequest.SignalID] = signalRequest
	}
	rowMap := map[string]sqlplugin.SignalsRequestedSetsRow{}
	for _, signalRequest := range rows {
		rowMap[signalRequest.SignalID] = signalRequest
	}
	s.Equal(signalRequestMap, rowMap)
}

func (s *historyExecutionSignalRequestSuite) TestDeleteSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	signalID := shuffle.String(testHistoryExecutionSignalID)

	deleteFilter := sqlplugin.SignalsRequestedSetsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		SignalIDs:   []string{signalID},
	}
	result, err := s.store.DeleteFromSignalsRequestedSets(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	selectFilter := sqlplugin.SignalsRequestedSetsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromSignalsRequestedSets(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.SignalsRequestedSetsRow(nil), rows)
}

func (s *historyExecutionSignalRequestSuite) TestDeleteSelect_Multiple() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	deleteFilter := sqlplugin.SignalsRequestedSetsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		SignalIDs:   []string{shuffle.String(testHistoryExecutionSignalID), shuffle.String(testHistoryExecutionSignalID)},
	}
	result, err := s.store.DeleteFromSignalsRequestedSets(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	selectFilter := sqlplugin.SignalsRequestedSetsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromSignalsRequestedSets(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.SignalsRequestedSetsRow(nil), rows)
}

func (s *historyExecutionSignalRequestSuite) TestDeleteSelect_All() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	deleteFilter := sqlplugin.SignalsRequestedSetsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	result, err := s.store.DeleteAllFromSignalsRequestedSets(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	selectFilter := sqlplugin.SignalsRequestedSetsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromSignalsRequestedSets(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.SignalsRequestedSetsRow(nil), rows)
}

func (s *historyExecutionSignalRequestSuite) TestReplaceDeleteSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	signalID := shuffle.String(testHistoryExecutionSignalID)

	signalRequest := s.newRandomExecutionSignalRequestRow(shardID, namespaceID, workflowID, runID, signalID)
	result, err := s.store.ReplaceIntoSignalsRequestedSets(newExecutionContext(), []sqlplugin.SignalsRequestedSetsRow{signalRequest})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	deleteFilter := sqlplugin.SignalsRequestedSetsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		SignalIDs:   []string{signalID},
	}
	result, err = s.store.DeleteFromSignalsRequestedSets(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	selectFilter := sqlplugin.SignalsRequestedSetsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromSignalsRequestedSets(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.SignalsRequestedSetsRow(nil), rows)
}

func (s *historyExecutionSignalRequestSuite) TestReplaceDeleteSelect_Multiple() {
	numSignalRequests := 20

	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	var signalRequests []sqlplugin.SignalsRequestedSetsRow
	var signalRequestIDs []string
	for i := 0; i < numSignalRequests; i++ {
		signalRequestID := shuffle.String(testHistoryExecutionSignalID)
		signalRequest := s.newRandomExecutionSignalRequestRow(shardID, namespaceID, workflowID, runID, signalRequestID)
		signalRequestIDs = append(signalRequestIDs, signalRequestID)
		signalRequests = append(signalRequests, signalRequest)
	}
	result, err := s.store.ReplaceIntoSignalsRequestedSets(newExecutionContext(), signalRequests)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numSignalRequests, int(rowsAffected))

	deleteFilter := sqlplugin.SignalsRequestedSetsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		SignalIDs:   signalRequestIDs,
	}
	result, err = s.store.DeleteFromSignalsRequestedSets(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(numSignalRequests, int(rowsAffected))

	selectFilter := sqlplugin.SignalsRequestedSetsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromSignalsRequestedSets(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.SignalsRequestedSetsRow(nil), rows)
}

func (s *historyExecutionSignalRequestSuite) TestReplaceDeleteSelect_All() {
	numSignalRequests := 20

	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	var signalRequests []sqlplugin.SignalsRequestedSetsRow
	for i := 0; i < numSignalRequests; i++ {
		signalRequest := s.newRandomExecutionSignalRequestRow(shardID, namespaceID, workflowID, runID, shuffle.String(testHistoryExecutionSignalID))
		signalRequests = append(signalRequests, signalRequest)
	}
	result, err := s.store.ReplaceIntoSignalsRequestedSets(newExecutionContext(), signalRequests)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numSignalRequests, int(rowsAffected))

	deleteFilter := sqlplugin.SignalsRequestedSetsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	result, err = s.store.DeleteAllFromSignalsRequestedSets(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(numSignalRequests, int(rowsAffected))

	selectFilter := sqlplugin.SignalsRequestedSetsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromSignalsRequestedSets(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.SignalsRequestedSetsRow(nil), rows)
}

func (s *historyExecutionSignalRequestSuite) newRandomExecutionSignalRequestRow(
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
	signalID string,
) sqlplugin.SignalsRequestedSetsRow {
	return sqlplugin.SignalsRequestedSetsRow{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		SignalID:    signalID,
	}
}
