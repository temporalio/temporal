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
	result, err := s.store.ReplaceIntoSignalInfoMaps(newExecutionContext(), []sqlplugin.SignalInfoMapsRow{signal})
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
	result, err := s.store.ReplaceIntoSignalInfoMaps(newExecutionContext(), []sqlplugin.SignalInfoMapsRow{signal1, signal2})
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
	result, err := s.store.ReplaceIntoSignalInfoMaps(newExecutionContext(), []sqlplugin.SignalInfoMapsRow{signal})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	selectFilter := sqlplugin.SignalInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromSignalInfoMaps(newExecutionContext(), selectFilter)
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
	result, err := s.store.ReplaceIntoSignalInfoMaps(newExecutionContext(), signals)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numSignals, int(rowsAffected))

	selectFilter := sqlplugin.SignalInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromSignalInfoMaps(newExecutionContext(), selectFilter)
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

	deleteFilter := sqlplugin.SignalInfoMapsFilter{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		InitiatedIDs: []int64{initiatedID},
	}
	result, err := s.store.DeleteFromSignalInfoMaps(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	selectFilter := sqlplugin.SignalInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromSignalInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.SignalInfoMapsRow(nil), rows)
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
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	selectFilter := sqlplugin.SignalInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromSignalInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.SignalInfoMapsRow(nil), rows)
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
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	selectFilter := sqlplugin.SignalInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromSignalInfoMaps(newExecutionContext(), selectFilter)
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
	result, err := s.store.ReplaceIntoSignalInfoMaps(newExecutionContext(), []sqlplugin.SignalInfoMapsRow{signal})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	deleteFilter := sqlplugin.SignalInfoMapsFilter{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		InitiatedIDs: []int64{initiatedID},
	}
	result, err = s.store.DeleteFromSignalInfoMaps(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	selectFilter := sqlplugin.SignalInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromSignalInfoMaps(newExecutionContext(), selectFilter)
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
	var signalInitiatedIDs []int64
	for i := 0; i < numSignals; i++ {
		signalInitiatedID := rand.Int63()
		signal := s.newRandomExecutionSignalRow(shardID, namespaceID, workflowID, runID, signalInitiatedID)
		signalInitiatedIDs = append(signalInitiatedIDs, signalInitiatedID)
		signals = append(signals, signal)
	}
	result, err := s.store.ReplaceIntoSignalInfoMaps(newExecutionContext(), signals)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numSignals, int(rowsAffected))

	deleteFilter := sqlplugin.SignalInfoMapsFilter{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		InitiatedIDs: signalInitiatedIDs,
	}
	result, err = s.store.DeleteFromSignalInfoMaps(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(numSignals, int(rowsAffected))

	selectFilter := sqlplugin.SignalInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromSignalInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.SignalInfoMapsRow(nil), rows)
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
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numSignals, int(rowsAffected))

	deleteFilter := sqlplugin.SignalInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	result, err = s.store.DeleteAllFromSignalInfoMaps(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(numSignals, int(rowsAffected))

	selectFilter := sqlplugin.SignalInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromSignalInfoMaps(newExecutionContext(), selectFilter)
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
