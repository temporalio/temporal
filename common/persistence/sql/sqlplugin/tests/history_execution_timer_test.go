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
	historyExecutionTimerSuite struct {
		suite.Suite
		*require.Assertions

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

func newHistoryExecutionTimerSuite(
	t *testing.T,
	store sqlplugin.HistoryExecutionTimer,
) *historyExecutionTimerSuite {
	return &historyExecutionTimerSuite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *historyExecutionTimerSuite) SetupSuite() {

}

func (s *historyExecutionTimerSuite) TearDownSuite() {

}

func (s *historyExecutionTimerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
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
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *historyExecutionTimerSuite) TestReplace_Multiple() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	timer1 := s.newRandomExecutionTimerRow(shardID, namespaceID, workflowID, runID, shuffle.String(testHistoryExecutionTimerID))
	timer2 := s.newRandomExecutionTimerRow(shardID, namespaceID, workflowID, runID, shuffle.String(testHistoryExecutionTimerID))
	result, err := s.store.ReplaceIntoTimerInfoMaps(newExecutionContext(), []sqlplugin.TimerInfoMapsRow{timer1, timer2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))
}

func (s *historyExecutionTimerSuite) TestReplaceSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	timerID := shuffle.String(testHistoryExecutionTimerID)

	timer := s.newRandomExecutionTimerRow(shardID, namespaceID, workflowID, runID, timerID)
	result, err := s.store.ReplaceIntoTimerInfoMaps(newExecutionContext(), []sqlplugin.TimerInfoMapsRow{timer})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	selectFilter := sqlplugin.TimerInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromTimerInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	rowMap := map[string]sqlplugin.TimerInfoMapsRow{}
	for _, timer := range rows {
		rowMap[timer.TimerID] = timer
	}
	s.Equal(map[string]sqlplugin.TimerInfoMapsRow{
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
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numTimers, int(rowsAffected))

	selectFilter := sqlplugin.TimerInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromTimerInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	timerMap := map[string]sqlplugin.TimerInfoMapsRow{}
	for _, timer := range timers {
		timerMap[timer.TimerID] = timer
	}
	rowMap := map[string]sqlplugin.TimerInfoMapsRow{}
	for _, timer := range rows {
		rowMap[timer.TimerID] = timer
	}
	s.Equal(timerMap, rowMap)
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
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	selectFilter := sqlplugin.TimerInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromTimerInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.TimerInfoMapsRow(nil), rows)
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
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	selectFilter := sqlplugin.TimerInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromTimerInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.TimerInfoMapsRow(nil), rows)
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
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	selectFilter := sqlplugin.TimerInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromTimerInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.TimerInfoMapsRow(nil), rows)
}

func (s *historyExecutionTimerSuite) TestReplaceDeleteSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	timerID := shuffle.String(testHistoryExecutionTimerID)

	timer := s.newRandomExecutionTimerRow(shardID, namespaceID, workflowID, runID, timerID)
	result, err := s.store.ReplaceIntoTimerInfoMaps(newExecutionContext(), []sqlplugin.TimerInfoMapsRow{timer})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	deleteFilter := sqlplugin.TimerInfoMapsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		TimerIDs:    []string{timerID},
	}
	result, err = s.store.DeleteFromTimerInfoMaps(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	selectFilter := sqlplugin.TimerInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromTimerInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.TimerInfoMapsRow(nil), rows)
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
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numTimers, int(rowsAffected))

	deleteFilter := sqlplugin.TimerInfoMapsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
		TimerIDs:    timerIDs,
	}
	result, err = s.store.DeleteFromTimerInfoMaps(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(numTimers, int(rowsAffected))

	selectFilter := sqlplugin.TimerInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromTimerInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.TimerInfoMapsRow(nil), rows)
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
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numTimers, int(rowsAffected))

	deleteFilter := sqlplugin.TimerInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	result, err = s.store.DeleteAllFromTimerInfoMaps(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(numTimers, int(rowsAffected))

	selectFilter := sqlplugin.TimerInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromTimerInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.TimerInfoMapsRow(nil), rows)
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
