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
	historyExecutionChildWorkflowSuite struct {
		suite.Suite
		*require.Assertions

		store sqlplugin.HistoryExecutionChildWorkflow
	}
)

const (
	testHistoryExecutionChildWorkflowEncoding = "random encoding"
)

var (
	testHistoryExecutionChildWorkflowData = []byte("random history execution child workflow data")
)

func newHistoryExecutionChildWorkflowSuite(
	t *testing.T,
	store sqlplugin.HistoryExecutionChildWorkflow,
) *historyExecutionChildWorkflowSuite {
	return &historyExecutionChildWorkflowSuite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *historyExecutionChildWorkflowSuite) SetupSuite() {

}

func (s *historyExecutionChildWorkflowSuite) TearDownSuite() {

}

func (s *historyExecutionChildWorkflowSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *historyExecutionChildWorkflowSuite) TearDownTest() {

}

func (s *historyExecutionChildWorkflowSuite) TestReplace_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	initiatedID := rand.Int63()

	childWorkflow := s.newRandomExecutionChildWorkflowRow(shardID, namespaceID, workflowID, runID, initiatedID)
	result, err := s.store.ReplaceIntoChildExecutionInfoMaps(newExecutionContext(), []sqlplugin.ChildExecutionInfoMapsRow{childWorkflow})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *historyExecutionChildWorkflowSuite) TestReplace_Multiple() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	childWorkflow1 := s.newRandomExecutionChildWorkflowRow(shardID, namespaceID, workflowID, runID, rand.Int63())
	childWorkflow2 := s.newRandomExecutionChildWorkflowRow(shardID, namespaceID, workflowID, runID, rand.Int63())
	result, err := s.store.ReplaceIntoChildExecutionInfoMaps(newExecutionContext(), []sqlplugin.ChildExecutionInfoMapsRow{childWorkflow1, childWorkflow2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))
}

func (s *historyExecutionChildWorkflowSuite) TestReplaceSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	initiatedID := rand.Int63()

	childWorkflow := s.newRandomExecutionChildWorkflowRow(shardID, namespaceID, workflowID, runID, initiatedID)
	result, err := s.store.ReplaceIntoChildExecutionInfoMaps(newExecutionContext(), []sqlplugin.ChildExecutionInfoMapsRow{childWorkflow})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	selectFilter := sqlplugin.ChildExecutionInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromChildExecutionInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	rowMap := map[int64]sqlplugin.ChildExecutionInfoMapsRow{}
	for _, childWorkflow := range rows {
		rowMap[childWorkflow.InitiatedID] = childWorkflow
	}
	s.Equal(map[int64]sqlplugin.ChildExecutionInfoMapsRow{
		childWorkflow.InitiatedID: childWorkflow,
	}, rowMap)
}

func (s *historyExecutionChildWorkflowSuite) TestReplaceSelect_Multiple() {
	numChildWorkflows := 20

	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	var childWorkflows []sqlplugin.ChildExecutionInfoMapsRow
	for i := 0; i < numChildWorkflows; i++ {
		childWorkflow := s.newRandomExecutionChildWorkflowRow(shardID, namespaceID, workflowID, runID, rand.Int63())
		childWorkflows = append(childWorkflows, childWorkflow)
	}
	result, err := s.store.ReplaceIntoChildExecutionInfoMaps(newExecutionContext(), childWorkflows)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numChildWorkflows, int(rowsAffected))

	selectFilter := sqlplugin.ChildExecutionInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromChildExecutionInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	childWorkflowMap := map[int64]sqlplugin.ChildExecutionInfoMapsRow{}
	for _, childWorkflow := range childWorkflows {
		childWorkflowMap[childWorkflow.InitiatedID] = childWorkflow
	}
	rowMap := map[int64]sqlplugin.ChildExecutionInfoMapsRow{}
	for _, childWorkflow := range rows {
		rowMap[childWorkflow.InitiatedID] = childWorkflow
	}
	s.Equal(childWorkflowMap, rowMap)
}

func (s *historyExecutionChildWorkflowSuite) TestDeleteSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	initiatedID := rand.Int63()

	deleteFilter := sqlplugin.ChildExecutionInfoMapsFilter{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		InitiatedIDs: []int64{initiatedID},
	}
	result, err := s.store.DeleteFromChildExecutionInfoMaps(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	selectFilter := sqlplugin.ChildExecutionInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromChildExecutionInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.ChildExecutionInfoMapsRow(nil), rows)
}

func (s *historyExecutionChildWorkflowSuite) TestDeleteSelect_Multiple() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	deleteFilter := sqlplugin.ChildExecutionInfoMapsFilter{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		InitiatedIDs: []int64{rand.Int63(), rand.Int63()},
	}
	result, err := s.store.DeleteFromChildExecutionInfoMaps(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	selectFilter := sqlplugin.ChildExecutionInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromChildExecutionInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.ChildExecutionInfoMapsRow(nil), rows)
}

func (s *historyExecutionChildWorkflowSuite) TestDeleteSelect_All() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	deleteFilter := sqlplugin.ChildExecutionInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	result, err := s.store.DeleteAllFromChildExecutionInfoMaps(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	selectFilter := sqlplugin.ChildExecutionInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromChildExecutionInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.ChildExecutionInfoMapsRow(nil), rows)
}

func (s *historyExecutionChildWorkflowSuite) TestReplaceDeleteSelect_Single() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()
	initiatedID := rand.Int63()

	childWorkflow := s.newRandomExecutionChildWorkflowRow(shardID, namespaceID, workflowID, runID, initiatedID)
	result, err := s.store.ReplaceIntoChildExecutionInfoMaps(newExecutionContext(), []sqlplugin.ChildExecutionInfoMapsRow{childWorkflow})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	deleteFilter := sqlplugin.ChildExecutionInfoMapsFilter{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		InitiatedIDs: []int64{initiatedID},
	}
	result, err = s.store.DeleteFromChildExecutionInfoMaps(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))

	selectFilter := sqlplugin.ChildExecutionInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromChildExecutionInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.ChildExecutionInfoMapsRow(nil), rows)
}

func (s *historyExecutionChildWorkflowSuite) TestReplaceDeleteSelect_Multiple() {
	numChildWorkflows := 20

	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	var childWorkflows []sqlplugin.ChildExecutionInfoMapsRow
	var childWorkflowInitiatedIDs []int64
	for i := 0; i < numChildWorkflows; i++ {
		childWorkflowInitiatedID := rand.Int63()
		childWorkflow := s.newRandomExecutionChildWorkflowRow(shardID, namespaceID, workflowID, runID, childWorkflowInitiatedID)
		childWorkflowInitiatedIDs = append(childWorkflowInitiatedIDs, childWorkflowInitiatedID)
		childWorkflows = append(childWorkflows, childWorkflow)
	}
	result, err := s.store.ReplaceIntoChildExecutionInfoMaps(newExecutionContext(), childWorkflows)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numChildWorkflows, int(rowsAffected))

	deleteFilter := sqlplugin.ChildExecutionInfoMapsFilter{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		InitiatedIDs: childWorkflowInitiatedIDs,
	}
	result, err = s.store.DeleteFromChildExecutionInfoMaps(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(numChildWorkflows, int(rowsAffected))

	selectFilter := sqlplugin.ChildExecutionInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromChildExecutionInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.ChildExecutionInfoMapsRow(nil), rows)
}

func (s *historyExecutionChildWorkflowSuite) TestReplaceDeleteSelect_All() {
	numChildWorkflows := 20

	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	var childWorkflows []sqlplugin.ChildExecutionInfoMapsRow
	for i := 0; i < numChildWorkflows; i++ {
		childWorkflow := s.newRandomExecutionChildWorkflowRow(shardID, namespaceID, workflowID, runID, rand.Int63())
		childWorkflows = append(childWorkflows, childWorkflow)
	}
	result, err := s.store.ReplaceIntoChildExecutionInfoMaps(newExecutionContext(), childWorkflows)
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numChildWorkflows, int(rowsAffected))

	deleteFilter := sqlplugin.ChildExecutionInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	result, err = s.store.DeleteAllFromChildExecutionInfoMaps(newExecutionContext(), deleteFilter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(numChildWorkflows, int(rowsAffected))

	selectFilter := sqlplugin.ChildExecutionInfoMapsAllFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectAllFromChildExecutionInfoMaps(newExecutionContext(), selectFilter)
	s.NoError(err)
	s.Equal([]sqlplugin.ChildExecutionInfoMapsRow(nil), rows)
}

func (s *historyExecutionChildWorkflowSuite) newRandomExecutionChildWorkflowRow(
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
	initiatedID int64,
) sqlplugin.ChildExecutionInfoMapsRow {
	return sqlplugin.ChildExecutionInfoMapsRow{
		ShardID:      shardID,
		NamespaceID:  namespaceID,
		WorkflowID:   workflowID,
		RunID:        runID,
		InitiatedID:  initiatedID,
		Data:         shuffle.Bytes(testHistoryExecutionChildWorkflowData),
		DataEncoding: testHistoryExecutionChildWorkflowEncoding,
	}
}
