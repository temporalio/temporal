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
	historyExecutionBufferSuite struct {
		suite.Suite
		*require.Assertions

		store sqlplugin.HistoryExecutionBuffer
	}
)

const (
	testHistoryExecutionBufferEncoding = "random encoding"
)

var (
	testHistoryExecutionBufferData = []byte("random history execution buffer data")
)

func newHistoryExecutionBufferSuite(
	t *testing.T,
	store sqlplugin.HistoryExecutionBuffer,
) *historyExecutionBufferSuite {
	return &historyExecutionBufferSuite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *historyExecutionBufferSuite) SetupSuite() {

}

func (s *historyExecutionBufferSuite) TearDownSuite() {

}

func (s *historyExecutionBufferSuite) SetupTest() {
	s.Assertions = require.New(s.T())
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
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(1, int(rowsAffected))
}

func (s *historyExecutionBufferSuite) TestInsert_Multiple() {
	shardID := rand.Int31()
	namespaceID := primitives.NewUUID()
	workflowID := shuffle.String(testHistoryExecutionWorkflowID)
	runID := primitives.NewUUID()

	buffer1 := s.newRandomExecutionBufferRow(shardID, namespaceID, workflowID, runID)
	buffer2 := s.newRandomExecutionBufferRow(shardID, namespaceID, workflowID, runID)
	result, err := s.store.InsertIntoBufferedEvents(newExecutionContext(), []sqlplugin.BufferedEventsRow{buffer1, buffer2})
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(2, int(rowsAffected))
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
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numBufferedEvents, int(rowsAffected))

	filter := sqlplugin.BufferedEventsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	rows, err := s.store.SelectFromBufferedEvents(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal(buffers, rows)
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
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(0, int(rowsAffected))

	rows, err := s.store.SelectFromBufferedEvents(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal([]sqlplugin.BufferedEventsRow(nil), rows)
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
	s.NoError(err)
	rowsAffected, err := result.RowsAffected()
	s.NoError(err)
	s.Equal(numBufferedEvents, int(rowsAffected))

	filter := sqlplugin.BufferedEventsFilter{
		ShardID:     shardID,
		NamespaceID: namespaceID,
		WorkflowID:  workflowID,
		RunID:       runID,
	}
	result, err = s.store.DeleteFromBufferedEvents(newExecutionContext(), filter)
	s.NoError(err)
	rowsAffected, err = result.RowsAffected()
	s.NoError(err)
	s.Equal(numBufferedEvents, int(rowsAffected))

	rows, err := s.store.SelectFromBufferedEvents(newExecutionContext(), filter)
	s.NoError(err)
	s.Equal([]sqlplugin.BufferedEventsRow(nil), rows)
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
