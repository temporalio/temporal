// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
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
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/shuffle"
)

type (
	historyExecutionSuite struct {
		suite.Suite
		*require.Assertions

		store sqlplugin.HistoryExecution
	}
)

const (
	testHistoryExecutionEncoding                 = "random encoding"
	testHistoryExecutionStateEncoding            = "random encoding"
	testHistoryExecutionVersionHistoriesEncoding = "random encoding"
)

var (
	testHistoryExecutionData                 = []byte("random history execution data")
	testHistoryExecutionStateData            = []byte("random history execution state data")
	testHistoryExecutionVersionHistoriesData = []byte("random history execution version histories data")
)

func newHistoryExecutionSuite(
	t *testing.T,
	store sqlplugin.HistoryExecution,
) *historyExecutionSuite {
	return &historyExecutionSuite{
		Assertions: require.New(t),
		store:      store,
	}
}

func (s *historyExecutionSuite) SetupSuite() {

}

func (s *historyExecutionSuite) TearDownSuite() {

}

func (s *historyExecutionSuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *historyExecutionSuite) TearDownTest() {

}

func (s *historyExecutionSuite) newRandomExecutionRow(
	shardID int32,
	namespaceID primitives.UUID,
	workflowID string,
	runID primitives.UUID,
	nextEventID int64,
	lastWriteVersion int64,
) sqlplugin.ExecutionsRow {
	return sqlplugin.ExecutionsRow{
		ShardID:                  shardID,
		NamespaceID:              namespaceID,
		WorkflowID:               workflowID,
		RunID:                    runID,
		NextEventID:              nextEventID,
		LastWriteVersion:         lastWriteVersion,
		Data:                     shuffle.Bytes(testHistoryExecutionData),
		DataEncoding:             testHistoryExecutionEncoding,
		State:                    shuffle.Bytes(testHistoryExecutionStateData),
		StateEncoding:            testHistoryExecutionStateEncoding,
		VersionHistories:         shuffle.Bytes(testHistoryExecutionVersionHistoriesData),
		VersionHistoriesEncoding: testHistoryExecutionVersionHistoriesEncoding,
	}
}
