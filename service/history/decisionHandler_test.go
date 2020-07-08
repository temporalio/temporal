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

package history

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	enumspb "go.temporal.io/temporal-proto/enums/v1"
	querypb "go.temporal.io/temporal-proto/query/v1"

	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log/loggerimpl"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/common/persistence"
)

type (
	DecisionHandlerSuite struct {
		*require.Assertions
		suite.Suite

		controller *gomock.Controller

		decisionHandler  *decisionHandlerImpl
		queryRegistry    queryRegistry
		mockMutableState *MockmutableState
	}
)

func TestDecisionHandlerSuite(t *testing.T) {
	suite.Run(t, new(DecisionHandlerSuite))
}

func (s *DecisionHandlerSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.decisionHandler = &decisionHandlerImpl{
		versionChecker: headers.NewVersionChecker(),
		metricsClient:  metrics.NewClient(tally.NoopScope, metrics.History),
		config:         NewDynamicConfigForTest(),
		logger:         loggerimpl.NewNopLogger(),
	}
	s.queryRegistry = s.constructQueryRegistry(10)
	s.mockMutableState = NewMockmutableState(s.controller)
	s.mockMutableState.EXPECT().GetQueryRegistry().Return(s.queryRegistry)
	workflowInfo := &persistence.WorkflowExecutionInfo{
		WorkflowID: testWorkflowID,
		RunID:      testRunID,
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(workflowInfo).AnyTimes()
}

func (s *DecisionHandlerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *DecisionHandlerSuite) TestHandleBufferedQueries_HeartbeatDecision() {
	s.assertQueryCounts(s.queryRegistry, 10, 0, 0, 0)
	queryResults := s.constructQueryResults(s.queryRegistry.getBufferedIDs()[0:5], 10)
	s.decisionHandler.handleBufferedQueries(s.mockMutableState, queryResults, false, testGlobalNamespaceEntry, true)
	s.assertQueryCounts(s.queryRegistry, 10, 0, 0, 0)
}

func (s *DecisionHandlerSuite) TestHandleBufferedQueries_NewDecisionTask() {
	s.assertQueryCounts(s.queryRegistry, 10, 0, 0, 0)
	queryResults := s.constructQueryResults(s.queryRegistry.getBufferedIDs()[0:5], 10)
	s.decisionHandler.handleBufferedQueries(s.mockMutableState, queryResults, true, testGlobalNamespaceEntry, false)
	s.assertQueryCounts(s.queryRegistry, 5, 5, 0, 0)
}

func (s *DecisionHandlerSuite) TestHandleBufferedQueries_NoNewDecisionTask() {
	s.assertQueryCounts(s.queryRegistry, 10, 0, 0, 0)
	queryResults := s.constructQueryResults(s.queryRegistry.getBufferedIDs()[0:5], 10)
	s.decisionHandler.handleBufferedQueries(s.mockMutableState, queryResults, false, testGlobalNamespaceEntry, false)
	s.assertQueryCounts(s.queryRegistry, 0, 5, 5, 0)
}

func (s *DecisionHandlerSuite) TestHandleBufferedQueries_QueryTooLarge() {
	s.assertQueryCounts(s.queryRegistry, 10, 0, 0, 0)
	bufferedIDs := s.queryRegistry.getBufferedIDs()
	queryResults := s.constructQueryResults(bufferedIDs[0:5], 10)
	largeQueryResults := s.constructQueryResults(bufferedIDs[5:10], 10*1024*1024)
	for k, v := range largeQueryResults {
		queryResults[k] = v
	}
	s.decisionHandler.handleBufferedQueries(s.mockMutableState, queryResults, false, testGlobalNamespaceEntry, false)
	s.assertQueryCounts(s.queryRegistry, 0, 5, 0, 5)
}

func (s *DecisionHandlerSuite) constructQueryResults(ids []string, resultSize int) map[string]*querypb.WorkflowQueryResult {
	results := make(map[string]*querypb.WorkflowQueryResult)
	for _, id := range ids {
		results[id] = &querypb.WorkflowQueryResult{
			ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
			Answer:     payloads.EncodeBytes(make([]byte, resultSize, resultSize)),
		}
	}
	return results
}

func (s *DecisionHandlerSuite) constructQueryRegistry(numQueries int) queryRegistry {
	queryRegistry := newQueryRegistry()
	for i := 0; i < numQueries; i++ {
		queryRegistry.bufferQuery(&querypb.WorkflowQuery{})
	}
	return queryRegistry
}

func (s *DecisionHandlerSuite) assertQueryCounts(queryRegistry queryRegistry, buffered, completed, unblocked, failed int) {
	s.Len(queryRegistry.getBufferedIDs(), buffered)
	s.Len(queryRegistry.getCompletedIDs(), completed)
	s.Len(queryRegistry.getUnblockedIDs(), unblocked)
	s.Len(queryRegistry.getFailedIDs(), failed)
}
