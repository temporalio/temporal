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
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/payloads"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
)

type (
	WorkflowTaskHandlerCallbackSuite struct {
		*require.Assertions
		suite.Suite

		controller *gomock.Controller

		workflowTaskHandlerCallback *workflowTaskHandlerCallbacksImpl
		queryRegistry               workflow.QueryRegistry
		mockMutableState            *workflow.MockMutableState
	}
)

func TestWorkflowTaskHandlerCallbackSuite(t *testing.T) {
	suite.Run(t, new(WorkflowTaskHandlerCallbackSuite))
}

func (s *WorkflowTaskHandlerCallbackSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.controller = gomock.NewController(s.T())

	s.workflowTaskHandlerCallback = &workflowTaskHandlerCallbacksImpl{
		metricsClient: metrics.NewNoopMetricsClient(),
		config:        tests.NewDynamicConfig(),
		logger:        log.NewNoopLogger(),
	}
	s.queryRegistry = s.constructQueryRegistry(10)
	s.mockMutableState = workflow.NewMockMutableState(s.controller)
	s.mockMutableState.EXPECT().GetQueryRegistry().Return(s.queryRegistry)
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		WorkflowId: tests.WorkflowID,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{
		RunId: tests.RunID,
	}).AnyTimes()
}

func (s *WorkflowTaskHandlerCallbackSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *WorkflowTaskHandlerCallbackSuite) TestHandleBufferedQueries_HeartbeatWorkflowTask() {
	s.assertQueryCounts(s.queryRegistry, 10, 0, 0, 0)
	queryResults := s.constructQueryResults(s.queryRegistry.GetBufferedIDs()[0:5], 10)
	s.workflowTaskHandlerCallback.handleBufferedQueries(s.mockMutableState, queryResults, false, tests.GlobalNamespaceEntry, true)
	s.assertQueryCounts(s.queryRegistry, 10, 0, 0, 0)
}

func (s *WorkflowTaskHandlerCallbackSuite) TestHandleBufferedQueries_NewWorkflowTask() {
	s.assertQueryCounts(s.queryRegistry, 10, 0, 0, 0)
	queryResults := s.constructQueryResults(s.queryRegistry.GetBufferedIDs()[0:5], 10)
	s.workflowTaskHandlerCallback.handleBufferedQueries(s.mockMutableState, queryResults, true, tests.GlobalNamespaceEntry, false)
	s.assertQueryCounts(s.queryRegistry, 5, 5, 0, 0)
}

func (s *WorkflowTaskHandlerCallbackSuite) TestHandleBufferedQueries_NoNewWorkflowTask() {
	s.assertQueryCounts(s.queryRegistry, 10, 0, 0, 0)
	queryResults := s.constructQueryResults(s.queryRegistry.GetBufferedIDs()[0:5], 10)
	s.workflowTaskHandlerCallback.handleBufferedQueries(s.mockMutableState, queryResults, false, tests.GlobalNamespaceEntry, false)
	s.assertQueryCounts(s.queryRegistry, 0, 5, 5, 0)
}

func (s *WorkflowTaskHandlerCallbackSuite) TestHandleBufferedQueries_QueryTooLarge() {
	s.assertQueryCounts(s.queryRegistry, 10, 0, 0, 0)
	bufferedIDs := s.queryRegistry.GetBufferedIDs()
	queryResults := s.constructQueryResults(bufferedIDs[0:5], 10)
	largeQueryResults := s.constructQueryResults(bufferedIDs[5:10], 10*1024*1024)
	for k, v := range largeQueryResults {
		queryResults[k] = v
	}
	s.workflowTaskHandlerCallback.handleBufferedQueries(s.mockMutableState, queryResults, false, tests.GlobalNamespaceEntry, false)
	s.assertQueryCounts(s.queryRegistry, 0, 5, 0, 5)
}

func (s *WorkflowTaskHandlerCallbackSuite) constructQueryResults(ids []string, resultSize int) map[string]*querypb.WorkflowQueryResult {
	results := make(map[string]*querypb.WorkflowQueryResult)
	for _, id := range ids {
		results[id] = &querypb.WorkflowQueryResult{
			ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
			Answer:     payloads.EncodeBytes(make([]byte, resultSize, resultSize)),
		}
	}
	return results
}

func (s *WorkflowTaskHandlerCallbackSuite) constructQueryRegistry(numQueries int) workflow.QueryRegistry {
	queryRegistry := workflow.NewQueryRegistry()
	for i := 0; i < numQueries; i++ {
		queryRegistry.BufferQuery(&querypb.WorkflowQuery{})
	}
	return queryRegistry
}

func (s *WorkflowTaskHandlerCallbackSuite) assertQueryCounts(queryRegistry workflow.QueryRegistry, buffered, completed, unblocked, failed int) {
	s.Len(queryRegistry.GetBufferedIDs(), buffered)
	s.Len(queryRegistry.GetCompletedIDs(), completed)
	s.Len(queryRegistry.GetUnblockedIDs(), unblocked)
	s.Len(queryRegistry.GetFailedIDs(), failed)
}
