// The MIT License (MIT)
//
// Copyright (c) 2019 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package history

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
)

type QuerySuite struct {
	*require.Assertions
	suite.Suite
}

func TestQuerySuite(t *testing.T) {
	suite.Run(t, new(QuerySuite))
}

func (s *QuerySuite) SetupTest() {
	s.Assertions = require.New(s.T())
}

func (s *QuerySuite) TestQuery() {
	q := newQuery(&shared.WorkflowQuery{})
	s.assertBufferedState(q)
	s.NoError(q.completeQuery(&shared.WorkflowQueryResult{
		ResultType: common.QueryResultTypePtr(shared.QueryResultTypeAnswered),
		Answer:     []byte{1, 2, 3},
	}))
	s.assertCompletedState(q)
}

func (s *QuerySuite) TestValidateQueryResult() {
	testCases := []struct {
		wqr       *shared.WorkflowQueryResult
		expectErr bool
	}{
		{
			wqr:       nil,
			expectErr: true,
		},
		{
			wqr:       &shared.WorkflowQueryResult{},
			expectErr: true,
		},
		{
			wqr: &shared.WorkflowQueryResult{
				ResultType: common.QueryResultTypePtr(shared.QueryResultTypeAnswered),
			},
			expectErr: true,
		},
		{
			wqr: &shared.WorkflowQueryResult{
				ResultType: common.QueryResultTypePtr(shared.QueryResultTypeAnswered),
				Answer:     []byte{1, 2, 3},
			},
			expectErr: false,
		},
		{
			wqr: &shared.WorkflowQueryResult{
				ResultType:  common.QueryResultTypePtr(shared.QueryResultTypeAnswered),
				Answer:      []byte{1, 2, 3},
				ErrorReason: common.StringPtr("should not exist"),
			},
			expectErr: true,
		},
		{
			wqr: &shared.WorkflowQueryResult{
				ResultType: common.QueryResultTypePtr(shared.QueryResultTypeFailed),
				Answer:     []byte{1, 2, 3},
			},
			expectErr: true,
		},
		{
			wqr: &shared.WorkflowQueryResult{
				ResultType:  common.QueryResultTypePtr(shared.QueryResultTypeFailed),
				ErrorReason: common.StringPtr("some error reason"),
			},
			expectErr: true,
		},
		{
			wqr: &shared.WorkflowQueryResult{
				ResultType:   common.QueryResultTypePtr(shared.QueryResultTypeFailed),
				ErrorReason:  common.StringPtr("some error reason"),
				ErrorDetails: []byte{1, 2, 3},
			},
			expectErr: false,
		},
	}
	for _, tc := range testCases {
		if tc.expectErr {
			s.Error(validateQueryResult(tc.wqr))
		} else {
			s.NoError(validateQueryResult(tc.wqr))
		}
	}
}

func (s *QuerySuite) assertBufferedState(q query) {
	snapshot := q.getQueryInternalState()
	s.Equal(queryStateBuffered, snapshot.state)
	s.NotNil(snapshot.queryInput)
	s.Nil(snapshot.queryResult)
	termCh := q.getQueryTermCh()
	s.False(closed(termCh))
}

func (s *QuerySuite) assertCompletedState(q query) {
	snapshot := q.getQueryInternalState()
	s.Equal(queryStateCompleted, snapshot.state)
	s.NotNil(snapshot.queryInput)
	s.NotNil(snapshot.queryResult)
	termCh := q.getQueryTermCh()
	s.True(closed(termCh))
}

func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
