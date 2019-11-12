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
	"errors"
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

func (s *QuerySuite) TestValidateTerminationState() {
	testCases := []struct {
		ts        *queryTerminationState
		expectErr bool
	}{
		{
			ts:        nil,
			expectErr: true,
		},
		{
			ts: &queryTerminationState{
				queryTerminationType: queryTerminationTypeCompleted,
			},
			expectErr: true,
		},
		{
			ts: &queryTerminationState{
				queryTerminationType: queryTerminationTypeCompleted,
				queryResult:          &shared.WorkflowQueryResult{},
				failure:              errors.New("err"),
			},
			expectErr: true,
		},
		{
			ts: &queryTerminationState{
				queryTerminationType: queryTerminationTypeCompleted,
				queryResult: &shared.WorkflowQueryResult{
					ResultType: common.QueryResultTypePtr(shared.QueryResultTypeAnswered),
				},
			},
			expectErr: true,
		},
		{
			ts: &queryTerminationState{
				queryTerminationType: queryTerminationTypeCompleted,
				queryResult: &shared.WorkflowQueryResult{
					ResultType:   common.QueryResultTypePtr(shared.QueryResultTypeAnswered),
					Answer:       []byte{1, 2, 3},
					ErrorMessage: common.StringPtr("err"),
				},
			},
			expectErr: true,
		},
		{
			ts: &queryTerminationState{
				queryTerminationType: queryTerminationTypeCompleted,
				queryResult: &shared.WorkflowQueryResult{
					ResultType: common.QueryResultTypePtr(shared.QueryResultTypeFailed),
					Answer:     []byte{1, 2, 3},
				},
			},
			expectErr: true,
		},
		{
			ts: &queryTerminationState{
				queryTerminationType: queryTerminationTypeCompleted,
				queryResult: &shared.WorkflowQueryResult{
					ResultType:   common.QueryResultTypePtr(shared.QueryResultTypeFailed),
					ErrorMessage: common.StringPtr("err"),
				},
			},
			expectErr: false,
		},
		{
			ts: &queryTerminationState{
				queryTerminationType: queryTerminationTypeCompleted,
				queryResult: &shared.WorkflowQueryResult{
					ResultType: common.QueryResultTypePtr(shared.QueryResultTypeAnswered),
					Answer:     []byte{1, 2, 3},
				},
			},
			expectErr: false,
		},
		{
			ts: &queryTerminationState{
				queryTerminationType: queryTerminationTypeUnblocked,
				queryResult:          &shared.WorkflowQueryResult{},
			},
			expectErr: true,
		},
		{
			ts: &queryTerminationState{
				queryTerminationType: queryTerminationTypeUnblocked,
				failure:              errors.New("err"),
			},
			expectErr: true,
		},
		{
			ts: &queryTerminationState{
				queryTerminationType: queryTerminationTypeUnblocked,
			},
			expectErr: false,
		},
		{
			ts: &queryTerminationState{
				queryTerminationType: queryTerminationTypeFailed,
			},
			expectErr: true,
		},
		{
			ts: &queryTerminationState{
				queryTerminationType: queryTerminationTypeFailed,
				queryResult:          &shared.WorkflowQueryResult{},
			},
			expectErr: true,
		},
		{
			ts: &queryTerminationState{
				queryTerminationType: queryTerminationTypeFailed,
				failure:              errors.New("err"),
			},
			expectErr: false,
		},
	}

	queryImpl := &queryImpl{}
	for _, tc := range testCases {
		if tc.expectErr {
			s.Error(queryImpl.validateTerminationState(tc.ts))
		} else {
			s.NoError(queryImpl.validateTerminationState(tc.ts))
		}
	}
}

func (s *QuerySuite) TestTerminationState_Failed() {
	failedTerminationState := &queryTerminationState{
		queryTerminationType: queryTerminationTypeFailed,
		failure:              errors.New("err"),
	}
	s.testSetTerminationState(failedTerminationState)
}

func (s *QuerySuite) TestTerminationState_Completed() {
	answeredTerminationState := &queryTerminationState{
		queryTerminationType: queryTerminationTypeCompleted,
		queryResult: &shared.WorkflowQueryResult{
			ResultType: common.QueryResultTypePtr(shared.QueryResultTypeAnswered),
			Answer:     []byte{1, 2, 3},
		},
	}
	s.testSetTerminationState(answeredTerminationState)
}

func (s *QuerySuite) TestTerminationState_Unblocked() {
	unblockedTerminationState := &queryTerminationState{
		queryTerminationType: queryTerminationTypeUnblocked,
	}
	s.testSetTerminationState(unblockedTerminationState)
}

func (s *QuerySuite) testSetTerminationState(terminationState *queryTerminationState) {
	query := newQuery(nil)
	ts, err := query.getTerminationState()
	s.Equal(errQueryNotInTerminalState, err)
	s.Nil(ts)
	s.False(closed(query.getQueryTermCh()))
	s.Equal(errTerminationStateInvalid, query.setTerminationState(nil))
	s.NoError(query.setTerminationState(terminationState))
	s.True(closed(query.getQueryTermCh()))
	actualTerminationState, err := query.getTerminationState()
	s.NoError(err)
	s.assertTerminationStateEqual(terminationState, actualTerminationState)
}

func (s *QuerySuite) assertTerminationStateEqual(expected *queryTerminationState, actual *queryTerminationState) {
	s.Equal(expected.queryTerminationType, actual.queryTerminationType)
	if expected.failure != nil {
		s.Equal(expected.failure.Error(), actual.failure.Error())
	}
	if expected.queryResult != nil {
		s.True(expected.queryResult.Equals(actual.queryResult))
	}
}

func closed(ch <-chan struct{}) bool {
	select {
	case <-ch:
		return true
	default:
		return false
	}
}
