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

package workflow

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"

	"go.temporal.io/server/common/payloads"
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
		ts        *QueryTerminationState
		expectErr bool
	}{
		{
			ts:        nil,
			expectErr: true,
		},
		{
			ts: &QueryTerminationState{
				QueryTerminationType: QueryTerminationTypeCompleted,
			},
			expectErr: true,
		},
		{
			ts: &QueryTerminationState{
				QueryTerminationType: QueryTerminationTypeCompleted,
				QueryResult:          &querypb.WorkflowQueryResult{},
				Failure:              errors.New("err"),
			},
			expectErr: true,
		},
		{
			ts: &QueryTerminationState{
				QueryTerminationType: QueryTerminationTypeCompleted,
				QueryResult: &querypb.WorkflowQueryResult{
					ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
				},
			},
			expectErr: true,
		},
		{
			ts: &QueryTerminationState{
				QueryTerminationType: QueryTerminationTypeCompleted,
				QueryResult: &querypb.WorkflowQueryResult{
					ResultType:   enumspb.QUERY_RESULT_TYPE_ANSWERED,
					Answer:       payloads.EncodeBytes([]byte{1, 2, 3}),
					ErrorMessage: "err",
				},
			},
			expectErr: true,
		},
		{
			ts: &QueryTerminationState{
				QueryTerminationType: QueryTerminationTypeCompleted,
				QueryResult: &querypb.WorkflowQueryResult{
					ResultType: enumspb.QUERY_RESULT_TYPE_FAILED,
					Answer:     payloads.EncodeBytes([]byte{1, 2, 3}),
				},
			},
			expectErr: true,
		},
		{
			ts: &QueryTerminationState{
				QueryTerminationType: QueryTerminationTypeCompleted,
				QueryResult: &querypb.WorkflowQueryResult{
					ResultType:   enumspb.QUERY_RESULT_TYPE_FAILED,
					ErrorMessage: "err",
				},
			},
			expectErr: false,
		},
		{
			ts: &QueryTerminationState{
				QueryTerminationType: QueryTerminationTypeCompleted,
				QueryResult: &querypb.WorkflowQueryResult{
					ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
					Answer:     payloads.EncodeBytes([]byte{1, 2, 3}),
				},
			},
			expectErr: false,
		},
		{
			ts: &QueryTerminationState{
				QueryTerminationType: QueryTerminationTypeUnblocked,
				QueryResult:          &querypb.WorkflowQueryResult{},
			},
			expectErr: true,
		},
		{
			ts: &QueryTerminationState{
				QueryTerminationType: QueryTerminationTypeUnblocked,
				Failure:              errors.New("err"),
			},
			expectErr: true,
		},
		{
			ts: &QueryTerminationState{
				QueryTerminationType: QueryTerminationTypeUnblocked,
			},
			expectErr: false,
		},
		{
			ts: &QueryTerminationState{
				QueryTerminationType: QueryTerminationTypeFailed,
			},
			expectErr: true,
		},
		{
			ts: &QueryTerminationState{
				QueryTerminationType: QueryTerminationTypeFailed,
				QueryResult:          &querypb.WorkflowQueryResult{},
			},
			expectErr: true,
		},
		{
			ts: &QueryTerminationState{
				QueryTerminationType: QueryTerminationTypeFailed,
				Failure:              errors.New("err"),
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
	failedTerminationState := &QueryTerminationState{
		QueryTerminationType: QueryTerminationTypeFailed,
		Failure:              errors.New("err"),
	}
	s.testSetTerminationState(failedTerminationState)
}

func (s *QuerySuite) TestTerminationState_Completed() {
	answeredTerminationState := &QueryTerminationState{
		QueryTerminationType: QueryTerminationTypeCompleted,
		QueryResult: &querypb.WorkflowQueryResult{
			ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
			Answer:     payloads.EncodeBytes([]byte{1, 2, 3}),
		},
	}
	s.testSetTerminationState(answeredTerminationState)
}

func (s *QuerySuite) TestTerminationState_Unblocked() {
	unblockedTerminationState := &QueryTerminationState{
		QueryTerminationType: QueryTerminationTypeUnblocked,
	}
	s.testSetTerminationState(unblockedTerminationState)
}

func (s *QuerySuite) testSetTerminationState(terminationState *QueryTerminationState) {
	query := newQuery(nil)
	ts, err := query.GetTerminationState()
	s.Equal(errQueryNotInTerminalState, err)
	s.Nil(ts)
	s.False(closed(query.getQueryTermCh()))
	s.Equal(errTerminationStateInvalid, query.setTerminationState(nil))
	s.NoError(query.setTerminationState(terminationState))
	s.True(closed(query.getQueryTermCh()))
	actualTerminationState, err := query.GetTerminationState()
	s.NoError(err)
	s.assertTerminationStateEqual(terminationState, actualTerminationState)
}

func (s *QuerySuite) assertTerminationStateEqual(expected *QueryTerminationState, actual *QueryTerminationState) {
	s.Equal(expected.QueryTerminationType, actual.QueryTerminationType)
	if expected.Failure != nil {
		s.Equal(expected.Failure.Error(), actual.Failure.Error())
	}
	if expected.QueryResult != nil {
		s.EqualValues(actual.QueryResult, expected.QueryResult)
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
