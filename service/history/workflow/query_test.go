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
	"go.temporal.io/server/service/history/definition"
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

func (s *QuerySuite) TestValidateCompletionState() {
	testCases := []struct {
		ts        *definition.QueryCompletionState
		expectErr bool
	}{
		{
			ts:        nil,
			expectErr: true,
		},
		{
			ts: &definition.QueryCompletionState{
				Type: QueryCompletionTypeSucceeded,
			},
			expectErr: true,
		},
		{
			ts: &definition.QueryCompletionState{
				Type:   QueryCompletionTypeSucceeded,
				Result: &querypb.WorkflowQueryResult{},
				Err:    errors.New("err"),
			},
			expectErr: true,
		},
		{
			ts: &definition.QueryCompletionState{
				Type: QueryCompletionTypeSucceeded,
				Result: &querypb.WorkflowQueryResult{
					ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
				},
			},
			expectErr: true,
		},
		{
			ts: &definition.QueryCompletionState{
				Type: QueryCompletionTypeSucceeded,
				Result: &querypb.WorkflowQueryResult{
					ResultType:   enumspb.QUERY_RESULT_TYPE_ANSWERED,
					Answer:       payloads.EncodeBytes([]byte{1, 2, 3}),
					ErrorMessage: "err",
				},
			},
			expectErr: true,
		},
		{
			ts: &definition.QueryCompletionState{
				Type: QueryCompletionTypeSucceeded,
				Result: &querypb.WorkflowQueryResult{
					ResultType: enumspb.QUERY_RESULT_TYPE_FAILED,
					Answer:     payloads.EncodeBytes([]byte{1, 2, 3}),
				},
			},
			expectErr: true,
		},
		{
			ts: &definition.QueryCompletionState{
				Type: QueryCompletionTypeSucceeded,
				Result: &querypb.WorkflowQueryResult{
					ResultType:   enumspb.QUERY_RESULT_TYPE_FAILED,
					ErrorMessage: "err",
				},
			},
			expectErr: false,
		},
		{
			ts: &definition.QueryCompletionState{
				Type: QueryCompletionTypeSucceeded,
				Result: &querypb.WorkflowQueryResult{
					ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
					Answer:     payloads.EncodeBytes([]byte{1, 2, 3}),
				},
			},
			expectErr: false,
		},
		{
			ts: &definition.QueryCompletionState{
				Type:   QueryCompletionTypeUnblocked,
				Result: &querypb.WorkflowQueryResult{},
			},
			expectErr: true,
		},
		{
			ts: &definition.QueryCompletionState{
				Type: QueryCompletionTypeUnblocked,
				Err:  errors.New("err"),
			},
			expectErr: true,
		},
		{
			ts: &definition.QueryCompletionState{
				Type: QueryCompletionTypeUnblocked,
			},
			expectErr: false,
		},
		{
			ts: &definition.QueryCompletionState{
				Type: QueryCompletionTypeFailed,
			},
			expectErr: true,
		},
		{
			ts: &definition.QueryCompletionState{
				Type:   QueryCompletionTypeFailed,
				Result: &querypb.WorkflowQueryResult{},
			},
			expectErr: true,
		},
		{
			ts: &definition.QueryCompletionState{
				Type: QueryCompletionTypeFailed,
				Err:  errors.New("err"),
			},
			expectErr: false,
		},
	}

	queryImpl := &queryImpl{}
	for _, tc := range testCases {
		if tc.expectErr {
			s.Error(queryImpl.validateCompletionState(tc.ts))
		} else {
			s.NoError(queryImpl.validateCompletionState(tc.ts))
		}
	}
}

func (s *QuerySuite) TestCompletionState_Failed() {
	completionStateFailed := &definition.QueryCompletionState{
		Type: QueryCompletionTypeFailed,
		Err:  errors.New("err"),
	}
	s.testSetCompletionState(completionStateFailed)
}

func (s *QuerySuite) TestCompletionState_Completed() {
	answeredCompletionState := &definition.QueryCompletionState{
		Type: QueryCompletionTypeSucceeded,
		Result: &querypb.WorkflowQueryResult{
			ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
			Answer:     payloads.EncodeBytes([]byte{1, 2, 3}),
		},
	}
	s.testSetCompletionState(answeredCompletionState)
}

func (s *QuerySuite) TestCompletionState_Unblocked() {
	unblockedCompletionState := &definition.QueryCompletionState{
		Type: QueryCompletionTypeUnblocked,
	}
	s.testSetCompletionState(unblockedCompletionState)
}

func (s *QuerySuite) testSetCompletionState(completionState *definition.QueryCompletionState) {
	query := newQuery(nil)
	ts, err := query.GetCompletionState()
	s.Equal(errQueryNotInCompletionState, err)
	s.Nil(ts)
	s.False(closed(query.GetCompletionCh()))
	s.Equal(errCompletionStateInvalid, query.SetCompletionState(nil))
	s.NoError(query.SetCompletionState(completionState))
	s.True(closed(query.GetCompletionCh()))
	actualCompletionState, err := query.GetCompletionState()
	s.NoError(err)
	s.assertCompletionStateEqual(completionState, actualCompletionState)
}

func (s *QuerySuite) assertCompletionStateEqual(expected *definition.QueryCompletionState, actual *definition.QueryCompletionState) {
	s.Equal(expected.Type, actual.Type)
	if expected.Err != nil {
		s.Equal(expected.Err.Error(), actual.Err.Error())
	}
	if expected.Result != nil {
		s.EqualValues(actual.Result, expected.Result)
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
