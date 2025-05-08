package workflow

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	querypb "go.temporal.io/api/query/v1"
	"go.temporal.io/server/common/payloads"
	historyi "go.temporal.io/server/service/history/interfaces"
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
		ts        *historyi.QueryCompletionState
		expectErr bool
	}{
		{
			ts:        nil,
			expectErr: true,
		},
		{
			ts: &historyi.QueryCompletionState{
				Type: QueryCompletionTypeSucceeded,
			},
			expectErr: true,
		},
		{
			ts: &historyi.QueryCompletionState{
				Type:   QueryCompletionTypeSucceeded,
				Result: &querypb.WorkflowQueryResult{},
				Err:    errors.New("err"),
			},
			expectErr: true,
		},
		{
			ts: &historyi.QueryCompletionState{
				Type: QueryCompletionTypeSucceeded,
				Result: &querypb.WorkflowQueryResult{
					ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
				},
			},
			expectErr: true,
		},
		{
			ts: &historyi.QueryCompletionState{
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
			ts: &historyi.QueryCompletionState{
				Type: QueryCompletionTypeSucceeded,
				Result: &querypb.WorkflowQueryResult{
					ResultType: enumspb.QUERY_RESULT_TYPE_FAILED,
					Answer:     payloads.EncodeBytes([]byte{1, 2, 3}),
				},
			},
			expectErr: true,
		},
		{
			ts: &historyi.QueryCompletionState{
				Type: QueryCompletionTypeSucceeded,
				Result: &querypb.WorkflowQueryResult{
					ResultType:   enumspb.QUERY_RESULT_TYPE_FAILED,
					ErrorMessage: "err",
				},
			},
			expectErr: false,
		},
		{
			ts: &historyi.QueryCompletionState{
				Type: QueryCompletionTypeSucceeded,
				Result: &querypb.WorkflowQueryResult{
					ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
					Answer:     payloads.EncodeBytes([]byte{1, 2, 3}),
				},
			},
			expectErr: false,
		},
		{
			ts: &historyi.QueryCompletionState{
				Type:   QueryCompletionTypeUnblocked,
				Result: &querypb.WorkflowQueryResult{},
			},
			expectErr: true,
		},
		{
			ts: &historyi.QueryCompletionState{
				Type: QueryCompletionTypeUnblocked,
				Err:  errors.New("err"),
			},
			expectErr: true,
		},
		{
			ts: &historyi.QueryCompletionState{
				Type: QueryCompletionTypeUnblocked,
			},
			expectErr: false,
		},
		{
			ts: &historyi.QueryCompletionState{
				Type: QueryCompletionTypeFailed,
			},
			expectErr: true,
		},
		{
			ts: &historyi.QueryCompletionState{
				Type:   QueryCompletionTypeFailed,
				Result: &querypb.WorkflowQueryResult{},
			},
			expectErr: true,
		},
		{
			ts: &historyi.QueryCompletionState{
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
	completionStateFailed := &historyi.QueryCompletionState{
		Type: QueryCompletionTypeFailed,
		Err:  errors.New("err"),
	}
	s.testSetCompletionState(completionStateFailed)
}

func (s *QuerySuite) TestCompletionState_Completed() {
	answeredCompletionState := &historyi.QueryCompletionState{
		Type: QueryCompletionTypeSucceeded,
		Result: &querypb.WorkflowQueryResult{
			ResultType: enumspb.QUERY_RESULT_TYPE_ANSWERED,
			Answer:     payloads.EncodeBytes([]byte{1, 2, 3}),
		},
	}
	s.testSetCompletionState(answeredCompletionState)
}

func (s *QuerySuite) TestCompletionState_Unblocked() {
	unblockedCompletionState := &historyi.QueryCompletionState{
		Type: QueryCompletionTypeUnblocked,
	}
	s.testSetCompletionState(unblockedCompletionState)
}

func (s *QuerySuite) testSetCompletionState(completionState *historyi.QueryCompletionState) {
	query := newQuery(nil)
	ts, err := query.GetCompletionState()
	s.Equal(errQueryNotInCompletionState, err)
	s.Nil(ts)
	s.False(closed(query.getCompletionCh()))
	s.Equal(errCompletionStateInvalid, query.setCompletionState(nil))
	s.NoError(query.setCompletionState(completionState))
	s.True(closed(query.getCompletionCh()))
	actualCompletionState, err := query.GetCompletionState()
	s.NoError(err)
	s.assertCompletionStateEqual(completionState, actualCompletionState)
}

func (s *QuerySuite) assertCompletionStateEqual(expected *historyi.QueryCompletionState, actual *historyi.QueryCompletionState) {
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
