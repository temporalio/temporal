package history

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	querypb "go.temporal.io/temporal-proto/query"
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
				queryResult:          &querypb.WorkflowQueryResult{},
				failure:              errors.New("err"),
			},
			expectErr: true,
		},
		{
			ts: &queryTerminationState{
				queryTerminationType: queryTerminationTypeCompleted,
				queryResult: &querypb.WorkflowQueryResult{
					ResultType: querypb.QueryResultType_Answered,
				},
			},
			expectErr: true,
		},
		{
			ts: &queryTerminationState{
				queryTerminationType: queryTerminationTypeCompleted,
				queryResult: &querypb.WorkflowQueryResult{
					ResultType:   querypb.QueryResultType_Answered,
					Answer:       []byte{1, 2, 3},
					ErrorMessage: "err",
				},
			},
			expectErr: true,
		},
		{
			ts: &queryTerminationState{
				queryTerminationType: queryTerminationTypeCompleted,
				queryResult: &querypb.WorkflowQueryResult{
					ResultType: querypb.QueryResultType_Failed,
					Answer:     []byte{1, 2, 3},
				},
			},
			expectErr: true,
		},
		{
			ts: &queryTerminationState{
				queryTerminationType: queryTerminationTypeCompleted,
				queryResult: &querypb.WorkflowQueryResult{
					ResultType:   querypb.QueryResultType_Failed,
					ErrorMessage: "err",
				},
			},
			expectErr: false,
		},
		{
			ts: &queryTerminationState{
				queryTerminationType: queryTerminationTypeCompleted,
				queryResult: &querypb.WorkflowQueryResult{
					ResultType: querypb.QueryResultType_Answered,
					Answer:     []byte{1, 2, 3},
				},
			},
			expectErr: false,
		},
		{
			ts: &queryTerminationState{
				queryTerminationType: queryTerminationTypeUnblocked,
				queryResult:          &querypb.WorkflowQueryResult{},
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
				queryResult:          &querypb.WorkflowQueryResult{},
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
		queryResult: &querypb.WorkflowQueryResult{
			ResultType: querypb.QueryResultType_Answered,
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
		s.EqualValues(actual.queryResult, expected.queryResult)
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
