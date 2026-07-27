package gcloud

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
)

type queryParserSuite struct {
	*require.Assertions
	suite.Suite

	parser QueryParser
}

func TestQueryParserSuite(t *testing.T) {
	suite.Run(t, new(queryParserSuite))
}

func (s *queryParserSuite) SetupTest() {
	s.Assertions = require.New(s.T())
	s.parser = NewQueryParser()
}

func (s *queryParserSuite) TestParseExecutionStatus() {
	const commonQueryPart = "CloseTime = 1000 and SearchPrecision = 'Day' and "

	testCases := []struct {
		query       string
		expectErr   bool
		status      *enumspb.WorkflowExecutionStatus
		emptyResult bool
	}{
		{
			query:  commonQueryPart + "ExecutionStatus = \"Completed\"",
			status: new(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED),
		},
		{
			query:  commonQueryPart + "ExecutionStatus = 'failed'",
			status: new(enumspb.WORKFLOW_EXECUTION_STATUS_FAILED),
		},
		{
			query:  commonQueryPart + "ExecutionStatus = 'TIMED_OUT'",
			status: new(enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT),
		},
		{
			query:  commonQueryPart + "ExecutionStatus = 4",
			status: new(enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED),
		},
		{
			// no ExecutionStatus filter leaves the field unset
			query:  "CloseTime = 1000 and SearchPrecision = 'Day'",
			status: nil,
		},
		{
			// conflicting values can never match, matching how gcloud treats
			// conflicting WorkflowId/RunId/WorkflowType filters
			query:       commonQueryPart + "ExecutionStatus = 'Failed' and ExecutionStatus = 'Completed'",
			status:      new(enumspb.WORKFLOW_EXECUTION_STATUS_FAILED),
			emptyResult: true,
		},
		{
			query:     commonQueryPart + "ExecutionStatus = \"unknown\"",
			expectErr: true,
		},
		{
			query:     commonQueryPart + "ExecutionStatus > \"Failed\"",
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		parsedQuery, err := s.parser.Parse(tc.query)
		if tc.expectErr {
			s.Error(err, tc.query)
			continue
		}
		s.NoError(err, tc.query)
		s.Equal(tc.status, parsedQuery.status, tc.query)
		s.Equal(tc.emptyResult, parsedQuery.emptyResult, tc.query)
	}
}
