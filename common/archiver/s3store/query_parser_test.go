package s3store

import (
	"fmt"
	"testing"
	"time"

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

func (s *queryParserSuite) TestParseWorkflowIDAndWorkflowTypeName() {
	testCases := []struct {
		query       string
		expectErr   bool
		parsedQuery *parsedQuery
	}{
		{
			query:     "WorkflowId = \"random workflowID\"",
			expectErr: false,
			parsedQuery: &parsedQuery{
				workflowID: new("random workflowID"),
			},
		},
		{
			query:     "WorkflowTypeName = \"random workflowTypeName\"",
			expectErr: false,
			parsedQuery: &parsedQuery{
				workflowType: new("random workflowTypeName"),
			},
		},
		{
			query:     "WorkflowType = \"random workflowType\"",
			expectErr: false,
			parsedQuery: &parsedQuery{
				workflowType: new("random workflowType"),
			},
		},
		{
			query:     "WorkflowId = \"random workflowID\" and WorkflowTypeName = \"random workflowTypeName\"",
			expectErr: true,
		},
		{
			query:     "WorkflowId = \"random workflowID\" and WorkflowType = \"random workflowTypeName\"",
			expectErr: true,
		},
		{
			query:     "WorkflowTypeName = \"random workflowTypeName\" and WorkflowType = \"random workflowTypeName\"",
			expectErr: true,
		},
		{
			query:     "WorkflowId = \"random workflowID\" and WorkflowId = \"random workflowID\"",
			expectErr: true,
		},
		{
			query:     "RunId = \"random runID\"",
			expectErr: true,
		},
		{
			query:     "WorkflowId = 'random workflowID'",
			expectErr: false,
			parsedQuery: &parsedQuery{
				workflowID: new("random workflowID"),
			},
		},
		{
			query:     "(WorkflowId = \"random workflowID\")",
			expectErr: false,
			parsedQuery: &parsedQuery{
				workflowID: new("random workflowID"),
			},
		},
		{
			query:     "runId = random workflowID",
			expectErr: true,
		},
		{
			query:     "WorkflowId = \"random workflowID\" or WorkflowId = \"another workflowID\"",
			expectErr: true,
		},
		{
			query:     "WorkflowId = \"random workflowID\" or runId = \"random runID\"",
			expectErr: true,
		},
		{
			query:     "workflowid = \"random workflowID\"",
			expectErr: true,
		},
		{
			query:     "runId > \"random workflowID\"",
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		parsedQuery, err := s.parser.Parse(tc.query)
		if tc.expectErr {
			s.Error(err)
			continue
		}
		s.NoError(err)
		s.Equal(tc.parsedQuery.workflowID, parsedQuery.workflowID)
		s.Equal(tc.parsedQuery.workflowType, parsedQuery.workflowType)

	}
}

func (s *queryParserSuite) TestParsePrecision() {
	commonQueryPart := "WorkflowId = \"random workflowID\" AND "
	testCases := []struct {
		query       string
		expectErr   bool
		parsedQuery *parsedQuery
	}{
		{
			query:     commonQueryPart + "CloseTime = 1000 and SearchPrecision = 'Day'",
			expectErr: false,
			parsedQuery: &parsedQuery{
				searchPrecision: new(PrecisionDay),
			},
		},
		{
			query:     commonQueryPart + "CloseTime = 1000 and SearchPrecision = 'Hour'",
			expectErr: false,
			parsedQuery: &parsedQuery{
				searchPrecision: new(PrecisionHour),
			},
		},
		{
			query:     commonQueryPart + "CloseTime = 1000 and SearchPrecision = 'Minute'",
			expectErr: false,
			parsedQuery: &parsedQuery{
				searchPrecision: new(PrecisionMinute),
			},
		},
		{
			query:     commonQueryPart + "StartTime = 1000 and SearchPrecision = 'Second'",
			expectErr: false,
			parsedQuery: &parsedQuery{
				searchPrecision: new(PrecisionSecond),
			},
		},
		{
			query:     commonQueryPart + "SearchPrecision = 'Second'",
			expectErr: true,
		},
		{
			query:     commonQueryPart + "SearchPrecision = 'Invalid string'",
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		parsedQuery, err := s.parser.Parse(tc.query)
		if tc.expectErr {
			s.Error(err)
			continue
		}
		s.NoError(err)
		s.Equal(tc.parsedQuery.searchPrecision, parsedQuery.searchPrecision)
	}
}

func (s *queryParserSuite) TestParseCloseTime() {
	commonQueryPart := "WorkflowId = \"random workflowID\" AND SearchPrecision = 'Day' AND "

	testCases := []struct {
		query       string
		expectErr   bool
		parsedQuery *parsedQuery
	}{
		{
			query:     commonQueryPart + "CloseTime = 1000",
			expectErr: false,
			parsedQuery: &parsedQuery{
				closeTime: new(time.Unix(0, 1000).UTC()),
			},
		},
		{
			query:     commonQueryPart + "CloseTime = \"2019-01-01T11:11:11Z\"",
			expectErr: false,
			parsedQuery: &parsedQuery{
				closeTime: new(time.Date(2019, 1, 1, 11, 11, 11, 0, time.UTC)),
			},
		},
		{
			query:     commonQueryPart + "closeTime = 2000",
			expectErr: true,
		},
		{
			query:     commonQueryPart + "CloseTime > \"2019-01-01 00:00:00\"",
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		parsedQuery, err := s.parser.Parse(tc.query)
		if tc.expectErr {
			s.Error(err)
			continue
		}
		s.NoError(err)
		s.Equal(tc.parsedQuery.closeTime, parsedQuery.closeTime)

	}
}

func (s *queryParserSuite) TestParseStartTime() {
	commonQueryPart := "WorkflowId = \"random workflowID\" AND SearchPrecision = 'Day' AND "

	testCases := []struct {
		query       string
		expectErr   bool
		parsedQuery *parsedQuery
	}{
		{
			query:     commonQueryPart + "StartTime = 1000",
			expectErr: false,
			parsedQuery: &parsedQuery{
				startTime: new(time.Unix(0, 1000).UTC()),
			},
		},
		{
			query:     commonQueryPart + "StartTime = \"2019-01-01T11:11:11Z\"",
			expectErr: false,
			parsedQuery: &parsedQuery{
				startTime: new(time.Date(2019, 1, 1, 11, 11, 11, 0, time.UTC)),
			},
		},
		{
			query:     commonQueryPart + "startTime = 2000",
			expectErr: true,
		},
		{
			query:     commonQueryPart + "StartTime > \"2019-01-01 00:00:00\"",
			expectErr: true,
		},
	}

	for _, tc := range testCases {
		parsedQuery, err := s.parser.Parse(tc.query)
		if tc.expectErr {
			s.Error(err)
			continue
		}
		s.NoError(err)
		s.Equal(tc.parsedQuery.startTime, parsedQuery.startTime)
	}
}

func TestParseExecutionStatus(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		value string
		want  enumspb.WorkflowExecutionStatus
	}{
		{name: "completed", value: "'Completed'", want: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED},
		{name: "failed", value: "'failed'", want: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED},
		{name: "canceled", value: "'canceled'", want: enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED},
		{name: "terminated", value: "'terminated'", want: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED},
		{name: "continued as new", value: "'continuedasnew'", want: enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW},
		{name: "continued as new underscore", value: "'continued_as_new'", want: enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW},
		{name: "timed out", value: "'timedout'", want: enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT},
		{name: "timed out underscore", value: "'timed_out'", want: enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT},
		{name: "case and whitespace", value: "'  FaIlEd  '", want: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED},
		{name: "quoted numeric", value: "'3'", want: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED},
		{name: "numeric completed", value: "2", want: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED},
		{name: "numeric failed", value: "3", want: enumspb.WORKFLOW_EXECUTION_STATUS_FAILED},
		{name: "numeric canceled", value: "4", want: enumspb.WORKFLOW_EXECUTION_STATUS_CANCELED},
		{name: "numeric terminated", value: "5", want: enumspb.WORKFLOW_EXECUTION_STATUS_TERMINATED},
		{name: "numeric continued as new", value: "6", want: enumspb.WORKFLOW_EXECUTION_STATUS_CONTINUED_AS_NEW},
		{name: "numeric timed out", value: "7", want: enumspb.WORKFLOW_EXECUTION_STATUS_TIMED_OUT},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := NewQueryParser().Parse("WorkflowId = 'workflow' AND ExecutionStatus = " + tc.value)
			require.NoError(t, err)
			require.Equal(t, &parsedQuery{workflowID: new("workflow"), status: new(tc.want)}, got)
		})
	}
	for _, tc := range []struct {
		name      string
		query     string
		wantError string
	}{
		{name: "unknown status", query: "ExecutionStatus = 'invalid'", wantError: "unknown workflow close status"},
		{name: "running", query: "ExecutionStatus = 'Running'", wantError: "unknown workflow close status"},
		{name: "unspecified", query: "ExecutionStatus = 0", wantError: "unknown workflow close status"},
		{name: "unknown numeric", query: "ExecutionStatus = 99", wantError: "unknown workflow close status"},
		{name: "repeat", query: "ExecutionStatus = 'Failed' AND ExecutionStatus = 'Failed'", wantError: "can not query ExecutionStatus multiple times"},
		{name: "conflict", query: "ExecutionStatus = 'Failed' AND ExecutionStatus = 'Completed'", wantError: "can not query ExecutionStatus multiple times"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewQueryParser().Parse("WorkflowId = 'workflow' AND " + tc.query)
			require.ErrorContains(t, err, tc.wantError)
		})
	}
	for _, op := range []string{"!=", ">", ">=", "<", "<="} {
		t.Run(fmt.Sprintf("operator %s", op), func(t *testing.T) {
			t.Parallel()
			_, err := NewQueryParser().Parse("WorkflowId = 'workflow' AND ExecutionStatus " + op + " 'Failed'")
			require.ErrorContains(t, err, "only operation = is support for ExecutionStatus")
		})
	}
	t.Run("index required", func(t *testing.T) {
		t.Parallel()
		_, err := NewQueryParser().Parse("ExecutionStatus = 'Failed'")
		require.ErrorContains(t, err, "WorkflowId or WorkflowType is required")
	})
}
