package gcloud

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
)

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
			got, err := NewQueryParser().Parse("CloseTime = 1000 AND SearchPrecision = 'Day' AND ExecutionStatus = " + tc.value)
			require.NoError(t, err)
			require.Equal(t, &parsedQuery{closeTime: time.Unix(0, 1000).UTC(), searchPrecision: new(PrecisionDay), status: new(tc.want)}, got)
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
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewQueryParser().Parse("CloseTime = 1000 AND SearchPrecision = 'Day' AND " + tc.query)
			require.ErrorContains(t, err, tc.wantError)
		})
	}
	for _, op := range []string{"!=", ">", ">=", "<", "<="} {
		t.Run(fmt.Sprintf("operator %s", op), func(t *testing.T) {
			t.Parallel()
			_, err := NewQueryParser().Parse("CloseTime = 1000 AND SearchPrecision = 'Day' AND ExecutionStatus " + op + " 'Failed'")
			require.ErrorContains(t, err, "only operation = is support for ExecutionStatus")
		})
	}
	t.Run("index required", func(t *testing.T) {
		t.Parallel()
		_, err := NewQueryParser().Parse("ExecutionStatus = 'Failed'")
		require.ErrorContains(t, err, "requires a StartTime or CloseTime")
	})
}

func TestParseRepeatedExecutionStatus(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		filter string
		empty  bool
	}{
		{name: "same", filter: "ExecutionStatus = 'Failed' AND ExecutionStatus = 'Failed'"},
		{name: "same numeric", filter: "ExecutionStatus = 'Failed' AND ExecutionStatus = 3"},
		{name: "conflicting", filter: "ExecutionStatus = 'Failed' AND ExecutionStatus = 'Completed'", empty: true},
		{name: "conflict remains", filter: "ExecutionStatus = 'Failed' AND ExecutionStatus = 'Completed' AND ExecutionStatus = 'Failed'", empty: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := NewQueryParser().Parse("CloseTime = 1000 AND SearchPrecision = 'Day' AND " + tc.filter)
			require.NoError(t, err)
			require.Equal(t, &parsedQuery{closeTime: time.Unix(0, 1000).UTC(), searchPrecision: new(PrecisionDay), status: new(enumspb.WORKFLOW_EXECUTION_STATUS_FAILED), emptyResult: tc.empty}, got)
		})
	}
}
