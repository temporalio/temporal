package cassandra

import (
	"fmt"
	enumspb "go.temporal.io/api/enums/v1"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var startTimeFrom = time.Now().Add(-time.Hour)
var startTimeTo = time.Now()
var startTimeRangeFilter = fmt.Sprintf(`StartTime BETWEEN "%v" AND "%v"`, startTimeFrom.Format(time.RFC3339Nano), startTimeTo.Format(time.RFC3339Nano))

var supportedQuery = map[string]*QueryFilters{
	"":                   {},
	startTimeRangeFilter: {HasStartTimeRangeFilter: true, StartTimeFrom: startTimeFrom, StartTimeTo: startTimeTo},
	"WorkflowID = abc":   {HasWorkflowIDFilter: true, WorkflowID: "abc"},
	"WorkflowID = abc AND " + startTimeRangeFilter:          {HasWorkflowIDFilter: true, WorkflowID: "abc", HasStartTimeRangeFilter: true, StartTimeFrom: startTimeFrom, StartTimeTo: startTimeTo},
	startTimeRangeFilter + " AND WorkflowID = abc":          {HasWorkflowIDFilter: true, WorkflowID: "abc", HasStartTimeRangeFilter: true, StartTimeFrom: startTimeFrom, StartTimeTo: startTimeTo},
	"WorkflowType = abc":                                    {HasWorkflowTypeFilter: true, WorkflowType: "abc"},
	"WorkflowType = abc AND " + startTimeRangeFilter:        {HasWorkflowTypeFilter: true, WorkflowType: "abc", HasStartTimeRangeFilter: true, StartTimeFrom: startTimeFrom, StartTimeTo: startTimeTo},
	startTimeRangeFilter + " AND WorkflowType = abc":        {HasWorkflowTypeFilter: true, WorkflowType: "abc", HasStartTimeRangeFilter: true, StartTimeFrom: startTimeFrom, StartTimeTo: startTimeTo},
	"ExecutionStatus = Running":                             {HasExecutionStatusFilter: true, ExecutionStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
	"ExecutionStatus = Running AND " + startTimeRangeFilter: {HasExecutionStatusFilter: true, ExecutionStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, HasStartTimeRangeFilter: true, StartTimeFrom: startTimeFrom, StartTimeTo: startTimeTo},
	startTimeRangeFilter + " AND ExecutionStatus = Running": {HasExecutionStatusFilter: true, ExecutionStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, HasStartTimeRangeFilter: true, StartTimeFrom: startTimeFrom, StartTimeTo: startTimeTo},
}

// This is not an exhaustive list.
var unsupportedQuery = []string{
	"order by WorkflowID",
	"WorkflowID = abc AND WorkflowType = xyz",
	"ExecutionStatus = Running AND WorkflowType = xyz",
	"WorkflowID = abc OR WorkflowType = xyz",
	"WorkflowID != abc",
}

func TestSupportedQueryFilters(t *testing.T) {
	for query, expectedFilters := range supportedQuery {
		filters, err := newQueryFilters(query)
		assert.NoError(t, err)

		assert.Equal(t, expectedFilters.HasWorkflowIDFilter, filters.HasWorkflowIDFilter)
		assert.Equal(t, expectedFilters.WorkflowID, filters.WorkflowID)
		assert.Equal(t, expectedFilters.HasWorkflowTypeFilter, filters.HasWorkflowTypeFilter)
		assert.Equal(t, expectedFilters.WorkflowType, filters.WorkflowType)
		assert.Equal(t, expectedFilters.HasExecutionStatusFilter, filters.HasExecutionStatusFilter)
		assert.Equal(t, expectedFilters.ExecutionStatus, filters.ExecutionStatus)
		assert.Equal(t, expectedFilters.HasStartTimeRangeFilter, filters.HasStartTimeRangeFilter)

		if filters.HasStartTimeRangeFilter {
			assert.True(t, filters.StartTimeFrom.Equal(expectedFilters.StartTimeFrom))
			assert.True(t, filters.StartTimeTo.Equal(expectedFilters.StartTimeTo))
		}
	}
}

func TestUnsupportedQueryFilters(t *testing.T) {
	for _, query := range unsupportedQuery {
		_, err := newQueryFilters(query)
		assert.Error(t, err)
	}
}
