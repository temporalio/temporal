// The MIT License
//
// Copyright (c) 2021 Temporal Technologies Inc.  All rights reserved.
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

package cassandra

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"
)

var startTimeFrom = time.Now().Add(-time.Hour)
var startTimeTo = time.Now()
var startTimeRangeFilter = fmt.Sprintf(`StartTime BETWEEN "%v" AND "%v"`, startTimeFrom.Format(time.RFC3339Nano), startTimeTo.Format(time.RFC3339Nano))

var supportedQuery = map[string]*queryFilters{
	"":                   {},
	startTimeRangeFilter: {HasStartTimeRangeFilter: true, StartTimeFrom: startTimeFrom, StartTimeTo: startTimeTo},
	`WorkflowID = "abc"`: {HasWorkflowIDFilter: true, WorkflowID: "abc"},
	`WorkflowID = "abc" AND ` + startTimeRangeFilter:          {HasWorkflowIDFilter: true, WorkflowID: "abc", HasStartTimeRangeFilter: true, StartTimeFrom: startTimeFrom, StartTimeTo: startTimeTo},
	startTimeRangeFilter + ` AND WorkflowID = "abc"`:          {HasWorkflowIDFilter: true, WorkflowID: "abc", HasStartTimeRangeFilter: true, StartTimeFrom: startTimeFrom, StartTimeTo: startTimeTo},
	`WorkflowType = "abc"`:                                    {HasWorkflowTypeFilter: true, WorkflowType: "abc"},
	`WorkflowType = "abc" AND ` + startTimeRangeFilter:        {HasWorkflowTypeFilter: true, WorkflowType: "abc", HasStartTimeRangeFilter: true, StartTimeFrom: startTimeFrom, StartTimeTo: startTimeTo},
	startTimeRangeFilter + ` AND WorkflowType = "abc"`:        {HasWorkflowTypeFilter: true, WorkflowType: "abc", HasStartTimeRangeFilter: true, StartTimeFrom: startTimeFrom, StartTimeTo: startTimeTo},
	`ExecutionStatus = "Running"`:                             {HasExecutionStatusFilter: true, ExecutionStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
	`ExecutionStatus = "Running" AND ` + startTimeRangeFilter: {HasExecutionStatusFilter: true, ExecutionStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, HasStartTimeRangeFilter: true, StartTimeFrom: startTimeFrom, StartTimeTo: startTimeTo},
	startTimeRangeFilter + ` AND ExecutionStatus = "Running"`: {HasExecutionStatusFilter: true, ExecutionStatus: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, HasStartTimeRangeFilter: true, StartTimeFrom: startTimeFrom, StartTimeTo: startTimeTo},
}

// This is not an exhaustive list.
var unsupportedQuery = []string{
	fmt.Sprintf(`StartTime NOT BETWEEN "%v" AND "%v"`, startTimeFrom.Format(time.RFC3339Nano), startTimeTo.Format(time.RFC3339Nano)),
	"order by WorkflowID",
	`WorkflowID = "abc" AND WorkflowType = "xyz"`,
	`ExecutionStatus = "Running" AND WorkflowType = "xyz"`,
	`WorkflowID = "abc" OR WorkflowType = "xyz"`,
	`WorkflowID != "abc"`,
}

func TestSupportedQueryFilters(t *testing.T) {
	for query, expectedFilters := range supportedQuery {
		converter := newQueryConverter()
		_, _, err := converter.ConvertWhereOrderBy(query)
		assert.NoError(t, err)

		filters := converter.QueryFilters()
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
		converter := newQueryConverter()
		_, _, err := converter.ConvertWhereOrderBy(query)
		assert.Error(t, err)
	}
}
