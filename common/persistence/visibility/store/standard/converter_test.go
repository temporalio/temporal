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

package standard

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
)

var startTimeFrom = time.Now().Add(-time.Hour)
var startTimeTo = time.Now()
var startTimeRangeFilter = fmt.Sprintf(`StartTime BETWEEN "%v" AND "%v"`, startTimeFrom.Format(time.RFC3339Nano), startTimeTo.Format(time.RFC3339Nano))

var supportedQuery = map[string]*sqlplugin.VisibilitySelectFilter{
	"":                   {},
	startTimeRangeFilter: {MinTime: &startTimeFrom, MaxTime: &startTimeTo},
	`WorkflowId = "abc"`: {WorkflowID: convert.StringPtr("abc")},
	`WorkflowId = "abc" AND ` + startTimeRangeFilter:          {WorkflowID: convert.StringPtr("abc"), MinTime: &startTimeFrom, MaxTime: &startTimeTo},
	startTimeRangeFilter + ` AND WorkflowId = "abc"`:          {WorkflowID: convert.StringPtr("abc"), MinTime: &startTimeFrom, MaxTime: &startTimeTo},
	`WorkflowType = "abc"`:                                    {WorkflowTypeName: convert.StringPtr("abc")},
	`WorkflowType = "abc" AND ` + startTimeRangeFilter:        {WorkflowTypeName: convert.StringPtr("abc"), MinTime: &startTimeFrom, MaxTime: &startTimeTo},
	startTimeRangeFilter + ` AND WorkflowType = "abc"`:        {WorkflowTypeName: convert.StringPtr("abc"), MinTime: &startTimeFrom, MaxTime: &startTimeTo},
	`ExecutionStatus = "Running"`:                             {Status: int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING)},
	`ExecutionStatus = "Running" AND ` + startTimeRangeFilter: {Status: int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING), MinTime: &startTimeFrom, MaxTime: &startTimeTo},
	startTimeRangeFilter + ` AND ExecutionStatus = "Running"`: {Status: int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING), MinTime: &startTimeFrom, MaxTime: &startTimeTo},
}

// This is not an exhaustive list.
var unsupportedQuery = []string{
	fmt.Sprintf(`StartTime NOT BETWEEN "%v" AND "%v"`, startTimeFrom.Format(time.RFC3339Nano), startTimeTo.Format(time.RFC3339Nano)),
	"order by WorkflowID",
	`WorkflowID = "abc" AND WorkflowType = "xyz"`,
	`ExecutionStatus = "Running" AND WorkflowType = "xyz"`,
	`WorkflowID = "abc" OR WorkflowType = "xyz"`,
	`WorkflowID != "abc"`,
	`StartTime < "2022-01-15T05:43:12.74127Z"`,
}

func TestSupportedQueryFilters(t *testing.T) {
	for query, expectedFilter := range supportedQuery {
		converter := newQueryConverter()
		filter, err := converter.GetFilter(query)
		assert.NoError(t, err)

		assert.EqualValues(t, expectedFilter.WorkflowID, filter.WorkflowID)
		assert.EqualValues(t, expectedFilter.WorkflowTypeName, filter.WorkflowTypeName)
		assert.EqualValues(t, expectedFilter.Status, filter.Status)

		if expectedFilter.MinTime == nil {
			assert.True(t, time.Unix(0, 0).Equal(*filter.MinTime))
		} else {
			assert.NotNil(t, filter.MinTime)
			assert.True(t, expectedFilter.MinTime.Equal(*filter.MinTime))
		}

		if expectedFilter.MaxTime == nil {
			assert.True(t, filter.MaxTime.After(time.Now()))
		} else {
			assert.NotNil(t, filter.MaxTime)
			assert.True(t, expectedFilter.MaxTime.Equal(*filter.MaxTime))
		}
	}
}

func TestUnsupportedQueryFilters(t *testing.T) {
	for _, query := range unsupportedQuery {
		converter := newQueryConverter()
		_, err := converter.GetFilter(query)
		assert.Error(t, err)
	}
}
