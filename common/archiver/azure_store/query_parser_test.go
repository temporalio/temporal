package azure_store

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/util"
)

func TestQueryParser_Parse(t *testing.T) {
	parser := NewQueryParser()

	testCases := []struct {
		name           string
		query          string
		expected       *parsedQuery
		expectedErr    bool
		expectedErrMsg string
	}{
		{
			name:  "valid close time query",
			query: "CloseTime = '2023-01-01T10:00:00Z' AND SearchPrecision = 'Day'",
			expected: &parsedQuery{
				closeTime:       time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC),
				searchPrecision: util.Ptr(PrecisionDay),
			},
		},
		{
			name:  "valid start time query",
			query: "StartTime = '2023-01-01T10:00:00Z' AND SearchPrecision = 'Hour'",
			expected: &parsedQuery{
				startTime:       time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC),
				searchPrecision: util.Ptr(PrecisionHour),
			},
		},
		{
			name:  "full query",
			query: "WorkflowId = 'wf-id' AND WorkflowType = 'wf-type' AND RunId = 'run-id' AND CloseTime = '2023-01-01T10:00:00Z' AND SearchPrecision = 'Second'",
			expected: &parsedQuery{
				workflowID:      util.Ptr("wf-id"),
				workflowType:    util.Ptr("wf-type"),
				runID:           util.Ptr("run-id"),
				closeTime:       time.Date(2023, 1, 1, 10, 0, 0, 0, time.UTC),
				searchPrecision: util.Ptr(PrecisionSecond),
			},
		},
		{
			name:           "missing search precision",
			query:          "CloseTime = '2023-01-01T10:00:00Z'",
			expectedErr:    true,
			expectedErrMsg: "SearchPrecision is required",
		},
		{
			name:           "missing both times",
			query:          "WorkflowId = 'wf-id' AND SearchPrecision = 'Day'",
			expectedErr:    true,
			expectedErrMsg: "requires a StartTime or CloseTime",
		},
		{
			name:           "both times provided",
			query:          "StartTime = '2023-01-01T10:00:00Z' AND CloseTime = '2023-01-01T11:00:00Z' AND SearchPrecision = 'Day'",
			expectedErr:    true,
			expectedErrMsg: "requires a StartTime or CloseTime",
		},
		{
			name:           "invalid precision",
			query:          "CloseTime = '2023-01-01T10:00:00Z' AND SearchPrecision = 'Milli'",
			expectedErr:    true,
			expectedErrMsg: "invalid value for SearchPrecision",
		},
		{
			name:           "unsupported operator",
			query:          "WorkflowId > 'wf-id' AND CloseTime = '2023-01-01T10:00:00Z' AND SearchPrecision = 'Day'",
			expectedErr:    true,
			expectedErrMsg: "only operation = is support",
		},
		{
			name:           "unknown field",
			query:          "UnknownField = 'val' AND CloseTime = '2023-01-01T10:00:00Z' AND SearchPrecision = 'Day'",
			expectedErr:    true,
			expectedErrMsg: "unknown filter name",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := parser.Parse(tc.query)
			if tc.expectedErr {
				assert.Error(t, err)
				if tc.expectedErrMsg != "" {
					assert.Contains(t, err.Error(), tc.expectedErrMsg)
				}
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tc.expected, got)
			}
		})
	}
}
