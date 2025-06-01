package matcher

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/sqlquery"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestBasicMutableStateMatchEvaluator(t *testing.T) {

	startTimeStr := "2023-10-26T14:30:00Z"
	beforeTimeStr := "2023-10-26T13:00:00Z"
	afterTimeStr := "2023-10-26T15:00:00Z"

	tests := []struct {
		name          string
		query         string
		expectedMatch bool
		expectedError bool
	}{
		{
			name:          "empty query",
			query:         "",
			expectedMatch: false,
			expectedError: true,
		},
		{
			name:          "query start with where",
			query:         "",
			expectedMatch: false,
			expectedError: true,
		},
		{
			name:          "query start with select",
			query:         "",
			expectedMatch: false,
			expectedError: true,
		},
		{
			name:          "workflow id",
			query:         fmt.Sprintf("%s = 'workflow_id'", workflowIDColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "workflow id - starts_with",
			query:         fmt.Sprintf("%s starts_with 'workflow_'", workflowIDColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "workflow id - not starts_with",
			query:         fmt.Sprintf("%s not starts_with 'other_workflow_'", workflowIDColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "workflow type",
			query:         fmt.Sprintf("%s = 'workflow_type'", workflowTypeNameColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "workflow status",
			query:         fmt.Sprintf("%s = 'Running'", workflowExecutionStatusColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "workflow status - negative",
			query:         fmt.Sprintf("%s = 'Terminated'", workflowExecutionStatusColName),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "workflow start time",
			query:         fmt.Sprintf("%s = '%s'", workflowStartTimeColName, startTimeStr),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "workflow start time, >= clause, equal",
			query:         fmt.Sprintf("%s >= '%s'", workflowStartTimeColName, startTimeStr),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "workflow start time, >= clause, after",
			query:         fmt.Sprintf("%s >= '%s'", workflowStartTimeColName, afterTimeStr),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "workflow start time, >= clause, before",
			query:         fmt.Sprintf("%s >= '%s'", workflowStartTimeColName, beforeTimeStr),
			expectedMatch: true,
			expectedError: false,
		},

		{
			name:          "workflow start time, > clause, equal",
			query:         fmt.Sprintf("%s > '%s'", workflowStartTimeColName, startTimeStr),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "workflow start time, > clause, after",
			query:         fmt.Sprintf("%s > '%s'", workflowStartTimeColName, afterTimeStr),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "workflow start time, > clause, before",
			query:         fmt.Sprintf("%s >= '%s'", workflowStartTimeColName, beforeTimeStr),
			expectedMatch: true,
			expectedError: false,
		},

		{
			name:          "workflow start time, <= clause, equal",
			query:         fmt.Sprintf("%s <= '%s'", workflowStartTimeColName, startTimeStr),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "workflow start time, <= clause, after",
			query:         fmt.Sprintf("%s <= '%s'", workflowStartTimeColName, afterTimeStr),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "workflow start time, <= clause, before",
			query:         fmt.Sprintf("%s <= '%s'", workflowStartTimeColName, beforeTimeStr),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "workflow start time < clause, equal",
			query:         fmt.Sprintf("%s < '%s'", workflowStartTimeColName, startTimeStr),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "workflow start time, < clause, after",
			query:         fmt.Sprintf("%s < '%s'", workflowStartTimeColName, afterTimeStr),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "workflow start time where < clause, before",
			query:         fmt.Sprintf("%s < '%s'", workflowStartTimeColName, beforeTimeStr),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "test AND, true",
			query:         fmt.Sprintf("%s = 'workflow_id' AND %s = 'workflow_type'", workflowIDColName, workflowTypeNameColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "test AND, false, left cause",
			query:         fmt.Sprintf("%s = 'workflow_id_unknown' AND %s = 'workflow_type'", workflowIDColName, workflowTypeNameColName),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "test and, false, right cause",
			query:         fmt.Sprintf("%s = 'workflow_id' AND %s = 'workflow_type_unknown'", workflowIDColName, workflowTypeNameColName),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "test OR, true",
			query:         fmt.Sprintf("%s = 'workflow_id' OR %s = 'workflow_type'", workflowIDColName, workflowTypeNameColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "test OR, true, left only",
			query:         fmt.Sprintf("%s = 'workflow_id' OR %s = 'workflow_type_unknown'", workflowIDColName, workflowTypeNameColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "test OR, true, right only",
			query:         fmt.Sprintf("%s = 'workflow_id_unknown' OR %s = 'workflow_type'", workflowIDColName, workflowTypeNameColName),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "test OR, false",
			query:         fmt.Sprintf("%s = 'workflow_id_unknown' OR %s = 'workflow_type_unknown'", workflowIDColName, workflowTypeNameColName),
			expectedMatch: false,
			expectedError: false,
		},
	}

	startTime, err := sqlquery.ConvertToTime(fmt.Sprintf("'%s'", startTimeStr))
	assert.NoError(t, err)

	ws := &persistencespb.WorkflowExecutionState{
		StartTime: timestamppb.New(startTime),
		Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}

	we := &persistencespb.WorkflowExecutionInfo{
		WorkflowId:       "workflow_id",
		WorkflowTypeName: "workflow_type",
	}

	controller := gomock.NewController(t)
	defer controller.Finish()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			match, err := MatchMutableState(we, ws, tt.query)
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedMatch, match)
			}
		})
	}
}

func TestAdvancedMutableStateMatchEvaluator(t *testing.T) {

	startTimeStr := "2023-10-26T14:30:00Z"
	beforeTimeStr := "2023-10-26T13:00:00Z"
	afterTimeStr := "2023-10-26T15:00:00Z"
	tests := []struct {
		name          string
		query         string
		expectedMatch bool
		expectedError bool
	}{
		{
			name:          "workflow start time between clause, match",
			query:         fmt.Sprintf("%s between '%s' and '%s'", workflowStartTimeColName, beforeTimeStr, afterTimeStr),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "workflow start time between clause, miss",
			query:         fmt.Sprintf("%s between '%s' and '%s'", workflowStartTimeColName, afterTimeStr, "2023-10-26T16:00:00Z"),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "workflow start time between clause, inclusive left",
			query:         fmt.Sprintf("%s between '%s' and '%s'", workflowStartTimeColName, beforeTimeStr, startTimeStr),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "workflow start time between clause, inclusive right",
			query:         fmt.Sprintf("%s between '%s' and '%s'", workflowStartTimeColName, startTimeStr, afterTimeStr),
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "workflow start time not between clause, miss",
			query:         fmt.Sprintf("%s not between '%s' and '%s'", workflowStartTimeColName, beforeTimeStr, afterTimeStr),
			expectedMatch: false,
			expectedError: false,
		},
		{
			name:          "workflow start time not between clause, match",
			query:         fmt.Sprintf("%s not between '%s' and '%s'", workflowStartTimeColName, afterTimeStr, "2023-10-26T16:00:00Z"),
			expectedMatch: true,
			expectedError: false,
		},
	}

	startTime, err := sqlquery.ConvertToTime(fmt.Sprintf("'%s'", startTimeStr))
	assert.NoError(t, err)

	ws := &persistencespb.WorkflowExecutionState{
		StartTime: timestamppb.New(startTime),
		Status:    enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	}

	we := &persistencespb.WorkflowExecutionInfo{
		WorkflowId:       "workflow_id",
		WorkflowTypeName: "workflow_type",
	}

	controller := gomock.NewController(t)
	defer controller.Finish()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			match, err := MatchMutableState(we, ws, tt.query)
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedMatch, match)
			}
		})
	}
}
