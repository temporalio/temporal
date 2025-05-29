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

func TestMatchMutableState(t *testing.T) {
	startTimeStr := "2023-10-26T14:30:00Z"
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
			name:          "absent where clause",
			query:         "",
			expectedMatch: false,
			expectedError: true,
		},
		{
			name:          "full sql query",
			query:         "select * from table where WorkflowID = 'my_workflow_id'",
			expectedMatch: false,
			expectedError: true,
		},
		{
			name:          "acceptance - positive",
			query:         "WorkflowId = 'workflow_id'",
			expectedMatch: true,
			expectedError: false,
		},
		{
			name:          "acceptance - negative",
			query:         "WorkflowId != 'other_workflow_id' ",
			expectedMatch: true,
			expectedError: false,
		},
	}

	// we don't need many SQL related tests here, SQL support is covered in other tests
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
			assert.Equal(t, tt.expectedMatch, match)
			if tt.expectedError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expectedMatch, match)
			}
		})
	}
}
