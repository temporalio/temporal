// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
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

package matcher

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/service/history/workflow"
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
	startTime, err := convertToTime(fmt.Sprintf("'%s'", startTimeStr))
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

	ms := workflow.NewMockMutableState(controller)
	ms.EXPECT().GetExecutionState().Return(ws).AnyTimes()
	ms.EXPECT().GetExecutionInfo().Return(we).AnyTimes()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			match, err := MatchMutableState(ms, tt.query)
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
