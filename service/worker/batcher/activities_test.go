// The MIT License
//
// Copyright (c) 2022 Temporal Technologies Inc.  All rights reserved.
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

package batcher

import (
	"context"
	"testing"
	"time"
	"unicode"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	history "go.temporal.io/api/history/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservicemock/v1"
	"go.temporal.io/sdk/testsuite"
	"golang.org/x/exp/slices"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/primitives/timestamp"
)

type activitiesSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	controller *gomock.Controller

	mockFrontendClient *workflowservicemock.MockWorkflowServiceClient
}

func (s *activitiesSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())

	s.mockFrontendClient = workflowservicemock.NewMockWorkflowServiceClient(s.controller)
}
func TestActivitiesSuite(t *testing.T) {
	suite.Run(t, new(activitiesSuite))
}

const NumTotalEvents = 10

// pattern contains either c or f representing completed or failed task
// Schedule events for each task has id of NumTotalEvents*i + 1 where i is the index of the character
// eventId for each task has id of NumTotalEvents*i+NumTotalEvents where is is the index of the character
func generateEventHistory(pattern string) *history.History {
	events := make([]*history.HistoryEvent, 0)
	for i, char := range pattern {
		// add a Schedule event independent of type of event
		scheduledEventId := int64(NumTotalEvents*i + 1)
		scheduledEvent := history.HistoryEvent{EventId: scheduledEventId, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED}
		events = append(events, &scheduledEvent)

		event := history.HistoryEvent{EventId: int64(NumTotalEvents*i + NumTotalEvents)}
		switch unicode.ToLower(char) {
		case 'c':
			event.EventType = enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED
			event.Attributes = &history.HistoryEvent_WorkflowTaskCompletedEventAttributes{
				WorkflowTaskCompletedEventAttributes: &history.WorkflowTaskCompletedEventAttributes{ScheduledEventId: scheduledEventId},
			}
		case 'f':
			event.EventType = enumspb.EVENT_TYPE_WORKFLOW_TASK_FAILED
		}
		events = append(events, &event)
	}

	return &history.History{Events: events}
}

func (s *activitiesSuite) TestGetLastWorkflowTaskEventID() {
	namespaceStr := "test-namespace"
	tests := []struct {
		name                    string
		history                 *history.History
		wantWorkflowTaskEventID int64
		wantErr                 bool
	}{
		{
			name:                    "Test history with all completed task event history",
			history:                 generateEventHistory("ccccc"),
			wantWorkflowTaskEventID: NumTotalEvents*4 + NumTotalEvents,
		},
		{
			name:                    "Test history with last task failing",
			history:                 generateEventHistory("ccccf"),
			wantWorkflowTaskEventID: NumTotalEvents*3 + NumTotalEvents,
		},
		{
			name:                    "Test history with all tasks failing",
			history:                 generateEventHistory("fffff"),
			wantWorkflowTaskEventID: 2,
		},
		{
			name:                    "Test history with some tasks failing in the middle",
			history:                 generateEventHistory("cfffc"),
			wantWorkflowTaskEventID: NumTotalEvents*4 + NumTotalEvents,
		},
		{
			name:    "Test history with empty history should error",
			history: generateEventHistory(""),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			ctx := context.Background()
			slices.Reverse(tt.history.Events)
			workflowExecution := &commonpb.WorkflowExecution{}
			s.mockFrontendClient.EXPECT().GetWorkflowExecutionHistoryReverse(ctx, gomock.Any()).Return(
				&workflowservice.GetWorkflowExecutionHistoryReverseResponse{History: tt.history, NextPageToken: nil}, nil)
			gotWorkflowTaskEventID, err := getLastWorkflowTaskEventID(ctx, namespaceStr, workflowExecution, s.mockFrontendClient, log.NewTestLogger())
			s.Equal(tt.wantErr, err != nil)
			s.Equal(tt.wantWorkflowTaskEventID, gotWorkflowTaskEventID)
		})
	}
}

func (s *activitiesSuite) TestGetFirstWorkflowTaskEventID() {
	namespaceStr := "test-namespace"
	workflowExecution := commonpb.WorkflowExecution{}
	tests := []struct {
		name                    string
		history                 *history.History
		wantWorkflowTaskEventID int64
		wantErr                 bool
	}{
		{
			name:                    "Test history with all completed task event history",
			history:                 generateEventHistory("ccccc"),
			wantWorkflowTaskEventID: NumTotalEvents,
		},
		{
			name:                    "Test history with last task failing",
			history:                 generateEventHistory("ccccf"),
			wantWorkflowTaskEventID: NumTotalEvents,
		},
		{
			name:                    "Test history with first task failing",
			history:                 generateEventHistory("fcccc"),
			wantWorkflowTaskEventID: NumTotalEvents*1 + NumTotalEvents,
		},
		{
			name:                    "Test history with all tasks failing",
			history:                 generateEventHistory("fffff"),
			wantWorkflowTaskEventID: 2,
		},
		{
			name:                    "Test history with some tasks failing in the middle",
			history:                 generateEventHistory("cfffc"),
			wantWorkflowTaskEventID: NumTotalEvents,
		},
		{
			name:    "Test history with empty history should error",
			history: generateEventHistory(""),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			ctx := context.Background()
			s.mockFrontendClient.EXPECT().GetWorkflowExecutionHistory(ctx, gomock.Any()).Return(
				&workflowservice.GetWorkflowExecutionHistoryResponse{History: tt.history, NextPageToken: nil}, nil)
			gotWorkflowTaskEventID, err := getFirstWorkflowTaskEventID(ctx, namespaceStr, &workflowExecution, s.mockFrontendClient, log.NewTestLogger())
			s.Equal(tt.wantErr, err != nil)
			s.Equal(tt.wantWorkflowTaskEventID, gotWorkflowTaskEventID)
		})
	}
}

func (s *activitiesSuite) TestGetResetPoint() {
	ctx := context.Background()
	logger := log.NewTestLogger()
	ns := "namespacename"
	tests := []struct {
		name                    string
		points                  []*workflowpb.ResetPointInfo
		buildId                 string
		currentRunOnly          bool
		wantWorkflowTaskEventID int64
		wantErr                 bool
		wantSetRunId            string
	}{
		{
			name: "not found",
			points: []*workflowpb.ResetPointInfo{
				&workflowpb.ResetPointInfo{
					BuildId:                      "build1",
					RunId:                        "run1",
					FirstWorkflowTaskCompletedId: 123,
					Resettable:                   true,
				},
			},
			buildId: "otherbuild",
			wantErr: true,
		},
		{
			name: "found",
			points: []*workflowpb.ResetPointInfo{
				&workflowpb.ResetPointInfo{
					BuildId:                      "build1",
					RunId:                        "run1",
					FirstWorkflowTaskCompletedId: 123,
					Resettable:                   true,
				},
			},
			buildId:                 "build1",
			wantWorkflowTaskEventID: 123,
		},
		{
			name: "not resettable",
			points: []*workflowpb.ResetPointInfo{
				&workflowpb.ResetPointInfo{
					BuildId:                      "build1",
					RunId:                        "run1",
					FirstWorkflowTaskCompletedId: 123,
					Resettable:                   false,
				},
			},
			buildId: "build1",
			wantErr: true,
		},
		{
			name: "from another run",
			points: []*workflowpb.ResetPointInfo{
				&workflowpb.ResetPointInfo{
					BuildId:                      "build1",
					RunId:                        "run0",
					FirstWorkflowTaskCompletedId: 34,
					Resettable:                   true,
				},
			},
			buildId:                 "build1",
			wantWorkflowTaskEventID: 34,
			wantSetRunId:            "run0",
		},
		{
			name: "from another run but not allowed",
			points: []*workflowpb.ResetPointInfo{
				&workflowpb.ResetPointInfo{
					BuildId:                      "build1",
					RunId:                        "run0",
					FirstWorkflowTaskCompletedId: 34,
					Resettable:                   true,
				},
			},
			buildId:        "build1",
			currentRunOnly: true,
			wantErr:        true,
		},
		{
			name: "expired",
			points: []*workflowpb.ResetPointInfo{
				&workflowpb.ResetPointInfo{
					BuildId:                      "build1",
					RunId:                        "run1",
					FirstWorkflowTaskCompletedId: 123,
					Resettable:                   true,
					ExpireTime:                   timestamp.TimePtr(time.Now().Add(-1 * time.Hour)),
				},
			},
			buildId: "build1",
			wantErr: true,
		},
	}
	for _, tt := range tests {
		s.Run(tt.name, func() {
			s.mockFrontendClient.EXPECT().DescribeWorkflowExecution(gomock.Any(), gomock.Any()).Return(
				&workflowservice.DescribeWorkflowExecutionResponse{
					WorkflowExecutionInfo: &workflowpb.WorkflowExecutionInfo{
						AutoResetPoints: &workflowpb.ResetPoints{
							Points: tt.points,
						},
					},
				},
				nil,
			)
			execution := &commonpb.WorkflowExecution{
				WorkflowId: "wfid",
				RunId:      "run1",
			}
			id, err := getResetPoint(ctx, ns, execution, s.mockFrontendClient, logger, tt.buildId, tt.currentRunOnly)
			s.Equal(tt.wantErr, err != nil)
			s.Equal(tt.wantWorkflowTaskEventID, id)
			if tt.wantSetRunId != "" {
				s.Equal(tt.wantSetRunId, execution.RunId)
			}
		})
	}
}
