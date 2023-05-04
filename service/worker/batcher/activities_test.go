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
	"unicode"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	history "go.temporal.io/api/history/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/api/workflowservicemock/v1"
	"go.temporal.io/sdk/testsuite"
	"go.temporal.io/server/common/log"
)

type activitiesSuite struct {
	suite.Suite
	testsuite.WorkflowTestSuite

	controller *gomock.Controller
	env        *testsuite.TestWorkflowEnvironment

	mockFrontendClient *workflowservicemock.MockWorkflowServiceClient
}

func (s *activitiesSuite) SetupTest() {
	s.controller = gomock.NewController(s.T())

	s.mockFrontendClient = workflowservicemock.NewMockWorkflowServiceClient(s.controller)
}
func TestActivitiesSuite(t *testing.T) {
	suite.Run(t, new(activitiesSuite))
}

// pattern contains either c or f representing completed or failed task
// Schedule events for each task has id of 2*i + 1 where i is the index of the character
// eventId for each task has id of 2*i+2 where is is the index of the character
func generateEventHistory(pattern string, shouldReverse bool) history.History {

	events := make([]*history.HistoryEvent, 0)
	for i, char := range pattern {
		// add a Schedule event independent of type of event
		scheduledEventId := int64(2*i + 1)
		scheduledEvent := history.HistoryEvent{EventId: scheduledEventId, EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED}
		events = append(events, &scheduledEvent)

		event := history.HistoryEvent{EventId: int64(2*i + 2)}
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

	if shouldReverse {
		for i, j := 0, len(events)-1; i < j; i, j = i+1, j-1 {
			events[i], events[j] = events[j], events[i]
		}
	}
	return history.History{Events: events}
}

func (s *activitiesSuite) TestGetLastWorkflowTaskEventID() {
	namespaceStr := "test-namespace"
	workflowExecution := commonpb.WorkflowExecution{}
	tests := []struct {
		name                    string
		history                 history.History
		wantWorkflowTaskEventID int64
		wantErr                 bool
	}{
		{
			name:                    "Test history with all completed task event history",
			history:                 generateEventHistory("ccccc", true),
			wantWorkflowTaskEventID: 2*4 + 2,
		},
		{
			name:                    "Test history with last task failing",
			history:                 generateEventHistory("ccccf", true),
			wantWorkflowTaskEventID: 2*3 + 2,
		},
		{
			name:                    "Test history with all tasks failing",
			history:                 generateEventHistory("fffff", true),
			wantWorkflowTaskEventID: 2,
		},
		{
			name:                    "Test history with some tasks failing in the middle",
			history:                 generateEventHistory("cfffc", true),
			wantWorkflowTaskEventID: 2*4 + 2,
		},
		{
			name:    "Test history with empty history should error",
			history: generateEventHistory("", true),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			s.mockFrontendClient.EXPECT().GetWorkflowExecutionHistoryReverse(ctx, gomock.Any()).Return(
				&workflowservice.GetWorkflowExecutionHistoryReverseResponse{History: &tt.history, NextPageToken: nil}, nil)
			gotWorkflowTaskEventID, err := getLastWorkflowTaskEventID(ctx, namespaceStr, &workflowExecution, s.mockFrontendClient, log.NewTestLogger())
			if (err != nil) != tt.wantErr {
				t.Errorf("getLastWorkflowTaskEventID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotWorkflowTaskEventID != tt.wantWorkflowTaskEventID {
				t.Errorf("%s: getLastWorkflowTaskEventID() = %v, want %v", tt.name, gotWorkflowTaskEventID, tt.wantWorkflowTaskEventID)
			}
		})
	}
}

func (s *activitiesSuite) TestGetFirstWorkflowTaskEventID() {
	namespaceStr := "test-namespace"
	workflowExecution := commonpb.WorkflowExecution{}
	tests := []struct {
		name                    string
		history                 history.History
		wantWorkflowTaskEventID int64
		wantErr                 bool
	}{
		{
			name:                    "Test history with all completed task event history",
			history:                 generateEventHistory("ccccc", false),
			wantWorkflowTaskEventID: 2,
		},
		{
			name:                    "Test history with last task failing",
			history:                 generateEventHistory("ccccf", false),
			wantWorkflowTaskEventID: 2,
		},
		{
			name:                    "Test history with first task failing",
			history:                 generateEventHistory("fcccc", false),
			wantWorkflowTaskEventID: 2*1 + 2,
		},
		{
			name:                    "Test history with all tasks failing",
			history:                 generateEventHistory("fffff", false),
			wantWorkflowTaskEventID: 2,
		},
		{
			name:                    "Test history with some tasks failing in the middle",
			history:                 generateEventHistory("cfffc", false),
			wantWorkflowTaskEventID: 2,
		},
		{
			name:    "Test history with empty history should error",
			history: generateEventHistory("", true),
			wantErr: true,
		},
	}
	for _, tt := range tests {
		s.T().Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			s.mockFrontendClient.EXPECT().GetWorkflowExecutionHistory(ctx, gomock.Any()).Return(
				&workflowservice.GetWorkflowExecutionHistoryResponse{History: &tt.history, NextPageToken: nil}, nil)
			gotWorkflowTaskEventID, err := getFirstWorkflowTaskEventID(ctx, namespaceStr, &workflowExecution, s.mockFrontendClient, log.NewTestLogger())
			if (err != nil) != tt.wantErr {
				t.Errorf("getLastWorkflowTaskEventID() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotWorkflowTaskEventID != tt.wantWorkflowTaskEventID {
				t.Errorf("%s: getLastWorkflowTaskEventID() = %v, want %v", tt.name, gotWorkflowTaskEventID, tt.wantWorkflowTaskEventID)
			}
		})
	}
}
