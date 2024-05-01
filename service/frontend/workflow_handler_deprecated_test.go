// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
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

package frontend

import (
	"context"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	failurepb "go.temporal.io/api/failure/v1"
	historypb "go.temporal.io/api/history/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common"
	dc "go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/payload"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/searchattribute"
)

// DEPRECATED: TBD
func (s *workflowHandlerSuite) TestTransientTaskInjection() {
	cfg := s.newConfig()
	baseEvents := []*historypb.HistoryEvent{
		{EventId: 1},
		{EventId: 2},
	}

	// Needed to execute test but not relevant
	s.mockSearchAttributesProvider.EXPECT().
		GetSearchAttributes(esIndexName, false).
		Return(searchattribute.NameTypeMap{}, nil).
		AnyTimes()

	// Install a test namespace into mock namespace registry
	ns := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Id:   s.testNamespaceID.String(),
			Name: s.testNamespace.String(),
		},
		&persistencespb.NamespaceConfig{},
		"target-cluster:not-relevant-to-this-test",
	)
	s.mockNamespaceCache.EXPECT().GetNamespace(s.testNamespace).Return(ns, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.testNamespaceID).Return(ns, nil).AnyTimes()

	// History read will return a base set of non-transient events from baseEvents above
	s.mockExecutionManager.EXPECT().ReadHistoryBranch(gomock.Any(), gomock.Any()).
		Return(&persistence.ReadHistoryBranchResponse{HistoryEvents: baseEvents}, nil).
		AnyTimes()

	pollRequest := workflowservice.PollWorkflowTaskQueueRequest{
		Namespace: s.testNamespace.String(),
		TaskQueue: &taskqueuepb.TaskQueue{Name: "taskqueue:" + s.T().Name()},
	}

	for _, tc := range []struct {
		name           string
		taskInfo       *historyspb.TransientWorkflowTaskInfo
		transientCount int
	}{
		{
			name: "HistorySuffix",
			taskInfo: &historyspb.TransientWorkflowTaskInfo{
				HistorySuffix: []*historypb.HistoryEvent{
					{EventId: 3},
					{EventId: 4},
					{EventId: 5},
					{EventId: 6},
				},
			},
			transientCount: 4,
		},
	} {
		s.Run(tc.name, func() {
			ctx, cancel := context.WithTimeout(context.TODO(), 1*time.Minute)
			defer cancel()
			s.mockMatchingClient.EXPECT().PollWorkflowTaskQueue(ctx, gomock.Any()).Return(
				&matchingservice.PollWorkflowTaskQueueResponse{
					NextEventId: int64(len(baseEvents) + 1),
					WorkflowExecution: &commonpb.WorkflowExecution{
						WorkflowId: "wfid:" + s.T().Name(),
						RunId:      "1",
					},
					TransientWorkflowTask: tc.taskInfo,
				},
				nil,
			)

			wh := s.getWorkflowHandler(cfg)
			pollResp, err := wh.PollWorkflowTaskQueue(ctx, &pollRequest)

			s.NoError(err)
			events := pollResp.GetHistory().GetEvents()
			s.Len(events, len(baseEvents)+tc.transientCount)
		})
	}
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/historyEngine_test.go
func (s *workflowHandlerSuite) TestGetHistory() {
	namespaceID := namespace.ID(uuid.New())
	namespaceName := namespace.Name("test-namespace")
	firstEventID := int64(100)
	nextEventID := int64(102)
	branchToken := []byte{1}
	we := &commonpb.WorkflowExecution{
		WorkflowId: "wid",
		RunId:      "rid",
	}
	shardID := common.WorkflowIDToHistoryShard(namespaceID.String(), we.WorkflowId, numHistoryShards)
	req := &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    firstEventID,
		MaxEventID:    nextEventID,
		PageSize:      2,
		NextPageToken: []byte{},
		ShardID:       shardID,
	}
	s.mockExecutionManager.EXPECT().ReadHistoryBranch(gomock.Any(), req).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: []*historypb.HistoryEvent{
			{
				EventId:   int64(100),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
			},
			{
				EventId:   int64(101),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_STARTED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionStartedEventAttributes{
					WorkflowExecutionStartedEventAttributes: &historypb.WorkflowExecutionStartedEventAttributes{
						SearchAttributes: &commonpb.SearchAttributes{
							IndexedFields: map[string]*commonpb.Payload{
								"CustomKeywordField":    payload.EncodeString("random-keyword"),
								"TemporalChangeVersion": payload.EncodeString("random-data"),
							},
						},
					},
				},
			},
		},
		NextPageToken: []byte{},
		Size:          1,
	}, nil)

	s.mockSearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).Return(searchattribute.TestNameTypeMap, nil)
	s.mockSearchAttributesMapperProvider.EXPECT().GetMapper(namespaceName).
		Return(&searchattribute.TestMapper{}, nil).AnyTimes()

	wh := s.getWorkflowHandler(s.newConfig())

	history, token, err := wh.getHistory(
		context.Background(),
		metrics.NoopMetricsHandler,
		namespaceID,
		namespaceName,
		we,
		firstEventID,
		nextEventID,
		2,
		[]byte{},
		nil,
		branchToken,
	)
	s.NoError(err)
	s.NotNil(history)
	s.Equal([]byte{}, token)

	s.EqualValues("Keyword", history.Events[1].GetWorkflowExecutionStartedEventAttributes().GetSearchAttributes().GetIndexedFields()["AliasForCustomKeywordField"].GetMetadata()["type"])
	s.EqualValues(`"random-data"`, history.Events[1].GetWorkflowExecutionStartedEventAttributes().GetSearchAttributes().GetIndexedFields()["TemporalChangeVersion"].GetData())
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/historyEngine_test.go
func (s *workflowHandlerSuite) TestGetWorkflowExecutionHistory() {
	namespaceID := namespace.ID(uuid.New())
	namespaceName := namespace.Name("namespace")
	we := &commonpb.WorkflowExecution{WorkflowId: "wid1", RunId: uuid.New()}
	newRunID := uuid.New()

	req := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:              namespaceName.String(),
		Execution:              we,
		MaximumPageSize:        10,
		NextPageToken:          nil,
		WaitNewEvent:           true,
		HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_CLOSE_EVENT,
		SkipArchival:           true,
	}

	// set up mocks to simulate a failed workflow with a retry policy. the failure event is id 5.
	branchToken := []byte{1, 2, 3}
	shardID := common.WorkflowIDToHistoryShard(namespaceID.String(), we.WorkflowId, numHistoryShards)
	versionHistoryItem := versionhistory.NewVersionHistoryItem(1, 1)
	currentVersionHistory := versionhistory.NewVersionHistory(branchToken, []*historyspb.VersionHistoryItem{versionHistoryItem})
	versionHistories := versionhistory.NewVersionHistories(currentVersionHistory)
	s.mockNamespaceCache.EXPECT().GetNamespaceID(namespaceName).Return(namespaceID, nil).AnyTimes()
	s.mockHistoryClient.EXPECT().PollMutableState(gomock.Any(), &historyservice.PollMutableStateRequest{
		NamespaceId:         namespaceID.String(),
		Execution:           we,
		ExpectedNextEventId: common.EndEventID,
		CurrentBranchToken:  nil,
		VersionHistoryItem:  nil,
	}).Return(&historyservice.PollMutableStateResponse{
		Execution:           we,
		WorkflowType:        &commonpb.WorkflowType{Name: "mytype"},
		NextEventId:         6,
		LastFirstEventId:    5,
		CurrentBranchToken:  branchToken,
		VersionHistories:    versionHistories,
		WorkflowState:       enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		WorkflowStatus:      enumspb.WORKFLOW_EXECUTION_STATUS_FAILED,
		LastFirstEventTxnId: 100,
	}, nil).Times(2)

	// GetWorkflowExecutionHistory will request the last event
	s.mockExecutionManager.EXPECT().ReadHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    5,
		MaxEventID:    6,
		PageSize:      10,
		NextPageToken: nil,
		ShardID:       shardID,
	}).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: []*historypb.HistoryEvent{
			{
				EventId:   int64(5),
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED,
				Attributes: &historypb.HistoryEvent_WorkflowExecutionFailedEventAttributes{
					WorkflowExecutionFailedEventAttributes: &historypb.WorkflowExecutionFailedEventAttributes{
						Failure:                      &failurepb.Failure{Message: "this workflow failed"},
						RetryState:                   enumspb.RETRY_STATE_IN_PROGRESS,
						WorkflowTaskCompletedEventId: 4,
						NewExecutionRunId:            newRunID,
					},
				},
			},
		},
		NextPageToken: []byte{},
		Size:          1,
	}, nil).Times(2)

	s.mockExecutionManager.EXPECT().TrimHistoryBranch(gomock.Any(), gomock.Any()).Return(nil, nil).AnyTimes()
	s.mockSearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), false).Return(searchattribute.TestNameTypeMap, nil).AnyTimes()

	config := s.newConfig()
	config.AccessHistoryFraction = dc.GetFloatPropertyFn(0.0)
	wh := s.getWorkflowHandler(config)

	oldGoSDKVersion := "1.9.1"
	newGoSDKVersion := "1.10.1"

	// new sdk: should see failed event
	ctx := headers.SetVersionsForTests(context.Background(), newGoSDKVersion, headers.ClientNameGoSDK, headers.SupportedServerVersions, headers.AllFeatures)
	resp, err := wh.GetWorkflowExecutionHistory(ctx, req)
	s.NoError(err)
	s.False(resp.Archived)
	event := resp.History.Events[0]
	s.Equal(int64(5), event.EventId)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_FAILED, event.EventType)
	attrs := event.GetWorkflowExecutionFailedEventAttributes()
	s.Equal("this workflow failed", attrs.Failure.Message)
	s.Equal(newRunID, attrs.NewExecutionRunId)
	s.Equal(enumspb.RETRY_STATE_IN_PROGRESS, attrs.RetryState)

	// old sdk: should see continued-as-new event
	// TODO: We can remove this once we no longer support SDK versions prior to around September 2021.
	// See comment in workflowHandler.go:GetWorkflowExecutionHistory
	ctx = headers.SetVersionsForTests(context.Background(), oldGoSDKVersion, headers.ClientNameGoSDK, headers.SupportedServerVersions, "")
	resp, err = wh.GetWorkflowExecutionHistory(ctx, req)
	s.NoError(err)
	s.False(resp.Archived)
	event = resp.History.Events[0]
	s.Equal(int64(5), event.EventId)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_CONTINUED_AS_NEW, event.EventType)
	attrs2 := event.GetWorkflowExecutionContinuedAsNewEventAttributes()
	s.Equal(newRunID, attrs2.NewExecutionRunId)
	s.Equal("this workflow failed", attrs2.Failure.Message)
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/historyEngine_test.go
func (s *workflowHandlerSuite) TestGetWorkflowExecutionHistory_RawHistoryWithTransientDecision() {
	namespaceID := namespace.ID(uuid.New())
	namespaceName := namespace.Name("namespace")
	we := &commonpb.WorkflowExecution{WorkflowId: "wid1", RunId: uuid.New()}

	config := s.newConfig()
	config.AccessHistoryFraction = dc.GetFloatPropertyFn(0.0)
	config.SendRawWorkflowHistory = dc.GetBoolPropertyFnFilteredByNamespace(true)
	wh := s.getWorkflowHandler(config)

	branchToken := []byte{1, 2, 3}
	persistenceToken := []byte("some random persistence token")
	nextPageToken, err := serializeHistoryToken(&tokenspb.HistoryContinuation{
		RunId:            we.GetRunId(),
		FirstEventId:     common.FirstEventID,
		NextEventId:      5,
		PersistenceToken: persistenceToken,
		TransientWorkflowTask: &historyspb.TransientWorkflowTaskInfo{
			HistorySuffix: []*historypb.HistoryEvent{
				{
					EventId:   5,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED,
				},
				{
					EventId:   6,
					EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
				},
			},
		},
		BranchToken: branchToken,
	})
	s.NoError(err)
	req := &workflowservice.GetWorkflowExecutionHistoryRequest{
		Namespace:              namespaceName.String(),
		Execution:              we,
		MaximumPageSize:        10,
		NextPageToken:          nextPageToken,
		WaitNewEvent:           false,
		HistoryEventFilterType: enumspb.HISTORY_EVENT_FILTER_TYPE_ALL_EVENT,
		SkipArchival:           true,
	}

	shardID := common.WorkflowIDToHistoryShard(namespaceID.String(), we.WorkflowId, numHistoryShards)

	s.mockNamespaceCache.EXPECT().GetNamespaceID(namespaceName).Return(namespaceID, nil).AnyTimes()

	historyBlob1, err := wh.payloadSerializer.SerializeEvent(
		&historypb.HistoryEvent{
			EventId:   int64(3),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED,
		},
		enumspb.ENCODING_TYPE_PROTO3,
	)
	s.NoError(err)
	historyBlob2, err := wh.payloadSerializer.SerializeEvent(
		&historypb.HistoryEvent{
			EventId:   int64(4),
			EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_TIMED_OUT,
		},
		enumspb.ENCODING_TYPE_PROTO3,
	)
	s.NoError(err)
	s.mockExecutionManager.EXPECT().ReadRawHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		BranchToken:   branchToken,
		MinEventID:    1,
		MaxEventID:    5,
		PageSize:      10,
		NextPageToken: persistenceToken,
		ShardID:       shardID,
	}).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{historyBlob1, historyBlob2},
		NextPageToken:     []byte{},
		Size:              1,
	}, nil).Times(1)

	resp, err := wh.GetWorkflowExecutionHistory(context.Background(), req)
	s.NoError(err)
	s.False(resp.Archived)
	s.Empty(resp.History.Events)
	s.Len(resp.RawHistory, 4)
	event, err := wh.payloadSerializer.DeserializeEvent(resp.RawHistory[2])
	s.NoError(err)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_SCHEDULED, event.EventType)
	event, err = wh.payloadSerializer.DeserializeEvent(resp.RawHistory[3])
	s.NoError(err)
	s.Equal(enumspb.EVENT_TYPE_WORKFLOW_TASK_STARTED, event.EventType)
}
