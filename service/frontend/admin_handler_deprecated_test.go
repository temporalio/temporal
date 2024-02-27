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
	"errors"
	"fmt"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
)

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/historyEngine_test.go
func (s *adminHandlerSuite) Test_GetWorkflowExecutionRawHistoryV2_FailedOnInvalidWorkflowID() {
	ctx := context.Background()
	_, err := s.handler.GetWorkflowExecutionRawHistoryV2(ctx,
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: s.namespaceID.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "",
				RunId:      uuid.New(),
			},
			StartEventId:      1,
			StartEventVersion: 100,
			EndEventId:        10,
			EndEventVersion:   100,
			MaximumPageSize:   1,
			NextPageToken:     nil,
		})
	s.Error(err)
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/historyEngine_test.go
func (s *adminHandlerSuite) Test_GetWorkflowExecutionRawHistoryV2_FailedOnInvalidRunID() {
	ctx := context.Background()
	_, err := s.handler.GetWorkflowExecutionRawHistoryV2(ctx,
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: s.namespaceID.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "workflowID",
				RunId:      "runID",
			},
			StartEventId:      1,
			StartEventVersion: 100,
			EndEventId:        10,
			EndEventVersion:   100,
			MaximumPageSize:   1,
			NextPageToken:     nil,
		})
	s.Error(err)
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/historyEngine_test.go
func (s *adminHandlerSuite) Test_GetWorkflowExecutionRawHistoryV2_FailedOnInvalidSize() {
	ctx := context.Background()
	_, err := s.handler.GetWorkflowExecutionRawHistoryV2(ctx,
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: s.namespaceID.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "workflowID",
				RunId:      uuid.New(),
			},
			StartEventId:      1,
			StartEventVersion: 100,
			EndEventId:        10,
			EndEventVersion:   100,
			MaximumPageSize:   -1,
			NextPageToken:     nil,
		})
	s.Error(err)
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/historyEngine_test.go
func (s *adminHandlerSuite) Test_GetWorkflowExecutionRawHistoryV2_FailedOnNamespaceCache() {
	ctx := context.Background()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(nil, fmt.Errorf("test"))
	_, err := s.handler.GetWorkflowExecutionRawHistoryV2(ctx,
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: s.namespaceID.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "workflowID",
				RunId:      uuid.New(),
			},
			StartEventId:      1,
			StartEventVersion: 100,
			EndEventId:        10,
			EndEventVersion:   100,
			MaximumPageSize:   1,
			NextPageToken:     nil,
		})
	s.Error(err)
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/historyEngine_test.go
func (s *adminHandlerSuite) Test_GetWorkflowExecutionRawHistoryV2() {
	ctx := context.Background()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(s.namespaceEntry, nil).AnyTimes()
	branchToken := []byte{1}
	versionHistory := versionhistory.NewVersionHistory(branchToken, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(int64(10), int64(100)),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	mState := &historyservice.GetMutableStateResponse{
		NextEventId:        11,
		CurrentBranchToken: branchToken,
		VersionHistories:   versionHistories,
	}
	s.mockHistoryClient.EXPECT().GetMutableState(gomock.Any(), gomock.Any()).Return(mState, nil).AnyTimes()

	s.mockExecutionMgr.EXPECT().ReadRawHistoryBranch(gomock.Any(), gomock.Any()).Return(&persistence.ReadRawHistoryBranchResponse{
		HistoryEventBlobs: []*commonpb.DataBlob{},
		NextPageToken:     []byte{},
		Size:              0,
	}, nil)
	_, err := s.handler.GetWorkflowExecutionRawHistoryV2(ctx,
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: s.namespaceID.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "workflowID",
				RunId:      uuid.New(),
			},
			StartEventId:      1,
			StartEventVersion: 100,
			EndEventId:        10,
			EndEventVersion:   100,
			MaximumPageSize:   10,
			NextPageToken:     nil,
		})
	s.NoError(err)
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/historyEngine_test.go
func (s *adminHandlerSuite) Test_GetWorkflowExecutionRawHistoryV2_SameStartIDAndEndID() {
	ctx := context.Background()
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(s.namespaceID).Return(s.namespaceEntry, nil).AnyTimes()
	branchToken := []byte{1}
	versionHistory := versionhistory.NewVersionHistory(branchToken, []*historyspb.VersionHistoryItem{
		versionhistory.NewVersionHistoryItem(int64(10), int64(100)),
	})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	mState := &historyservice.GetMutableStateResponse{
		NextEventId:        11,
		CurrentBranchToken: branchToken,
		VersionHistories:   versionHistories,
	}
	s.mockHistoryClient.EXPECT().GetMutableState(gomock.Any(), gomock.Any()).Return(mState, nil).AnyTimes()

	resp, err := s.handler.GetWorkflowExecutionRawHistoryV2(ctx,
		&adminservice.GetWorkflowExecutionRawHistoryV2Request{
			NamespaceId: s.namespaceID.String(),
			Execution: &commonpb.WorkflowExecution{
				WorkflowId: "workflowID",
				RunId:      uuid.New(),
			},
			StartEventId:      10,
			StartEventVersion: 100,
			EndEventId:        common.EmptyEventID,
			EndEventVersion:   common.EmptyVersion,
			MaximumPageSize:   1,
			NextPageToken:     nil,
		})
	s.Nil(resp.NextPageToken)
	s.NoError(err)
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/historyEngine_test.go
func (s *adminHandlerSuite) Test_SetRequestDefaultValueAndGetTargetVersionHistory_DefinedStartAndEnd() {
	inputStartEventID := int64(1)
	inputStartVersion := int64(10)
	inputEndEventID := int64(100)
	inputEndVersion := int64(11)
	firstItem := versionhistory.NewVersionHistoryItem(inputStartEventID, inputStartVersion)
	endItem := versionhistory.NewVersionHistoryItem(inputEndEventID, inputEndVersion)
	versionHistory := versionhistory.NewVersionHistory([]byte{}, []*historyspb.VersionHistoryItem{firstItem, endItem})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	request := &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId: s.namespaceID.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID",
			RunId:      uuid.New(),
		},
		StartEventId:      inputStartEventID,
		StartEventVersion: inputStartVersion,
		EndEventId:        inputEndEventID,
		EndEventVersion:   inputEndVersion,
		MaximumPageSize:   10,
		NextPageToken:     nil,
	}

	targetVersionHistory, err := s.handler.setRequestDefaultValueAndGetTargetVersionHistory(
		request,
		versionHistories,
	)
	s.Equal(request.GetStartEventId(), inputStartEventID)
	s.Equal(request.GetEndEventId(), inputEndEventID)
	s.Equal(targetVersionHistory, versionHistory)
	s.NoError(err)
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/historyEngine_test.go
func (s *adminHandlerSuite) Test_SetRequestDefaultValueAndGetTargetVersionHistory_DefinedEndEvent() {
	inputStartEventID := int64(1)
	inputEndEventID := int64(100)
	inputStartVersion := int64(10)
	inputEndVersion := int64(11)
	firstItem := versionhistory.NewVersionHistoryItem(inputStartEventID, inputStartVersion)
	targetItem := versionhistory.NewVersionHistoryItem(inputEndEventID, inputEndVersion)
	versionHistory := versionhistory.NewVersionHistory([]byte{}, []*historyspb.VersionHistoryItem{firstItem, targetItem})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	request := &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId: s.namespaceID.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID",
			RunId:      uuid.New(),
		},
		StartEventId:      common.EmptyEventID,
		StartEventVersion: common.EmptyVersion,
		EndEventId:        inputEndEventID,
		EndEventVersion:   inputEndVersion,
		MaximumPageSize:   10,
		NextPageToken:     nil,
	}

	targetVersionHistory, err := s.handler.setRequestDefaultValueAndGetTargetVersionHistory(
		request,
		versionHistories,
	)
	s.Equal(request.GetStartEventId(), inputStartEventID-1)
	s.Equal(request.GetEndEventId(), inputEndEventID)
	s.Equal(targetVersionHistory, versionHistory)
	s.NoError(err)
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/historyEngine_test.go
func (s *adminHandlerSuite) Test_SetRequestDefaultValueAndGetTargetVersionHistory_DefinedStartEvent() {
	inputStartEventID := int64(1)
	inputEndEventID := int64(100)
	inputStartVersion := int64(10)
	inputEndVersion := int64(11)
	firstItem := versionhistory.NewVersionHistoryItem(inputStartEventID, inputStartVersion)
	targetItem := versionhistory.NewVersionHistoryItem(inputEndEventID, inputEndVersion)
	versionHistory := versionhistory.NewVersionHistory([]byte{}, []*historyspb.VersionHistoryItem{firstItem, targetItem})
	versionHistories := versionhistory.NewVersionHistories(versionHistory)
	request := &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId: s.namespaceID.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID",
			RunId:      uuid.New(),
		},
		StartEventId:      inputStartEventID,
		StartEventVersion: inputStartVersion,
		EndEventId:        common.EmptyEventID,
		EndEventVersion:   common.EmptyVersion,
		MaximumPageSize:   10,
		NextPageToken:     nil,
	}

	targetVersionHistory, err := s.handler.setRequestDefaultValueAndGetTargetVersionHistory(
		request,
		versionHistories,
	)
	s.Equal(request.GetStartEventId(), inputStartEventID)
	s.Equal(request.GetEndEventId(), inputEndEventID+1)
	s.Equal(targetVersionHistory, versionHistory)
	s.NoError(err)
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/historyEngine_test.go
func (s *adminHandlerSuite) Test_SetRequestDefaultValueAndGetTargetVersionHistory_NonCurrentBranch() {
	inputStartEventID := int64(1)
	inputEndEventID := int64(100)
	inputStartVersion := int64(10)
	inputEndVersion := int64(101)
	item1 := versionhistory.NewVersionHistoryItem(inputStartEventID, inputStartVersion)
	item2 := versionhistory.NewVersionHistoryItem(inputEndEventID, inputEndVersion)
	versionHistory1 := versionhistory.NewVersionHistory([]byte{}, []*historyspb.VersionHistoryItem{item1, item2})
	item3 := versionhistory.NewVersionHistoryItem(int64(10), int64(20))
	item4 := versionhistory.NewVersionHistoryItem(int64(20), int64(51))
	versionHistory2 := versionhistory.NewVersionHistory([]byte{}, []*historyspb.VersionHistoryItem{item1, item3, item4})
	versionHistories := versionhistory.NewVersionHistories(versionHistory1)
	_, _, err := versionhistory.AddVersionHistory(versionHistories, versionHistory2)
	s.NoError(err)
	request := &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		NamespaceId: s.namespaceID.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: "workflowID",
			RunId:      uuid.New(),
		},
		StartEventId:      9,
		StartEventVersion: 20,
		EndEventId:        inputEndEventID,
		EndEventVersion:   inputEndVersion,
		MaximumPageSize:   10,
		NextPageToken:     nil,
	}

	targetVersionHistory, err := s.handler.setRequestDefaultValueAndGetTargetVersionHistory(
		request,
		versionHistories,
	)
	s.Equal(request.GetStartEventId(), inputStartEventID)
	s.Equal(request.GetEndEventId(), inputEndEventID)
	s.Equal(targetVersionHistory, versionHistory1)
	s.NoError(err)
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/historyEngine_test.go
func (s *adminHandlerSuite) TestDeleteWorkflowExecution_DeleteCurrentExecution() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "workflowID",
	}

	request := &adminservice.DeleteWorkflowExecutionRequest{
		Namespace: s.namespace.String(),
		Execution: &execution,
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceID(s.namespace).Return(s.namespaceID, nil).AnyTimes()

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil, errors.New("some random error"))
	resp, err := s.handler.DeleteWorkflowExecution(context.Background(), request)
	s.Nil(resp)
	s.Error(err)

	mutableState := &persistencespb.WorkflowMutableState{
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			VersionHistories: &historyspb.VersionHistories{
				CurrentVersionHistoryIndex: 0,
				Histories: []*historyspb.VersionHistory{
					{BranchToken: []byte("branch1")},
					{BranchToken: []byte("branch2")},
					{BranchToken: []byte("branch3")},
				},
			},
		},
	}

	shardID := common.WorkflowIDToHistoryShard(
		s.namespaceID.String(),
		execution.GetWorkflowId(),
		s.handler.numberOfHistoryShards,
	)
	runID := uuid.New()
	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetCurrentExecutionResponse{
		StartRequestID: uuid.New(),
		RunID:          runID,
		State:          enums.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:         enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	}, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: s.namespaceID.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       runID,
	}).Return(&persistence.GetWorkflowExecutionResponse{State: mutableState}, nil)
	s.mockHistoryClient.EXPECT().DeleteWorkflowVisibilityRecord(gomock.Any(), &historyservice.DeleteWorkflowVisibilityRecordRequest{
		NamespaceId: s.namespaceID.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: execution.WorkflowId,
			RunId:      runID,
		},
	}).Return(&historyservice.DeleteWorkflowVisibilityRecordResponse{}, nil)
	s.mockExecutionMgr.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), &persistence.DeleteCurrentWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: s.namespaceID.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       runID,
	}).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), &persistence.DeleteWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: s.namespaceID.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       runID,
	}).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteHistoryBranch(gomock.Any(), gomock.Any()).Times(len(mutableState.ExecutionInfo.VersionHistories.Histories))

	_, err = s.handler.DeleteWorkflowExecution(context.Background(), request)
	s.NoError(err)
}

// DEPRECATED: DO NOT MODIFY UNLESS ALSO APPLIED TO ./service/history/historyEngine_test.go
func (s *adminHandlerSuite) TestDeleteWorkflowExecution_LoadMutableStateFailed() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "workflowID",
		RunId:      uuid.New(),
	}

	request := &adminservice.DeleteWorkflowExecutionRequest{
		Namespace: s.namespace.String(),
		Execution: &execution,
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceID(s.namespace).Return(s.namespaceID, nil).AnyTimes()

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, errors.New("some random error"))
	s.mockHistoryClient.EXPECT().DeleteWorkflowVisibilityRecord(gomock.Any(), gomock.Any()).Return(&historyservice.DeleteWorkflowVisibilityRecordResponse{}, nil)
	s.mockExecutionMgr.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)

	_, err := s.handler.DeleteWorkflowExecution(context.Background(), request)
	s.NoError(err)
}
