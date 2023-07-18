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

package history

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/persistence/visibility/store/standard/cassandra"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/tests"
)

type (
	historyUtilSuite struct {
		suite.Suite
		*require.Assertions

		controller         *gomock.Controller
		logger             log.Logger
		mockExecutionMgr   *persistence.MockExecutionManager
		mockVisibilityMgr  *manager.MockVisibilityManager
		mockNamespaceCache *namespace.MockRegistry
	}
)

func TestHistoryUtilSuite(t *testing.T) {
	s := new(historyUtilSuite)
	suite.Run(t, s)
}

func (s *historyUtilSuite) SetupSuite() {

}

func (s *historyUtilSuite) TearDownSuite() {
}

func (s *historyUtilSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.logger = log.NewTestLogger()
	s.mockExecutionMgr = persistence.NewMockExecutionManager(s.controller)
	s.mockVisibilityMgr = manager.NewMockVisibilityManager(s.controller)
	s.mockNamespaceCache = namespace.NewMockRegistry(s.controller)
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.LocalNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceCache.EXPECT().GetNamespace(tests.Namespace).Return(tests.LocalNamespaceEntry, nil).AnyTimes()
}

func (s *historyUtilSuite) TearDownTest() {
	s.controller.Finish()
}
func (s *historyUtilSuite) TestDeleteWorkflowExecution_DeleteCurrentExecution() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "workflowID",
	}

	shardID := common.WorkflowIDToHistoryShard(
		tests.NamespaceID.String(),
		execution.GetWorkflowId(),
		1,
	)

	request := &historyservice.ForceDeleteWorkflowExecutionRequest{
		NamespaceId: tests.NamespaceID.String(),
		ShardId:     shardID,
		Execution:   &execution,
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceID(tests.Namespace).Return(tests.NamespaceID, nil).AnyTimes()
	s.mockVisibilityMgr.EXPECT().HasStoreName(cassandra.CassandraPersistenceName).Return(false)
	s.mockVisibilityMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).AnyTimes()

	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(nil, errors.New("some random error"))
	resp, err := forceDeleteWorkflowExecution(context.Background(), request, s.mockExecutionMgr, s.mockVisibilityMgr, s.logger)
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

	runID := uuid.New()
	s.mockExecutionMgr.EXPECT().GetCurrentExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetCurrentExecutionResponse{
		StartRequestID: uuid.New(),
		RunID:          runID,
		State:          enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
		Status:         enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	}, nil)
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       runID,
	}).Return(&persistence.GetWorkflowExecutionResponse{State: mutableState}, nil)
	s.mockExecutionMgr.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), &persistence.DeleteCurrentWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       runID,
	}).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), &persistence.DeleteWorkflowExecutionRequest{
		ShardID:     shardID,
		NamespaceID: tests.NamespaceID.String(),
		WorkflowID:  execution.WorkflowId,
		RunID:       runID,
	}).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteHistoryBranch(gomock.Any(), gomock.Any()).Times(len(mutableState.ExecutionInfo.VersionHistories.Histories))

	_, err = forceDeleteWorkflowExecution(context.Background(), request, s.mockExecutionMgr, s.mockVisibilityMgr, s.logger)
	s.NoError(err)
}

func (s *historyUtilSuite) TestDeleteWorkflowExecution_LoadMutableStateFailed() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "workflowID",
		RunId:      uuid.New(),
	}

	shardID := common.WorkflowIDToHistoryShard(
		tests.NamespaceID.String(),
		execution.GetWorkflowId(),
		1,
	)

	request := &historyservice.ForceDeleteWorkflowExecutionRequest{
		NamespaceId: tests.NamespaceID.String(),
		ShardId:     shardID,
		Execution:   &execution,
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceID(tests.Namespace).Return(tests.NamespaceID, nil).AnyTimes()
	s.mockVisibilityMgr.EXPECT().HasStoreName(cassandra.CassandraPersistenceName).Return(false)
	s.mockVisibilityMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).AnyTimes()

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil, errors.New("some random error"))
	s.mockExecutionMgr.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)

	_, err := forceDeleteWorkflowExecution(context.Background(), request, s.mockExecutionMgr, s.mockVisibilityMgr, s.logger)
	s.NoError(err)
}

func (s *historyUtilSuite) TestDeleteWorkflowExecution_CassandraVisibilityBackend() {
	execution := commonpb.WorkflowExecution{
		WorkflowId: "workflowID",
		RunId:      uuid.New(),
	}

	shardID := common.WorkflowIDToHistoryShard(
		tests.NamespaceID.String(),
		execution.GetWorkflowId(),
		1,
	)

	request := &historyservice.ForceDeleteWorkflowExecutionRequest{
		NamespaceId: tests.NamespaceID.String(),
		ShardId:     shardID,
		Execution:   &execution,
	}

	s.mockNamespaceCache.EXPECT().GetNamespaceID(tests.Namespace).Return(tests.NamespaceID, nil).AnyTimes()
	s.mockVisibilityMgr.EXPECT().HasStoreName(cassandra.CassandraPersistenceName).Return(true).AnyTimes()

	// test delete open records
	branchToken := []byte("branchToken")
	version := int64(100)
	mutableState := &persistencespb.WorkflowMutableState{
		ExecutionState: &persistencespb.WorkflowExecutionState{
			CreateRequestId: uuid.New(),
			RunId:           execution.RunId,
			State:           enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING,
			Status:          enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
		},
		NextEventId: 12,
		ExecutionInfo: &persistencespb.WorkflowExecutionInfo{
			CompletionEventBatchId: 10,
			StartTime:              timestamp.TimePtr(time.Now()),
			VersionHistories: &historyspb.VersionHistories{
				CurrentVersionHistoryIndex: 0,
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: branchToken,
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 11, Version: version},
						},
					},
				},
			},
		},
	}

	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: mutableState}, nil)
	s.mockExecutionMgr.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteHistoryBranch(gomock.Any(), gomock.Any()).Times(len(mutableState.ExecutionInfo.VersionHistories.Histories))
	s.mockVisibilityMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).AnyTimes()

	_, err := forceDeleteWorkflowExecution(context.Background(), request, s.mockExecutionMgr, s.mockVisibilityMgr, s.logger)
	s.NoError(err)

	// test delete close records
	mutableState.ExecutionState.State = enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED
	mutableState.ExecutionState.Status = enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED

	closeTime := time.Now()
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), gomock.Any()).Return(&persistence.GetWorkflowExecutionResponse{State: mutableState}, nil)
	s.mockExecutionMgr.EXPECT().ReadHistoryBranch(gomock.Any(), &persistence.ReadHistoryBranchRequest{
		ShardID:     shardID,
		BranchToken: branchToken,
		MinEventID:  mutableState.ExecutionInfo.CompletionEventBatchId,
		MaxEventID:  mutableState.NextEventId,
		PageSize:    1,
	}).Return(&persistence.ReadHistoryBranchResponse{
		HistoryEvents: []*historypb.HistoryEvent{
			{
				EventId:   10,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_TASK_COMPLETED,
				Version:   version,
				EventTime: timestamp.TimePtr(closeTime.Add(-time.Millisecond)),
			},
			{
				EventId:   11,
				EventType: enumspb.EVENT_TYPE_WORKFLOW_EXECUTION_COMPLETED,
				Version:   version,
				EventTime: timestamp.TimePtr(closeTime),
			},
		},
	}, nil)
	s.mockExecutionMgr.EXPECT().DeleteCurrentWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteWorkflowExecution(gomock.Any(), gomock.Any()).Return(nil)
	s.mockExecutionMgr.EXPECT().DeleteHistoryBranch(gomock.Any(), gomock.Any()).Times(len(mutableState.ExecutionInfo.VersionHistories.Histories))

	_, err = forceDeleteWorkflowExecution(context.Background(), request, s.mockExecutionMgr, s.mockVisibilityMgr, s.logger)
	s.NoError(err)
}
