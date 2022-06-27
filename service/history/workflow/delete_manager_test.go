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

package workflow

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	carchiver "go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/worker/archiver"
)

type (
	deleteManagerWorkflowSuite struct {
		suite.Suite
		*require.Assertions

		controller            *gomock.Controller
		mockCache             *MockCache
		mockArchivalClient    *archiver.MockClient
		mockShardContext      *shard.MockContext
		mockClock             *clock.EventTimeSource
		mockNamespaceRegistry *namespace.MockRegistry
		mockMetadata          *cluster.MockMetadata

		deleteManager DeleteManager
	}
)

func TestDeleteManagerSuite(t *testing.T) {
	s := &deleteManagerWorkflowSuite{}
	suite.Run(t, s)
}

func (s *deleteManagerWorkflowSuite) SetupSuite() {

}

func (s *deleteManagerWorkflowSuite) TearDownSuite() {

}

func (s *deleteManagerWorkflowSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockCache = NewMockCache(s.controller)
	s.mockArchivalClient = archiver.NewMockClient(s.controller)
	s.mockClock = clock.NewEventTimeSource()
	s.mockNamespaceRegistry = namespace.NewMockRegistry(s.controller)
	s.mockMetadata = cluster.NewMockMetadata(s.controller)

	config := tests.NewDynamicConfig()
	s.mockShardContext = shard.NewMockContext(s.controller)
	s.mockShardContext.EXPECT().GetMetricsClient().Return(metrics.NoopClient).AnyTimes()
	s.mockShardContext.EXPECT().GetNamespaceRegistry().Return(s.mockNamespaceRegistry).AnyTimes()
	s.mockShardContext.EXPECT().GetClusterMetadata().Return(s.mockMetadata).AnyTimes()

	s.deleteManager = NewDeleteManager(
		s.mockShardContext,
		s.mockCache,
		config,
		s.mockArchivalClient,
		s.mockClock,
	)
}

func (s *deleteManagerWorkflowSuite) TestDeleteDeletedWorkflowExecution() {
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}

	mockWeCtx := NewMockContext(s.controller)
	mockMutableState := NewMockMutableState(s.controller)
	mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte{22, 8, 78}, nil)
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED})
	closeTime := time.Date(1978, 8, 22, 1, 2, 3, 4, time.UTC)
	mockMutableState.EXPECT().GetWorkflowCloseTime(gomock.Any()).Return(&closeTime, nil)

	s.mockShardContext.EXPECT().DeleteWorkflowExecution(
		gomock.Any(),
		definition.WorkflowKey{
			NamespaceID: tests.NamespaceID.String(),
			WorkflowID:  tests.WorkflowID,
			RunID:       tests.RunID,
		},
		[]byte{22, 8, 78},
		nil,
		&closeTime,
	).Return(nil)
	mockWeCtx.EXPECT().Clear()

	err := s.deleteManager.DeleteWorkflowExecution(
		context.Background(),
		tests.NamespaceID,
		we,
		mockWeCtx,
		mockMutableState,
		false,
	)
	s.NoError(err)
}

func (s *deleteManagerWorkflowSuite) TestDeleteDeletedWorkflowExecution_Error() {
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}

	mockWeCtx := NewMockContext(s.controller)
	mockMutableState := NewMockMutableState(s.controller)
	mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte{22, 8, 78}, nil)
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED})
	closeTime := time.Date(1978, 8, 22, 1, 2, 3, 4, time.UTC)
	mockMutableState.EXPECT().GetWorkflowCloseTime(gomock.Any()).Return(&closeTime, nil)

	s.mockShardContext.EXPECT().DeleteWorkflowExecution(
		gomock.Any(),
		definition.WorkflowKey{
			NamespaceID: tests.NamespaceID.String(),
			WorkflowID:  tests.WorkflowID,
			RunID:       tests.RunID,
		},
		[]byte{22, 8, 78},
		nil,
		&closeTime,
	).Return(serviceerror.NewInternal("test error"))

	err := s.deleteManager.DeleteWorkflowExecution(
		context.Background(),
		tests.NamespaceID,
		we,
		mockWeCtx,
		mockMutableState,
		false,
	)
	s.Error(err)
}

func (s *deleteManagerWorkflowSuite) TestDeleteWorkflowExecution_OpenWorkflow() {
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	now := time.Now()

	mockWeCtx := NewMockContext(s.controller)
	mockMutableState := NewMockMutableState(s.controller)
	mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte{22, 8, 78}, nil)
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{StartTime: &now})
	s.mockShardContext.EXPECT().DeleteWorkflowExecution(
		gomock.Any(),
		definition.WorkflowKey{
			NamespaceID: tests.NamespaceID.String(),
			WorkflowID:  tests.WorkflowID,
			RunID:       tests.RunID,
		},
		[]byte{22, 8, 78},
		&now,
		nil,
	).Return(nil)
	mockWeCtx.EXPECT().Clear()

	err := s.deleteManager.DeleteWorkflowExecution(
		context.Background(),
		tests.NamespaceID,
		we,
		mockWeCtx,
		mockMutableState,
		true,
	)
	s.NoError(err)
}

func (s *deleteManagerWorkflowSuite) TestAddDeleteWorkflowExecutionTask() {
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}
	mockMutableState := NewMockMutableState(s.controller)
	mockMutableState.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()
	mockMutableState.EXPECT().GetWorkflowKey().Return(definition.NewWorkflowKey(tests.NamespaceID.String(), tests.WorkflowID, tests.RunID)).AnyTimes()
	s.mockShardContext.EXPECT().GetShardID().Return(int32(1)).AnyTimes()
	s.mockShardContext.EXPECT().AddTasks(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	// Both queues are right at the minimum level.
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		CloseTransferTaskId:   1000,
		CloseVisibilityTaskId: 1001}).
		Times(4)
	err := s.deleteManager.AddDeleteWorkflowExecutionTask(
		context.Background(),
		tests.NamespaceID,
		we,
		mockMutableState,
		1000,
		1001,
	)
	s.NoError(err)

	// Workflow execution is not closed.
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		CloseTransferTaskId:   0,
		CloseVisibilityTaskId: 0}).
		Times(2)
	err = s.deleteManager.AddDeleteWorkflowExecutionTask(
		context.Background(),
		tests.NamespaceID,
		we,
		mockMutableState,
		1000,
		1001,
	)
	s.NoError(err)

	// Visibility close task is not processed.
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		CloseTransferTaskId:   1000,
		CloseVisibilityTaskId: 0}).
		Times(3)
	err = s.deleteManager.AddDeleteWorkflowExecutionTask(
		context.Background(),
		tests.NamespaceID,
		we,
		mockMutableState,
		1000,
		1001,
	)
	s.NoError(err)

	// Both queues are behind in active cluster.
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		CloseTransferTaskId:   1000,
		CloseVisibilityTaskId: 1001}).
		Times(2)
	mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry)
	s.mockMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName)
	err = s.deleteManager.AddDeleteWorkflowExecutionTask(
		context.Background(),
		tests.NamespaceID,
		we,
		mockMutableState,
		200,
		201,
	)
	s.ErrorIs(err, consts.ErrWorkflowNotReady)

	// Only visibility queue is behind (cluster doesn't matter).
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		CloseTransferTaskId:   1000,
		CloseVisibilityTaskId: 1001}).
		Times(4)
	err = s.deleteManager.AddDeleteWorkflowExecutionTask(
		context.Background(),
		tests.NamespaceID,
		we,
		mockMutableState,
		1000,
		1000,
	)
	s.ErrorIs(err, consts.ErrWorkflowNotReady)

	// Only transfer queue is behind in active cluster.
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		CloseTransferTaskId:   1000,
		CloseVisibilityTaskId: 1001}).
		Times(2)
	mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry)
	s.mockMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName)
	err = s.deleteManager.AddDeleteWorkflowExecutionTask(
		context.Background(),
		tests.NamespaceID,
		we,
		mockMutableState,
		999,
		1001,
	)
	s.ErrorIs(err, consts.ErrWorkflowNotReady)

	// Only transfer queue is behind in standby cluster.
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		CloseTransferTaskId:   1000,
		CloseVisibilityTaskId: 1001}).
		Times(4)
	mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry)
	s.mockMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestAlternativeClusterName)
	err = s.deleteManager.AddDeleteWorkflowExecutionTask(
		context.Background(),
		tests.NamespaceID,
		we,
		mockMutableState,
		999,
		1001,
	)
	s.NoError(err)

	// Both queues are behind in standby cluster.
	mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		CloseTransferTaskId:   1000,
		CloseVisibilityTaskId: 1001}).
		Times(4)
	mockMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry)
	s.mockMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestAlternativeClusterName)
	err = s.deleteManager.AddDeleteWorkflowExecutionTask(
		context.Background(),
		tests.NamespaceID,
		we,
		mockMutableState,
		999,
		1000,
	)
	s.ErrorIs(err, consts.ErrWorkflowNotReady)
}

func (s *deleteManagerWorkflowSuite) TestDeleteWorkflowExecutionRetention_ArchivalNotInline() {
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}

	mockWeCtx := NewMockContext(s.controller)
	mockMutableState := NewMockMutableState(s.controller)

	mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte{22, 8, 78}, nil)
	mockMutableState.EXPECT().GetExecutionState().Return(&persistencespb.WorkflowExecutionState{State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED})
	closeTime := time.Date(1978, 8, 22, 1, 2, 3, 4, time.UTC)
	mockMutableState.EXPECT().GetWorkflowCloseTime(gomock.Any()).Return(&closeTime, nil)

	// ====================== Archival mocks =======================================
	mockMutableState.EXPECT().GetNamespaceEntry().Return(namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Name: tests.Namespace.String(),
		},
		&persistencespb.NamespaceConfig{
			HistoryArchivalState: enums.ARCHIVAL_STATE_ENABLED,
		},
		"target-cluster",
	))

	mockClusterArchivalMetadata := carchiver.NewMockArchivalMetadata(s.controller)
	mockClusterArchivalConfig := carchiver.NewMockArchivalConfig(s.controller)
	s.mockShardContext.EXPECT().GetArchivalMetadata().Return(mockClusterArchivalMetadata)
	mockClusterArchivalMetadata.EXPECT().GetHistoryConfig().Return(mockClusterArchivalConfig)
	mockClusterArchivalConfig.EXPECT().ClusterConfiguredForArchival().Return(true)
	mockMutableState.EXPECT().GetLastWriteVersion().Return(int64(1), nil)
	s.mockShardContext.EXPECT().GetShardID().Return(int32(1))
	mockMutableState.EXPECT().GetNextEventID().Return(int64(1))
	mockWeCtx.EXPECT().LoadExecutionStats(gomock.Any()).Return(&persistencespb.ExecutionStats{
		HistorySize: 22,
	}, nil)
	mockSearchAttributesProvider := searchattribute.NewMockProvider(s.controller)
	mockSearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), gomock.Any()).Return(searchattribute.TestNameTypeMap, nil)
	s.mockShardContext.EXPECT().GetSearchAttributesProvider().Return(mockSearchAttributesProvider)
	s.mockArchivalClient.EXPECT().Archive(gomock.Any(), archiverClientRequestMatcher{inline: true}).Return(&archiver.ClientResponse{
		HistoryArchivedInline: false,
	}, nil)
	// =============================================================

	s.mockShardContext.EXPECT().DeleteWorkflowExecution(
		gomock.Any(),
		definition.WorkflowKey{
			NamespaceID: tests.NamespaceID.String(),
			WorkflowID:  tests.WorkflowID,
			RunID:       tests.RunID,
		},
		nil,
		nil,
		&closeTime,
	).Return(nil)
	mockWeCtx.EXPECT().Clear()

	err := s.deleteManager.DeleteWorkflowExecutionByRetention(
		context.Background(),
		tests.NamespaceID,
		we,
		mockWeCtx,
		mockMutableState,
	)
	s.NoError(err)
}

func (s *deleteManagerWorkflowSuite) TestDeleteWorkflowExecutionRetention_Archival_SendSignalErr() {
	we := commonpb.WorkflowExecution{
		WorkflowId: tests.WorkflowID,
		RunId:      tests.RunID,
	}

	mockWeCtx := NewMockContext(s.controller)
	mockMutableState := NewMockMutableState(s.controller)

	mockMutableState.EXPECT().GetCurrentBranchToken().Return([]byte{22, 8, 78}, nil)

	// ====================== Archival mocks =======================================
	mockMutableState.EXPECT().GetNamespaceEntry().Return(namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Name: tests.Namespace.String(),
		},
		&persistencespb.NamespaceConfig{
			HistoryArchivalState: enums.ARCHIVAL_STATE_ENABLED,
		},
		"target-cluster",
	))

	mockClusterArchivalMetadata := carchiver.NewMockArchivalMetadata(s.controller)
	mockClusterArchivalConfig := carchiver.NewMockArchivalConfig(s.controller)
	s.mockShardContext.EXPECT().GetArchivalMetadata().Return(mockClusterArchivalMetadata)
	mockClusterArchivalMetadata.EXPECT().GetHistoryConfig().Return(mockClusterArchivalConfig)
	mockClusterArchivalConfig.EXPECT().ClusterConfiguredForArchival().Return(true)
	mockMutableState.EXPECT().GetLastWriteVersion().Return(int64(1), nil)
	s.mockShardContext.EXPECT().GetShardID().Return(int32(1))
	mockMutableState.EXPECT().GetNextEventID().Return(int64(1))
	mockWeCtx.EXPECT().LoadExecutionStats(gomock.Any()).Return(&persistencespb.ExecutionStats{
		HistorySize: 22 * 1024 * 1024 * 1024,
	}, nil)
	mockSearchAttributesProvider := searchattribute.NewMockProvider(s.controller)
	mockSearchAttributesProvider.EXPECT().GetSearchAttributes(gomock.Any(), gomock.Any()).Return(searchattribute.TestNameTypeMap, nil)
	s.mockShardContext.EXPECT().GetSearchAttributesProvider().Return(mockSearchAttributesProvider)
	s.mockArchivalClient.EXPECT().Archive(gomock.Any(), archiverClientRequestMatcher{inline: false}).Return(nil, errors.New("failed to send signal"))
	// =============================================================

	err := s.deleteManager.DeleteWorkflowExecutionByRetention(
		context.Background(),
		tests.NamespaceID,
		we,
		mockWeCtx,
		mockMutableState,
	)
	s.Error(err)
}

type (
	archiverClientRequestMatcher struct {
		inline bool
	}
)

func (m archiverClientRequestMatcher) Matches(x interface{}) bool {
	req := x.(*archiver.ClientRequest)
	return req.CallerService == common.HistoryServiceName &&
		req.AttemptArchiveInline == m.inline &&
		req.ArchiveRequest.Targets[0] == archiver.ArchiveTargetHistory
}

func (m archiverClientRequestMatcher) String() string {
	return "archiverClientRequestMatcher"
}
