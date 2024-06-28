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

package ndc

import (
	"context"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"google.golang.org/protobuf/types/known/timestamppb"

	enumsspb "go.temporal.io/server/api/enums/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	activityReplicatorStateSuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *shard.ContextTest
		mockNamespaceCache  *namespace.MockRegistry
		mockClusterMetadata *cluster.MockMetadata
		mockMutableState    *workflow.MockMutableState

		mockExecutionMgr *persistence.MockExecutionManager

		workflowCache *wcache.CacheImpl
		logger        log.Logger

		nDCActivityStateReplicator *ActivityStateReplicatorImpl
	}
)

func TestActivityStateReplicatorSuite(t *testing.T) {
	s := new(activityReplicatorStateSuite)
	suite.Run(t, s)
}

func (s *activityReplicatorStateSuite) SetupSuite() {

}

func (s *activityReplicatorStateSuite) TearDownSuite() {

}

func (s *activityReplicatorStateSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockMutableState = workflow.NewMockMutableState(s.controller)
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 1,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)
	s.workflowCache = wcache.NewHostLevelCache(s.mockShard.GetConfig(), metrics.NoopMetricsHandler).(*wcache.CacheImpl)

	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockExecutionMgr = s.mockShard.Resource.ExecutionMgr
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetAllClusterInfo().Return(cluster.TestAllClusterInfo).AnyTimes()

	s.logger = s.mockShard.GetLogger()

	s.nDCActivityStateReplicator = NewActivityStateReplicator(
		s.mockShard,
		s.workflowCache,
		s.logger,
	)
}

func (s *activityReplicatorStateSuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *activityReplicatorStateSuite) TestRefreshTask_DiffCluster() {
	version := int64(99)
	attempt := int32(1)
	localActivityInfo := &persistencespb.ActivityInfo{
		Version: int64(100),
		Attempt: attempt,
	}

	s.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(version, localActivityInfo.Version).Return(false)

	shouldReset := s.nDCActivityStateReplicator.shouldResetActivityTimerTaskMask(
		version,
		attempt,
		localActivityInfo,
	)
	s.True(shouldReset)
}

func (s *activityReplicatorStateSuite) TestRefreshTask_SameCluster_DiffAttempt() {
	version := int64(99)
	attempt := int32(1)
	localActivityInfo := &persistencespb.ActivityInfo{
		Version: version,
		Attempt: attempt + 1,
	}

	s.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(version, version).Return(true)

	shouldReset := s.nDCActivityStateReplicator.shouldResetActivityTimerTaskMask(
		version,
		attempt,
		localActivityInfo,
	)
	s.True(shouldReset)
}

func (s *activityReplicatorStateSuite) TestRefreshTask_SameCluster_SameAttempt() {
	version := int64(99)
	attempt := int32(1)
	localActivityInfo := &persistencespb.ActivityInfo{
		Version: version,
		Attempt: attempt,
	}

	s.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(version, version).Return(true)

	shouldReset := s.nDCActivityStateReplicator.shouldResetActivityTimerTaskMask(
		version,
		attempt,
		localActivityInfo,
	)
	s.False(shouldReset)
}

func (s *activityReplicatorStateSuite) TestActivity_LocalVersionLarger() {
	version := int64(123)
	attempt := int32(1)
	lastHeartbeatTime := time.Now()
	localActivityInfo := &persistencespb.ActivityInfo{
		Version: version + 1,
		Attempt: attempt,
	}

	apply := s.nDCActivityStateReplicator.compareActivity(
		version,
		attempt,
		lastHeartbeatTime,
		localActivityInfo,
	)
	s.False(apply)
}

func (s *activityReplicatorStateSuite) TestActivity_IncomingVersionLarger() {
	version := int64(123)
	attempt := int32(1)
	lastHeartbeatTime := time.Now()
	localActivityInfo := &persistencespb.ActivityInfo{
		Version: version - 1,
		Attempt: attempt,
	}

	apply := s.nDCActivityStateReplicator.compareActivity(
		version,
		attempt,
		lastHeartbeatTime,
		localActivityInfo,
	)
	s.True(apply)
}

func (s *activityReplicatorStateSuite) TestActivity_SameVersion_LocalAttemptLarger() {
	version := int64(123)
	attempt := int32(1)
	lastHeartbeatTime := time.Now()
	localActivityInfo := &persistencespb.ActivityInfo{
		Version: version,
		Attempt: attempt + 1,
	}

	apply := s.nDCActivityStateReplicator.compareActivity(
		version,
		attempt,
		lastHeartbeatTime,
		localActivityInfo,
	)
	s.False(apply)
}

func (s *activityReplicatorStateSuite) TestActivity_SameVersion_IncomingAttemptLarger() {
	version := int64(123)
	attempt := int32(1)
	lastHeartbeatTime := time.Now()
	localActivityInfo := &persistencespb.ActivityInfo{
		Version: version,
		Attempt: attempt - 1,
	}

	apply := s.nDCActivityStateReplicator.compareActivity(
		version,
		attempt,
		lastHeartbeatTime,
		localActivityInfo,
	)
	s.True(apply)
}

func (s *activityReplicatorStateSuite) TestActivity_SameVersion_SameAttempt_LocalHeartbeatLater() {
	version := int64(123)
	attempt := int32(1)
	lastHeartbeatTime := time.Now()
	localActivityInfo := &persistencespb.ActivityInfo{
		Version:                 version,
		Attempt:                 attempt,
		LastHeartbeatUpdateTime: timestamppb.New(lastHeartbeatTime.Add(time.Second)),
	}

	apply := s.nDCActivityStateReplicator.compareActivity(
		version,
		attempt,
		lastHeartbeatTime,
		localActivityInfo,
	)
	s.False(apply)
}

func (s *activityReplicatorStateSuite) TestActivity_SameVersion_SameAttempt_IncomingHeartbeatLater() {
	version := int64(123)
	attempt := int32(1)
	lastHeartbeatTime := time.Now()
	localActivityInfo := &persistencespb.ActivityInfo{
		Version:                 version,
		Attempt:                 attempt,
		LastHeartbeatUpdateTime: timestamppb.New(lastHeartbeatTime.Add(-time.Second)),
	}

	apply := s.nDCActivityStateReplicator.compareActivity(
		version,
		attempt,
		lastHeartbeatTime,
		localActivityInfo,
	)
	s.True(apply)
}

func (s *activityReplicatorStateSuite) TestVersionHistory_LocalIsSuperSet() {
	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID + 10,
					Version: version,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID,
				Version: version,
			},
		},
	}

	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).AnyTimes()

	apply, err := s.nDCActivityStateReplicator.compareVersionHistory(
		namespaceID,
		workflowID,
		runID,
		scheduledEventID,
		s.mockMutableState,
		incomingVersionHistory,
	)
	s.NoError(err)
	s.True(apply)
}

func (s *activityReplicatorStateSuite) TestVersionHistory_IncomingIsSuperSet_NoResend() {
	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID,
					Version: version,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID + 10,
				Version: version,
			},
		},
	}

	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).AnyTimes()

	apply, err := s.nDCActivityStateReplicator.compareVersionHistory(
		namespaceID,
		workflowID,
		runID,
		scheduledEventID,
		s.mockMutableState,
		incomingVersionHistory,
	)
	s.NoError(err)
	s.True(apply)
}

func (s *activityReplicatorStateSuite) TestVersionHistory_IncomingIsSuperSet_Resend() {
	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID - 1,
					Version: version,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID + 10,
				Version: version,
			},
		},
	}

	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).AnyTimes()

	apply, err := s.nDCActivityStateReplicator.compareVersionHistory(
		namespaceID,
		workflowID,
		runID,
		scheduledEventID,
		s.mockMutableState,
		incomingVersionHistory,
	)
	s.Equal(serviceerrors.NewRetryReplication(
		resendMissingEventMessage,
		namespaceID.String(),
		workflowID,
		runID,
		scheduledEventID-1,
		version,
		common.EmptyEventID,
		common.EmptyVersion,
	), err)
	s.False(apply)
}

func (s *activityReplicatorStateSuite) TestVersionHistory_Diverge_LocalLarger() {
	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID,
					Version: version,
				},
				{
					EventId: scheduledEventID + 1,
					Version: version + 2,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID + 10,
				Version: version,
			},
			{
				EventId: scheduledEventID + 1,
				Version: version + 1,
			},
		},
	}

	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).AnyTimes()

	apply, err := s.nDCActivityStateReplicator.compareVersionHistory(
		namespaceID,
		workflowID,
		runID,
		scheduledEventID,
		s.mockMutableState,
		incomingVersionHistory,
	)
	s.NoError(err)
	s.False(apply)
}

func (s *activityReplicatorStateSuite) TestVersionHistory_Diverge_IncomingLarger() {
	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID,
					Version: version,
				},
				{
					EventId: scheduledEventID + 1,
					Version: version + 1,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID,
				Version: version,
			},
			{
				EventId: scheduledEventID + 1,
				Version: version + 2,
			},
		},
	}

	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).AnyTimes()

	apply, err := s.nDCActivityStateReplicator.compareVersionHistory(
		namespaceID,
		workflowID,
		runID,
		scheduledEventID,
		s.mockMutableState,
		incomingVersionHistory,
	)
	s.Equal(serviceerrors.NewRetryReplication(
		resendHigherVersionMessage,
		namespaceID.String(),
		workflowID,
		runID,
		scheduledEventID,
		version,
		common.EmptyEventID,
		common.EmptyVersion,
	), err)
	s.False(apply)
}

func (s *activityReplicatorStateSuite) TestSyncActivity_WorkflowNotFound() {
	namespaceName := namespace.Name("some random namespace name")
	namespaceID := tests.NamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(100)

	request := &historyservice.SyncActivityRequest{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
		RunId:       runID,
	}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		RunID:       runID,
	}).Return(nil, serviceerror.NewNotFound(""))
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		namespace.NewGlobalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
			&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			version,
		), nil,
	).AnyTimes()

	err := s.nDCActivityStateReplicator.SyncActivityState(context.Background(), request)
	s.Nil(err)
}

func (s *activityReplicatorStateSuite) TestSyncActivities_WorkflowNotFound() {
	namespaceName := namespace.Name("some random namespace name")
	namespaceID := tests.NamespaceID
	workflowID := "some random workflow ID"
	runID := uuid.New()
	version := int64(100)

	request := &historyservice.SyncActivitiesRequest{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
		RunId:       runID,
	}
	s.mockExecutionMgr.EXPECT().GetWorkflowExecution(gomock.Any(), &persistence.GetWorkflowExecutionRequest{
		ShardID:     s.mockShard.GetShardID(),
		NamespaceID: namespaceID.String(),
		WorkflowID:  workflowID,
		RunID:       runID,
	}).Return(nil, serviceerror.NewNotFound(""))
	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		namespace.NewGlobalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
			&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			version,
		), nil,
	).AnyTimes()

	err := s.nDCActivityStateReplicator.SyncActivitiesState(context.Background(), request)
	s.Nil(err)
}

func (s *activityReplicatorStateSuite) TestSyncActivity_WorkflowClosed() {
	namespaceName := tests.Namespace
	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)
	lastWriteVersion := version

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID + 10,
					Version: version,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID,
				Version: version,
			},
		},
	}

	key := wcache.Key{
		WorkflowKey: definition.NewWorkflowKey(namespaceID.String(), workflowID, runID),
		ShardUUID:   s.mockShard.GetOwner(),
	}
	weContext := workflow.NewMockContext(s.controller)
	weContext.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(s.mockMutableState, nil)
	weContext.EXPECT().Lock(gomock.Any(), workflow.LockPriorityHigh).Return(nil)
	weContext.EXPECT().Unlock(workflow.LockPriorityHigh)
	weContext.EXPECT().IsDirty().Return(false).AnyTimes()

	_, err := s.workflowCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	request := &historyservice.SyncActivityRequest{
		NamespaceId:      namespaceID.String(),
		WorkflowId:       workflowID,
		RunId:            runID,
		Version:          version,
		ScheduledEventId: scheduledEventID,
		VersionHistory:   incomingVersionHistory,
	}

	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	).AnyTimes()

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		namespace.NewGlobalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
			&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			lastWriteVersion,
		), nil,
	).AnyTimes()

	err = s.nDCActivityStateReplicator.SyncActivityState(context.Background(), request)
	s.Nil(err)
}

func (s *activityReplicatorStateSuite) TestSyncActivities_WorkflowClosed() {
	namespaceName := tests.Namespace
	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)
	lastWriteVersion := version

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID + 10,
					Version: version,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID,
				Version: version,
			},
		},
	}

	key := wcache.Key{
		WorkflowKey: definition.NewWorkflowKey(namespaceID.String(), workflowID, runID),
		ShardUUID:   s.mockShard.GetOwner(),
	}
	weContext := workflow.NewMockContext(s.controller)
	weContext.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(s.mockMutableState, nil)
	weContext.EXPECT().Lock(gomock.Any(), workflow.LockPriorityHigh).Return(nil)
	weContext.EXPECT().Unlock(workflow.LockPriorityHigh)
	weContext.EXPECT().IsDirty().Return(false).AnyTimes()

	_, err := s.workflowCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	request := &historyservice.SyncActivitiesRequest{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
		RunId:       runID,
		ActivitiesInfo: []*historyservice.ActivitySyncInfo{
			{
				Version:          version,
				ScheduledEventId: scheduledEventID,
				VersionHistory:   incomingVersionHistory,
			},
		},
	}

	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
	).AnyTimes()

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		namespace.NewGlobalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
			&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			lastWriteVersion,
		), nil,
	).AnyTimes()

	err = s.nDCActivityStateReplicator.SyncActivitiesState(context.Background(), request)
	s.Nil(err)
}

func (s *activityReplicatorStateSuite) TestSyncActivity_ActivityNotFound() {
	namespaceName := tests.Namespace
	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)
	lastWriteVersion := version

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID + 10,
					Version: version,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID,
				Version: version,
			},
		},
	}

	key := wcache.Key{
		WorkflowKey: definition.NewWorkflowKey(namespaceID.String(), workflowID, runID),
		ShardUUID:   s.mockShard.GetOwner(),
	}
	weContext := workflow.NewMockContext(s.controller)
	weContext.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(s.mockMutableState, nil)
	weContext.EXPECT().Lock(gomock.Any(), workflow.LockPriorityHigh).Return(nil)
	weContext.EXPECT().Unlock(workflow.LockPriorityHigh)
	weContext.EXPECT().IsDirty().Return(false).AnyTimes()

	_, err := s.workflowCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	request := &historyservice.SyncActivityRequest{
		NamespaceId:      namespaceID.String(),
		WorkflowId:       workflowID,
		RunId:            runID,
		Version:          version,
		ScheduledEventId: scheduledEventID,
		VersionHistory:   incomingVersionHistory,
	}

	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityInfo(scheduledEventID).Return(nil, false)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		namespace.NewGlobalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
			&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			lastWriteVersion,
		), nil,
	).AnyTimes()

	err = s.nDCActivityStateReplicator.SyncActivityState(context.Background(), request)
	s.Nil(err)
}

func (s *activityReplicatorStateSuite) TestSyncActivities_ActivityNotFound() {
	namespaceName := tests.Namespace
	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)
	lastWriteVersion := version

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID + 10,
					Version: version,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID,
				Version: version,
			},
		},
	}

	key := wcache.Key{
		WorkflowKey: definition.NewWorkflowKey(namespaceID.String(), workflowID, runID),
		ShardUUID:   s.mockShard.GetOwner(),
	}
	weContext := workflow.NewMockContext(s.controller)
	weContext.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(s.mockMutableState, nil)
	weContext.EXPECT().Lock(gomock.Any(), workflow.LockPriorityHigh).Return(nil)
	weContext.EXPECT().Unlock(workflow.LockPriorityHigh)
	weContext.EXPECT().IsDirty().Return(false).AnyTimes()

	_, err := s.workflowCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	request := &historyservice.SyncActivitiesRequest{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
		RunId:       runID,
		ActivitiesInfo: []*historyservice.ActivitySyncInfo{
			{
				Version:          version,
				ScheduledEventId: scheduledEventID,
				VersionHistory:   incomingVersionHistory,
			},
		},
	}

	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityInfo(scheduledEventID).Return(nil, false)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		namespace.NewGlobalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
			&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			lastWriteVersion,
		), nil,
	).AnyTimes()

	err = s.nDCActivityStateReplicator.SyncActivitiesState(context.Background(), request)
	s.Nil(err)
}

func (s *activityReplicatorStateSuite) TestSyncActivity_ActivityFound_Zombie() {
	namespaceName := tests.Namespace
	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)
	lastWriteVersion := version

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID + 10,
					Version: version,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID,
				Version: version,
			},
		},
	}

	key := wcache.Key{
		WorkflowKey: definition.NewWorkflowKey(namespaceID.String(), workflowID, runID),
		ShardUUID:   s.mockShard.GetOwner(),
	}
	weContext := workflow.NewMockContext(s.controller)
	weContext.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(s.mockMutableState, nil)
	weContext.EXPECT().Lock(gomock.Any(), workflow.LockPriorityHigh).Return(nil)
	weContext.EXPECT().Unlock(workflow.LockPriorityHigh)
	weContext.EXPECT().IsDirty().Return(false).AnyTimes()

	_, err := s.workflowCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	now := time.Now()
	request := &historyservice.SyncActivityRequest{
		NamespaceId:      namespaceID.String(),
		WorkflowId:       workflowID,
		RunId:            runID,
		Version:          version,
		ScheduledEventId: scheduledEventID,
		ScheduledTime:    timestamppb.New(now),
		VersionHistory:   incomingVersionHistory,
	}

	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityInfo(scheduledEventID).Return(&persistencespb.ActivityInfo{
		Version: version,
	}, true)

	s.mockMutableState.EXPECT().UpdateActivityInfo(&historyservice.ActivitySyncInfo{
		Version:          version,
		ScheduledEventId: scheduledEventID,
		ScheduledTime:    timestamppb.New(now),
		VersionHistory:   incomingVersionHistory,
	}, false).Return(nil)
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{})
	s.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(version, version).Return(true)

	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(),
		s.mockShard,
		persistence.UpdateWorkflowModeBypassCurrent,
		workflow.Context(nil),
		workflow.MutableState(nil),
		workflow.TransactionPolicyPassive,
		(*workflow.TransactionPolicy)(nil),
	).Return(nil)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		namespace.NewGlobalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
			&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			lastWriteVersion,
		), nil,
	).AnyTimes()

	err = s.nDCActivityStateReplicator.SyncActivityState(context.Background(), request)
	s.Nil(err)
}

func (s *activityReplicatorStateSuite) TestSyncActivities_ActivityFound_Zombie() {
	namespaceName := tests.Namespace
	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)
	lastWriteVersion := version

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID + 10,
					Version: version,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID,
				Version: version,
			},
		},
	}

	key := wcache.Key{
		WorkflowKey: definition.NewWorkflowKey(namespaceID.String(), workflowID, runID),
		ShardUUID:   s.mockShard.GetOwner(),
	}
	weContext := workflow.NewMockContext(s.controller)
	weContext.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(s.mockMutableState, nil)
	weContext.EXPECT().Lock(gomock.Any(), workflow.LockPriorityHigh).Return(nil)
	weContext.EXPECT().Unlock(workflow.LockPriorityHigh)
	weContext.EXPECT().IsDirty().Return(false).AnyTimes()

	_, err := s.workflowCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	now := time.Now()
	request := &historyservice.SyncActivitiesRequest{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
		RunId:       runID,
		ActivitiesInfo: []*historyservice.ActivitySyncInfo{
			{
				Version:          version,
				ScheduledEventId: scheduledEventID,
				VersionHistory:   incomingVersionHistory,
				ScheduledTime:    timestamppb.New(now),
			},
		},
	}

	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_ZOMBIE, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityInfo(scheduledEventID).Return(&persistencespb.ActivityInfo{
		Version: version,
	}, true)
	s.mockMutableState.EXPECT().UpdateActivityInfo(&historyservice.ActivitySyncInfo{
		Version:          version,
		ScheduledEventId: scheduledEventID,
		ScheduledTime:    timestamppb.New(now),
		VersionHistory:   incomingVersionHistory,
	}, false).Return(nil)
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{})
	s.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(version, version).Return(true)

	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(),
		s.mockShard,
		persistence.UpdateWorkflowModeBypassCurrent,
		workflow.Context(nil),
		workflow.MutableState(nil),
		workflow.TransactionPolicyPassive,
		(*workflow.TransactionPolicy)(nil),
	).Return(nil)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		namespace.NewGlobalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
			&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			lastWriteVersion,
		), nil,
	).AnyTimes()

	err = s.nDCActivityStateReplicator.SyncActivitiesState(context.Background(), request)
	s.Nil(err)
}

func (s *activityReplicatorStateSuite) TestSyncActivity_ActivityFound_NonZombie() {
	namespaceName := tests.Namespace
	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)
	lastWriteVersion := version

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID + 10,
					Version: version,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID,
				Version: version,
			},
		},
	}

	key := wcache.Key{
		WorkflowKey: definition.NewWorkflowKey(namespaceID.String(), workflowID, runID),
		ShardUUID:   s.mockShard.GetOwner(),
	}
	weContext := workflow.NewMockContext(s.controller)
	weContext.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(s.mockMutableState, nil)
	weContext.EXPECT().Lock(gomock.Any(), workflow.LockPriorityHigh).Return(nil)
	weContext.EXPECT().Unlock(workflow.LockPriorityHigh)
	weContext.EXPECT().IsDirty().Return(false).AnyTimes()

	_, err := s.workflowCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	now := time.Now()
	request := &historyservice.SyncActivityRequest{
		NamespaceId:      namespaceID.String(),
		WorkflowId:       workflowID,
		RunId:            runID,
		Version:          version,
		ScheduledEventId: scheduledEventID,
		ScheduledTime:    timestamppb.New(now),
		VersionHistory:   incomingVersionHistory,
	}

	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityInfo(scheduledEventID).Return(&persistencespb.ActivityInfo{
		Version: version,
	}, true)
	s.mockMutableState.EXPECT().UpdateActivityInfo(&historyservice.ActivitySyncInfo{
		Version:          version,
		ScheduledEventId: scheduledEventID,
		ScheduledTime:    timestamppb.New(now),
		VersionHistory:   incomingVersionHistory,
	}, false).Return(nil)
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{})

	s.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(version, version).Return(true)

	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(),
		s.mockShard,
		persistence.UpdateWorkflowModeUpdateCurrent,
		workflow.Context(nil),
		workflow.MutableState(nil),
		workflow.TransactionPolicyPassive,
		(*workflow.TransactionPolicy)(nil),
	).Return(nil)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		namespace.NewGlobalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
			&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			lastWriteVersion,
		), nil,
	).AnyTimes()

	err = s.nDCActivityStateReplicator.SyncActivityState(context.Background(), request)
	s.Nil(err)
}

func (s *activityReplicatorStateSuite) TestSyncActivities_ActivityFound_NonZombie() {
	namespaceName := tests.Namespace
	namespaceID := tests.NamespaceID
	workflowID := tests.WorkflowID
	runID := uuid.New()
	scheduledEventID := int64(99)
	version := int64(100)
	lastWriteVersion := version

	localVersionHistories := &historyspb.VersionHistories{
		CurrentVersionHistoryIndex: 0,
		Histories: []*historyspb.VersionHistory{{
			BranchToken: []byte{},
			Items: []*historyspb.VersionHistoryItem{
				{
					EventId: scheduledEventID + 10,
					Version: version,
				},
			},
		}},
	}
	incomingVersionHistory := &historyspb.VersionHistory{
		BranchToken: []byte{},
		Items: []*historyspb.VersionHistoryItem{
			{
				EventId: scheduledEventID,
				Version: version,
			},
		},
	}

	key := wcache.Key{
		WorkflowKey: definition.NewWorkflowKey(namespaceID.String(), workflowID, runID),
		ShardUUID:   s.mockShard.GetOwner(),
	}
	weContext := workflow.NewMockContext(s.controller)
	weContext.EXPECT().LoadMutableState(gomock.Any(), s.mockShard).Return(s.mockMutableState, nil)
	weContext.EXPECT().Lock(gomock.Any(), workflow.LockPriorityHigh).Return(nil)
	weContext.EXPECT().Unlock(workflow.LockPriorityHigh)
	weContext.EXPECT().IsDirty().Return(false).AnyTimes()

	_, err := s.workflowCache.PutIfNotExist(key, weContext)
	s.NoError(err)

	now := time.Now()
	request := &historyservice.SyncActivitiesRequest{
		NamespaceId: namespaceID.String(),
		WorkflowId:  workflowID,
		RunId:       runID,
		ActivitiesInfo: []*historyservice.ActivitySyncInfo{
			{
				Version:          version,
				ScheduledEventId: scheduledEventID,
				VersionHistory:   incomingVersionHistory,
				ScheduledTime:    timestamppb.New(now),
			},
		},
	}

	s.mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
		VersionHistories: localVersionHistories,
	}).AnyTimes()
	s.mockMutableState.EXPECT().GetWorkflowStateStatus().Return(
		enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING,
	).AnyTimes()
	s.mockMutableState.EXPECT().GetActivityInfo(scheduledEventID).Return(&persistencespb.ActivityInfo{
		Version: version,
	}, true)
	s.mockMutableState.EXPECT().UpdateActivityInfo(&historyservice.ActivitySyncInfo{
		Version:          version,
		ScheduledEventId: scheduledEventID,
		ScheduledTime:    timestamppb.New(now),
		VersionHistory:   incomingVersionHistory,
	}, false).Return(nil)
	s.mockMutableState.EXPECT().GetPendingActivityInfos().Return(map[int64]*persistencespb.ActivityInfo{})

	s.mockClusterMetadata.EXPECT().IsVersionFromSameCluster(version, version).Return(true)

	weContext.EXPECT().UpdateWorkflowExecutionWithNew(
		gomock.Any(),
		s.mockShard,
		persistence.UpdateWorkflowModeUpdateCurrent,
		workflow.Context(nil),
		workflow.MutableState(nil),
		workflow.TransactionPolicyPassive,
		(*workflow.TransactionPolicy)(nil),
	).Return(nil)

	s.mockNamespaceCache.EXPECT().GetNamespaceByID(namespaceID).Return(
		namespace.NewGlobalNamespaceForTest(
			&persistencespb.NamespaceInfo{Id: namespaceID.String(), Name: namespaceName.String()},
			&persistencespb.NamespaceConfig{Retention: timestamp.DurationFromDays(1)},
			&persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: cluster.TestCurrentClusterName,
				Clusters: []string{
					cluster.TestCurrentClusterName,
					cluster.TestAlternativeClusterName,
				},
			},
			lastWriteVersion,
		), nil,
	).AnyTimes()

	err = s.nDCActivityStateReplicator.SyncActivitiesState(context.Background(), request)
	s.Nil(err)
}
