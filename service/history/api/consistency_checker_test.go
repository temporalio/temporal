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

package api

import (
	"context"
	"math/rand"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"

	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/common/testing/protomock"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	workflowConsistencyCheckerSuite struct {
		suite.Suite
		*require.Assertions

		controller    *gomock.Controller
		shardContext  *shard.MockContext
		workflowCache *wcache.MockCache
		config        *configs.Config

		shardID      int32
		namespaceID  string
		workflowID   string
		currentRunID string

		checker *WorkflowConsistencyCheckerImpl
	}
)

func TestWorkflowConsistencyCheckerSuite(t *testing.T) {
	s := new(workflowConsistencyCheckerSuite)
	suite.Run(t, s)
}

func (s *workflowConsistencyCheckerSuite) SetupSuite() {
}

func (s *workflowConsistencyCheckerSuite) TearDownSuite() {
}

func (s *workflowConsistencyCheckerSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.shardContext = shard.NewMockContext(s.controller)
	s.workflowCache = wcache.NewMockCache(s.controller)
	s.config = tests.NewDynamicConfig()

	s.shardID = rand.Int31()
	s.namespaceID = uuid.New().String()
	s.workflowID = uuid.New().String()
	s.currentRunID = uuid.New().String()

	s.shardContext.EXPECT().GetShardID().Return(s.shardID).AnyTimes()
	s.shardContext.EXPECT().GetConfig().Return(s.config).AnyTimes()

	s.checker = NewWorkflowConsistencyChecker(s.shardContext, s.workflowCache)
}

func (s *workflowConsistencyCheckerSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *workflowConsistencyCheckerSuite) TestGetWorkflowContextValidatedByCheck_Success_PassCheck() {
	ctx := context.Background()
	shardOwnershipAsserted := false

	wfContext := workflow.NewMockContext(s.controller)
	mutableState := workflow.NewMockMutableState(s.controller)
	released := false
	releaseFn := func(err error) { released = true }

	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx,
		s.shardContext,
		namespace.ID(s.namespaceID),
		protomock.Eq(&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.currentRunID,
		}),
		workflow.LockPriorityHigh,
	).Return(wfContext, releaseFn, nil)
	wfContext.EXPECT().LoadMutableState(ctx, s.shardContext).Return(mutableState, nil)

	workflowLease, err := s.checker.getWorkflowLeaseValidatedByCheck(
		ctx,
		&shardOwnershipAsserted,
		BypassMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(s.namespaceID, s.workflowID, s.currentRunID),
		workflow.LockPriorityHigh,
	)
	s.NoError(err)
	s.Equal(mutableState, workflowLease.GetMutableState())
	s.False(released)
}

func (s *workflowConsistencyCheckerSuite) TestGetWorkflowContextValidatedByCheck_Success_FailedCheck() {
	ctx := context.Background()
	shardOwnershipAsserted := false

	wfContext := workflow.NewMockContext(s.controller)
	mutableState1 := workflow.NewMockMutableState(s.controller)
	mutableState2 := workflow.NewMockMutableState(s.controller)
	released := false
	releaseFn := func(err error) { released = true }

	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx,
		s.shardContext,
		namespace.ID(s.namespaceID),
		protomock.Eq(&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.currentRunID,
		}),
		workflow.LockPriorityHigh,
	).Return(wfContext, releaseFn, nil)
	gomock.InOrder(
		wfContext.EXPECT().LoadMutableState(ctx, s.shardContext).Return(mutableState1, nil),
		wfContext.EXPECT().Clear(),
		wfContext.EXPECT().LoadMutableState(ctx, s.shardContext).Return(mutableState2, nil),
	)

	workflowLease, err := s.checker.getWorkflowLeaseValidatedByCheck(
		ctx,
		&shardOwnershipAsserted,
		FailMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(s.namespaceID, s.workflowID, s.currentRunID),
		workflow.LockPriorityHigh,
	)
	s.NoError(err)
	s.Equal(mutableState2, workflowLease.GetMutableState())
	s.False(released)
}

func (s *workflowConsistencyCheckerSuite) TestGetWorkflowContextValidatedByCheck_NotFound_OwnershipAsserted() {
	ctx := context.Background()
	shardOwnershipAsserted := false

	wfContext := workflow.NewMockContext(s.controller)
	released := false
	releaseFn := func(err error) { released = true }

	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx,
		s.shardContext,
		namespace.ID(s.namespaceID),
		protomock.Eq(&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.currentRunID,
		}),
		workflow.LockPriorityHigh,
	).Return(wfContext, releaseFn, nil)
	wfContext.EXPECT().LoadMutableState(ctx, s.shardContext).Return(nil, serviceerror.NewNotFound(""))

	s.shardContext.EXPECT().AssertOwnership(ctx).Return(nil)

	workflowLease, err := s.checker.getWorkflowLeaseValidatedByCheck(
		ctx,
		&shardOwnershipAsserted,
		FailMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(s.namespaceID, s.workflowID, s.currentRunID),
		workflow.LockPriorityHigh,
	)
	s.IsType(&serviceerror.NotFound{}, err)
	s.Nil(workflowLease)
	s.True(released)
}

func (s *workflowConsistencyCheckerSuite) TestGetWorkflowContextValidatedByCheck_NotFound_OwnershipLost() {
	ctx := context.Background()
	shardOwnershipAsserted := false

	wfContext := workflow.NewMockContext(s.controller)
	released := false
	releaseFn := func(err error) { released = true }

	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx,
		s.shardContext,
		namespace.ID(s.namespaceID),
		protomock.Eq(&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.currentRunID,
		}),
		workflow.LockPriorityHigh,
	).Return(wfContext, releaseFn, nil)
	wfContext.EXPECT().LoadMutableState(ctx, s.shardContext).Return(nil, serviceerror.NewNotFound(""))

	s.shardContext.EXPECT().AssertOwnership(ctx).Return(&persistence.ShardOwnershipLostError{})

	workflowLease, err := s.checker.getWorkflowLeaseValidatedByCheck(
		ctx,
		&shardOwnershipAsserted,
		FailMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(s.namespaceID, s.workflowID, s.currentRunID),
		workflow.LockPriorityHigh,
	)
	s.IsType(&persistence.ShardOwnershipLostError{}, err)
	s.Nil(workflowLease)
	s.True(released)
}

func (s *workflowConsistencyCheckerSuite) TestGetWorkflowContextValidatedByCheck_Error() {
	ctx := context.Background()
	shardOwnershipAsserted := false

	wfContext := workflow.NewMockContext(s.controller)
	released := false
	releaseFn := func(err error) { released = true }

	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(
		ctx,
		s.shardContext,
		namespace.ID(s.namespaceID),
		protomock.Eq(&commonpb.WorkflowExecution{
			WorkflowId: s.workflowID,
			RunId:      s.currentRunID,
		}),
		workflow.LockPriorityHigh,
	).Return(wfContext, releaseFn, nil)
	wfContext.EXPECT().LoadMutableState(ctx, s.shardContext).Return(nil, serviceerror.NewUnavailable(""))

	workflowLease, err := s.checker.getWorkflowLeaseValidatedByCheck(
		ctx,
		&shardOwnershipAsserted,
		FailMutableStateConsistencyPredicate,
		definition.NewWorkflowKey(s.namespaceID, s.workflowID, s.currentRunID),
		workflow.LockPriorityHigh,
	)
	s.IsType(&serviceerror.Unavailable{}, err)
	s.Nil(workflowLease)
	s.True(released)
}

func (s *workflowConsistencyCheckerSuite) TestGetCurrentRunID_Success() {
	ctx := context.Background()
	shardOwnershipAsserted := false

	released := false
	releaseFn := func(err error) { released = true }

	s.workflowCache.EXPECT().GetOrCreateCurrentWorkflowExecution(
		ctx,
		s.shardContext,
		namespace.ID(s.namespaceID),
		s.workflowID,
		workflow.LockPriorityHigh,
	).Return(releaseFn, nil)
	s.shardContext.EXPECT().GetCurrentExecution(
		ctx,
		&persistence.GetCurrentExecutionRequest{
			ShardID:     s.shardContext.GetShardID(),
			NamespaceID: s.namespaceID,
			WorkflowID:  s.workflowID,
		},
	).Return(&persistence.GetCurrentExecutionResponse{RunID: s.currentRunID}, nil)

	runID, err := s.checker.getCurrentRunID(ctx, &shardOwnershipAsserted, s.namespaceID, s.workflowID, workflow.LockPriorityHigh)
	s.NoError(err)
	s.Equal(s.currentRunID, runID)
	s.True(released)
}

func (s *workflowConsistencyCheckerSuite) TestGetCurrentRunID_NotFound_OwnershipAsserted() {
	ctx := context.Background()
	shardOwnershipAsserted := false

	released := false
	releaseFn := func(err error) { released = true }

	s.workflowCache.EXPECT().GetOrCreateCurrentWorkflowExecution(
		ctx,
		s.shardContext,
		namespace.ID(s.namespaceID),
		s.workflowID,
		workflow.LockPriorityHigh,
	).Return(releaseFn, nil)
	s.shardContext.EXPECT().GetCurrentExecution(
		ctx,
		&persistence.GetCurrentExecutionRequest{
			ShardID:     s.shardContext.GetShardID(),
			NamespaceID: s.namespaceID,
			WorkflowID:  s.workflowID,
		},
	).Return(nil, serviceerror.NewNotFound(""))
	s.shardContext.EXPECT().AssertOwnership(ctx).Return(nil)

	runID, err := s.checker.getCurrentRunID(ctx, &shardOwnershipAsserted, s.namespaceID, s.workflowID, workflow.LockPriorityHigh)
	s.IsType(&serviceerror.NotFound{}, err)
	s.Empty(runID)
	s.True(released)
}

func (s *workflowConsistencyCheckerSuite) TestGetCurrentRunID_NotFound_OwnershipLost() {
	ctx := context.Background()
	shardOwnershipAsserted := false

	released := false
	releaseFn := func(err error) { released = true }

	s.workflowCache.EXPECT().GetOrCreateCurrentWorkflowExecution(
		ctx,
		s.shardContext,
		namespace.ID(s.namespaceID),
		s.workflowID,
		workflow.LockPriorityHigh,
	).Return(releaseFn, nil)
	s.shardContext.EXPECT().GetCurrentExecution(
		ctx,
		&persistence.GetCurrentExecutionRequest{
			ShardID:     s.shardContext.GetShardID(),
			NamespaceID: s.namespaceID,
			WorkflowID:  s.workflowID,
		},
	).Return(nil, serviceerror.NewNotFound(""))
	s.shardContext.EXPECT().AssertOwnership(ctx).Return(&persistence.ShardOwnershipLostError{})

	runID, err := s.checker.getCurrentRunID(ctx, &shardOwnershipAsserted, s.namespaceID, s.workflowID, workflow.LockPriorityHigh)
	s.IsType(&persistence.ShardOwnershipLostError{}, err)
	s.Empty(runID)
	s.True(released)
}

func (s *workflowConsistencyCheckerSuite) TestGetCurrentRunID_Error() {
	ctx := context.Background()
	shardOwnershipAsserted := false

	released := false
	releaseFn := func(err error) { released = true }

	s.workflowCache.EXPECT().GetOrCreateCurrentWorkflowExecution(
		ctx,
		s.shardContext,
		namespace.ID(s.namespaceID),
		s.workflowID,
		workflow.LockPriorityHigh,
	).Return(releaseFn, nil)
	s.shardContext.EXPECT().GetCurrentExecution(
		ctx,
		&persistence.GetCurrentExecutionRequest{
			ShardID:     s.shardContext.GetShardID(),
			NamespaceID: s.namespaceID,
			WorkflowID:  s.workflowID,
		},
	).Return(nil, serviceerror.NewUnavailable(""))

	runID, err := s.checker.getCurrentRunID(ctx, &shardOwnershipAsserted, s.namespaceID, s.workflowID, workflow.LockPriorityHigh)
	s.IsType(&serviceerror.Unavailable{}, err)
	s.Empty(runID)
	s.True(released)
}

func (s *workflowConsistencyCheckerSuite) TestHistoryEventConsistencyPredicate() {
	eventID := int64(400)
	eventVersion := int64(200)
	predicate := HistoryEventConsistencyPredicate(eventID, eventVersion)

	testCases := []struct {
		name             string
		versionHistories *historyspb.VersionHistories
		pass             bool
	}{
		{
			name: "Pass_OnCurrentBranch",
			versionHistories: versionhistory.NewVersionHistories(
				&historyspb.VersionHistory{
					BranchToken: []byte{1, 2, 3},
					Items: []*historyspb.VersionHistoryItem{
						{EventId: 123, Version: 100},
						{EventId: 456, Version: 200},
					},
				},
			),
			pass: true,
		},
		{
			name: "Pass_OnNonCurrentBranch",
			versionHistories: &historyspb.VersionHistories{
				CurrentVersionHistoryIndex: 0,
				Histories: []*historyspb.VersionHistory{
					{
						BranchToken: []byte{1, 2, 3},
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 123, Version: 100},
						},
					},
					{
						BranchToken: []byte{4, 5, 6},
						Items: []*historyspb.VersionHistoryItem{
							{EventId: 123, Version: 100},
							{EventId: 456, Version: 200},
						},
					},
				},
			},
			pass: true,
		},
		{
			name: "Fail_NotFound",
			versionHistories: versionhistory.NewVersionHistories(
				&historyspb.VersionHistory{
					BranchToken: []byte{1, 2, 3},
					Items: []*historyspb.VersionHistoryItem{
						{EventId: 123, Version: 100},
					},
				},
			),
			pass: false,
		},
	}

	for _, tc := range testCases {
		s.Run(tc.name, func() {
			mockMutableState := workflow.NewMockMutableState(s.controller)
			mockMutableState.EXPECT().GetExecutionInfo().Return(&persistencespb.WorkflowExecutionInfo{
				VersionHistories: tc.versionHistories,
			})
			s.Equal(tc.pass, predicate(mockMutableState))
		})
	}
}
