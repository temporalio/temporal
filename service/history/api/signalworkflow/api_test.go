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

package signalworkflow

import (
	"context"
	"testing"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/server/common/cluster/clustertest"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
)

type (
	signalWorkflowSuite struct {
		suite.Suite
		*require.Assertions

		controller        *gomock.Controller
		shardContext      *shard.MockContext
		namespaceRegistry *namespace.MockRegistry

		workflowCache              *wcache.MockCache
		workflowConsistencyChecker api.WorkflowConsistencyChecker

		currentContext      *workflow.MockContext
		currentMutableState *workflow.MockMutableState
	}
)

func TestSignalWorkflowSuite(t *testing.T) {
	s := new(signalWorkflowSuite)
	suite.Run(t, s)
}

func (s *signalWorkflowSuite) SetupSuite() {
}

func (s *signalWorkflowSuite) TearDownSuite() {
}

func (s *signalWorkflowSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.namespaceRegistry = namespace.NewMockRegistry(s.controller)
	s.namespaceRegistry.EXPECT().GetNamespaceByID(tests.GlobalNamespaceEntry.ID()).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

	s.shardContext = shard.NewMockContext(s.controller)
	s.shardContext.EXPECT().GetConfig().Return(tests.NewDynamicConfig()).AnyTimes()
	s.shardContext.EXPECT().GetLogger().Return(log.NewTestLogger()).AnyTimes()
	s.shardContext.EXPECT().GetThrottledLogger().Return(log.NewTestLogger()).AnyTimes()
	s.shardContext.EXPECT().GetMetricsHandler().Return(metrics.NoopMetricsHandler).AnyTimes()
	s.shardContext.EXPECT().GetTimeSource().Return(clock.NewRealTimeSource()).AnyTimes()
	s.shardContext.EXPECT().GetNamespaceRegistry().Return(s.namespaceRegistry).AnyTimes()
	s.shardContext.EXPECT().GetClusterMetadata().Return(clustertest.NewMetadataForTest(cluster.NewTestClusterMetadataConfig(true, true))).AnyTimes()

	s.currentMutableState = workflow.NewMockMutableState(s.controller)
	s.currentMutableState.EXPECT().GetNamespaceEntry().Return(tests.GlobalNamespaceEntry).AnyTimes()
	s.currentMutableState.EXPECT().GetExecutionInfo().Return(&persistence.WorkflowExecutionInfo{
		WorkflowId: tests.WorkflowID,
	}).AnyTimes()
	s.currentMutableState.EXPECT().GetExecutionState().Return(&persistence.WorkflowExecutionState{
		RunId: tests.RunID,
	}).AnyTimes()

	s.currentContext = workflow.NewMockContext(s.controller)
	s.currentContext.EXPECT().LoadMutableState(gomock.Any(), s.shardContext).Return(s.currentMutableState, nil).AnyTimes()

	s.workflowCache = wcache.NewMockCache(s.controller)
	s.workflowCache.EXPECT().GetOrCreateWorkflowExecution(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), workflow.LockPriorityHigh).
		Return(s.currentContext, wcache.NoopReleaseFn, nil).AnyTimes()

	s.workflowConsistencyChecker = api.NewWorkflowConsistencyChecker(
		s.shardContext,
		s.workflowCache,
	)
}

func (s *signalWorkflowSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *signalWorkflowSuite) TestSignalWorkflow_WorkflowCloseAttempted() {
	s.currentMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	s.currentMutableState.EXPECT().IsWorkflowCloseAttempted().Return(true)
	s.currentMutableState.EXPECT().HasStartedWorkflowTask().Return(true)

	resp, err := Invoke(
		context.Background(),
		&historyservice.SignalWorkflowExecutionRequest{
			NamespaceId: tests.NamespaceID.String(),
			SignalRequest: &workflowservice.SignalWorkflowExecutionRequest{
				Namespace: tests.Namespace.String(),
				WorkflowExecution: &commonpb.WorkflowExecution{
					WorkflowId: tests.WorkflowID,
					RunId:      tests.RunID,
				},
				SignalName: "signal-name",
				Input:      nil,
			},
		},
		s.shardContext,
		s.workflowConsistencyChecker,
	)
	s.Nil(resp)
	s.Error(consts.ErrWorkflowClosing, err)
}
