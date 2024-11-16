// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
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

package pauseactivity

import (
	"testing"

	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	commonpb "go.temporal.io/api/common/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/versionhistory"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow"
	"go.uber.org/mock/gomock"
)

type (
	pauseActivitySuite struct {
		suite.Suite
		*require.Assertions

		controller          *gomock.Controller
		mockShard           *shard.ContextTest
		mockEventsCache     *events.MockCache
		mockNamespaceCache  *namespace.MockRegistry
		mockTaskGenerator   *workflow.MockTaskGenerator
		mockMutableState    *workflow.MockMutableState
		mockClusterMetadata *cluster.MockMetadata

		executionInfo *persistencespb.WorkflowExecutionInfo

		validator *api.CommandAttrValidator

		logger log.Logger
	}
)

func TestActivityOptionsSuite(t *testing.T) {
	s := new(pauseActivitySuite)
	suite.Run(t, s)
}

func (s *pauseActivitySuite) SetupSuite() {
}

func (s *pauseActivitySuite) TearDownSuite() {
}

func (s *pauseActivitySuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockTaskGenerator = workflow.NewMockTaskGenerator(s.controller)
	s.mockMutableState = workflow.NewMockMutableState(s.controller)

	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
		},
		tests.NewDynamicConfig(),
	)

	s.mockNamespaceCache = s.mockShard.Resource.NamespaceCache
	s.mockClusterMetadata = s.mockShard.Resource.ClusterMetadata
	s.mockEventsCache = s.mockShard.MockEventsCache
	s.mockClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	s.mockClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockEventsCache.EXPECT().PutEvent(gomock.Any(), gomock.Any()).AnyTimes()

	s.logger = s.mockShard.GetLogger()
	s.executionInfo = &persistencespb.WorkflowExecutionInfo{
		VersionHistories:                 versionhistory.NewVersionHistories(&historyspb.VersionHistory{}),
		FirstExecutionRunId:              uuid.New(),
		WorkflowExecutionTimerTaskStatus: workflow.TimerTaskStatusCreated,
	}
	s.mockMutableState.EXPECT().GetExecutionInfo().Return(s.executionInfo).AnyTimes()
	s.mockMutableState.EXPECT().GetCurrentVersion().Return(int64(1)).AnyTimes()

	s.validator = api.NewCommandAttrValidator(
		s.mockShard.GetNamespaceRegistry(),
		s.mockShard.GetConfig(),
		nil,
	)
}

func (s *pauseActivitySuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *pauseActivitySuite) TestPauseActivityAcceptance() {
	activityId := "activity_id"
	activityInfo := &persistencespb.ActivityInfo{
		TaskQueue:  "task_queue_name",
		ActivityId: activityId,
		ActivityType: &commonpb.ActivityType{
			Name: "activity_type",
		},
	}

	s.mockMutableState.EXPECT().IsWorkflowExecutionRunning().Return(true)
	s.mockMutableState.EXPECT().GetActivityByActivityID(gomock.Any()).Return(activityInfo, true)
	s.mockMutableState.EXPECT().UpdateActivity(gomock.Any(), gomock.Any())

	err := workflow.PauseActivityById(s.mockMutableState, activityId)
	s.NoError(err)
}
