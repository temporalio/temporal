// The MIT License
//
// Copyright (c) 2024 Uber Technologies, Inc.
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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tests"
)

type (
	activitySuite struct {
		suite.Suite
		*require.Assertions

		controller            *gomock.Controller
		mockShard             *shard.ContextTest
		mockNamespaceRegistry *namespace.MockRegistry

		mutableState *MockMutableState
	}
)

func TestActivitySuite(t *testing.T) {
	s := new(activitySuite)
	suite.Run(t, s)
}

func (s *activitySuite) SetupTest() {
	s.Assertions = require.New(s.T())

	config := tests.NewDynamicConfig()
	s.controller = gomock.NewController(s.T())
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{ShardId: 1},
		config,
	)

	s.mutableState = NewMockMutableState(s.controller)

	s.mockNamespaceRegistry = s.mockShard.Resource.NamespaceCache

	s.mockNamespaceRegistry.EXPECT().GetNamespaceByID(tests.NamespaceID).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	s.mockNamespaceRegistry.EXPECT().GetNamespace(tests.Namespace).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()

	s.mockShard.Resource.ClusterMetadata.EXPECT().IsGlobalNamespaceEnabled().Return(true).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	s.mockShard.Resource.ClusterMetadata.EXPECT().ClusterNameForFailoverVersion(true, tests.GlobalNamespaceEntry.FailoverVersion()).Return(cluster.TestCurrentClusterName).AnyTimes()
}

func (s *activitySuite) TearDownTest() {
	s.controller.Finish()
	s.mockShard.StopForTest()
}

func (s *activitySuite) TestGetActivityState() {
	testCases := []struct {
		ai    *persistencespb.ActivityInfo
		state enumspb.PendingActivityState
	}{
		{
			ai: &persistencespb.ActivityInfo{
				CancelRequested: true,
				StartedEventId:  1,
			},
			state: enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED,
		},
		{
			ai: &persistencespb.ActivityInfo{
				CancelRequested: true,
				StartedEventId:  common.EmptyEventID,
			},
			state: enumspb.PENDING_ACTIVITY_STATE_CANCEL_REQUESTED,
		},
		{
			ai: &persistencespb.ActivityInfo{
				CancelRequested: false,
				StartedEventId:  common.EmptyEventID,
			},
			state: enumspb.PENDING_ACTIVITY_STATE_SCHEDULED,
		},
		{
			ai: &persistencespb.ActivityInfo{
				CancelRequested: false,
				StartedEventId:  1,
			},
			state: enumspb.PENDING_ACTIVITY_STATE_STARTED,
		},
	}

	for _, tc := range testCases {
		state := ActivityState(tc.ai)
		s.Equal(tc.state, state)
	}
}

func (s *activitySuite) TestGetPendingActivityInfo() {
	now := s.mockShard.GetTimeSource().Now().UTC().Round(time.Hour)
	activityType := commonpb.ActivityType{
		Name: "activityType",
	}
	ai := &persistencespb.ActivityInfo{
		ActivityType:            &activityType,
		ActivityId:              "activityID",
		CancelRequested:         false,
		StartedEventId:          1,
		Attempt:                 2,
		ScheduledTime:           timestamppb.New(now),
		LastAttemptCompleteTime: timestamppb.New(now.Add(-1 * time.Hour)),
		HasRetryPolicy:          false,
	}

	s.mutableState.EXPECT().GetActivityType(gomock.Any(), gomock.Any()).Return(&activityType, nil).Times(1)
	pi, err := GetPendingActivityInfo(context.Background(), s.mockShard, s.mutableState, ai)
	s.NoError(err)
	s.NotNil(pi)
}
