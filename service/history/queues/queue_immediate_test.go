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

package queues

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type (
	immediateQueueSuite struct {
		suite.Suite
		*require.Assertions

		controller           *gomock.Controller
		mockShard            *shard.ContextTest
		mockExecutionManager *persistence.MockExecutionManager

		immediateQueue *immediateQueue
	}
)

func TestImmediateQueueSuite(t *testing.T) {
	s := new(immediateQueueSuite)
	suite.Run(t, s)
}

func (s *immediateQueueSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockShard = shard.NewTestContext(
		s.controller,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
			Owner:   "test-shard-owner",
		},
		tests.NewDynamicConfig(),
	)
	s.mockExecutionManager = s.mockShard.Resource.ExecutionMgr
	s.mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	s.immediateQueue = NewImmediateQueue(
		s.mockShard,
		tasks.CategoryTransfer,
		nil, // scheduler
		nil, // rescheduler
		testQueueOptions,
		NewReaderPriorityRateLimiter(
			func() float64 { return 10 },
			1,
		),
		GrouperNamespaceID{},
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
		nil, // execuable factory
	)
}

func (s *immediateQueueSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *immediateQueueSuite) TestPaginationFnProvider_ShardOwnershipLost() {
	paginationFnProvider := s.immediateQueue.paginationFnProvider

	s.mockExecutionManager.EXPECT().GetHistoryTasks(gomock.Any(), gomock.Any()).Return(nil, &persistence.ShardOwnershipLostError{
		ShardID: s.mockShard.GetShardID(),
	}).Times(1)

	paginationFn := paginationFnProvider(NewRandomRange())
	_, _, err := paginationFn(nil)
	s.True(shard.IsShardOwnershipLostError(err))

	// make sure shard is also marked as invalid
	s.False(s.mockShard.IsValid())
}
