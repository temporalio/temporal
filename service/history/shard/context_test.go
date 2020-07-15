// The MIT License (MIT)
// 
// Copyright (c) 2017-2020 Uber Technologies Inc.
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

package shard

import (
	"errors"
	"testing"
	"time"

	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"

	"github.com/stretchr/testify/mock"
	"github.com/uber/cadence/common/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/events"
	"github.com/uber/cadence/service/history/resource"
)

type (
	contextTestSuite struct {
		suite.Suite
		*require.Assertions

		controller       *gomock.Controller
		mockResource     *resource.Test
		mockShardManager *mocks.ShardManager

		metricsClient metrics.Client
		logger        log.Logger

		context *contextImpl
	}
)

func TestContextSuite(t *testing.T) {
	s := new(contextTestSuite)
	suite.Run(t, s)
}

func (s *contextTestSuite) SetupTest() {
	s.Assertions = require.New(s.T())

	s.controller = gomock.NewController(s.T())
	s.mockResource = resource.NewTest(s.controller, metrics.History)
	s.mockShardManager = s.mockResource.ShardMgr

	s.metricsClient = metrics.NewClient(tally.NoopScope, metrics.History)
	s.logger = loggerimpl.NewNopLogger()

	s.context = s.newContext()
}

func (s *contextTestSuite) newContext() *contextImpl {
	eventsCache := events.NewMockCache(s.controller)
	config := config.NewForTest()
	shardInfo := &persistence.ShardInfo{
		ShardID:          0,
		RangeID:          1,
		TransferAckLevel: 0,
	}
	context := &contextImpl{
		Resource:                  s.mockResource,
		shardID:                   shardInfo.ShardID,
		rangeID:                   shardInfo.RangeID,
		shardInfo:                 shardInfo,
		executionManager:          s.mockResource.ExecutionMgr,
		config:                    config,
		logger:                    s.logger,
		throttledLogger:           s.logger,
		transferSequenceNumber:    1,
		transferMaxReadLevel:      0,
		maxTransferSequenceNumber: 100000,
		timerMaxReadLevelMap:      make(map[string]time.Time),
		remoteClusterCurrentTime:  make(map[string]time.Time),
		eventsCache:               eventsCache,
	}
	return context
}

func (s *contextTestSuite) TearDownTest() {
	s.controller.Finish()
}

func (s *contextTestSuite) TestRenewRangeLockedSuccess() {
	s.mockShardManager.On("UpdateShard", mock.Anything).Once().Return(nil)

	err := s.context.renewRangeLocked(false)
	s.NoError(err)
}

func (s *contextTestSuite) TestRenewRangeLockedSuccessAfterRetries() {
	retryCount := conditionalRetryCount
	someError := errors.New("some error")
	s.mockShardManager.On("UpdateShard", mock.Anything).Times(retryCount - 1).Return(someError)
	s.mockShardManager.On("UpdateShard", mock.Anything).Return(nil)

	err := s.context.renewRangeLocked(false)
	s.NoError(err)
}

func (s *contextTestSuite) TestRenewRangeLockedRetriesExceeded() {
	retryCount := conditionalRetryCount
	someError := errors.New("some error")
	s.mockShardManager.On("UpdateShard", mock.Anything).Times(retryCount).Return(someError)

	err := s.context.renewRangeLocked(false)
	s.Error(err)
}

func (s *contextTestSuite) TestReplicateFailoverMarkersSuccess() {
	s.mockResource.ExecutionMgr.On("CreateFailoverMarkerTasks", mock.Anything).Once().Return(nil)

	markers := make([]*persistence.FailoverMarkerTask, 0)
	err := s.context.ReplicateFailoverMarkers(markers)
	s.NoError(err)
}
