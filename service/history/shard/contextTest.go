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

package shard

import (
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"

	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/service/history/config"
	"github.com/uber/cadence/service/history/events"
	"github.com/uber/cadence/service/history/resource"
)

// TestContext is a test implementation for shard Context interface
type TestContext struct {
	*contextImpl

	Resource        *resource.Test
	MockEventsCache *events.MockCache
}

var _ Context = (*TestContext)(nil)

// NewTestContext create a new shardContext for test
func NewTestContext(
	ctrl *gomock.Controller,
	shardInfo *persistence.ShardInfo,
	config *config.Config,
) *TestContext {
	resource := resource.NewTest(ctrl, metrics.History)
	eventsCache := events.NewMockCache(ctrl)
	shard := &contextImpl{
		Resource:                  resource,
		shardID:                   shardInfo.ShardID,
		rangeID:                   shardInfo.RangeID,
		shardInfo:                 shardInfo,
		executionManager:          resource.ExecutionMgr,
		config:                    config,
		logger:                    resource.GetLogger(),
		throttledLogger:           resource.GetThrottledLogger(),
		transferSequenceNumber:    1,
		transferMaxReadLevel:      0,
		maxTransferSequenceNumber: 100000,
		timerMaxReadLevelMap:      make(map[string]time.Time),
		remoteClusterCurrentTime:  make(map[string]time.Time),
		eventsCache:               eventsCache,
	}
	return &TestContext{
		contextImpl:     shard,
		Resource:        resource,
		MockEventsCache: eventsCache,
	}
}

// ShardInfo is a test hook for getting shard info
func (s *TestContext) ShardInfo() *persistence.ShardInfo {
	return s.shardInfo
}

// SetEventsCache is a test hook for setting events cache
func (s *TestContext) SetEventsCache(
	eventsCache events.Cache,
) {
	s.eventsCache = eventsCache
	s.MockEventsCache = nil
}

// Finish checks whether expectations are met
func (s *TestContext) Finish(
	t mock.TestingT,
) {
	s.Resource.Finish(t)
}
