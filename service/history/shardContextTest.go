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
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"

	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resource"
)

type shardContextTest struct {
	*shardContextImpl

	resource        *resource.Test
	mockEventsCache *MockeventsCache
}

var _ ShardContext = (*shardContextTest)(nil)

func newTestShardContext(
	ctrl *gomock.Controller,
	shardInfo *persistence.ShardInfoWithFailover,
	config *Config,
) *shardContextTest {
	resource := resource.NewTest(ctrl, metrics.History)
	eventsCache := NewMockeventsCache(ctrl)
	shard := &shardContextImpl{
		Resource:                  resource,
		shardID:                   int(shardInfo.GetShardId()),
		rangeID:                   shardInfo.GetRangeId(),
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
	return &shardContextTest{
		shardContextImpl: shard,
		resource:         resource,
		mockEventsCache:  eventsCache,
	}
}

func (s *shardContextTest) Finish(
	t mock.TestingT,
) {
	s.resource.Finish(t)
}
