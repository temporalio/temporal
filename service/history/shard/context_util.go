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

package shard

import (
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"

	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
)

type ContextTest struct {
	*ContextImpl

	Resource        *resource.Test
	MockEventsCache *events.MockCache
}

var _ Context = (*ContextTest)(nil)

func NewTestContext(
	ctrl *gomock.Controller,
	shardInfo *persistence.ShardInfoWithFailover,
	config *configs.Config,
) *ContextTest {
	resource := resource.NewTest(ctrl, metrics.History)
	eventsCache := events.NewMockCache(ctrl)
	shard := &ContextImpl{
		Resource:                  resource,
		shardID:                   shardInfo.GetShardId(),
		rangeID:                   shardInfo.GetRangeId(),
		shardInfo:                 shardInfo,
		executionManager:          resource.ExecutionMgr,
		config:                    config,
		logger:                    resource.GetLogger(),
		throttledLogger:           resource.GetThrottledLogger(),
		transferSequenceNumber:    1,
		transferMaxReadLevel:      0,
		visibilityMaxReadLevel:    0,
		maxTransferSequenceNumber: 100000,
		timerMaxReadLevelMap:      make(map[string]time.Time),
		remoteClusterCurrentTime:  make(map[string]time.Time),
		EventsCache:               eventsCache,
	}
	return &ContextTest{
		ContextImpl:     shard,
		Resource:        resource,
		MockEventsCache: eventsCache,
	}
}

func (s *ContextTest) Finish(
	t mock.TestingT,
) {
	s.Resource.Finish(t)
}
