package history

import (
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/mock"

	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/resource"
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
		isClosed:                  false,
		closeCh:                   make(chan int, 100),
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
