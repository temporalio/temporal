package persistence

import (
	"context"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/tasks"
)

const (
	emitDLQMetricsInterval = 3 * time.Hour
)

// DLQMetricsEmitter emits the number of messages in DLQ in each task category.
// This only has to be emitted from one history service instance. For this, DLQMetricsEmitter will only emit metrics
// if the history service it currently run hosts shard 1.
type (
	DLQMetricsEmitter struct {
		status                  int32
		shutdownCh              chan struct{}
		metricsHandler          metrics.Handler
		logger                  log.Logger
		emitMetricsTimer        *time.Ticker
		historyTaskQueueManager HistoryTaskQueueManager
		historyServiceResolver  membership.ServiceResolver
		hostInfoProvider        membership.HostInfoProvider
		taskCategoryRegistry    tasks.TaskCategoryRegistry
	}
)

func NewDLQMetricsEmitter(
	metricsHandler metrics.Handler,
	logger log.Logger,
	manager HistoryTaskQueueManager,
	historyServiceResolver membership.ServiceResolver,
	hostInfoProvider membership.HostInfoProvider,
	taskCategoryRegistry tasks.TaskCategoryRegistry,
) *DLQMetricsEmitter {
	return &DLQMetricsEmitter{
		status:                  common.DaemonStatusInitialized,
		shutdownCh:              make(chan struct{}),
		metricsHandler:          metricsHandler,
		emitMetricsTimer:        time.NewTicker(emitDLQMetricsInterval),
		logger:                  logger,
		historyTaskQueueManager: manager,
		historyServiceResolver:  historyServiceResolver,
		hostInfoProvider:        hostInfoProvider,
		taskCategoryRegistry:    taskCategoryRegistry,
	}
}

func (s *DLQMetricsEmitter) Start() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}
	go s.emitMetricsLoop()
}

func (s *DLQMetricsEmitter) Stop() {
	if !atomic.CompareAndSwapInt32(&s.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}
	close(s.shutdownCh)
	s.emitMetricsTimer.Stop()
}

func (s *DLQMetricsEmitter) emitMetricsLoop() {
	for {
		select {
		case <-s.shutdownCh:
			return
		case <-s.emitMetricsTimer.C:
			if s.shouldEmitMetrics() {
				s.emitMetrics()
			} else {
				// We have to clear the gauge dlq_message_count for this host when another host starts
				// emitting this metric.
				s.emitZeroMetrics()
			}
		}
	}
}

func (s *DLQMetricsEmitter) emitMetrics() {
	categories := s.taskCategoryRegistry.GetCategories()
	messageCounts := make(map[int]int64)
	for category := range categories {
		messageCounts[category] = 0
	}
	queues, err := s.getDLQList()
	if err != nil {
		s.logger.Error("Failed to list DLQs to emit metrics", tag.Error(err))
		return
	}
	for _, q := range queues {
		category, err := GetHistoryTaskQueueCategoryID(q.QueueName)
		if err != nil {
			s.logger.Error("Failed to process DLQ queue name", tag.Error(err))
		}
		messageCounts[category] += q.MessageCount
	}
	for id, count := range messageCounts {
		category, ok := categories[id]
		if !ok {
			s.logger.Error("Failed to find category from ID", tag.TaskCategoryID(id))
		}
		metrics.DLQMessageCount.With(s.metricsHandler).Record(float64(count), metrics.TaskCategoryTag(category.Name()))
	}
}

func (s *DLQMetricsEmitter) emitZeroMetrics() {
	for _, category := range s.taskCategoryRegistry.GetCategories() {
		metrics.DLQMessageCount.With(s.metricsHandler).Record(float64(0), metrics.TaskCategoryTag(category.Name()))
	}
}

func (s *DLQMetricsEmitter) getDLQList() ([]QueueInfo, error) {
	var queues []QueueInfo
	var nextPageToken []byte
	for {
		ctx := headers.SetCallerInfo(context.Background(), headers.SystemPreemptableCallerInfo)
		resp, err := s.historyTaskQueueManager.ListQueues(ctx, &ListQueuesRequest{
			QueueType:     QueueTypeHistoryDLQ,
			PageSize:      100,
			NextPageToken: nextPageToken,
		})
		if err != nil {
			return nil, err
		}
		queues = append(queues, resp.Queues...)
		nextPageToken = resp.NextPageToken
		if len(nextPageToken) == 0 {
			return queues, nil
		}
	}
}

// shouldEmitMetrics determines if DLQMetricsEmitter should emit metrics. It returns true only if this instance of
// history service is hosting shard 1.
func (s *DLQMetricsEmitter) shouldEmitMetrics() bool {
	ownerInfo, err := s.historyServiceResolver.Lookup("1")
	if err != nil {
		s.logger.Error("Failed to get the history service hosting shard 1")
		return false
	}

	hostInfo := s.hostInfoProvider.HostInfo()
	if ownerInfo.Identity() == hostInfo.Identity() {
		return true
	}

	return false
}
