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

package persistence

import (
	"context"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/observability/log"
	"go.temporal.io/server/common/observability/log/tag"
	"go.temporal.io/server/common/observability/metrics"
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
			if !s.shouldEmitMetrics() {
				continue
			}
			s.emitMetrics()
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
