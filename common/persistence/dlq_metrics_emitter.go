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

package persistence

import (
	"context"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/tasks"
)

const (
	emitDLQMetricsInterval = 1 * time.Hour
)

type (
	DLQMetricsEmitter struct {
		status                  int32
		shutdownCh              chan struct{}
		metricsHandler          metrics.Handler
		logger                  log.Logger
		emitMetricsTimer        *time.Ticker
		historyTaskQueueManager HistoryTaskQueueManager
	}
)

func NewDLQMetricsEmitter(
	metricsHandler metrics.Handler,
	logger log.Logger,
	manager HistoryTaskQueueManager,
) *DLQMetricsEmitter {
	return &DLQMetricsEmitter{
		status:                  common.DaemonStatusInitialized,
		shutdownCh:              make(chan struct{}),
		metricsHandler:          metricsHandler,
		emitMetricsTimer:        time.NewTicker(emitDLQMetricsInterval),
		logger:                  logger,
		historyTaskQueueManager: manager,
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
			messageCounts := make(map[int]int64)
			queues, err := s.getDLQList()
			if err != nil {
				s.logger.Error("Failed to list DLQs to emit metrics", tag.Error(err))
				continue
			}
			for _, q := range queues {
				category, err := GetHistoryTaskQueueCategoryID(q.QueueName)
				if err != nil {
					s.logger.Error("Failed to process DLQ queue name", tag.Error(err))
				}
				messageCounts[category] += q.MessageCount
			}
			for id, name := range tasks.CategoryIDToName {
				metrics.DLQMessageCount.With(s.metricsHandler).Record(float64(messageCounts[id]), metrics.TaskCategoryTag(name))
			}
		}
	}
}

func (s *DLQMetricsEmitter) getDLQList() ([]QueueInfo, error) {
	var queues []QueueInfo
	var nextPageToken []byte
	for {
		resp, err := s.historyTaskQueueManager.ListQueues(context.Background(), &ListQueuesRequest{
			QueueType:     QueueTypeHistoryDLQ,
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
