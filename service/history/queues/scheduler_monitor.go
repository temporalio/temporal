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
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
)

var noopScheduleMonitor = &noopSchedulerMonitor{}

var defaultSchedulerMonitorOptions = schedulerMonitorOptions{
	aggregationCount:    100,
	aggregationDuration: time.Second * 30,
}

type (
	schedulerMonitor struct {
		taskChannelKeyFn  TaskChannelKeyFn
		namespaceRegistry namespace.Registry
		timeSource        clock.TimeSource
		metricsHandler    metrics.Handler
		options           schedulerMonitorOptions

		status     int32
		shutdownCh chan struct{}

		sync.Mutex
		scheduleStats map[TaskChannelKey]*scheduleStats
	}

	schedulerMonitorOptions struct {
		aggregationCount    int
		aggregationDuration time.Duration
	}

	scheduleStats struct {
		lastStartTime time.Time
		totalLatency  time.Duration
		numStarted    int

		taggedMetricsHandler metrics.Handler
		lastEmissionTime     time.Time
	}

	noopSchedulerMonitor struct{}
)

func newSchedulerMonitor(
	taskChannelKeyFn TaskChannelKeyFn,
	namespaceRegistry namespace.Registry,
	timeSource clock.TimeSource,
	metricsHandler metrics.Handler,
	options schedulerMonitorOptions,
) *schedulerMonitor {
	return &schedulerMonitor{
		taskChannelKeyFn:  taskChannelKeyFn,
		namespaceRegistry: namespaceRegistry,
		timeSource:        timeSource,
		metricsHandler:    metricsHandler,
		options:           options,

		status:     common.DaemonStatusInitialized,
		shutdownCh: make(chan struct{}),

		scheduleStats: make(map[TaskChannelKey]*scheduleStats),
	}
}

func (m *schedulerMonitor) Start() {
	if !atomic.CompareAndSwapInt32(&m.status, common.DaemonStatusInitialized, common.DaemonStatusStarted) {
		return
	}

	go m.metricEmissionLoop()
}

func (m *schedulerMonitor) Stop() {
	if !atomic.CompareAndSwapInt32(&m.status, common.DaemonStatusStarted, common.DaemonStatusStopped) {
		return
	}

	close(m.shutdownCh)
}

func (m *schedulerMonitor) RecordStart(executable Executable) {
	startTime := m.timeSource.Now()
	taskChanKey := m.taskChannelKeyFn(executable)

	m.Lock()
	defer m.Unlock()

	stats := m.getOrCreateScheduleStatsLocked(taskChanKey)

	// The latency we want to measure is the duration between
	// two task start time for one task channel.
	// However, it's possible that the second task is only queued
	// long after the first task is started. In that case, the
	// latency becomes the duration between schedule to start time
	// for the second task.
	latency := min(
		startTime.Sub(stats.lastStartTime),
		startTime.Sub(executable.GetScheduledTime()),
	)
	stats.lastStartTime = startTime
	stats.numStarted++
	stats.totalLatency += latency

	if stats.numStarted >= m.options.aggregationCount {
		m.emitMetric(taskChanKey, stats)
	}
}

func (m *schedulerMonitor) metricEmissionLoop() {
	emissionTicker := time.NewTicker(m.options.aggregationDuration)
	defer emissionTicker.Stop()

	for {
		select {
		case <-m.shutdownCh:
			return
		case <-emissionTicker.C:
			m.Lock()

			now := m.timeSource.Now()
			for taskChanKey, stats := range m.scheduleStats {
				if now.Sub(stats.lastEmissionTime) > m.options.aggregationDuration {
					m.emitMetric(taskChanKey, stats)
				}
			}

			m.Unlock()
		}
	}
}

func (m *schedulerMonitor) getOrCreateScheduleStatsLocked(
	taskChanKey TaskChannelKey,
) *scheduleStats {
	if stats, ok := m.scheduleStats[taskChanKey]; ok {
		return stats
	}

	namespaceName, _ := m.namespaceRegistry.GetNamespaceName(namespace.ID(taskChanKey.NamespaceID))

	stats := &scheduleStats{
		taggedMetricsHandler: m.metricsHandler.WithTags(
			metrics.NamespaceTag(namespaceName.String()),
			metrics.TaskPriorityTag(taskChanKey.Priority.String()),
		),
		lastEmissionTime: m.timeSource.Now(),
	}
	m.scheduleStats[taskChanKey] = stats
	return stats
}

func (m *schedulerMonitor) emitMetric(
	taskChanKey TaskChannelKey,
	stats *scheduleStats,
) {
	if stats.numStarted == 0 {
		return
	}

	totalLatency := stats.totalLatency
	if stats.numStarted != m.options.aggregationCount {
		totalLatency = totalLatency / time.Duration(stats.numStarted) * time.Duration(m.options.aggregationCount)
	}

	metrics.QueueScheduleLatency.With(stats.taggedMetricsHandler).Record(totalLatency)
	m.resetStats(stats)
}

func (m *schedulerMonitor) resetStats(
	stats *scheduleStats,
) {
	stats.numStarted = 0
	stats.totalLatency = 0
	stats.lastEmissionTime = m.timeSource.Now()
}

func (m *noopSchedulerMonitor) Start()                   {}
func (m *noopSchedulerMonitor) Stop()                    {}
func (m *noopSchedulerMonitor) RecordStart(_ Executable) {}
