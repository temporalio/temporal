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
	"time"

	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/service/history/tasks"
)

var _ Monitor = (*monitorImpl)(nil)

const (
	monitorWatermarkPrecision   = time.Second
	defaultAlertSilenceDuration = 10 * time.Second

	alertChSize = 10
)

type (
	// Monitor tracks Queue statistics and sends an Alert to the AlertCh
	// if any statistics becomes abnormal
	Monitor interface {
		GetTotalPendingTaskCount() int
		GetSlicePendingTaskCount(slice Slice) int
		SetSlicePendingTaskCount(slice Slice, count int)

		GetReaderWatermark(readerID int64) (tasks.Key, bool)
		SetReaderWatermark(readerID int64, watermark tasks.Key)

		GetTotalSliceCount() int
		GetSliceCount(readerID int64) int
		SetSliceCount(readerID int64, count int)

		RemoveSlice(slice Slice)
		RemoveReader(readerID int64)

		ResolveAlert(AlertType)
		SilenceAlert(AlertType)
		AlertCh() <-chan *Alert
		Close()
	}

	MonitorOptions struct {
		PendingTasksCriticalCount   dynamicconfig.IntPropertyFn
		ReaderStuckCriticalAttempts dynamicconfig.IntPropertyFn
		SliceCountCriticalThreshold dynamicconfig.IntPropertyFn
	}

	monitorImpl struct {
		sync.Mutex

		totalPendingTaskCount int
		totalSliceCount       int

		readerStats map[int64]readerStats
		sliceStats  map[Slice]sliceStats

		categoryType tasks.CategoryType
		timeSource   clock.TimeSource
		options      *MonitorOptions

		pendingAlerts  map[AlertType]struct{}
		silencedAlerts map[AlertType]time.Time // silenced alertType => expiration
		alertCh        chan *Alert
		shutdownCh     chan struct{}
	}

	readerStats struct {
		progress   readerProgress
		sliceCount int
	}

	readerProgress struct {
		watermark tasks.Key
		attempts  int
	}

	sliceStats struct {
		pendingTaskCount int
	}
)

func newMonitor(
	categoryType tasks.CategoryType,
	timeSource clock.TimeSource,
	options *MonitorOptions,
) *monitorImpl {
	return &monitorImpl{
		readerStats:    make(map[int64]readerStats),
		sliceStats:     make(map[Slice]sliceStats),
		categoryType:   categoryType,
		timeSource:     timeSource,
		options:        options,
		pendingAlerts:  make(map[AlertType]struct{}),
		silencedAlerts: make(map[AlertType]time.Time),
		alertCh:        make(chan *Alert, alertChSize),
		shutdownCh:     make(chan struct{}),
	}
}

func (m *monitorImpl) GetTotalPendingTaskCount() int {
	m.Lock()
	defer m.Unlock()

	return m.totalPendingTaskCount
}

func (m *monitorImpl) GetSlicePendingTaskCount(slice Slice) int {
	m.Lock()
	defer m.Unlock()

	if stats, ok := m.sliceStats[slice]; ok {
		return stats.pendingTaskCount
	}
	return 0
}

func (m *monitorImpl) SetSlicePendingTaskCount(slice Slice, count int) {
	m.Lock()
	defer m.Unlock()

	stats := m.sliceStats[slice]
	m.totalPendingTaskCount = m.totalPendingTaskCount - stats.pendingTaskCount + count

	stats.pendingTaskCount = count
	m.sliceStats[slice] = stats

	criticalTotalTasks := m.options.PendingTasksCriticalCount()
	if criticalTotalTasks > 0 && m.totalPendingTaskCount > criticalTotalTasks {
		m.sendAlertLocked(&Alert{
			AlertType: AlertTypeQueuePendingTaskCount,
			AlertAttributesQueuePendingTaskCount: &AlertAttributesQueuePendingTaskCount{
				CurrentPendingTaskCount:   m.totalPendingTaskCount,
				CiriticalPendingTaskCount: criticalTotalTasks,
			},
		})
	}
}

func (m *monitorImpl) GetReaderWatermark(readerID int64) (tasks.Key, bool) {
	m.Lock()
	defer m.Unlock()

	stats, ok := m.readerStats[readerID]
	if !ok {
		return tasks.MinimumKey, false
	}

	return stats.progress.watermark, true
}

func (m *monitorImpl) SetReaderWatermark(readerID int64, watermark tasks.Key) {
	// TODO: currently only tracking default reader progress for scheduled queue
	if readerID != DefaultReaderId || m.categoryType != tasks.CategoryTypeScheduled {
		return
	}

	m.Lock()
	defer m.Unlock()

	watermark.FireTime = watermark.FireTime.Truncate(monitorWatermarkPrecision)
	watermark.TaskID = 0

	stats := m.readerStats[readerID]
	if stats.progress.watermark.CompareTo(watermark) != 0 {
		stats.progress = readerProgress{
			watermark: watermark,
			attempts:  1,
		}
		m.readerStats[readerID] = stats
		return
	}

	stats.progress.attempts++
	m.readerStats[readerID] = stats

	criticalAttempts := m.options.ReaderStuckCriticalAttempts()
	if criticalAttempts > 0 && stats.progress.attempts >= criticalAttempts {
		m.sendAlertLocked(&Alert{
			AlertType: AlertTypeReaderStuck,
			AlertAttributesReaderStuck: &AlertAttributesReaderStuck{
				ReaderID:         readerID,
				CurrentWatermark: stats.progress.watermark,
			},
		})
	}
}

func (m *monitorImpl) GetTotalSliceCount() int {
	m.Lock()
	defer m.Unlock()

	count := 0
	for _, stats := range m.readerStats {
		count += stats.sliceCount
	}

	return count
}

func (m *monitorImpl) GetSliceCount(readerID int64) int {
	m.Lock()
	defer m.Unlock()

	if stats, ok := m.readerStats[readerID]; ok {
		return stats.sliceCount
	}
	return 0
}

func (m *monitorImpl) SetSliceCount(readerID int64, count int) {
	m.Lock()
	defer m.Unlock()

	stats := m.readerStats[readerID]
	m.totalSliceCount = m.totalSliceCount - stats.sliceCount + count

	stats.sliceCount = count
	m.readerStats[readerID] = stats

	criticalSliceCount := m.options.SliceCountCriticalThreshold()
	if criticalSliceCount > 0 && m.totalSliceCount > criticalSliceCount {
		m.sendAlertLocked(&Alert{
			AlertType: AlertTypeSliceCount,
			AlertAttributesSliceCount: &AlertAttributesSlicesCount{
				CurrentSliceCount:  m.totalSliceCount,
				CriticalSliceCount: criticalSliceCount,
			},
		})
	}
}

func (m *monitorImpl) RemoveSlice(slice Slice) {
	m.Lock()
	defer m.Unlock()

	stats, ok := m.sliceStats[slice]
	if !ok {
		return
	}

	m.totalPendingTaskCount -= stats.pendingTaskCount
	delete(m.sliceStats, slice)
}

func (m *monitorImpl) RemoveReader(readerID int64) {
	m.Lock()
	defer m.Unlock()

	stats, ok := m.readerStats[readerID]
	if !ok {
		return
	}

	m.totalSliceCount -= stats.sliceCount
	delete(m.readerStats, readerID)
}

func (m *monitorImpl) ResolveAlert(alertType AlertType) {
	m.Lock()
	defer m.Unlock()

	delete(m.pendingAlerts, alertType)
}

func (m *monitorImpl) SilenceAlert(alertType AlertType) {
	m.Lock()
	defer m.Unlock()

	delete(m.pendingAlerts, alertType)
	m.silencedAlerts[alertType] = m.timeSource.Now().Add(defaultAlertSilenceDuration)
}

func (m *monitorImpl) AlertCh() <-chan *Alert {
	return m.alertCh
}

func (m *monitorImpl) Close() {
	m.Lock()
	defer m.Unlock()

	close(m.shutdownCh)

	for {
		select {
		case <-m.alertCh:
			// drain alertCh
		default:
			close(m.alertCh)
			return
		}
	}
}

func (m *monitorImpl) sendAlertLocked(alert *Alert) {
	if m.isClosed() {
		// make sure alert won't be sent to a closed chan
		return
	}

	if m.timeSource.Now().Before(m.silencedAlerts[alert.AlertType]) {
		return
	}

	// dedup alerts, we only need one outstanding alert per alert type
	if _, ok := m.pendingAlerts[alert.AlertType]; ok {
		return
	}

	select {
	case m.alertCh <- alert:
		m.pendingAlerts[alert.AlertType] = struct{}{}
	default:
		// do not block if alertCh full
	}
}

func (m *monitorImpl) isClosed() bool {
	select {
	case <-m.shutdownCh:
		return true
	default:
		return false
	}
}
