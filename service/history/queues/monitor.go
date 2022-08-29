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

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/service/history/tasks"
)

var _ Monitor = (*monitorImpl)(nil)

const (
	monitorWatermarkPrecision = time.Second

	alertChSize = 10
)

type (
	// Monitor tracks Queue statistics and sends an Alert to the AlertCh
	// if any statistics becomes abnormal
	Monitor interface {
		GetReaderWatermark(readerID int32) (tasks.Key, bool)
		SetReaderWatermark(readerID int32, watermark tasks.Key)

		GetTotalSliceCount() int
		GetSliceCount(readerID int32) int
		SetSliceCount(readerID int32, count int)

		AlertCh() <-chan *Alert
		Close()
	}

	MonitorOptions struct {
		ReaderStuckCriticalAttempts dynamicconfig.IntPropertyFn
		SliceCountCriticalThreshold dynamicconfig.IntPropertyFn
	}

	monitorImpl struct {
		sync.Mutex

		totalSliceCount int

		readerStats map[int32]readerStats

		categoryType tasks.CategoryType
		options      *MonitorOptions

		alertCh    chan *Alert
		shutdownCh chan struct{}
	}

	readerStats struct {
		progress   readerProgess
		sliceCount int
	}

	readerProgess struct {
		watermark tasks.Key
		attempts  int
	}
)

func newMonitor(
	categoryType tasks.CategoryType,
	options *MonitorOptions,
) *monitorImpl {
	return &monitorImpl{
		readerStats:  make(map[int32]readerStats),
		categoryType: categoryType,
		options:      options,
		alertCh:      make(chan *Alert, alertChSize),
		shutdownCh:   make(chan struct{}),
	}
}

func (m *monitorImpl) GetReaderWatermark(readerID int32) (tasks.Key, bool) {
	m.Lock()
	defer m.Unlock()

	stats, ok := m.readerStats[readerID]
	if !ok {
		return tasks.MinimumKey, false
	}

	return stats.progress.watermark, true
}

func (m *monitorImpl) SetReaderWatermark(readerID int32, watermark tasks.Key) {
	// TODO: currently only tracking default reader progress for scheduled queue
	if readerID != defaultReaderId || m.categoryType != tasks.CategoryTypeScheduled {
		return
	}

	m.Lock()
	defer m.Unlock()

	watermark.FireTime = watermark.FireTime.Truncate(monitorWatermarkPrecision)
	watermark.TaskID = 0

	stats := m.readerStats[readerID]
	if stats.progress.watermark.CompareTo(watermark) != 0 {
		stats.progress = readerProgess{
			watermark: watermark,
			attempts:  1,
		}
		m.readerStats[readerID] = stats
		return
	}

	stats.progress.attempts++
	m.readerStats[readerID] = stats

	ciriticalAttempts := m.options.ReaderStuckCriticalAttempts()
	if ciriticalAttempts > 0 && stats.progress.attempts >= ciriticalAttempts {
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

func (m *monitorImpl) GetSliceCount(readerID int32) int {
	m.Lock()
	defer m.Unlock()

	if stats, ok := m.readerStats[readerID]; ok {
		return stats.sliceCount
	}
	return 0
}

func (m *monitorImpl) SetSliceCount(readerID int32, count int) {
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

	select {
	case m.alertCh <- alert:
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
