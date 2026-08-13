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

		SetSliceReadWatermark(slice Slice, readerID int64, watermark tasks.Key)

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

		readerSliceCount map[int64]int
		sliceStats       map[Slice]sliceStats

		categoryType tasks.CategoryType
		timeSource   clock.TimeSource
		options      *MonitorOptions

		pendingAlerts  map[AlertType]struct{}
		silencedAlerts map[AlertType]time.Time // silenced alertType => expiration
		alertCh        chan *Alert
		shutdownCh     chan struct{}
	}

	// readProgress counts consecutive reads of one slice by one reader that ended on
	// the same watermark. Another slice covering the same window, or a previous owner
	// of this slice, must not count. alerted limits a window to one alert.
	readProgress struct {
		readerID  int64
		watermark tasks.Key
		attempts  int
		alerted   bool
	}

	sliceStats struct {
		pendingTaskCount int
		progress         readProgress
	}
)

func newMonitor(
	categoryType tasks.CategoryType,
	timeSource clock.TimeSource,
	options *MonitorOptions,
) *monitorImpl {
	return &monitorImpl{
		readerSliceCount: make(map[int64]int),
		sliceStats:       make(map[Slice]sliceStats),
		categoryType:     categoryType,
		timeSource:       timeSource,
		options:          options,
		pendingAlerts:    make(map[AlertType]struct{}),
		silencedAlerts:   make(map[AlertType]time.Time),
		alertCh:          make(chan *Alert, alertChSize),
		shutdownCh:       make(chan struct{}),
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

func (m *monitorImpl) SetSliceReadWatermark(slice Slice, readerID int64, watermark tasks.Key) {
	// Immediate task keys all carry tasks.DefaultFireTime, so a window derived from
	// a watermark would cover the whole queue.
	if m.categoryType != tasks.CategoryTypeScheduled {
		return
	}

	m.Lock()
	defer m.Unlock()

	watermark.FireTime = watermark.FireTime.Truncate(monitorWatermarkPrecision)
	watermark.TaskID = 0

	stats := m.sliceStats[slice]
	if stats.progress.readerID != readerID || stats.progress.watermark.CompareTo(watermark) != 0 {
		stats.progress = readProgress{
			readerID:  readerID,
			watermark: watermark,
			attempts:  1,
		}
		m.sliceStats[slice] = stats
		return
	}

	stats.progress.attempts++

	criticalAttempts := m.options.ReaderStuckCriticalAttempts()
	if !stats.progress.alerted && criticalAttempts > 0 && stats.progress.attempts >= criticalAttempts {
		stats.progress.alerted = m.sendAlertLocked(&Alert{
			AlertType: AlertTypeReaderStuck,
			AlertAttributesReaderStuck: &AlertAttributesReaderStuck{
				ReaderID:         readerID,
				CurrentWatermark: stats.progress.watermark,
			},
		})
	}

	m.sliceStats[slice] = stats
}

func (m *monitorImpl) GetTotalSliceCount() int {
	m.Lock()
	defer m.Unlock()

	count := 0
	for _, sliceCount := range m.readerSliceCount {
		count += sliceCount
	}

	return count
}

func (m *monitorImpl) GetSliceCount(readerID int64) int {
	m.Lock()
	defer m.Unlock()

	return m.readerSliceCount[readerID]
}

func (m *monitorImpl) SetSliceCount(readerID int64, count int) {
	m.Lock()
	defer m.Unlock()

	m.totalSliceCount = m.totalSliceCount - m.readerSliceCount[readerID] + count
	m.readerSliceCount[readerID] = count

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

	m.totalSliceCount -= m.readerSliceCount[readerID]
	delete(m.readerSliceCount, readerID)
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

// sendAlertLocked reports whether the alert was queued, so callers that latch on
// having reported a condition do not latch on an alert that was dropped.
func (m *monitorImpl) sendAlertLocked(alert *Alert) bool {
	if m.isClosed() {
		// make sure alert won't be sent to a closed chan
		return false
	}

	if m.timeSource.Now().Before(m.silencedAlerts[alert.AlertType]) {
		return false
	}

	// dedup alerts, we only need one outstanding alert per alert type
	if _, ok := m.pendingAlerts[alert.AlertType]; ok {
		return false
	}

	select {
	case m.alertCh <- alert:
		m.pendingAlerts[alert.AlertType] = struct{}{}
		return true
	default:
		// do not block if alertCh full
		return false
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
