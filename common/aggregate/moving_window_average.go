package aggregate

import (
	"sync"
	"time"
)

type (
	MovingWindowAverage interface {
		Record(val int64)
		Average() float64
	}

	timestampedData struct {
		value     int64
		timestamp time.Time
	}

	MovingWindowAvgImpl struct {
		sync.Mutex
		windowSize    time.Duration
		maxBufferSize int
		buffer        []timestampedData
		headIdx       int
		tailIdx       int
		sum           int64
		count         int64
	}
)

func NewMovingWindowAvgImpl(
	windowSize time.Duration,
	maxBufferSize int,
) *MovingWindowAvgImpl {
	return &MovingWindowAvgImpl{
		windowSize:    windowSize,
		maxBufferSize: maxBufferSize,
		buffer:        make([]timestampedData, maxBufferSize),
	}
}

func (a *MovingWindowAvgImpl) Record(val int64) {
	a.Lock()
	defer a.Unlock()

	if a.count == int64(a.maxBufferSize) {
		a.sum -= a.buffer[a.headIdx].value
		a.count--
		a.headIdx = (a.headIdx + 1) % a.maxBufferSize
	}

	a.buffer[a.tailIdx] = timestampedData{timestamp: time.Now(), value: val}
	a.tailIdx = (a.tailIdx + 1) % a.maxBufferSize

	a.sum += val
	a.count++
}

func (a *MovingWindowAvgImpl) Average() float64 {
	a.Lock()
	defer a.Unlock()

	a.expireOldValuesLocked()
	if a.count == 0 {
		return 0
	}
	return float64(a.sum) / float64(a.count)
}

func (a *MovingWindowAvgImpl) expireOldValuesLocked() {
	for a.count > 0 {
		if time.Since(a.buffer[a.headIdx].timestamp) < a.windowSize {
			break
		}
		a.sum -= a.buffer[a.headIdx].value
		a.count--
		a.headIdx = (a.headIdx + 1) % a.maxBufferSize
	}
}
