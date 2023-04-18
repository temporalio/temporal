package backpressure

import (
	"sync/atomic"
	"time"
)

type (
	TimestampedData struct {
		Value     int64
		Timestamp time.Time
	}

	MovingWindowAvg interface {
		Add(val int64)
		Average() float64
	}

	MovingAvgWindowImpl struct {
		windowSize    time.Duration
		buffer        chan TimestampedData
		forceExpireCh chan interface{}
		sum           atomic.Int64
		count         atomic.Int64
	}
)

func NewMovingWindowAvgImpl(
	windowSize time.Duration,
	maxBufferSize int,
) *MovingAvgWindowImpl {
	ret := &MovingAvgWindowImpl{
		windowSize:    windowSize,
		buffer:        make(chan TimestampedData, maxBufferSize),
		forceExpireCh: make(chan interface{}),
	}
	ret.startExpireLoop()
	return ret
}

func (a *MovingAvgWindowImpl) Add(val int64) {
	if len(a.buffer) == cap(a.buffer) {
		// blocks until there is room in the buffer to add more data
		a.forceExpireCh <- struct{}{}
	}

	a.sum.Add(val)
	a.count.Add(1)
	a.buffer <- TimestampedData{Value: val, Timestamp: time.Now()}
}

func (a *MovingAvgWindowImpl) Average() float64 {
	return float64(a.sum.Load() / a.count.Load())
}

func (a *MovingAvgWindowImpl) startExpireLoop() {
	go func() {
		for {
			select {
			case toExpire := <-a.buffer:
				if time.Since(toExpire.Timestamp) > a.windowSize {
					// element already outside of window, remove from average
					a.sum.Add(-toExpire.Value)
					a.count.Add(-1)
				} else {
					// the first item out of the channel should be the oldest so wait until it moves out of the window
					// before trying to remove more elements from the buffer
					timer := time.NewTimer(a.windowSize - time.Since(toExpire.Timestamp))
					select {
					case <-timer.C:
					case <-a.forceExpireCh: // if the buffer is full, remove one item so new adds don't get blocked
						timer.Stop()
						a.sum.Add(-toExpire.Value)
						a.count.Add(-1)
					}
				}
			}
		}
	}()
}
