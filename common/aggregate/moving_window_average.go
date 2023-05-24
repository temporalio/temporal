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

package aggregate

import (
	"container/ring"
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
		sync.RWMutex
		windowSize    time.Duration
		maxBufferSize int
		head          *ring.Ring
		tail          *ring.Ring
		sum           int64
		count         int
	}
)

func NewMovingWindowAvgImpl(
	windowSize time.Duration,
	maxBufferSize int,
) *MovingWindowAvgImpl {
	buffer := ring.New(maxBufferSize)
	return &MovingWindowAvgImpl{
		windowSize:    windowSize,
		maxBufferSize: maxBufferSize,
		head:          buffer,
		tail:          buffer,
	}
}

func (a *MovingWindowAvgImpl) Record(val int64) {
	a.Lock()
	defer a.Unlock()

	if a.count == a.maxBufferSize {
		a.expireOneLocked()
	}

	a.tail.Value = timestampedData{value: val, timestamp: time.Now()}
	a.tail = a.tail.Next()

	a.sum += val
	a.count++
}

func (a *MovingWindowAvgImpl) Average() float64 {
	a.expireOldValues()

	a.RLock()
	defer a.RUnlock()

	if a.count == 0 {
		return 0
	}
	return float64(a.sum / int64(a.count))
}

func (a *MovingWindowAvgImpl) expireOldValues() {
	a.Lock()
	defer a.Unlock()

	for ; a.head != a.tail; a.head = a.head.Next() {
		data, ok := a.head.Value.(timestampedData)
		if !ok || time.Since(data.timestamp) < a.windowSize {
			break
		}
		a.sum -= data.value
		a.count--
	}
}

func (a *MovingWindowAvgImpl) expireOneLocked() {
	if data, ok := a.head.Value.(timestampedData); ok {
		a.sum -= data.value
		a.count--
	}
	a.head = a.head.Next()
}
