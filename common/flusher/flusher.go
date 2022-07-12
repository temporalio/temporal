// The MIT License
//
// Copyright (c) 2022 Temporal Technologies Inc.  All rights reserved.
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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination flusher_mock.go

package flusher

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/channel"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

type (
	Flusher[T any] interface {
		AddItemToBeFlushed(item T) *future.FutureImpl[struct{}]
	}
	//Type Generic Flush Buffer that is size bound and time bound.
	//The Flush Buffer will flush after a configurable amount of time as well as once the buffer reaches a configurable capacity.
	//The number of flush buffers can also be configured.
	//Starts with x free buffers, once a free buffer reaches capacity or if the timer is up, the free buffer will get switched to a full buffer.
	//A full buffer will get flushed in the background and switched back to a free buffer.
	//When a free buffer switches to a full buffer, another free buffer will take its place if there are any available at the moment.
	flusherImpl[T any] struct {
		status                 int32
		flushDuration          int
		bufferCount            int
		bufferItemCapacity     int
		bufferNearItemCapacity int
		flushNotifierChan      chan struct{}
		logger                 log.Logger
		shutdownChan           channel.ShutdownOnce
		writer                 ItemWriter[T]

		sync.Mutex
		flushTimer       *time.Timer
		flushItemsBuffer []FlushItem[T]
		fullBufferChan   chan []FlushItem[T]
		freeBufferChan   chan []FlushItem[T]
	}
	FlushItem[T any] struct {
		item T
		fut  *future.FutureImpl[struct{}]
	}
	ItemWriter[T any] interface {
		Write(flushItem ...FlushItem[T]) error
	}
	noopItemWriterImpl[T any] struct {
		logger log.Logger
		sync.Mutex
	}
)

var _ ItemWriter = (*noopItemWriterImpl)(nil)
var _ Flusher = (*flusherImpl)(nil)

const (
	bufferCapacityGapPercent = 0.9
)

func NewFlusher[T any](
	bufferCount int,
	bufferItemCapacity int,
	flushDuration int,
	writer ItemWriter[T],
	logger log.Logger,
) Flusher[T] {
	return &flusherImpl[T]{
		status:                 common.DaemonStatusInitialized,
		flushDuration:          flushDuration,                                               // time waited after first item insertion before flushing the buffer
		bufferCount:            bufferCount,                                                 // no of total flush buffers
		bufferItemCapacity:     bufferItemCapacity,                                          // buffer size, will flush a buffer once no of items added to the buffer nears this limit
		bufferNearItemCapacity: int(bufferCapacityGapPercent * float64(bufferItemCapacity)), // ^ will flush buffer once buffer size hits this number
		flushTimer:             nil,
		flushNotifierChan:      make(chan struct{}, 1),
		writer:                 writer,
		flushItemsBuffer:       make([]FlushItem[T], 0, bufferItemCapacity),
		fullBufferChan:         make(chan []FlushItem[T], bufferCount),
		freeBufferChan:         make(chan []FlushItem[T], bufferCount),
		logger:                 logger,
		shutdownChan:           channel.NewShutdownOnce(),
	}
}

func NewNoopItemWriter[T any](logger log.Logger) *noopItemWriterImpl[T] {
	return &noopItemWriterImpl[T]{
		logger: logger,
	}
}

func (iwi *noopItemWriterImpl[T]) Write(flushItem ...FlushItem[T]) error {
	return nil
}

func (f *flusherImpl[T]) Start() {
	if !atomic.CompareAndSwapInt32(
		&f.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	f.flushTimer = time.NewTimer(time.Duration(f.flushDuration))
	f.flushTimer.Stop() // Stop the timer after creation since we only want timer to start running upon first item insertion
	f.initFreeBuffers(f.bufferCount, f.bufferItemCapacity)

	go f.timerHandler()
	go f.fullBufferChanHandler()
}

func (f *flusherImpl[T]) Stop() {
	if !atomic.CompareAndSwapInt32(
		&f.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	f.shutdownChan.Shutdown()

	for _, flushItem := range f.flushItemsBuffer {
		flushItem.fut.Set(struct{}{}, serviceerror.NewCanceled("Unable to write item due to Stop invocation"))
	}
}

func (f *flusherImpl[T]) initFreeBuffers(bufferCount int, bufferCapacity int) {
	for i := 0; i < bufferCount-1; i++ { // -1 since flushItemsBuffer counts as the first free buffer
		f.freeBufferChan <- make([]FlushItem[T], 0, bufferCapacity)
	}
}

func (f *flusherImpl[T]) fullBufferChanHandler() {
chanLoop:
	for {
		select {
		case fullBuffer := <-f.fullBufferChan:
			f.flushBuffer(fullBuffer)
			freeBuffer := fullBuffer[:0]
			f.freeBufferChan <- freeBuffer
		case <-f.shutdownChan.Channel():
			f.Stop()
			break chanLoop
		}
	}
}

func (f *flusherImpl[T]) timerHandler() {
TimerLoop:
	for {
		select {
		case <-f.flushTimer.C:
			f.Lock()
			if len(f.flushItemsBuffer) > 0 {
				f.clearBufferLocked()
			}
			f.Unlock()
		case <-f.shutdownChan.Channel():
			f.Stop()
			break TimerLoop
		}
	}
}

func (f *flusherImpl[T]) getFreeBuffer() []FlushItem[T] {
	var newFreeBuffer []FlushItem[T]
	select {
	case freeBuffer := <-f.freeBufferChan:
		newFreeBuffer = freeBuffer
	default:
		newFreeBuffer = nil // set to nil to indicate no available flush buffer
	}
	return newFreeBuffer
}

func (f *flusherImpl[T]) clearBufferLocked() {
	freeBuffer := f.getFreeBuffer()
	fullBuffer := f.flushItemsBuffer
	f.flushItemsBuffer = freeBuffer
	f.fullBufferChan <- fullBuffer
}

func (f *flusherImpl[T]) insertAndClearBufferIfFullLocked(flushItem FlushItem[T]) {
	if len(f.flushItemsBuffer) == 0 { // start timer if it's first item insertion
		f.stopFlushTimer()
		f.flushTimer.Reset(time.Duration(f.flushDuration))
	}
	f.flushItemsBuffer = append(f.flushItemsBuffer, flushItem)
	if f.isBufferCloseToFullLocked(f.flushItemsBuffer) {
		f.stopFlushTimer()
		f.clearBufferLocked()
	}
}

func (f *flusherImpl[T]) AddItemToBeFlushed(item T) *future.FutureImpl[struct{}] {
	f.Lock()
	defer f.Unlock()
	return f.addItemToBeFlushedLocked(item)
}

func (f *flusherImpl[T]) addItemToBeFlushedLocked(item T) *future.FutureImpl[struct{}] {
	flushItem := FlushItem[T]{item, future.NewFuture[struct{}]()}
	if f.shutdownChan.IsShutdown() {
		flushItem.fut.Set(struct{}{}, serviceerror.NewUnavailable("Unable to process item since service is stopping"))
		return flushItem.fut
	}

	currFlushBuffer := f.flushItemsBuffer
	if currFlushBuffer != nil { // nil check to make sure there is a usable flush buffer
		f.insertAndClearBufferIfFullLocked(flushItem)
	} else {
		newFlushBuffer := f.getFreeBuffer()
		if newFlushBuffer != nil { // nil check to make sure there is a usable flush buffer
			f.flushItemsBuffer = newFlushBuffer
			f.insertAndClearBufferIfFullLocked(flushItem)
		} else {
			err := serviceerror.NewUnavailable(fmt.Sprint("Flush buffer is currently full"))
			flushItem.fut.Set(struct{}{}, err)
		}
	}

	return flushItem.fut
}

func (f *flusherImpl[T]) flushBuffer(flushBuffer []FlushItem[T]) {
	err := f.writer.Write(flushBuffer...)
	if err != nil {
		f.logger.Error("Flusher failed to write", tag.Error(err))
	}
	for _, flushItem := range flushBuffer {
		flushItem.fut.Set(struct{}{}, err)
	}
}

func (f *flusherImpl[T]) isBufferCloseToFullLocked(flushBuffer []FlushItem[T]) bool {
	return len(flushBuffer) >= f.bufferNearItemCapacity
}

func (f *flusherImpl[T]) stopFlushTimer() {
	if !f.flushTimer.Stop() {
		select {
		case <-f.flushTimer.C: // drain the timer if already fired
		default:
		}
	}
}
