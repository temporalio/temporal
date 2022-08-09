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

package flusher

import (
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

var (
	ErrFull     = serviceerror.NewUnavailable("flush buffer is full")
	ErrShutdown = serviceerror.NewUnavailable("flush buffer is shutdown")
)

type (
	//Type Generic Flush Buffer that is size bound and time bound.
	//The Flush Buffer will flush after a configurable amount of time as well as once the buffer reaches a configurable capacity.
	//The number of flush buffers can also be configured.
	//Starts with x free buffers, once a free buffer reaches capacity or if the timer is up, the free buffer will get switched to a full buffer.
	//A full buffer will get flushed in the background and switched back to a free buffer.
	//When a free buffer switches to a full buffer, another free buffer will take its place if there are any available at the moment.
	flusherImpl[T any] struct {
		status            int32
		flushTimeout      time.Duration
		bufferCapacity    int
		numBuffer         int
		flushNotifierChan chan struct{}
		logger            log.Logger
		shutdownChan      channel.ShutdownOnce
		writer            Writer[T]

		sync.Mutex
		flushTimer         *time.Timer
		flushBufferPointer *[]FlushItem[T]

		flushBuffer    []FlushItem[T]
		fullBufferChan chan []FlushItem[T]
		freeBufferChan chan []FlushItem[T]
	}
)

func NewFlusher[T any](
	bufferCapacity int,
	numBuffer int,
	flushTimeout time.Duration,
	writer Writer[T],
	logger log.Logger,
) *flusherImpl[T] {
	if bufferCapacity < 1 {
		panic("bufferCapacity must be >= 1")
	} else if numBuffer < 2 {
		panic("numBuffer must be 2= 1")
	}

	flushTimer := time.NewTimer(flushTimeout)
	flushTimer.Stop() // Stop the timer after creation since we only want timer to start running upon first Item insertion

	freeBufferChan := make(chan []FlushItem[T], numBuffer)
	fullBufferChan := make(chan []FlushItem[T], numBuffer)
	for i := 0; i < numBuffer-1; i++ { // -1 since flushBuffer counts as the first free buffer
		freeBufferChan <- make([]FlushItem[T], 0, bufferCapacity)
	}
	return &flusherImpl[T]{
		status:            common.DaemonStatusInitialized,
		flushTimeout:      flushTimeout,   // time waited after first Item insertion before flushing the buffer
		numBuffer:         numBuffer,      // no of total flush buffers
		bufferCapacity:    bufferCapacity, // buffer size, will flush a buffer once no of items added to the buffer nears this limit
		flushTimer:        flushTimer,
		flushNotifierChan: make(chan struct{}, 1),
		writer:            writer,
		flushBuffer:       make([]FlushItem[T], 0, bufferCapacity),
		freeBufferChan:    freeBufferChan,
		fullBufferChan:    fullBufferChan,
		logger:            logger,
		shutdownChan:      channel.NewShutdownOnce(),
	}
}

func (f *flusherImpl[T]) Start() {
	if !atomic.CompareAndSwapInt32(
		&f.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	go f.timeEventLoop()
	go f.flushEventLoop()
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

	f.Lock()
	defer f.Unlock()

	f.cancel(f.flushBuffer)
	f.flushBuffer = nil
	f.stopTimerLocked()
FreeBufferLoop:
	for {
		select {
		case <-f.freeBufferChan:
			// noop
		default:
			break FreeBufferLoop
		}
	}
FullBufferLoop:
	for {
		select {
		case buffer := <-f.fullBufferChan:
			f.cancel(buffer)
		default:
			break FullBufferLoop
		}
	}
}

func (f *flusherImpl[T]) flushEventLoop() {
Loop:
	for {
		select {
		case fullBuffer := <-f.fullBufferChan:
			f.flush(fullBuffer)
			freeBuffer := fullBuffer[:0]
			f.freeBufferChan <- freeBuffer
		case <-f.shutdownChan.Channel():
			f.Stop()
			break Loop
		}
	}
}

func (f *flusherImpl[T]) timeEventLoop() {
Loop:
	for {
		select {
		case <-f.flushTimer.C:
			f.Lock()
			if &f.flushBuffer == f.flushBufferPointer {
				f.pushDirtyBufferLocked()
				f.stopTimerLocked()
			}
			f.Unlock()
		case <-f.shutdownChan.Channel():
			f.Stop()
			break Loop
		}
	}
}

func (f *flusherImpl[T]) pullCleanBufferLocked() []FlushItem[T] {
	var newFreeBuffer []FlushItem[T]
	select {
	case freeBuffer := <-f.freeBufferChan:
		newFreeBuffer = freeBuffer
	default:
		newFreeBuffer = nil // set to nil to indicate no available flush buffer
	}
	return newFreeBuffer
}

func (f *flusherImpl[T]) pushDirtyBufferLocked() {
	freeBuffer := f.pullCleanBufferLocked()
	fullBuffer := f.flushBuffer
	f.flushBuffer = freeBuffer
	f.fullBufferChan <- fullBuffer
}

func (f *flusherImpl[T]) Buffer(item T) future.Future[struct{}] {
	if f.shutdownChan.IsShutdown() {
		return future.NewReadyFuture[struct{}](struct{}{}, ErrShutdown)
	}

	flushItem := FlushItem[T]{
		Item:   item,
		Future: future.NewFuture[struct{}](),
	}
	f.Lock()
	defer f.Unlock()

	if f.shutdownChan.IsShutdown() {
		return future.NewReadyFuture[struct{}](struct{}{}, ErrShutdown)
	}

	if f.flushBuffer != nil { // nil check to make sure there is a usable flush buffer
		f.appendLocked(flushItem)
	} else {
		newFlushBuffer := f.pullCleanBufferLocked()
		if newFlushBuffer != nil { // nil check to make sure there is a usable flush buffer
			f.flushBuffer = newFlushBuffer
			f.appendLocked(flushItem)
		} else {
			flushItem.Future.Set(struct{}{}, ErrFull)
		}
	}

	return flushItem.Future
}

func (f *flusherImpl[T]) appendLocked(flushItem FlushItem[T]) {
	if len(f.flushBuffer) == 0 { // start timer if it's first Item insertion
		f.startTimerLocked()
	}
	f.flushBuffer = append(f.flushBuffer, flushItem)
	if len(f.flushBuffer) >= f.bufferCapacity {
		f.stopTimerLocked()
		f.pushDirtyBufferLocked()
	}
}

func (f *flusherImpl[T]) flush(flushBuffer []FlushItem[T]) {
	items := make([]T, len(flushBuffer))
	for i := 0; i < len(items); i++ {
		items[i] = flushBuffer[i].Item
	}
	err := f.writer.Write(items)
	if err != nil {
		f.logger.Error("Flusher failed to write", tag.Error(err))
	}
	for _, flushItem := range flushBuffer {
		flushItem.Future.Set(struct{}{}, err)
	}
}

func (f *flusherImpl[T]) cancel(flushBuffer []FlushItem[T]) {
	for _, flushItem := range flushBuffer {
		flushItem.Future.Set(struct{}{}, ErrShutdown)
	}
}

func (f *flusherImpl[T]) startTimerLocked() {
	f.flushTimer.Reset(f.flushTimeout)
	f.flushBufferPointer = &f.flushBuffer
}

func (f *flusherImpl[T]) stopTimerLocked() {
	f.flushTimer.Stop()
	f.flushBufferPointer = nil
}
