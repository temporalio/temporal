//Type Generic Flush Buffer that is size bound and time bound.
//The Flush Buffer will flush after a configurable amount of time as well as once the buffer reaches a configurable capacity.
//The number of flush buffers can also be configured.
//Starts with x free buffers, once a free buffer reaches capacity or if the timer is up, the free buffer will get switched to a full buffer.
//A full buffer will get flushed in the background and switched back to a free buffer.
//When a free buffer switches to a full buffer, another free buffer will take its place if there are any available at the moment.

package flusher

import (
	"fmt"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/channel"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"sync"
	"time"
)

type (
	Flusher[T any] struct {
		flushDuration     int
		bufferCount       int
		bufferCapacity    int
		flushNotifierChan chan struct{}
		logger            log.Logger
		shutdownChan      channel.ShutdownOnce
		writer            *itemWriterImpl[T]

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
		Write(item ...T) error
	}
	itemWriterImpl[T any] struct {
		logger log.Logger
		sync.Mutex
	}
)

const (
	bufferCapacityGapPercent = 0.9
)

func NewFlusher[T any](
	bufferCount int,
	bufferCapacity int,
	flushDuration int,
	writer *itemWriterImpl[T],
	logger log.Logger,
	shutdownChan channel.ShutdownOnce,
) *Flusher[T] {
	return &Flusher[T]{
		flushDuration:     flushDuration,  // time waited after first item insertion before flushing the buffer
		bufferCount:       bufferCount,    // no of total flush buffers
		bufferCapacity:    bufferCapacity, // buffer size, will flush a buffer once no of items added to the buffer nears this limit
		flushTimer:        nil,
		flushNotifierChan: make(chan struct{}, 1),
		writer:            writer,
		flushItemsBuffer:  make([]FlushItem[T], 0, bufferCapacity),
		fullBufferChan:    make(chan []FlushItem[T], bufferCount),
		freeBufferChan:    make(chan []FlushItem[T], bufferCount),
		logger:            logger,
		shutdownChan:      shutdownChan,
	}
}

func NewItemWriterImpl[T any](logger log.Logger) *itemWriterImpl[T] {
	return &itemWriterImpl[T]{
		logger: logger,
	}
}

func (iwi *itemWriterImpl[T]) Write(flushItem ...FlushItem[T]) error {
	iwi.Lock()
	defer iwi.Unlock()
	return nil
}

func (f *Flusher[T]) Start() error {
	f.flushTimer = time.NewTimer(time.Duration(f.flushDuration))
	f.flushTimer.Stop() // Stop the timer after creation since we only want timer to start running upon first item insertion
	f.initFreeBuffers(f.bufferCount, f.bufferCapacity)

	go f.timerHandler()
	go f.fullBufferChanHandler()
	return nil
}

func (f *Flusher[T]) Stop() error {
	f.shutdownChan.Shutdown()

	return nil
}

func (f *Flusher[T]) initFreeBuffers(bufferCount int, bufferCapacity int) {
	for i := 0; i < bufferCount-1; i++ { // -1 since flushItemsBuffer counts as the first free buffer
		f.freeBufferChan <- make([]FlushItem[T], 0, bufferCapacity)
	}
}

func (f *Flusher[T]) fullBufferChanHandler() {
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

func (f *Flusher[T]) timerHandler() {
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

func (f *Flusher[T]) getFreeBuffer() []FlushItem[T] {
	var newFreeBuffer []FlushItem[T]
	select {
	case freeBuffer := <-f.freeBufferChan:
		newFreeBuffer = freeBuffer
	default:
		newFreeBuffer = nil // set to nil to indicate no available flush buffer
	}
	return newFreeBuffer
}

func (f *Flusher[T]) clearBufferLocked() {
	freeBuffer := f.getFreeBuffer()
	fullBuffer := f.flushItemsBuffer
	f.flushItemsBuffer = freeBuffer
	f.fullBufferChan <- fullBuffer
}

func (f *Flusher[T]) insertAndClearBufferIfFullLocked(flushItem FlushItem[T]) {
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

func (f *Flusher[T]) addItemToBeFlushed(item T) *future.FutureImpl[struct{}] {
	f.Lock()
	defer f.Unlock()
	return f.addItemToBeFlushedLocked(item)
}

func (f *Flusher[T]) addItemToBeFlushedLocked(item T) *future.FutureImpl[struct{}] {
	flushItem := FlushItem[T]{item, future.NewFuture[struct{}]()}
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

func (f *Flusher[T]) flushBuffer(flushBuffer []FlushItem[T]) {
	err := f.writer.Write(flushBuffer...)
	if err != nil {
		f.logger.Error("Flusher failed to write", tag.Error(err))
	}
	for _, flushItem := range flushBuffer {
		flushItem.fut.Set(struct{}{}, err)
	}
}

func (f *Flusher[T]) isBufferCloseToFullLocked(flushBuffer []FlushItem[T]) bool {
	bufferCap := int(bufferCapacityGapPercent * float64(cap(flushBuffer)))
	return len(flushBuffer) >= bufferCap
}

func (f *Flusher[T]) stopFlushTimer() {
	if !f.flushTimer.Stop() {
		select {
		case <-f.flushTimer.C: // drain the timer if already fired
		default:
		}
	}
}
