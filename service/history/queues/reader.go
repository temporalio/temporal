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
	"container/list"
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/quotas"
)

var (
	throttleRetryDelay = 3 * time.Second
)

type (
	Reader interface {
		Scopes() []Scope

		WalkSlices(SliceIterator)
		SplitSlices(SliceSplitter)
		MergeSlices(...Slice)
		AppendSlices(...Slice)
		ClearSlices(SlicePredicate)
		CompactSlices(SlicePredicate)
		ShrinkSlices() int

		Notify()
		Pause(time.Duration)
		Start()
		Stop()
	}

	ReaderOptions struct {
		BatchSize            dynamicconfig.IntPropertyFn
		MaxPendingTasksCount dynamicconfig.IntPropertyFn
		PollBackoffInterval  dynamicconfig.DurationPropertyFn
	}

	SliceIterator func(s Slice)

	SliceSplitter func(s Slice) (remaining []Slice, split bool)

	SlicePredicate func(s Slice) bool

	ReaderCompletionFn func(readerID int64)

	ReaderImpl struct {
		sync.Mutex

		readerID       int64
		options        *ReaderOptions
		scheduler      Scheduler
		rescheduler    Rescheduler
		timeSource     clock.TimeSource
		ratelimiter    quotas.RequestRateLimiter
		monitor        Monitor
		completionFn   ReaderCompletionFn
		logger         log.Logger
		metricsHandler metrics.Handler

		status     int32
		shutdownCh chan struct{}
		shutdownWG sync.WaitGroup

		slices        *list.List
		nextReadSlice *list.Element
		notifyCh      chan struct{}

		throttleTimer *time.Timer
		retrier       backoff.Retrier

		rateLimitContext       context.Context
		rateLimitContextCancel context.CancelFunc
		rateLimiterRequest     quotas.Request
	}
)

var (
	NoopReaderCompletionFn = func(_ int64) {}
)

func NewReader(
	readerID int64,
	slices []Slice,
	options *ReaderOptions,
	scheduler Scheduler,
	rescheduler Rescheduler,
	timeSource clock.TimeSource,
	ratelimiter quotas.RequestRateLimiter,
	monitor Monitor,
	completionFn ReaderCompletionFn,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *ReaderImpl {

	sliceList := list.New()
	for _, slice := range slices {
		sliceList.PushBack(slice)
	}
	monitor.SetSliceCount(readerID, len(slices))

	rateLimitContext, rateLimitContextCancel := context.WithCancel(context.Background())
	return &ReaderImpl{
		readerID:       readerID,
		options:        options,
		scheduler:      scheduler,
		rescheduler:    rescheduler,
		timeSource:     timeSource,
		ratelimiter:    ratelimiter,
		monitor:        monitor,
		completionFn:   completionFn,
		logger:         log.With(logger, tag.QueueReaderID(readerID)),
		metricsHandler: metricsHandler,

		status:     common.DaemonStatusInitialized,
		shutdownCh: make(chan struct{}),

		slices:        sliceList,
		nextReadSlice: sliceList.Front(),
		notifyCh:      make(chan struct{}, 1),

		retrier: backoff.NewRetrier(
			common.CreateReadTaskRetryPolicy(),
			clock.NewRealTimeSource(),
		),

		rateLimitContext:       rateLimitContext,
		rateLimitContextCancel: rateLimitContextCancel,
		rateLimiterRequest:     newReaderRequest(readerID),
	}
}

func (r *ReaderImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&r.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	r.shutdownWG.Add(1)
	go r.eventLoop()

	r.notify()

	r.logger.Info("queue reader started", tag.LifeCycleStarted)
}

func (r *ReaderImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&r.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	r.monitor.RemoveReader(r.readerID)

	close(r.shutdownCh)
	r.rateLimitContextCancel()
	if success := common.AwaitWaitGroup(&r.shutdownWG, time.Minute); !success {
		r.logger.Warn("queue reader shutdown timed out waiting for event loop", tag.LifeCycleStopTimedout)
	}
	r.logger.Info("queue reader stopped", tag.LifeCycleStopped)
}

func (r *ReaderImpl) Scopes() []Scope {
	r.Lock()
	defer r.Unlock()

	scopes := make([]Scope, 0, r.slices.Len())
	for element := r.slices.Front(); element != nil; element = element.Next() {
		scopes = append(scopes, element.Value.(Slice).Scope())
	}

	return scopes
}

func (r *ReaderImpl) WalkSlices(iterator SliceIterator) {
	r.Lock()
	defer r.Unlock()

	for element := r.slices.Front(); element != nil; element = element.Next() {
		iterator(element.Value.(Slice))
	}
}

func (r *ReaderImpl) SplitSlices(splitter SliceSplitter) {
	r.Lock()
	defer r.Unlock()

	splitSlices := list.New()
	for element := r.slices.Front(); element != nil; element = element.Next() {
		slice := element.Value.(Slice)
		newSlices, split := splitter(slice)
		if !split {
			splitSlices.PushBack(slice)
			continue
		}

		for _, newSlice := range newSlices {
			splitSlices.PushBack(newSlice)
		}
	}

	// clear existing list
	r.slices.Init()
	r.slices = splitSlices

	r.resetNextReadSliceLocked()
	r.monitor.SetSliceCount(r.readerID, r.slices.Len())
}

func (r *ReaderImpl) MergeSlices(incomingSlices ...Slice) {
	if len(incomingSlices) == 0 {
		return
	}

	validateSlicesOrderedDisjoint(incomingSlices)

	r.Lock()
	defer r.Unlock()

	mergedSlices := list.New()

	currentSliceElement := r.slices.Front()
	incomingSliceIdx := 0

	for currentSliceElement != nil && incomingSliceIdx < len(incomingSlices) {
		currentSlice := currentSliceElement.Value.(Slice)
		incomingSlice := incomingSlices[incomingSliceIdx]

		if currentSlice.Scope().Range.InclusiveMin.CompareTo(incomingSlice.Scope().Range.InclusiveMin) < 0 {
			mergeOrAppendSlice(mergedSlices, currentSlice)
			currentSliceElement = currentSliceElement.Next()
		} else {
			mergeOrAppendSlice(mergedSlices, incomingSlice)
			incomingSliceIdx++
		}
	}

	for ; currentSliceElement != nil; currentSliceElement = currentSliceElement.Next() {
		mergeOrAppendSlice(mergedSlices, currentSliceElement.Value.(Slice))
	}
	for _, slice := range incomingSlices[incomingSliceIdx:] {
		mergeOrAppendSlice(mergedSlices, slice)
	}

	// clear existing list
	r.slices.Init()
	r.slices = mergedSlices

	r.resetNextReadSliceLocked()
	r.monitor.SetSliceCount(r.readerID, r.slices.Len())
}

func (r *ReaderImpl) AppendSlices(incomingSlices ...Slice) {
	if len(incomingSlices) == 0 {
		return
	}

	validateSlicesOrderedDisjoint(incomingSlices)
	if back := r.slices.Back(); back != nil {
		lastSliceRange := back.Value.(Slice).Scope().Range
		firstIncomingRange := incomingSlices[0].Scope().Range
		if lastSliceRange.ExclusiveMax.CompareTo(firstIncomingRange.InclusiveMin) > 0 {
			panic(fmt.Sprintf(
				"Can not append slice to existing list of slices, incoming slice range: %v, existing slice range: %v ",
				firstIncomingRange,
				lastSliceRange,
			))
		}
	}

	r.Lock()
	defer r.Unlock()

	for _, incomingSlice := range incomingSlices {
		r.slices.PushBack(incomingSlice)
	}

	r.resetNextReadSliceLocked()
	r.monitor.SetSliceCount(r.readerID, r.slices.Len())
}

func (r *ReaderImpl) ClearSlices(predicate SlicePredicate) {
	r.Lock()
	defer r.Unlock()

	for element := r.slices.Front(); element != nil; element = element.Next() {
		slice := element.Value.(Slice)
		if predicate(slice) {
			slice.Clear()
		}
	}

	r.resetNextReadSliceLocked()
}

func (r *ReaderImpl) CompactSlices(predicate SlicePredicate) {
	r.Lock()
	defer r.Unlock()

	var prev *list.Element
	var next *list.Element
	for element := r.slices.Front(); element != nil; element = next {
		next = element.Next()

		slice := element.Value.(Slice)
		if prev == nil || !predicate(slice) {
			prev = element
			continue
		}

		compacted := r.slices.InsertAfter(
			prev.Value.(Slice).CompactWithSlice(slice),
			element,
		)

		r.slices.Remove(prev)
		r.slices.Remove(element)

		prev = compacted
	}

	r.resetNextReadSliceLocked()
	r.monitor.SetSliceCount(r.readerID, r.slices.Len())
}

// Shrink all queue slices, returning the number of tasks removed (completed)
func (r *ReaderImpl) ShrinkSlices() int {
	r.Lock()
	defer r.Unlock()

	var tasksCompleted int
	var next *list.Element
	for element := r.slices.Front(); element != nil; element = next {
		next = element.Next()

		slice := element.Value.(Slice)
		tasksCompleted += slice.ShrinkScope()
		if scope := slice.Scope(); scope.IsEmpty() {
			r.monitor.RemoveSlice(slice)
			r.slices.Remove(element)
		}
	}

	r.monitor.SetSliceCount(r.readerID, r.slices.Len())
	return tasksCompleted
}

func (r *ReaderImpl) Notify() {
	r.Lock()
	defer r.Unlock()

	if r.throttleTimer != nil {
		r.throttleTimer.Stop()
		r.throttleTimer = nil
	}

	r.notify()
}

func (r *ReaderImpl) Pause(duration time.Duration) {
	r.Lock()
	defer r.Unlock()

	r.pauseLocked(duration)
}

func (r *ReaderImpl) pauseLocked(duration time.Duration) {
	if r.throttleTimer != nil {
		r.throttleTimer.Stop()
	}

	r.throttleTimer = time.AfterFunc(duration, func() {
		r.Lock()
		defer r.Unlock()

		r.throttleTimer = nil
		r.notify()
	})
}

func (r *ReaderImpl) eventLoop() {
	defer func() {
		r.shutdownWG.Done()
	}()

	for {
		// prioritize shutdown
		select {
		case <-r.shutdownCh:
			return
		default:
		}

		select {
		case <-r.shutdownCh:
			return
		case <-r.notifyCh:
			r.loadAndSubmitTasks()
		}
	}
}

func (r *ReaderImpl) loadAndSubmitTasks() {
	if err := r.ratelimiter.Wait(r.rateLimitContext, r.rateLimiterRequest); err != nil {
		if r.rateLimitContext.Err() != nil {
			return
		}

		// this should never happen
		r.logger.Error("Queue reader rate limiter burst size is smaller than required token count")
	}

	r.Lock()
	defer r.Unlock()

	if !r.verifyPendingTaskSize() {
		r.pauseLocked(r.options.PollBackoffInterval())
	}

	if r.throttleTimer != nil {
		return
	}

	if r.nextReadSlice == nil {
		r.completionFn(r.readerID)
		return
	}

	loadSlice := r.nextReadSlice.Value.(Slice)
	tasks, err := loadSlice.SelectTasks(r.readerID, r.options.BatchSize())
	if err != nil {
		r.logger.Error("Queue reader unable to retrieve tasks", tag.Error(err))
		if common.IsResourceExhausted(err) {
			r.pauseLocked(throttleRetryDelay)
		} else {
			r.pauseLocked(r.retrier.NextBackOff())
		}
		return
	}
	r.retrier.Reset()

	if len(tasks) != 0 {
		for _, task := range tasks {
			r.submit(task)
		}
		r.monitor.SetReaderWatermark(r.readerID, tasks[len(tasks)-1].GetKey())
	}

	if loadSlice.MoreTasks() {
		r.notify()
		return
	}

	if r.nextReadSlice = r.nextReadSlice.Next(); r.nextReadSlice != nil {
		r.notify()
		return
	}

	// No more tasks to load, trigger completion callback.
	r.completionFn(r.readerID)
}

func (r *ReaderImpl) resetNextReadSliceLocked() {
	r.nextReadSlice = nil
	for element := r.slices.Front(); element != nil; element = element.Next() {
		if element.Value.(Slice).MoreTasks() {
			r.nextReadSlice = element
			break
		}
	}

	if r.nextReadSlice != nil {
		r.notify()
		return
	}

	// No more tasks to load, trigger completion callback.
	r.completionFn(r.readerID)
}

func (r *ReaderImpl) notify() {
	select {
	case r.notifyCh <- struct{}{}:
	default:
	}
}

func (r *ReaderImpl) submit(
	executable Executable,
) {
	now := r.timeSource.Now()
	// Persistence layer may lose precision when persisting the task, which essentially moves
	// task fire time backward. Need to account for that when submitting the task.
	fireTime := executable.GetKey().FireTime.Add(persistence.ScheduledTaskMinPrecision)
	if now.Before(fireTime) {
		r.rescheduler.Add(executable, fireTime)
		return
	}

	executable.SetScheduledTime(now)
	if !r.scheduler.TrySubmit(executable) {
		executable.Reschedule()
	}
}

func (r *ReaderImpl) verifyPendingTaskSize() bool {
	return r.monitor.GetTotalPendingTaskCount() < r.options.MaxPendingTasksCount()
}

func mergeOrAppendSlice(
	slices *list.List,
	incomingSlice Slice,
) {
	if slices.Len() == 0 {
		slices.PushBack(incomingSlice)
		return
	}

	lastElement := slices.Back()
	lastSlice := lastElement.Value.(Slice)
	if !lastSlice.CanMergeWithSlice(incomingSlice) {
		slices.PushBack(incomingSlice)
		return
	}

	mergedSlices := lastSlice.MergeWithSlice(incomingSlice)
	slices.Remove(lastElement)
	for _, mergedSlice := range mergedSlices {
		slices.PushBack(mergedSlice)
	}
}

func validateSlicesOrderedDisjoint(
	slices []Slice,
) {
	if len(slices) <= 1 {
		return
	}

	for idx, slice := range slices[:len(slices)-1] {
		nextSlice := slices[idx+1]
		if slice.Scope().Range.ExclusiveMax.CompareTo(nextSlice.Scope().Range.InclusiveMin) > 0 {
			panic(fmt.Sprintf(
				"Found overlapping incoming slices, left slice range: %v, right slice range: %v",
				slice.Scope().Range,
				nextSlice.Scope().Range,
			))
		}
	}
}
