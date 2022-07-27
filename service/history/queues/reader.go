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
	"sync"
	"sync/atomic"
	"time"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
)

type (
	Reader interface {
		common.Daemon

		Scopes() []Scope

		WalkSlices(SliceIterator)
		SplitSlices(SliceSplitter)
		MergeSlices(...Slice)
		ClearSlices(SlicePredicate)
		ShrinkSlices()

		Pause(time.Duration)
	}

	ReaderOptions struct {
		BatchSize            dynamicconfig.IntPropertyFn
		MaxPendingTasksCount dynamicconfig.IntPropertyFn
		PollBackoffInterval  dynamicconfig.DurationPropertyFn
	}

	SliceIterator func(s Slice)

	SliceSplitter func(s Slice) (remaining []Slice, split bool)

	SlicePredicate func(s Slice) bool

	ReaderImpl struct {
		sync.Mutex

		options        *ReaderOptions
		scheduler      Scheduler
		rescheduler    Rescheduler
		timeSource     clock.TimeSource
		ratelimiter    quotas.RateLimiter
		logger         log.Logger
		metricsHandler metrics.MetricsHandler

		status     int32
		shutdownCh chan struct{}
		shutdownWG sync.WaitGroup

		slices        *list.List
		nextReadSlice *list.Element
		notifyCh      chan struct{}

		throttleTimer *time.Timer
	}
)

func NewReader(
	paginationFnProvider PaginationFnProvider,
	executableInitializer ExecutableInitializer,
	scopes []Scope,
	options *ReaderOptions,
	scheduler Scheduler,
	rescheduler Rescheduler,
	timeSource clock.TimeSource,
	ratelimiter quotas.RateLimiter,
	logger log.Logger,
	metricsHandler metrics.MetricsHandler,
) *ReaderImpl {

	slices := list.New()
	for _, scope := range scopes {
		slices.PushBack(NewSlice(
			paginationFnProvider,
			executableInitializer,
			scope,
		))
	}

	return &ReaderImpl{
		options:        options,
		scheduler:      scheduler,
		rescheduler:    rescheduler,
		timeSource:     timeSource,
		ratelimiter:    ratelimiter,
		logger:         logger,
		metricsHandler: metricsHandler,

		status:     common.DaemonStatusInitialized,
		shutdownCh: make(chan struct{}),

		slices:        slices,
		nextReadSlice: slices.Front(),
		notifyCh:      make(chan struct{}, 1),
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

	close(r.shutdownCh)
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
}

func (r *ReaderImpl) MergeSlices(incomingSlices ...Slice) {
	r.Lock()
	defer r.Unlock()

	mergedSlices := list.New()

	currentSliceElement := r.slices.Front()
	incomingSliceIdx := 0

	for currentSliceElement != nil && incomingSliceIdx < len(incomingSlices) {
		currentSlice := currentSliceElement.Value.(Slice)
		incomingSlice := incomingSlices[incomingSliceIdx]

		if currentSlice.Scope().Range.InclusiveMin.CompareTo(incomingSlice.Scope().Range.InclusiveMin) < 0 {
			appendSlice(mergedSlices, currentSlice)
			currentSliceElement = currentSliceElement.Next()
		} else {
			appendSlice(mergedSlices, incomingSlice)
			incomingSliceIdx++
		}
	}

	for ; currentSliceElement != nil; currentSliceElement = currentSliceElement.Next() {
		appendSlice(mergedSlices, currentSliceElement.Value.(Slice))
	}
	for _, slice := range incomingSlices[incomingSliceIdx:] {
		appendSlice(mergedSlices, slice)
	}

	// clear existing list
	r.slices.Init()
	r.slices = mergedSlices

	r.resetNextReadSliceLocked()
}

func (r *ReaderImpl) ClearSlices(selector SlicePredicate) {
	r.Lock()
	defer r.Unlock()

	for element := r.slices.Front(); element != nil; element = element.Next() {
		slice := element.Value.(Slice)
		if selector(slice) {
			slice.Clear()
		}
	}

	r.resetNextReadSliceLocked()
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
	defer r.shutdownWG.Done()

	for {
		select {
		case <-r.shutdownCh:
			return
		case <-r.notifyCh:
			r.loadAndSubmitTasks()
		}
	}
}

func (r *ReaderImpl) loadAndSubmitTasks() {
	_ = r.ratelimiter.Wait(context.Background())

	r.Lock()
	defer r.Unlock()

	if !r.verifyPendingTaskSize() {
		r.pauseLocked(r.options.PollBackoffInterval())
	}

	if r.throttleTimer != nil {
		return
	}

	if r.nextReadSlice == nil {
		return
	}

	loadSlice := r.nextReadSlice.Value.(Slice)
	tasks, err := loadSlice.SelectTasks(r.options.BatchSize())
	if err != nil {
		r.logger.Error("Queue reader unable to retrieve tasks", tag.Error(err))
	}

	for _, task := range tasks {
		r.submit(task)
	}

	// TODO: in error case, we need some backoff
	// do this together with the retry improvement for existing queue implementation

	if loadSlice.MoreTasks() {
		r.notify()
		return
	}

	if r.nextReadSlice = r.nextReadSlice.Next(); r.nextReadSlice != nil {
		r.notify()
	}
}

func (r *ReaderImpl) ShrinkSlices() {
	r.Lock()
	defer r.Unlock()

	var next *list.Element
	for element := r.slices.Front(); element != nil; element = next {
		next = element.Next()

		slice := element.Value.(Slice)
		slice.ShrinkRange()
		if scope := slice.Scope(); scope.IsEmpty() {
			r.slices.Remove(element)
		}
	}
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
	}
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
	if fireTime := executable.GetKey().FireTime; now.Before(fireTime) {
		r.rescheduler.Add(executable, fireTime)
		return
	}

	submitted, err := r.scheduler.TrySubmit(executable)
	if err != nil {
		r.logger.Error("Failed to submit task", tag.Error(err))
		executable.Reschedule()
	} else if !submitted {
		executable.Reschedule()
	}
}

func (r *ReaderImpl) verifyPendingTaskSize() bool {
	return r.rescheduler.Len() < r.options.MaxPendingTasksCount()
}

func appendSlice(
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
