// The MIT License
//
// Copyright (c) 2025 Temporal Technologies Inc.  All rights reserved.
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

package matching

import (
	"container/heap"
	"context"
	"slices"
	"sync"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/util"
)

type pollerPQ struct {
	heap []*waitingPoller
}

// implements heap.Interface
func (p *pollerPQ) Len() int {
	return len(p.heap)
}

// implements heap.Interface, do not call directly
func (p *pollerPQ) Less(i int, j int) bool {
	a, b := p.heap[i], p.heap[j]
	if !(a.isTaskForwarder || a.isTaskValidator) && (b.isTaskForwarder || b.isTaskValidator) {
		return true
	} else if a.startTime.Before(b.startTime) {
		return true
	}
	return false
}

func (p *pollerPQ) Add(poller *waitingPoller) {
	heap.Push(p, poller)
}

func (p *pollerPQ) Remove(poller *waitingPoller) {
	heap.Remove(p, poller.matchHeapIndex)
}

// implements heap.Interface, do not call directly
func (p *pollerPQ) Swap(i int, j int) {
	p.heap[i], p.heap[j] = p.heap[j], p.heap[i]
	p.heap[i].matchHeapIndex = i
	p.heap[j].matchHeapIndex = j
}

// implements heap.Interface, do not call directly
func (p *pollerPQ) Push(x any) {
	poller := x.(*waitingPoller)
	poller.matchHeapIndex = len(p.heap)
	p.heap = append(p.heap, poller)
}

// implements heap.Interface, do not call directly
func (p *pollerPQ) Pop() any {
	last := len(p.heap) - 1
	poller := p.heap[last]
	p.heap = p.heap[:last]
	poller.matchHeapIndex = -11
	return poller
}

type taskPQ struct {
	heap []*internalTask
	// ages holds task create time for tasks from merged local backlogs (not forwarded).
	// note that matcherData may get tasks from multiple versioned backlogs due to
	// versioning redirection.
	ages backlogAgeTracker
}

func (t *taskPQ) Add(task *internalTask) {
	heap.Push(t, task)
}

func (t *taskPQ) Remove(task *internalTask) {
	heap.Remove(t, task.matchHeapIndex)
}

// implements heap.Interface
func (t *taskPQ) Len() int {
	return len(t.heap)
}

// implements heap.Interface, do not call directly
func (t *taskPQ) Less(i int, j int) bool {
	a, b := t.heap[i], t.heap[j]
	if !a.isPollForwarder && b.isPollForwarder {
		return true
	}
	// TODO(pri): use priority, task id, etc.
	return false
}

// implements heap.Interface, do not call directly
func (t *taskPQ) Swap(i int, j int) {
	t.heap[i], t.heap[j] = t.heap[j], t.heap[i]
	t.heap[i].matchHeapIndex = i
	t.heap[j].matchHeapIndex = j
}

// implements heap.Interface, do not call directly
func (t *taskPQ) Push(x any) {
	task := x.(*internalTask)
	task.matchHeapIndex = len(t.heap)
	t.heap = append(t.heap, task)

	if task.source == enumsspb.TASK_SOURCE_DB_BACKLOG && task.forwardInfo == nil {
		t.ages.record(task.event.Data.CreateTime, 1)
	}
}

// implements heap.Interface, do not call directly
func (t *taskPQ) Pop() any {
	last := len(t.heap) - 1
	task := t.heap[last]
	t.heap = t.heap[:last]
	task.matchHeapIndex = -13

	if task.source == enumsspb.TASK_SOURCE_DB_BACKLOG && task.forwardInfo == nil {
		t.ages.record(task.event.Data.CreateTime, -1)
	}

	return task
}

// Calls pred on each task. If it returns true, call post on the task and remove it
// from the queue, otherwise keep it.
// pred and post must not make any other calls on taskPQ until ForEachTask returns!
func (t *taskPQ) ForEachTask(pred func(*internalTask) bool, post func(*internalTask)) {
	t.heap = slices.DeleteFunc(t.heap, func(task *internalTask) bool {
		if task.isPollForwarder || !pred(task) {
			return false
		}
		task.matchHeapIndex = -14 // maintain heap/index invariant
		if task.source == enumsspb.TASK_SOURCE_DB_BACKLOG && task.forwardInfo == nil {
			t.ages.record(task.event.Data.CreateTime, -1)
		}
		post(task)
		return true
	})
	// re-establish heap
	for i, task := range t.heap {
		task.matchHeapIndex = i
	}
	heap.Init(t)
}

type matcherData struct {
	config      *taskQueueConfig
	rateLimiter quotas.RateLimiter // whole-queue rate limiter

	lock sync.Mutex // covers everything below, and all fields in any waitableMatchResult

	rateLimitTimer         resettableTimer
	reconsiderForwardTimer resettableTimer

	// waiting pollers and tasks
	// invariant: all pollers and tasks in these data structures have matchResult == nil
	pollers pollerPQ
	tasks   taskPQ

	lastPoller time.Time // most recent poll start time
}

func newMatcherData(config *taskQueueConfig, limiter quotas.RateLimiter) matcherData {
	return matcherData{
		config:      config,
		rateLimiter: limiter,
		tasks: taskPQ{
			ages: newBacklogAgeTracker(),
		},
	}
}

func (d *matcherData) EnqueueTaskNoWait(task *internalTask) {
	d.lock.Lock()
	defer d.lock.Unlock()

	task.initMatch(d)
	d.tasks.Add(task)
	d.match()
}

func (d *matcherData) EnqueueTaskAndWait(ctxs []context.Context, task *internalTask) *matchResult {
	d.lock.Lock()
	defer d.lock.Unlock()

	// add and look for match
	task.initMatch(d)
	d.tasks.Add(task)
	d.match()

	// if already matched, return
	if task.matchResult != nil {
		return task.matchResult
	}

	// arrange to wake up on context close
	for i, ctx := range ctxs {
		stop := context.AfterFunc(ctx, func() {
			d.lock.Lock()
			defer d.lock.Unlock()

			if task.matchResult == nil {
				d.tasks.Remove(task)
				task.wake(&matchResult{ctxErr: ctx.Err(), ctxErrIdx: i})
			}
		})
		defer stop()
	}

	return task.waitForMatch()
}

func (d *matcherData) ReenqueuePollerIfNotMatched(poller *waitingPoller) {
	d.lock.Lock()
	defer d.lock.Unlock()

	if poller.matchResult == nil {
		d.pollers.Add(poller)
		d.match()
	}
}

func (d *matcherData) EnqueuePollerAndWait(ctxs []context.Context, poller *waitingPoller) *matchResult {
	d.lock.Lock()
	defer d.lock.Unlock()

	// update this for timeSinceLastPoll
	d.lastPoller = util.MaxTime(d.lastPoller, poller.startTime)

	// add and look for match
	poller.initMatch(d)
	d.pollers.Add(poller)
	d.match()

	// if already matched, return
	if poller.matchResult != nil {
		return poller.matchResult
	}

	// arrange to wake up on context close
	for i, ctx := range ctxs {
		stop := context.AfterFunc(ctx, func() {
			d.lock.Lock()
			defer d.lock.Unlock()

			if poller.matchResult == nil {
				// if poll was being forwarded, it would be absent from heap even though
				// matchResult == nil
				if poller.matchHeapIndex >= 0 {
					d.pollers.Remove(poller)
				}
				poller.wake(&matchResult{ctxErr: ctx.Err(), ctxErrIdx: i})
			}
		})
		defer stop()
	}

	return poller.waitForMatch()
}

func (d *matcherData) MatchNextPoller(task *internalTask) (canSyncMatch, gotSyncMatch bool) {
	d.lock.Lock()
	defer d.lock.Unlock()

	if !d.isBacklogNegligible() {
		// To ensure better dispatch ordering, we block sync match when a significant backlog is present.
		// Note that this check does not make a noticeable difference for history tasks, as they do not wait for a
		// poller to become available. In presence of a backlog the chance of a poller being available when sync match
		// request comes is almost zero.
		// This check is mostly effective for the sync match requests that come from child partitions for spooled tasks.
		return false, false
	}

	task.initMatch(d)
	d.tasks.Add(task)
	d.match()
	// don't wait, check if match() picked this one already
	if task.matchResult != nil {
		return true, true
	}
	d.tasks.Remove(task)
	return true, false
}

func (d *matcherData) ReprocessTasks(pred func(*internalTask) bool) []*internalTask {
	d.lock.Lock()
	defer d.lock.Unlock()

	reprocess := make([]*internalTask, 0, len(d.tasks.heap))
	d.tasks.ForEachTask(
		pred,
		func(task *internalTask) {
			// for sync tasks: wake up waiters with a fake context error
			// for backlog tasks: the caller should call finish()
			task.wake(&matchResult{ctxErr: errReprocessTask, ctxErrIdx: -1})
			reprocess = append(reprocess, task)
		},
	)
	return reprocess
}

// findMatch should return the highest priority task+poller match even if the per-task rate
// limit doesn't allow the task to be matched yet.
// call with lock held
func (d *matcherData) findMatch(allowForwarding bool) (*internalTask, *waitingPoller) {
	// FIXME: optimize so it's not O(d*n) worst case
	// FIXME: this isn't actually correct (iterates over heap as slice)
	for _, task := range d.tasks.heap {
		if !allowForwarding && task.isPollForwarder {
			continue
		}

		for _, poller := range d.pollers.heap {
			// can't match cases:
			if poller.queryOnly && !(task.isQuery() || task.isPollForwarder) {
				continue
			} else if task.isPollForwarder && poller.forwardCtx == nil {
				continue
			} else if poller.isTaskForwarder && !allowForwarding {
				continue
			} else if poller.isTaskValidator && task.forwardCtx != nil {
				continue
			}

			return task, poller
		}
	}
	return nil, nil
}

// call with lock held
func (d *matcherData) allowForwarding() (allowForwarding bool) {
	// If there is a non-negligible backlog, we pause forwarding to make sure
	// root and leaf partitions are treated equally and can process their
	// backlog at the same rate. Stopping task forwarding, prevent poll
	// forwarding as well (in presence of a backlog). This ensures all partitions
	// receive polls and tasks at the same rate.
	//
	// Exception: we allow forward if this partition has not got any polls
	// recently. This is helpful when there are very few pollers and they
	// and they are all stuck in the wrong (root) partition. (Note that since
	// frontend balanced the number of pending pollers per partition this only
	// becomes an issue when the pollers are fewer than the partitions)
	//
	// If allowForwarding was false and changes to true due solely to the passage
	// of time, then we should ensure that match() is called again so that
	// pending tasks/polls can now be forwarded. When does that happen? if
	// isBacklogNegligible changes from false to true, or if we no longer have
	// recent polls.
	//
	// With time, backlog age gets larger, so isBacklogNegligible can go from
	// true to false and not the other way, so that's safe. But it is possible
	// that we no longer have recent polls. So we need to ensure that match() is
	// called again in that case, using reconsiderForwardTimer.
	if d.isBacklogNegligible() {
		d.reconsiderForwardTimer.unset()
		return true
	}
	delayToForwardingAllowed := d.config.MaxWaitForPollerBeforeFwd() - time.Since(d.lastPoller)
	d.reconsiderForwardTimer.set(d.rematch, delayToForwardingAllowed)
	return delayToForwardingAllowed <= 0
}

// call with lock held
func (d *matcherData) match() {
	allowForwarding := d.allowForwarding()

	for {
		// search for highest priority match
		task, poller := d.findMatch(allowForwarding)
		if task == nil || poller == nil {
			// no more current matches, stop rate limit timer if was running
			d.rateLimitTimer.unset()
			return
		}

		// got task, check rate limit
		if !d.rateLimitTask(task) {
			return // not ready yet, timer will call match later
		}

		// ready to signal match
		d.tasks.Remove(task)
		d.pollers.Remove(poller)

		res := &matchResult{task: task, poller: poller}

		task.wake(res)
		// for poll forwarder: skip waking poller, forwarder will call finishMatchAfterPollForward
		if !task.isPollForwarder {
			poller.wake(res)
		}
		// TODO(pri): consider having task forwarding work the same way, with a half-match,
		// instead of full match and then pass forward result on response channel?
		// TODO(pri): maybe consider leaving tasks and polls in the heap while forwarding and
		// allow them to be matched locally while forwarded (and then cancel the forward)?
	}
}

// call with lock held
// returns true if task can go now
func (d *matcherData) rateLimitTask(task *internalTask) bool {
	if task.recycleToken != nil {
		// we use task.recycleToken as a signal that we've already got a token for this task,
		// so the next time we see it we'll skip the wait.
		return true
	} else if task.isForwarded() {
		// don't count rate limit for forwarded tasks, it was counted on the child
		return true
	}

	delay := d.rateLimiter.Reserve().Delay()
	// TODO(pri): RecycleToken doesn't actually work since we're not using Wait
	task.recycleToken = d.rateLimiter.RecycleToken

	// TODO(pri): account for per-priority/fairness key rate limits also, e.g.
	// delay = max(delay, perTaskDelay)

	d.rateLimitTimer.set(d.rematch, delay)
	return delay <= 0
}

// called from timer
func (d *matcherData) rematch() {
	d.lock.Lock()
	defer d.lock.Unlock()
	d.match()
}

func (d *matcherData) finishMatchAfterPollForward(poller *waitingPoller, task *internalTask) {
	d.lock.Lock()
	defer d.lock.Unlock()

	if poller.matchResult == nil {
		poller.wake(&matchResult{task: task, poller: poller})
	}
}

// isBacklogNegligible returns true if the age of the task backlog is less than the threshold.
// call with lock held.
func (d *matcherData) isBacklogNegligible() bool {
	return d.tasks.ages.getAge() < d.config.BacklogNegligibleAge()
}

func (d *matcherData) TimeSinceLastPoll() time.Duration {
	d.lock.Lock()
	defer d.lock.Unlock()
	return time.Since(d.lastPoller)
}

// waitable match result:

type waitableMatchResult struct {
	// these fields are under matcherData.lock even though they're embedded in other structs
	matchCond      sync.Cond
	matchResult    *matchResult
	matchHeapIndex int // current heap index for easy removal
}

func (w *waitableMatchResult) initMatch(d *matcherData) {
	w.matchCond.L = &d.lock
	w.matchResult = nil
}

// call with matcherData.lock held.
// w.matchResult must be nil (can't call wake twice).
// w must not be in queues anymore.
func (w *waitableMatchResult) wake(res *matchResult) {
	bugIf(w.matchResult != nil, "bug: wake called twice")
	bugIf(w.matchHeapIndex >= 0, "bug: wake called but still in heap")
	w.matchResult = res
	w.matchCond.Signal()
}

// call with matcherData.lock held
func (w *waitableMatchResult) waitForMatch() *matchResult {
	for w.matchResult == nil {
		w.matchCond.Wait()
	}
	return w.matchResult
}

// resettable timer:

type resettableTimer struct {
	timer  *time.Timer // AfterFunc timer
	target time.Time   // target time of timer
}

// set sets rt to call f after delay. If it was already set to a future time, it's reset
// to only the sooner time. If it was already set to a sooner time, it's unchanged.
// set to <= 0 stops the timer.
func (rt *resettableTimer) set(f func(), delay time.Duration) {
	if delay <= 0 {
		rt.unset()
	} else if rt.timer == nil {
		rt.target = time.Now().Add(delay)
		rt.timer = time.AfterFunc(delay, f)
	} else if target := time.Now().Add(delay); target.Before(rt.target) {
		rt.target = target
		rt.timer.Reset(delay)
	}
}

// unset stops the timer.
func (rt *resettableTimer) unset() {
	if rt.timer != nil {
		rt.timer.Stop()
		rt.timer = nil
	}
}

func bugIf(cond bool, msg string) {
	if cond {
		panic(msg)
	}
}
