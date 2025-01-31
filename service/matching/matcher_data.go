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

	enumspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/util"
)

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

	// backlogAge holds task create time for tasks from merged local backlogs (not forwarded).
	// note that matcherData may get tasks from multiple versioned backlogs due to
	// versioning redirection.
	backlogAge backlogAgeTracker
	lastPoller time.Time // most recent poll start time
}

type pollerPQ struct {
	heap []*waitingPoller
}

func (p *pollerPQ) Len() int {
	return len(p.heap)
}

func (p *pollerPQ) Less(i int, j int) bool {
	a, b := p.heap[i], p.heap[j]
	if !(a.isTaskForwarder || a.isTaskValidator) && (b.isTaskForwarder || b.isTaskValidator) {
		return true
	} else if a.startTime.Before(b.startTime) {
		return true
	}
	return false
}

func (p *pollerPQ) Swap(i int, j int) {
	p.heap[i], p.heap[j] = p.heap[j], p.heap[i]
	p.heap[i].matchHeapIndex = i
	p.heap[j].matchHeapIndex = j
}

func (p *pollerPQ) Push(x any) {
	poller := x.(*waitingPoller)
	poller.matchHeapIndex = len(p.heap)
	p.heap = append(p.heap, poller)
}

func (p *pollerPQ) Pop() any {
	last := len(p.heap) - 1
	poller := p.heap[last]
	p.heap = p.heap[:last]
	poller.matchHeapIndex = -11
	return poller
}

type taskPQ struct {
	heap []*internalTask
}

func (t *taskPQ) Len() int {
	return len(t.heap)
}

func (t *taskPQ) Less(i int, j int) bool {
	a, b := t.heap[i], t.heap[j]
	if !a.isPollForwarder && b.isPollForwarder {
		return true
	}
	// TODO(pri): use priority, task id, etc.
	return false
}

func (t *taskPQ) Swap(i int, j int) {
	t.heap[i], t.heap[j] = t.heap[j], t.heap[i]
	t.heap[i].matchHeapIndex = i
	t.heap[j].matchHeapIndex = j
}

func (t *taskPQ) Push(x any) {
	task := x.(*internalTask)
	task.matchHeapIndex = len(t.heap)
	t.heap = append(t.heap, task)
}

func (t *taskPQ) Pop() any {
	last := len(t.heap) - 1
	task := t.heap[last]
	t.heap = t.heap[:last]
	task.matchHeapIndex = -13
	return task
}

func (d *matcherData) EnqueueTaskNoWait(task *internalTask) {
	d.lock.Lock()
	defer d.lock.Unlock()

	if task.source == enumspb.TASK_SOURCE_DB_BACKLOG {
		d.backlogAge.record(task.event.Data.CreateTime, 1)
	}

	task.initMatch(d)
	heap.Push(&d.tasks, task)
	d.match()
}

func (d *matcherData) EnqueueTaskAndWait(ctxs []context.Context, task *internalTask) *matchResult {
	d.lock.Lock()
	defer d.lock.Unlock()

	// add and look for match
	task.initMatch(d)
	heap.Push(&d.tasks, task)
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
				heap.Remove(&d.tasks, task.matchHeapIndex)
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
		heap.Push(&d.pollers, poller)
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
	heap.Push(&d.pollers, poller)
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
					heap.Remove(&d.pollers, poller.matchHeapIndex)
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
	heap.Push(&d.tasks, task)
	d.match()
	// don't wait, check if match() picked this one already
	if task.matchResult != nil {
		return true, true
	}
	heap.Remove(&d.tasks, task.matchHeapIndex)
	return true, false
}

func (d *matcherData) RecheckAllRedirects() {
	d.lock.Lock()

	// TODO(pri): do we have to do _all_ backlog tasks or can we determine somehow which are
	// potentially redirected?
	redirectable := make([]*internalTask, 0, len(d.tasks.heap))
	d.tasks.heap = slices.DeleteFunc(d.tasks.heap, func(task *internalTask) bool {
		if task.forwardInfo.GetTaskSource() == enumspb.TASK_SOURCE_DB_BACKLOG {
			// forwarded from a backlog, kick this back to the child so they can redirect it,
			// by faking a context cancel
			task.matchHeapIndex = -13
			task.wake(&matchResult{ctxErr: context.Canceled})
			return true
		} else if task.checkRedirect != nil {
			redirectable = append(redirectable, task)
			if task.source == enumspb.TASK_SOURCE_DB_BACKLOG {
				d.backlogAge.record(task.event.Data.CreateTime, -1)
			}
			task.matchHeapIndex = -13
			return true
		}
		return false
	})

	// fix indexes and re-establish heap
	for i, task := range d.tasks.heap {
		task.matchHeapIndex = i
	}
	heap.Init(&d.tasks)

	d.lock.Unlock()

	// re-redirect them all (or put back if redirect fails)
	for _, task := range redirectable {
		d.RedirectOrEnqueue(task)
	}
}

func (d *matcherData) RedirectOrEnqueue(task *internalTask) {
	if task.checkRedirect == nil || !task.checkRedirect() {
		d.EnqueueTaskNoWait(task)
	}
}

// findMatch should return the highest priority task+poller match even if the per-task rate
// limit doesn't allow the task to be matched yet.
// call with lock held
func (d *matcherData) findMatch(allowForwarding bool) (*internalTask, *waitingPoller) {
	// FIXME: optimize so it's not O(d*n) worst case
	// FIXME: this isn't actually correct
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
	defer func() {
		if allowForwarding {
			d.reconsiderForwardTimer.unset()
		}
	}()
	if d.isBacklogNegligible() {
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
			// no more current matches, stop timers if they were running
			d.rateLimitTimer.unset()
			d.reconsiderForwardTimer.unset()
			return
		}

		// got task, check rate limit
		if !d.rateLimitTask(task) {
			return // not ready yet, timer will call match later
		}

		// ready to signal match
		heap.Remove(&d.tasks, task.matchHeapIndex)
		heap.Remove(&d.pollers, poller.matchHeapIndex)

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

		if task.source == enumspb.TASK_SOURCE_DB_BACKLOG {
			d.backlogAge.record(task.event.Data.CreateTime, -1)
		}
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
	task.recycleToken = d.rateLimiter.RecycleToken

	// TODO: account for per-priority/fairness key rate limits also, e.g.
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
	return d.backlogAge.getAge() < d.config.BacklogNegligibleAge()
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
