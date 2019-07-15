// Copyright (c) 2019 Uber Technologies, Inc.
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
	"context"
	"errors"
	"time"

	"github.com/uber/cadence/common/metrics"
	"golang.org/x/time/rate"
)

// TaskMatcher matches a task producer with a task consumer
// Producers are usually rpc calls from history or taskReader
// that drains backlog from db. Consumers are the task list pollers
type TaskMatcher struct {
	// synchronous task channel to match producer/consumer
	taskC chan *internalTask
	// synchronous task channel to match query task - the reason to have
	// separate channel for this is because there are cases when consumers
	// are interested in queryTasks but not others. Example is when domain is
	// not active in a cluster
	queryTaskC chan *internalTask
	// ratelimiter that limits the rate at which tasks can be dispatched to consumers
	limiter *rateLimiter
	// domain metric scope
	scope func() metrics.Scope
}

const (
	_defaultTaskDispatchRPS    = 100000.0
	_defaultTaskDispatchRPSTTL = 60 * time.Second
)

var errTasklistThrottled = errors.New("cannot add to tasklist, limit exceeded")

// newTaskMatcher returns an task matcher instance. The returned instance can be
// used by task producers and consumers to find a match. Both sync matches and non-sync
// matches should use this implementation
func newTaskMatcher(config *taskListConfig, scopeFunc func() metrics.Scope) *TaskMatcher {
	dPtr := _defaultTaskDispatchRPS
	limiter := newRateLimiter(&dPtr, _defaultTaskDispatchRPSTTL, config.MinTaskThrottlingBurstSize())
	return &TaskMatcher{
		limiter:    limiter,
		scope:      scopeFunc,
		taskC:      make(chan *internalTask),
		queryTaskC: make(chan *internalTask),
	}
}

// Offer offers a task to a potential consumer (poller)
// If the task is successfully matched with a consumer, this
// method will return true and no error. If the task is matched
// but consumer returned error, then this method will return
// true and error message. Both regular tasks and query tasks
// should use this method to match with a consumer. Likewise, sync matches
// and non-sync matches both should use this method.
// returns error when:
//  - ratelimit is exceeded (does not apply to query task)
//  - context deadline is exceeded
//  - task is matched and consumer returns error in response channel
func (tm *TaskMatcher) Offer(ctx context.Context, task *internalTask) (bool, error) {
	if task.isQuery() {
		select {
		case tm.queryTaskC <- task:
			<-task.responseC
			return true, nil
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}

	rsv, err := tm.ratelimit(ctx)
	if err != nil {
		tm.scope().IncCounter(metrics.SyncThrottleCounter)
		return false, err
	}

	select {
	case tm.taskC <- task: // poller picked up the task
		if task.responseC != nil {
			// if there is a response channel, block until resp is received
			// and return error if the response contains error
			err = <-task.responseC
			return true, err
		}
		return false, nil
	default: // no poller waiting for tasks
		if rsv != nil {
			// there was a ratelimit token we consumed
			// return it since we did not really do any work
			rsv.Cancel()
		}
		return false, nil
	}
}

// MustOffer blocks until a consumer is found to handle this task
// Returns error only when context is canceled or the ratelimit is set to zero (allow nothing)
// The passed in context MUST NOT have a deadline associated with it
func (tm *TaskMatcher) MustOffer(ctx context.Context, task *internalTask) error {
	if _, err := tm.ratelimit(ctx); err != nil {
		return err
	}
	select {
	case tm.taskC <- task:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Poll blocks until a task is found or context deadline is exceeded
// On success, the returned task could be a query task or a regular task
// Returns ErrNoTasks when context deadline is exceeded
func (tm *TaskMatcher) Poll(ctx context.Context) (*internalTask, error) {
	select {
	case task := <-tm.taskC:
		if task.responseC != nil {
			tm.scope().IncCounter(metrics.PollSuccessWithSyncCounter)
		}
		tm.scope().IncCounter(metrics.PollSuccessCounter)
		return task, nil
	case task := <-tm.queryTaskC:
		tm.scope().IncCounter(metrics.PollSuccessWithSyncCounter)
		tm.scope().IncCounter(metrics.PollSuccessCounter)
		return task, nil
	case <-ctx.Done():
		tm.scope().IncCounter(metrics.PollTimeoutCounter)
		return nil, ErrNoTasks
	}
}

// PollForQuery blocks until a *query* task is found or context deadline is exceeded
// Returns ErrNoTasks when context deadline is exceeded
func (tm *TaskMatcher) PollForQuery(ctx context.Context) (*internalTask, error) {
	select {
	case task := <-tm.queryTaskC:
		tm.scope().IncCounter(metrics.PollSuccessWithSyncCounter)
		tm.scope().IncCounter(metrics.PollSuccessCounter)
		return task, nil
	case <-ctx.Done():
		tm.scope().IncCounter(metrics.PollTimeoutCounter)
		return nil, ErrNoTasks
	}
}

// UpdateRatelimit updates the task dispatch rate
func (tm *TaskMatcher) UpdateRatelimit(rps *float64) {
	tm.limiter.UpdateMaxDispatch(rps)
}

// Rate returns the current rate at which tasks are dispatched
func (tm *TaskMatcher) Rate() float64 {
	return tm.limiter.Limit()
}

func (tm *TaskMatcher) ratelimit(ctx context.Context) (*rate.Reservation, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	deadline, ok := ctx.Deadline()
	if !ok {
		if err := tm.limiter.Wait(ctx); err != nil {
			return nil, err
		}
		return nil, nil
	}

	rsv := tm.limiter.Reserve()
	// If we have to wait too long for reservation, give up and return
	if !rsv.OK() || rsv.Delay() > deadline.Sub(time.Now()) {
		if rsv.OK() { // if we were indeed given a reservation, return it before we bail out
			rsv.Cancel()
		}
		return nil, errTasklistThrottled
	}

	time.Sleep(rsv.Delay())
	return rsv, nil
}
