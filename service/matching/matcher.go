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

package matching

import (
	"context"
	"math"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/quotas"
)

// TaskMatcher matches a task producer with a task consumer
// Producers are usually rpc calls from history or taskReader
// that drains backlog from db. Consumers are the task queue pollers
type TaskMatcher struct {
	config *taskQueueConfig

	// synchronous task channel to match producer/consumer
	taskC chan *internalTask
	// synchronous task channel to match query task - the reason to have
	// separate channel for this is because there are cases when consumers
	// are interested in queryTasks but not others. Example is when namespace is
	// not active in a cluster
	queryTaskC chan *internalTask

	// dynamicRate is the dynamic rate & burst for rate limiter
	dynamicRateBurst quotas.MutableRateBurst
	// rateLimiter that limits the rate at which tasks can be dispatched to consumers
	rateLimiter quotas.RateLimiter

	fwdr          *Forwarder
	scope         metrics.Scope // namespace metric scope
	numPartitions func() int    // number of task queue partitions
}

const (
	defaultTaskDispatchRPS    = 100000.0
	defaultTaskDispatchRPSTTL = time.Minute
)

// newTaskMatcher returns an task matcher instance. The returned instance can be
// used by task producers and consumers to find a match. Both sync matches and non-sync
// matches should use this implementation
func newTaskMatcher(config *taskQueueConfig, fwdr *Forwarder, scope metrics.Scope) *TaskMatcher {
	dynamicRateBurst := quotas.NewMutableRateBurst(
		defaultTaskDispatchRPS,
		int(defaultTaskDispatchRPS),
	)
	limiter := quotas.NewMultiRateLimiter([]quotas.RateLimiter{
		quotas.NewDynamicRateLimiter(
			dynamicRateBurst,
			defaultTaskDispatchRPSTTL,
		),
		quotas.NewDefaultOutgoingRateLimiter(
			config.AdminNamespaceTaskQueueToPartitionDispatchRate,
		),
		quotas.NewDefaultOutgoingRateLimiter(
			config.AdminNamespaceToPartitionDispatchRate,
		),
	})
	return &TaskMatcher{
		config:           config,
		dynamicRateBurst: dynamicRateBurst,
		rateLimiter:      limiter,
		scope:            scope,
		fwdr:             fwdr,
		taskC:            make(chan *internalTask),
		queryTaskC:       make(chan *internalTask),
		numPartitions:    config.NumReadPartitions,
	}
}

// Offer offers a task to a potential consumer (poller)
// If the task is successfully matched with a consumer, this
// method will return true and no error. If the task is matched
// but consumer returned error, then this method will return
// true and error message. This method should not be used for query
// task. This method should ONLY be used for sync match.
//
// When a local poller is not available and forwarding to a parent
// task queue partition is possible, this method will attempt forwarding
// to the parent partition.
//
// Cases when this method will block:
//
// Ratelimit:
// When a ratelimit token is not available, this method might block
// waiting for a token until the provided context timeout. Rate limits are
// not enforced for forwarded tasks from child partition.
//
// Forwarded tasks that originated from db backlog:
// When this method is called with a task that is forwarded from a
// remote partition and if (1) this task queue is root (2) task
// was from db backlog - this method will block until context timeout
// trying to match with a poller. The caller is expected to set the
// correct context timeout.
//
// returns error when:
//  - ratelimit is exceeded (does not apply to query task)
//  - context deadline is exceeded
//  - task is matched and consumer returns error in response channel
func (tm *TaskMatcher) Offer(ctx context.Context, task *internalTask) (bool, error) {
	if !task.isForwarded() {
		if err := tm.rateLimiter.Wait(ctx); err != nil {
			tm.scope.IncCounter(metrics.SyncThrottlePerTaskQueueCounter)
			return false, err
		}
	}

	select {
	case tm.taskC <- task: // poller picked up the task
		if task.responseC != nil {
			// if there is a response channel, block until resp is received
			// and return error if the response contains error
			err := <-task.responseC
			return true, err
		}
		return false, nil
	default:
		// no poller waiting for tasks, try forwarding this task to the
		// root partition if possible
		select {
		case token := <-tm.fwdrAddReqTokenC():
			if err := tm.fwdr.ForwardTask(ctx, task); err == nil {
				// task was remotely sync matched on the parent partition
				token.release()
				return true, nil
			}
			token.release()
		default:
			if !tm.isForwardingAllowed() && // we are the root partition and forwarding is not possible
				task.source == enumsspb.TASK_SOURCE_DB_BACKLOG && // task was from backlog (stored in db)
				task.isForwarded() { // task came from a child partition
				// a forwarded backlog task from a child partition, block trying
				// to match with a poller until ctx timeout
				return tm.offerOrTimeout(ctx, task)
			}
		}

		return false, nil
	}
}

func (tm *TaskMatcher) offerOrTimeout(ctx context.Context, task *internalTask) (bool, error) {
	select {
	case tm.taskC <- task: // poller picked up the task
		if task.responseC != nil {
			select {
			case err := <-task.responseC:
				return true, err
			case <-ctx.Done():
				return false, nil
			}
		}
		return false, nil
	case <-ctx.Done():
		return false, nil
	}
}

// OfferQuery will either match task to local poller or will forward query task.
// Local match is always attempted before forwarding is attempted. If local match occurs
// response and error are both nil, if forwarding occurs then response or error is returned.
func (tm *TaskMatcher) OfferQuery(ctx context.Context, task *internalTask) (*matchingservice.QueryWorkflowResponse, error) {
	select {
	case tm.queryTaskC <- task:
		<-task.responseC
		return nil, nil
	default:
	}

	fwdrTokenC := tm.fwdrAddReqTokenC()

	for {
		select {
		case tm.queryTaskC <- task:
			<-task.responseC
			return nil, nil
		case token := <-fwdrTokenC:
			resp, err := tm.fwdr.ForwardQueryTask(ctx, task)
			token.release()
			if err == nil {
				return resp, nil
			}
			if err == errForwarderSlowDown {
				// if we are rate limited, try only local match for the
				// remainder of the context timeout left
				fwdrTokenC = nil
				continue
			}
			return nil, err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// MustOffer blocks until a consumer is found to handle this task
// Returns error only when context is canceled or the ratelimit is set to zero (allow nothing)
// The passed in context MUST NOT have a deadline associated with it
func (tm *TaskMatcher) MustOffer(ctx context.Context, task *internalTask) error {
	if err := tm.rateLimiter.Wait(ctx); err != nil {
		return err
	}

	// attempt a match with local poller first. When that
	// doesn't succeed, try both local match and remote match
	select {
	case tm.taskC <- task:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

forLoop:
	for {
		select {
		case tm.taskC <- task:
			return nil
		case token := <-tm.fwdrAddReqTokenC():
			childCtx, cancel := context.WithTimeout(ctx, time.Second*2)
			err := tm.fwdr.ForwardTask(childCtx, task)
			token.release()
			if err != nil {
				tm.scope.IncCounter(metrics.ForwardTaskErrorsPerTaskQueue)
				// forwarder returns error only when the call is rate limited. To
				// avoid a busy loop on such rate limiting events, we only attempt to make
				// the next forwarded call after this childCtx expires. Till then, we block
				// hoping for a local poller match
				select {
				case tm.taskC <- task:
					cancel()
					return nil
				case <-childCtx.Done():
				case <-ctx.Done():
					cancel()
					return ctx.Err()
				}
				cancel()
				continue forLoop
			}
			cancel()
			// at this point, we forwarded the task to a parent partition which
			// in turn dispatched the task to a poller. Make sure we delete the
			// task from the database
			task.finish(nil)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Poll blocks until a task is found or context deadline is exceeded
// On success, the returned task could be a query task or a regular task
// Returns ErrNoTasks when context deadline is exceeded
func (tm *TaskMatcher) Poll(ctx context.Context) (*internalTask, error) {
	return tm.poll(ctx, false)
}

// PollForQuery blocks until a *query* task is found or context deadline is exceeded
// Returns ErrNoTasks when context deadline is exceeded
func (tm *TaskMatcher) PollForQuery(ctx context.Context) (*internalTask, error) {
	return tm.poll(ctx, true)
}

// UpdateRatelimit updates the task dispatch rate
func (tm *TaskMatcher) UpdateRatelimit(rps *float64) {
	if rps == nil {
		return
	}

	rate := *rps
	nPartitions := float64(tm.numPartitions())
	if nPartitions > 0 {
		// divide the rate equally across all partitions
		rate = rate / nPartitions
	}
	burst := int(math.Ceil(rate))

	minTaskThrottlingBurstSize := tm.config.MinTaskThrottlingBurstSize()
	if burst < minTaskThrottlingBurstSize {
		burst = minTaskThrottlingBurstSize
	}

	tm.dynamicRateBurst.SetRate(rate)
	tm.dynamicRateBurst.SetBurst(burst)
}

// Rate returns the current rate at which tasks are dispatched
func (tm *TaskMatcher) Rate() float64 {
	return tm.rateLimiter.Rate()
}

func (tm *TaskMatcher) poll(ctx context.Context, queryOnly bool) (*internalTask, error) {
	taskC, queryTaskC := tm.taskC, tm.queryTaskC
	if queryOnly {
		taskC = nil
	}

	// We want to effectively do a prioritized select, but Go select is random
	// if multiple cases are ready, so split into multiple selects.
	// The priority order is:
	// 1. ctx.Done
	// 2. taskC and queryTaskC
	// 3. forwarding
	// 4. block looking locally for remainder of context lifetime
	// To correctly handle priorities and allow any case to succeed, all select
	// statements except for the last one must be non-blocking, and the last one
	// must include all the previous cases.

	// 1. ctx.Done
	select {
	case <-ctx.Done():
		tm.scope.IncCounter(metrics.PollTimeoutPerTaskQueueCounter)
		return nil, ErrNoTasks
	default:
	}

	// 2. taskC and queryTaskC
	select {
	case task := <-taskC:
		if task.responseC != nil {
			tm.scope.IncCounter(metrics.PollSuccessWithSyncPerTaskQueueCounter)
		}
		tm.scope.IncCounter(metrics.PollSuccessPerTaskQueueCounter)
		return task, nil
	case task := <-queryTaskC:
		tm.scope.IncCounter(metrics.PollSuccessWithSyncPerTaskQueueCounter)
		tm.scope.IncCounter(metrics.PollSuccessPerTaskQueueCounter)
		return task, nil
	default:
	}

	// 3. forwarding (and all other clauses repeated again)
	select {
	case <-ctx.Done():
		tm.scope.IncCounter(metrics.PollTimeoutPerTaskQueueCounter)
		return nil, ErrNoTasks
	case task := <-taskC:
		if task.responseC != nil {
			tm.scope.IncCounter(metrics.PollSuccessWithSyncPerTaskQueueCounter)
		}
		tm.scope.IncCounter(metrics.PollSuccessPerTaskQueueCounter)
		return task, nil
	case task := <-queryTaskC:
		tm.scope.IncCounter(metrics.PollSuccessWithSyncPerTaskQueueCounter)
		tm.scope.IncCounter(metrics.PollSuccessPerTaskQueueCounter)
		return task, nil
	case token := <-tm.fwdrPollReqTokenC():
		if task, err := tm.fwdr.ForwardPoll(ctx); err == nil {
			token.release()
			return task, nil
		}
		token.release()
	}

	// 4. blocking local poll
	select {
	case <-ctx.Done():
		tm.scope.IncCounter(metrics.PollTimeoutPerTaskQueueCounter)
		return nil, ErrNoTasks
	case task := <-taskC:
		if task.responseC != nil {
			tm.scope.IncCounter(metrics.PollSuccessWithSyncPerTaskQueueCounter)
		}
		tm.scope.IncCounter(metrics.PollSuccessPerTaskQueueCounter)
		return task, nil
	case task := <-queryTaskC:
		tm.scope.IncCounter(metrics.PollSuccessWithSyncPerTaskQueueCounter)
		tm.scope.IncCounter(metrics.PollSuccessPerTaskQueueCounter)
		return task, nil
	}
}

func (tm *TaskMatcher) fwdrPollReqTokenC() <-chan *ForwarderReqToken {
	if tm.fwdr == nil {
		return nil
	}
	return tm.fwdr.PollReqTokenC()
}

func (tm *TaskMatcher) fwdrAddReqTokenC() <-chan *ForwarderReqToken {
	if tm.fwdr == nil {
		return nil
	}
	return tm.fwdr.AddReqTokenC()
}

func (tm *TaskMatcher) isForwardingAllowed() bool {
	return tm.fwdr != nil
}
