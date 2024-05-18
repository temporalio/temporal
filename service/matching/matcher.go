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
	"errors"
	"math"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TaskMatcher matches a task producer with a task consumer
// Producers are usually rpc calls from history or taskReader
// that drains backlog from db. Consumers are the task queue pollers
type TaskMatcher struct {
	config *taskQueueConfig

	// synchronous task channel to match producer/consumer
	taskC chan *internalTask
	// synchronous task channel to match query task - the reason to have a
	// separate channel for this is that there are cases where consumers
	// are interested in queryTasks but not others. One example is when a
	// namespace is not active in a cluster.
	queryTaskC chan *internalTask
	// channel closed when task queue is closed, to interrupt pollers
	closeC chan struct{}

	// dynamicRate is the dynamic rate & burst for rate limiter
	dynamicRateBurst quotas.MutableRateBurst
	// dynamicRateLimiter is the dynamic rate limiter that can be used to force refresh on new rates.
	dynamicRateLimiter *quotas.DynamicRateLimiterImpl
	// forceRefreshRateOnce is used to force refresh rate limit for first time
	forceRefreshRateOnce sync.Once
	// rateLimiter that limits the rate at which tasks can be dispatched to consumers
	rateLimiter quotas.RateLimiter

	fwdr                   *Forwarder
	metricsHandler         metrics.Handler // namespace metric scope
	numPartitions          func() int      // number of task queue partitions
	backlogTasksCreateTime map[int64]int   // task creation time (unix nanos) -> number of tasks with that time
	backlogTasksLock       sync.Mutex
	lastPoller             atomic.Int64 // unix nanos of most recent poll start time
}

const (
	defaultTaskDispatchRPS                  = 100000.0
	defaultTaskDispatchRPSTTL               = time.Minute
	emptyBacklogAge           time.Duration = -1
)

var (
	// Sentinel error to redirect while blocked in matcher.
	errInterrupted    = errors.New("interrupted offer")
	errNoRecentPoller = status.Error(codes.FailedPrecondition, "no poller seen for task queue recently, worker may be down")
)

// newTaskMatcher returns a task matcher instance. The returned instance can be used by task producers and consumers to
// find a match. Both sync matches and non-sync matches should use this implementation
func newTaskMatcher(config *taskQueueConfig, fwdr *Forwarder, metricsHandler metrics.Handler) *TaskMatcher {
	dynamicRateBurst := quotas.NewMutableRateBurst(
		defaultTaskDispatchRPS,
		int(defaultTaskDispatchRPS),
	)
	dynamicRateLimiter := quotas.NewDynamicRateLimiter(
		dynamicRateBurst,
		defaultTaskDispatchRPSTTL,
	)
	limiter := quotas.NewMultiRateLimiter([]quotas.RateLimiter{
		dynamicRateLimiter,
		quotas.NewDefaultOutgoingRateLimiter(
			config.AdminNamespaceTaskQueueToPartitionDispatchRate,
		),
		quotas.NewDefaultOutgoingRateLimiter(
			config.AdminNamespaceToPartitionDispatchRate,
		),
	})
	return &TaskMatcher{
		config:                 config,
		dynamicRateBurst:       dynamicRateBurst,
		dynamicRateLimiter:     dynamicRateLimiter,
		rateLimiter:            limiter,
		metricsHandler:         metricsHandler,
		fwdr:                   fwdr,
		taskC:                  make(chan *internalTask),
		queryTaskC:             make(chan *internalTask),
		closeC:                 make(chan struct{}),
		numPartitions:          config.NumReadPartitions,
		backlogTasksCreateTime: make(map[int64]int),
	}
}

func (tm *TaskMatcher) Stop() {
	close(tm.closeC)
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
//   - ratelimit is exceeded (does not apply to query task)
//   - context deadline is exceeded
//   - task is matched and consumer returns error in response channel
func (tm *TaskMatcher) Offer(ctx context.Context, task *internalTask) (bool, error) {
	if !tm.isBacklogNegligible() {
		// To ensure better dispatch ordering, we block sync match when a significant backlog is present.
		// Note that this check does not make a noticeable difference for history tasks, as they do not wait for a
		// poller to become available. In presence of a backlog the chance of a poller being available when sync match
		// request comes is almost zero.
		// This check is mostly effective for the sync match requests that come from child partitions for spooled tasks.
		return false, nil
	}

	if !task.isForwarded() {
		if err := tm.rateLimiter.Wait(ctx); err != nil {
			metrics.SyncThrottlePerTaskQueueCounter.With(tm.metricsHandler).Record(1)
			return false, err
		}
	}

	select {
	case tm.taskC <- task: // poller picked up the task
		if task.responseC != nil {
			// if there is a response channel, block until resp is received
			// and return error if the response contains error
			err := <-task.responseC

			if err == nil && !task.isForwarded() {
				tm.emitDispatchLatency(task, false)
			}
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
				tm.emitDispatchLatency(task, true)
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

func syncOfferTask[T any](
	ctx context.Context,
	tm *TaskMatcher,
	task *internalTask,
	taskChan chan *internalTask,
	forwardFunc func(context.Context, *internalTask) (T, error),
) (T, error) {
	var t T
	select {
	case taskChan <- task:
		<-task.responseC
		return t, nil
	default:
	}

	fwdrTokenC := tm.fwdrAddReqTokenC()
	var noPollerCtxC <-chan struct{}

	if deadline, ok := ctx.Deadline(); ok && fwdrTokenC == nil {
		// Reserving 1sec to customize the timeout error if user is querying a workflow
		// without having started the workers.
		noPollerTimeout := time.Until(deadline) - time.Second
		noPollerCtx, cancel := context.WithTimeout(ctx, noPollerTimeout)
		noPollerCtxC = noPollerCtx.Done()
		defer cancel()
	}

	for {
		select {
		case taskChan <- task:
			<-task.responseC
			return t, nil
		case token := <-fwdrTokenC:
			resp, err := forwardFunc(ctx, task)
			token.release()
			if err == nil {
				return resp, nil
			}
			if errors.Is(err, errForwarderSlowDown) {
				// if we are rate limited, try only local match for the remainder of the context timeout
				// left
				fwdrTokenC = nil
				continue
			}
			return t, err
		case <-noPollerCtxC:
			// only error if there has not been a recent poller. Otherwise, let it wait for the remaining time
			// hopping for a match, or ultimately returning the default CDE error.
			// TODO: rename this to clarify it applies to nexus tasks and queries.
			if tm.timeSinceLastPoll() > tm.config.QueryPollerUnavailableWindow() {
				return t, errNoRecentPoller
			}
			continue
		case <-ctx.Done():
			return t, ctx.Err()
		}
	}
}

// OfferQuery will either match task to local poller or will forward query task.
// Local match is always attempted before forwarding is attempted. If local match occurs
// response and error are both nil, if forwarding occurs then response or error is returned.
func (tm *TaskMatcher) OfferQuery(ctx context.Context, task *internalTask) (*matchingservice.QueryWorkflowResponse, error) {
	return syncOfferTask(ctx, tm, task, tm.queryTaskC, tm.fwdr.ForwardQueryTask)
}

// OfferNexusTask either matchs a task to a local poller or forwards it if no local pollers available.
// Local match is always attempted before forwarding. If local match occurs response and error are both nil, if
// forwarding occurs then response or error is returned.
func (tm *TaskMatcher) OfferNexusTask(ctx context.Context, task *internalTask) (*matchingservice.DispatchNexusTaskResponse, error) {
	return syncOfferTask(ctx, tm, task, tm.taskC, tm.fwdr.ForwardNexusTask)
}

// MustOffer blocks until a consumer is found to handle this task
// Returns error only when context is canceled or the ratelimit is set to zero (allow nothing)
// The passed in context MUST NOT have a deadline associated with it
// Note that calling MustOffer is the only way that matcher knows there are spooled tasks in the
// backlog, in absence of a pending MustOffer call, the forwarding logic assumes that backlog is empty.
func (tm *TaskMatcher) MustOffer(ctx context.Context, task *internalTask, interruptCh <-chan struct{}) error {
	tm.registerBacklogTask(task)
	defer tm.unregisterBacklogTask(task)

	if err := tm.rateLimiter.Wait(ctx); err != nil {
		return err
	}

	// attempt a match with local poller first. When that
	// doesn't succeed, try both local match and remote match
	select {
	case tm.taskC <- task:
		tm.emitDispatchLatency(task, false)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	var reconsiderFwdTimer *time.Timer
	defer func() {
		if reconsiderFwdTimer != nil {
			reconsiderFwdTimer.Stop()
		}
	}()

forLoop:
	for {
		fwdTokenC := tm.fwdrAddReqTokenC()
		reconsiderFwdTimer = nil
		var reconsiderFwdTimerC <-chan time.Time
		if fwdTokenC != nil && !tm.isBacklogNegligible() {
			// If there is a non-negligible backlog, we stop forwarding to make sure
			// root and leaf partitions are treated equally and can process their
			// backlog at the same rate. Stopping task forwarding, prevent poll
			// forwarding as well (in presence of a backlog). This ensures all partitions
			// receive polls and tasks at the same rate.

			// Exception: we allow forward if this partition has not got any polls
			// recently. This is helpful when there are very few pollers and they
			// and they are all stuck in the wrong (root) partition. (Note that since
			// frontend balanced the number of pending pollers per partition this only
			// becomes an issue when the pollers are fewer than the partitions)
			lp := tm.timeSinceLastPoll()
			maxWaitForLocalPoller := tm.config.MaxWaitForPollerBeforeFwd()
			if lp < maxWaitForLocalPoller {
				fwdTokenC = nil
				reconsiderFwdTimer = time.NewTimer(maxWaitForLocalPoller - lp)
				reconsiderFwdTimerC = reconsiderFwdTimer.C
			}
		}

		select {
		case tm.taskC <- task:
			tm.emitDispatchLatency(task, false)
			return nil
		case token := <-fwdTokenC:
			childCtx, cancel := context.WithTimeout(ctx, time.Second*2)
			err := tm.fwdr.ForwardTask(childCtx, task)
			token.release()
			if err != nil {
				metrics.ForwardTaskErrorsPerTaskQueue.With(tm.metricsHandler).Record(1)
				// forwarder returns error only when the call is rate limited. To
				// avoid a busy loop on such rate limiting events, we only attempt to make
				// the next forwarded call after this childCtx expires. Till then, we block
				// hoping for a local poller match
				select {
				case tm.taskC <- task:
					cancel()
					tm.emitDispatchLatency(task, false)
					return nil
				case <-childCtx.Done():
				case <-ctx.Done():
					cancel()
					return ctx.Err()
				case <-interruptCh:
					cancel()
					return errInterrupted
				}
				cancel()
				continue forLoop
			}
			cancel()
			// at this point, we forwarded the task to a parent partition which
			// in turn dispatched the task to a poller. Make sure we delete the
			// task from the database
			task.finish(nil)
			tm.emitDispatchLatency(task, true)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-reconsiderFwdTimerC:
			continue forLoop
		case <-interruptCh:
			return errInterrupted
		}
	}
}

func (tm *TaskMatcher) emitDispatchLatency(task *internalTask, forwarded bool) {
	if task.event.Data.CreateTime == nil {
		return // should not happen but for safety
	}

	metrics.TaskDispatchLatencyPerTaskQueue.With(tm.metricsHandler).Record(
		time.Since(timestamp.TimeValue(task.event.Data.CreateTime)),
		metrics.StringTag("source", task.source.String()),
		metrics.StringTag("forwarded", strconv.FormatBool(forwarded)),
	)
}

// Poll blocks until a task is found or context deadline is exceeded
// On success, the returned task could be a query task or a regular task
// Returns errNoTasks when context deadline is exceeded
func (tm *TaskMatcher) Poll(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, error) {
	task, _, err := tm.poll(ctx, pollMetadata, false)
	return task, err
}

// PollForQuery blocks until a *query* task is found or context deadline is exceeded
// Returns errNoTasks when context deadline is exceeded
func (tm *TaskMatcher) PollForQuery(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, error) {
	task, _, err := tm.poll(ctx, pollMetadata, true)
	return task, err
}

// UpdateRatelimit updates the task dispatch rate
func (tm *TaskMatcher) UpdateRatelimit(rpsPtr *float64) {
	if rpsPtr == nil {
		return
	}

	rps := *rpsPtr
	nPartitions := float64(tm.numPartitions())
	if nPartitions > 0 {
		// divide the rate equally across all partitions
		rps = rps / nPartitions
	}
	burst := int(math.Ceil(rps))

	minTaskThrottlingBurstSize := tm.config.MinTaskThrottlingBurstSize()
	if burst < minTaskThrottlingBurstSize {
		burst = minTaskThrottlingBurstSize
	}

	tm.dynamicRateBurst.SetRPS(rps)
	tm.dynamicRateBurst.SetBurst(burst)
	tm.forceRefreshRateOnce.Do(func() {
		// Dynamic rate limiter only refresh its rate every 1m. Before that initial 1m interval, it uses default rate
		// which is 10K and is too large in most cases. We need to force refresh for the first time this rate is set
		// by poller. Only need to do that once. If the rate change later, it will be refresh in 1m.
		tm.dynamicRateLimiter.Refresh()
	})
}

// Rate returns the current rate at which tasks are dispatched
func (tm *TaskMatcher) Rate() float64 {
	return tm.rateLimiter.Rate()
}

func (tm *TaskMatcher) poll(
	ctx context.Context, pollMetadata *pollMetadata, queryOnly bool,
) (task *internalTask, forwardedPoll bool, err error) {
	taskC, queryTaskC := tm.taskC, tm.queryTaskC
	if queryOnly {
		taskC = nil
	}

	start := time.Now()
	tm.lastPoller.Store(start.UnixNano())

	defer func() {
		if pollMetadata.forwardedFrom == "" {
			// Only recording for original polls
			metrics.PollLatencyPerTaskQueue.With(tm.metricsHandler).Record(
				time.Since(start), metrics.StringTag("forwarded", strconv.FormatBool(forwardedPoll)))
		}

		if err == nil {
			tm.emitForwardedSourceStats(task.isForwarded(), pollMetadata.forwardedFrom, forwardedPoll)
		}
	}()

	// We want to effectively do a prioritized select, but Go select is random
	// if multiple cases are ready, so split into multiple selects.
	// The priority order is:
	// 1. ctx.Done or tm.closeC
	// 2. taskC and queryTaskC
	// 3. forwarding
	// 4. block looking locally for remainder of context lifetime
	// To correctly handle priorities and allow any case to succeed, all select
	// statements except for the last one must be non-blocking, and the last one
	// must include all the previous cases.

	// 1. ctx.Done
	select {
	case <-ctx.Done():
		metrics.PollTimeoutPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
		return nil, false, errNoTasks
	case <-tm.closeC:
		return nil, false, errNoTasks
	default:
	}

	// 2. taskC and queryTaskC
	select {
	case task := <-taskC:
		if task.responseC != nil {
			metrics.PollSuccessWithSyncPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
		}
		metrics.PollSuccessPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
		return task, false, nil
	case task := <-queryTaskC:
		metrics.PollSuccessWithSyncPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
		metrics.PollSuccessPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
		return task, false, nil
	default:
	}

	if tm.isBacklogNegligible() {
		// 3. forwarding (and all other clauses repeated)
		// We don't forward pollers if there is a non-negligible backlog in this partition.
		select {
		case <-ctx.Done():
			metrics.PollTimeoutPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
			return nil, false, errNoTasks
		case <-tm.closeC:
			return nil, false, errNoTasks
		case task := <-taskC:
			if task.responseC != nil {
				metrics.PollSuccessWithSyncPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
			}
			metrics.PollSuccessPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
			return task, false, nil
		case task := <-queryTaskC:
			metrics.PollSuccessWithSyncPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
			metrics.PollSuccessPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
			return task, false, nil
		case token := <-tm.fwdrPollReqTokenC():
			// Arrange to cancel this request if closeC is closed
			fwdCtx, cancel := contextWithCancelOnChannelClose(ctx, tm.closeC)
			task, err := tm.fwdr.ForwardPoll(fwdCtx, pollMetadata)
			cancel()
			token.release()
			if err == nil {
				return task, true, nil
			}
		}
	}

	// 4. blocking local poll
	select {
	case <-ctx.Done():
		metrics.PollTimeoutPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
		return nil, false, errNoTasks
	case <-tm.closeC:
		return nil, false, errNoTasks
	case task := <-taskC:
		if task.responseC != nil {
			metrics.PollSuccessWithSyncPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
		}
		metrics.PollSuccessPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
		return task, false, nil
	case task := <-queryTaskC:
		metrics.PollSuccessWithSyncPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
		metrics.PollSuccessPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
		return task, false, nil
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

// isBacklogNegligible returns true of the age of backlog is less than the threshold. Note that this relies on
// MustOffer being called when there is a backlog, otherwise we'd not know.
func (tm *TaskMatcher) isBacklogNegligible() bool {
	return tm.getBacklogAge() < tm.config.BacklogNegligibleAge()
}

func (tm *TaskMatcher) registerBacklogTask(task *internalTask) {
	if task.event.Data.CreateTime == nil {
		return // should not happen but for safety
	}

	tm.backlogTasksLock.Lock()
	defer tm.backlogTasksLock.Unlock()

	ts := timestamp.TimeValue(task.event.Data.CreateTime).UnixNano()
	tm.backlogTasksCreateTime[ts] += 1
}

func (tm *TaskMatcher) unregisterBacklogTask(task *internalTask) {
	if task.event.Data.CreateTime == nil {
		return // should not happen but for safety
	}

	tm.backlogTasksLock.Lock()
	defer tm.backlogTasksLock.Unlock()

	ts := timestamp.TimeValue(task.event.Data.CreateTime).UnixNano()
	counter := tm.backlogTasksCreateTime[ts]
	if counter == 1 {
		delete(tm.backlogTasksCreateTime, ts)
	} else {
		tm.backlogTasksCreateTime[ts] = counter - 1
	}
}

func (tm *TaskMatcher) getBacklogAge() time.Duration {
	tm.backlogTasksLock.Lock()
	defer tm.backlogTasksLock.Unlock()

	if len(tm.backlogTasksCreateTime) == 0 {
		return emptyBacklogAge
	}

	oldest := int64(math.MaxInt64)
	for createTime := range tm.backlogTasksCreateTime {
		if createTime < oldest {
			oldest = createTime
		}
	}

	return time.Since(time.Unix(0, oldest))
}

func (tm *TaskMatcher) emitForwardedSourceStats(
	isTaskForwarded bool,
	pollForwardedSource string,
	forwardedPoll bool,
) {
	if forwardedPoll {
		// This means we forwarded the poll to another partition. Skipping this to prevent duplicate emits.
		// Only the partition in which the match happened should emit this metric.
		return
	}

	isPollForwarded := len(pollForwardedSource) > 0
	switch {
	case isTaskForwarded && isPollForwarded:
		metrics.RemoteToRemoteMatchPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
	case isTaskForwarded:
		metrics.RemoteToLocalMatchPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
	case isPollForwarded:
		metrics.LocalToRemoteMatchPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
	default:
		metrics.LocalToLocalMatchPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
	}
}

func (tm *TaskMatcher) timeSinceLastPoll() time.Duration {
	return time.Since(time.Unix(0, tm.lastPoller.Load()))
}

// contextWithCancelOnChannelClose returns a child Context and CancelFunc just like
// context.WithCancel, but additionally propagates cancellation from another channel (besides
// the parent's cancellation channel).
func contextWithCancelOnChannelClose(parent context.Context, closeC <-chan struct{}) (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(parent)
	go func() {
		select {
		case <-closeC:
			cancel()
		case <-ctx.Done():
		}
	}()
	return ctx, cancel
}
