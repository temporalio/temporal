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
	"strconv"
	"sync"
	"time"

	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/util"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TaskMatcher matches a task producer with a task consumer
// Producers are usually rpc calls from history or taskReader
// that drains backlog from db. Consumers are the task queue pollers
type TaskMatcher struct {
	config *taskQueueConfig

	// holds waiting polls and tasks
	data matcherData

	// Background context used for forwarding tasks. Closed when task queue is closed.
	matcherCtx       context.Context
	matcherCtxCancel context.CancelFunc

	// dynamicRate is the dynamic rate & burst for rate limiter
	dynamicRateBurst quotas.MutableRateBurst
	// dynamicRateLimiter is the dynamic rate limiter that can be used to force refresh on new rates.
	dynamicRateLimiter *quotas.DynamicRateLimiterImpl
	// forceRefreshRateOnce is used to force refresh rate limit for first time
	forceRefreshRateOnce sync.Once

	partition      tqid.Partition
	fwdr           *Forwarder
	validator      taskValidator
	metricsHandler metrics.Handler // namespace metric scope
	numPartitions  func() int      // number of task queue partitions
}

type waitingPoller struct {
	waitableMatchResult
	startTime       time.Time
	forwardCtx      context.Context // non-nil iff poll can be forwarded
	pollMetadata    *pollMetadata   // non-nil iff poll can be forwarded
	queryOnly       bool            // if true, poller can be given only query task, otherwise any task
	isTaskForwarder bool
	isTaskValidator bool
}

type matchResult struct {
	task      *internalTask
	poller    *waitingPoller
	ctxErr    error // set if context timed out/canceled or reprocess task
	ctxErrIdx int   // index of context that closed first
}

const (
	defaultTaskDispatchRPS    = 100000.0
	defaultTaskDispatchRPSTTL = time.Minute

	// TODO(pri): make dynamic config
	backlogTaskForwardTimeout = 60 * time.Second
)

var (
	errNoRecentPoller = status.Error(codes.FailedPrecondition, "no poller seen for task queue recently, worker may be down")

	// This is a fake error used to force reprocessing of task redirection as used by versioning.
	// Situations where we do this:
	// - after validateTasksOnRoot maybe-validates a task (only local backlog)
	// - when userdata changes, on in-mem tasks (may be either sync or local backlog)
	// This must be an error type that taskReader will treat as transient and re-enqueue the task.
	errReprocessTask = serviceerror.NewCanceled("reprocess task")
)

// newTaskMatcher returns a task matcher instance. The returned instance can be used by task producers and consumers to
// find a match. Both sync matches and non-sync matches should use this implementation
func newTaskMatcher(config *taskQueueConfig, partition tqid.Partition, fwdr *Forwarder, validator taskValidator, metricsHandler metrics.Handler) *TaskMatcher {
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

	matcherCtx := headers.SetCallerInfo(context.Background(), config.CallerInfo)
	matcherCtx, matcherCtxCancel := context.WithCancel(matcherCtx)

	return &TaskMatcher{
		config:             config,
		data:               newMatcherData(config, limiter),
		dynamicRateBurst:   dynamicRateBurst,
		dynamicRateLimiter: dynamicRateLimiter,
		metricsHandler:     metricsHandler,
		partition:          partition,
		fwdr:               fwdr,
		validator:          validator,
		matcherCtx:         matcherCtx,
		matcherCtxCancel:   matcherCtxCancel,
		numPartitions:      config.NumReadPartitions,
	}
}

func (tm *TaskMatcher) Start() {
	policy := backoff.NewExponentialRetryPolicy(time.Second).
		WithMaximumInterval(backlogTaskForwardTimeout).
		WithExpirationInterval(backoff.NoInterval)
	retrier := backoff.NewRetrier(policy, clock.NewRealTimeSource())
	lim := quotas.NewDefaultOutgoingRateLimiter(tm.config.ForwarderMaxRatePerSecond)

	if tm.fwdr == nil {
		// Root doesn't forward. But it does need something to validate tasks.
		go tm.validateTasksOnRoot(lim, retrier)
		return
	}

	// Child partitions:
	for range tm.config.ForwarderMaxOutstandingTasks() {
		go tm.forwardTasks(lim, retrier)
	}
	for range tm.config.ForwarderMaxOutstandingPolls() {
		go tm.forwardPolls()
	}
}

func (tm *TaskMatcher) Stop() {
	tm.matcherCtxCancel()
}

func (tm *TaskMatcher) forwardTasks(lim quotas.RateLimiter, retrier backoff.Retrier) {
	ctxs := []context.Context{tm.matcherCtx}
	poller := waitingPoller{isTaskForwarder: true}
	for {
		if lim.Wait(tm.matcherCtx) != nil {
			return
		}

		res := tm.data.EnqueuePollerAndWait(ctxs, &poller)
		if res.ctxErr != nil {
			return // task queue closing
		}
		bugIf(res.task == nil, "bug: bad match result in forwardTasks")

		err := tm.forwardTask(res.task)

		// backoff on resource exhausted errors
		if common.IsResourceExhausted(err) {
			util.InterruptibleSleep(tm.matcherCtx, retrier.NextBackOff(err))
		} else {
			retrier.Reset()
		}
	}
}

func (tm *TaskMatcher) forwardTask(task *internalTask) error {
	var ctx context.Context
	var cancel context.CancelFunc
	if task.forwardCtx != nil {
		// Use sync match context if we have it (for deadline, headers, etc.)
		// TODO(pri): does it make sense to subtract 1s from the context deadline here?
		ctx = task.forwardCtx
	} else {
		// Task is from local backlog.

		// Before we forward, ask task validator. This will happen every backlogTaskForwardTimeout
		// to the head of the backlog, which is what taskValidator expects.
		maybeValid := tm.validator.maybeValidate(task.event.AllocatedTaskInfo, tm.fwdr.partition.TaskType())
		if !maybeValid {
			task.finish(nil, false)
			tm.metricsHandler.Counter(metrics.ExpiredTasksPerTaskQueueCounter.Name()).Record(1)
			return nil
		}

		// Add a timeout for forwarding.
		// Note that this does not block local match of other local backlog tasks.
		ctx, cancel = context.WithTimeout(tm.matcherCtx, backlogTaskForwardTimeout)
		defer cancel()
	}

	if task.isQuery() {
		res, err := tm.fwdr.ForwardQueryTask(ctx, task)
		task.finishForward(res, err, true)
		return err
	}

	if task.isNexus() {
		res, err := tm.fwdr.ForwardNexusTask(ctx, task)
		task.finishForward(res, err, true)
		return err
	}

	// normal wf/activity task
	err := tm.fwdr.ForwardTask(ctx, task)
	task.finishForward(nil, err, true)

	return err
}

func (tm *TaskMatcher) validateTasksOnRoot(lim quotas.RateLimiter, retrier backoff.Retrier) {
	ctxs := []context.Context{tm.matcherCtx}
	poller := &waitingPoller{isTaskForwarder: true, isTaskValidator: true}
	for {
		if lim.Wait(tm.matcherCtx) != nil {
			return
		}

		res := tm.data.EnqueuePollerAndWait(ctxs, poller)
		if res.ctxErr != nil {
			return // task queue closing
		}
		bugIf(res.task == nil, "bug: bad match result in validateTasksOnRoot")

		task := res.task
		bugIf(task.forwardCtx != nil || task.isSyncMatchTask() || task.source != enumsspb.TASK_SOURCE_DB_BACKLOG,
			"bug: validator got a sync task")
		maybeValid := tm.validator == nil || tm.validator.maybeValidate(task.event.AllocatedTaskInfo, tm.partition.TaskType())
		if !maybeValid {
			// We found an invalid one, complete it and go back for another immediately.
			task.finish(nil, false)
			tm.metricsHandler.Counter(metrics.ExpiredTasksPerTaskQueueCounter.Name()).Record(1)
			retrier.Reset()
		} else {
			// Task was valid, put it back and slow down checking.
			task.finish(errReprocessTask, true)
			// retrier's max interval is backlogTaskForwardTimeout, so for just valid tasks,
			// this loop will essentially be limited to that interval.
			util.InterruptibleSleep(tm.matcherCtx, retrier.NextBackOff(nil))
		}
	}
}

func (tm *TaskMatcher) forwardPolls() {
	forwarderTask := &internalTask{isPollForwarder: true}
	ctxs := []context.Context{tm.matcherCtx}
	for {
		res := tm.data.EnqueueTaskAndWait(ctxs, forwarderTask)
		if res.ctxErr != nil {
			return // task queue closing
		}
		bugIf(res.poller == nil, "bug: bad match result in forwardPolls")

		poller := res.poller
		// We need to use the real source poller context since it has the poller id and
		// identity, plus the right deadline.
		task, err := tm.fwdr.ForwardPoll(poller.forwardCtx, poller.pollMetadata)
		if err == nil {
			tm.data.finishMatchAfterPollForward(poller, task)
		} else {
			// Re-enqueue to let it match again, if it hasn't gotten a context timeout already.
			poller.forwardCtx = nil // disable forwarding next time
			tm.data.ReenqueuePollerIfNotMatched(poller)
		}
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
//   - ratelimit is exceeded (does not apply to query task)
//   - context deadline is exceeded
//   - task is matched and consumer returns error in response channel
func (tm *TaskMatcher) Offer(ctx context.Context, task *internalTask) (bool, error) {
	finish := func() (bool, error) {
		res, ok := task.getResponse()
		bugIf(!ok, "Offer must be given a sync match task")
		if res.forwarded {
			if res.forwardErr == nil {
				// task was remotely sync matched on the parent partition
				tm.emitDispatchLatency(task, true)
				return true, nil
			}
			return false, nil // forward error, give up here
		}
		// TODO(pri): can we just always do this on the parent and simplify this to:
		// if res.startErr == nil { tm.emitDispatchLatency(task, task.isForwarded) }
		// and get rid of the call above so there's only one?
		if res.startErr == nil && !task.isForwarded() {
			tm.emitDispatchLatency(task, false)
		}
		return true, res.startErr
	}

	// Fast path if we have a waiting poller (or forwarder).
	// Forwarding happens here if we match with the task forwarding poller.
	task.forwardCtx = ctx
	if canMatch, gotMatch := tm.data.MatchNextPoller(task); gotMatch {
		return finish()
	} else if !canMatch {
		return false, nil
	}

	// We only block if we are the root and the task is forwarded from a backlog.
	// Otherwise, stop here.
	if tm.isForwardingAllowed() ||
		task.source != enumsspb.TASK_SOURCE_DB_BACKLOG ||
		!task.isForwarded() {
		return false, nil
	}

	res := tm.data.EnqueueTaskAndWait([]context.Context{ctx, tm.matcherCtx}, task)

	if res.ctxErr != nil {
		return false, res.ctxErr
	}
	bugIf(res.poller == nil, "bug: bad match result in Offer")
	return finish()
}

func (tm *TaskMatcher) syncOfferTask(
	ctx context.Context,
	task *internalTask,
	returnNoPollerErr bool,
) (any, error) {
	ctxs := []context.Context{ctx, tm.matcherCtx}

	if returnNoPollerErr {
		if deadline, ok := ctx.Deadline(); ok && tm.fwdr == nil {
			// Reserving 1sec to customize the timeout error if user is querying a workflow
			// without having started the workers.
			noPollerDeadline := deadline.Add(-returnEmptyTaskTimeBudget)
			noPollerCtx, cancel := context.WithDeadline(context.Background(), noPollerDeadline)
			defer cancel()
			ctxs = append(ctxs, noPollerCtx)
		}
	}

	task.forwardCtx = ctx
again:
	res := tm.data.EnqueueTaskAndWait(ctxs, task)

	if res.ctxErr != nil {
		if res.ctxErrIdx == 2 {
			// Index 2 is the noPollerCtx. Only error if there has not been a recent poller.
			// Otherwise, let it wait for the remaining time hopping for a match, or ultimately
			// returning the default CDE error.
			if tm.data.TimeSinceLastPoll() > tm.config.QueryPollerUnavailableWindow() {
				return nil, errNoRecentPoller
			}
			ctxs = ctxs[:2] // remove noPollerCtx otherwise we'll fail immediately again
			goto again
		}
		return nil, res.ctxErr
	}
	bugIf(res.poller == nil, "bug: bad match result in syncOfferTask")
	response, ok := task.getResponse()
	bugIf(!ok, "OfferQuery/OfferNexusTask must be given a sync match task")
	// Note: if task was not forwarded, this will just be the zero value and nil.
	// That's intended: the query/nexus handler in matchingEngine will wait for the real
	// result separately.
	return response.forwardRes, response.forwardErr
}

// OfferQuery will either match task to local poller or will forward query task.
// Local match is always attempted before forwarding is attempted. If local match occurs
// response and error are both nil, if forwarding occurs then response or error is returned.
func (tm *TaskMatcher) OfferQuery(ctx context.Context, task *internalTask) (*matchingservice.QueryWorkflowResponse, error) {
	res, err := tm.syncOfferTask(ctx, task, true)
	if res != nil { // note res may be non-nil "any" containing nil pointer
		return res.(*matchingservice.QueryWorkflowResponse), err
	}
	return nil, err
}

// OfferNexusTask either matchs a task to a local poller or forwards it if no local pollers available.
// Local match is always attempted before forwarding. If local match occurs response and error are both nil, if
// forwarding occurs then response or error is returned.
func (tm *TaskMatcher) OfferNexusTask(ctx context.Context, task *internalTask) (*matchingservice.DispatchNexusTaskResponse, error) {
	res, err := tm.syncOfferTask(ctx, task, true)
	if res != nil { // note res may be non-nil "any" containing nil pointer
		return res.(*matchingservice.DispatchNexusTaskResponse), err
	}
	return nil, err
}

func (tm *TaskMatcher) AddTask(task *internalTask) {
	tm.data.EnqueueTaskNoWait(task)
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
	return tm.poll(ctx, pollMetadata, false)
}

// PollForQuery blocks until a *query* task is found or context deadline is exceeded
// Returns errNoTasks when context deadline is exceeded
func (tm *TaskMatcher) PollForQuery(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, error) {
	return tm.poll(ctx, pollMetadata, true)
}

func (tm *TaskMatcher) ReprocessAllTasks() {
	tasks := tm.data.ReprocessTasks(func(task *internalTask) (shouldRemove bool) {
		// TODO(pri): do we have to reprocess _all_ backlog tasks or can we determine
		// somehow which are potentially redirected?
		return true
	})
	// ReprocessTasks will have woken sync tasks, but for backlog we also need to call finish.
	for _, task := range tasks {
		if !task.isSyncMatchTask() {
			task.finish(errReprocessTask, true)
		}
	}
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
	return tm.data.rateLimiter.Rate()
}

func (tm *TaskMatcher) poll(
	ctx context.Context, pollMetadata *pollMetadata, queryOnly bool,
) (*internalTask, error) {
	start := time.Now()
	pollWasForwarded := false

	defer func() {
		// TODO(pri): can we consolidate all the metrics code below?
		if pollMetadata.forwardedFrom == "" {
			// Only recording for original polls
			metrics.PollLatencyPerTaskQueue.With(tm.metricsHandler).Record(
				time.Since(start), metrics.StringTag("forwarded", strconv.FormatBool(pollWasForwarded)))
		}
	}()

	ctxs := []context.Context{ctx, tm.matcherCtx}
	poller := &waitingPoller{
		startTime:    start,
		queryOnly:    queryOnly,
		forwardCtx:   ctx,
		pollMetadata: pollMetadata,
	}
	res := tm.data.EnqueuePollerAndWait(ctxs, poller)

	if res.ctxErr != nil {
		if res.ctxErrIdx == 0 {
			metrics.PollTimeoutPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
		}
		return nil, errNoTasks
	}
	bugIf(res.task == nil, "bug: bad match result in poll")

	task := res.task
	pollWasForwarded = task.isStarted()

	if !task.isQuery() {
		if task.isSyncMatchTask() {
			metrics.PollSuccessWithSyncPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
		}
		metrics.PollSuccessPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
	} else {
		metrics.PollSuccessWithSyncPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
		metrics.PollSuccessPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
	}
	tm.emitForwardedSourceStats(task.isForwarded(), pollMetadata.forwardedFrom, pollWasForwarded)

	return task, nil
}

func (tm *TaskMatcher) isForwardingAllowed() bool {
	return tm.fwdr != nil
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
