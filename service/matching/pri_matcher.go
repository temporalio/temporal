package matching

import (
	"context"
	"strconv"
	"time"

	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/softassert"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/util"
)

// priTaskMatcher matches a task producer with a task consumer
// Producers are usually rpc calls from history or taskReader
// that drains backlog from db. Consumers are the task queue pollers
type priTaskMatcher struct {
	config *taskQueueConfig

	// holds waiting polls and tasks
	data matcherData

	// Background context used for forwarding tasks. Closed when task queue is closed.
	tqCtx context.Context

	partition        tqid.Partition
	fwdr             *priForwarder
	validator        taskValidator
	rateLimitManager *rateLimitManager
	metricsHandler   metrics.Handler // namespace metric scope
	logger           log.Logger
	numPartitions    func() int // number of task queue partitions
	markAlive        func()     // function to mark the physical task queue alive
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

var (
	// TODO(pri): old matcher cleanup, move to here
	// errNoRecentPoller = status.Error(codes.FailedPrecondition, "no poller seen for task queue recently, worker may be down")

	// This is a fake error used to force reprocessing of task redirection as used by versioning.
	// Situations where we do this:
	// - after validateTasksOnRoot maybe-validates a task (only local backlog)
	// - when userdata changes, on in-mem tasks (may be either sync or local backlog)
	// This must be an error type that taskReader will treat as transient and re-enqueue the task.
	errReprocessTask      = serviceerror.NewCanceled("reprocess task")
	errInternalMatchError = serviceerror.NewInternal("internal matcher error")
)

// newPriTaskMatcher returns a task matcher instance
func newPriTaskMatcher(
	tqCtx context.Context,
	config *taskQueueConfig,
	partition tqid.Partition,
	fwdr *priForwarder,
	validator taskValidator,
	logger log.Logger,
	metricsHandler metrics.Handler,
	rateLimitManager *rateLimitManager,
	markAlive func(),
) *priTaskMatcher {
	tm := &priTaskMatcher{
		config:           config,
		data:             newMatcherData(config, logger, clock.NewRealTimeSource(), fwdr != nil, rateLimitManager),
		tqCtx:            tqCtx,
		logger:           logger,
		metricsHandler:   metricsHandler,
		partition:        partition,
		fwdr:             fwdr,
		validator:        validator,
		rateLimitManager: rateLimitManager,
		numPartitions:    config.NumReadPartitions,
		markAlive:        markAlive,
	}

	return tm
}

func (tm *priTaskMatcher) Start() {
	policy := backoff.NewExponentialRetryPolicy(time.Second).
		WithMaximumInterval(tm.config.BacklogTaskForwardTimeout()).
		WithExpirationInterval(backoff.NoInterval)
	retrier := backoff.NewRetrier(policy, clock.NewRealTimeSource())
	lim := quotas.NewDefaultOutgoingRateLimiter(tm.config.ForwarderMaxRatePerSecond)

	if tm.fwdr == nil {
		// Root doesn't forward. But it does need something to validate tasks.
		go tm.validateTasksOnRoot(retrier)
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

func (tm *priTaskMatcher) Stop() {}

func (tm *priTaskMatcher) forwardTasks(lim quotas.RateLimiter, retrier backoff.Retrier) {
	ctxs := []context.Context{tm.tqCtx}
	poller := waitingPoller{isTaskForwarder: true}
	skipLimiter := false
	var err error
	for {
		if !skipLimiter && lim.Wait(tm.tqCtx) != nil {
			return
		}

		res := tm.data.EnqueuePollerAndWait(ctxs, &poller)
		if res.ctxErr != nil {
			return // task queue closing
		}
		if !softassert.That(tm.logger, res.task != nil, "expected a task from match") {
			continue
		}

		skipLimiter, err = tm.forwardTask(res.task)

		// backoff on resource exhausted errors
		if common.IsResourceExhausted(err) {
			util.InterruptibleSleep(tm.tqCtx, retrier.NextBackOff(err))
		} else {
			retrier.Reset()
		}
	}
}

func (tm *priTaskMatcher) forwardTask(task *internalTask) (bool, error) {
	var ctx context.Context
	var cancel context.CancelFunc
	if task.forwardCtx != nil {
		// Use sync match context if we have it (for deadline, headers, etc.)
		// TODO(pri): does it make sense to subtract 1s from the context deadline here?
		ctx = task.forwardCtx
	} else {
		// Task is from local backlog.

		// Before we forward, ask task validator. This will happen every BacklogTaskForwardTimeout
		// to the head of the backlog, which is what taskValidator expects.
		maybeValid := tm.validator.maybeValidate(task.event.AllocatedTaskInfo, tm.fwdr.partition.TaskType())
		if !maybeValid {
			task.finish(nil, false)
			var invalidTaskTag = getInvalidTaskTag(task)

			// consider this task expired while processing.
			tm.metricsHandler.Counter(metrics.ExpiredTasksPerTaskQueueCounter.Name()).Record(1, invalidTaskTag)

			// Stay alive as long as we're invalidating tasks
			tm.markAlive()

			return true, nil
		}

		// Add a timeout for forwarding.
		// Note that this does not block local match of other local backlog tasks.
		ctx, cancel = context.WithTimeout(tm.tqCtx, tm.config.BacklogTaskForwardTimeout())
		defer cancel()
	}

	if task.isQuery() {
		res, err := tm.fwdr.ForwardQueryTask(ctx, task)
		task.finishForward(res, err, true)
		return false, err
	}

	if task.isNexus() {
		res, err := tm.fwdr.ForwardNexusTask(ctx, task)
		task.finishForward(res, err, true)
		return false, err
	}

	// normal wf/activity task
	err := tm.fwdr.ForwardTask(ctx, task)
	task.finishForward(nil, err, true)

	return false, err
}

func (tm *priTaskMatcher) validateTasksOnRoot(retrier backoff.Retrier) {
	ctxs := []context.Context{tm.tqCtx}
	poller := &waitingPoller{isTaskForwarder: true, isTaskValidator: true}
	for {
		res := tm.data.EnqueuePollerAndWait(ctxs, poller)
		if res.ctxErr != nil {
			return // task queue closing
		}
		if !softassert.That(tm.logger, res.task != nil, "expected a task from match") {
			continue
		}

		task := res.task
		if !softassert.That(tm.logger, task.forwardCtx == nil, "expected non-forwarded task") ||
			!softassert.That(tm.logger, !task.isSyncMatchTask(), "expected non-sync match task") ||
			!softassert.That(tm.logger, task.source == enumsspb.TASK_SOURCE_DB_BACKLOG, "expected backlog task") {
			continue
		}

		maybeValid := tm.validator == nil || tm.validator.maybeValidate(task.event.AllocatedTaskInfo, tm.partition.TaskType())
		if !maybeValid {
			// We found an invalid one, complete it and go back for another immediately.
			task.finish(nil, false)
			var invalidStageTag = getInvalidTaskTag(task)
			tm.metricsHandler.Counter(metrics.ExpiredTasksPerTaskQueueCounter.Name()).Record(1, invalidStageTag)

			// Stay alive as long as we're invalidating tasks
			tm.markAlive()

			retrier.Reset()
		} else {
			// Task was valid, put it back and slow down checking.
			task.finish(errReprocessTask, true)
			// retrier's max interval is backlogTaskForwardTimeout, so for just valid tasks,
			// this loop will essentially be limited to that interval.
			util.InterruptibleSleep(tm.tqCtx, retrier.NextBackOff(nil))
		}
	}
}

func (tm *priTaskMatcher) forwardPolls() {
	forwarderTask := newPollForwarderTask()
	ctxs := []context.Context{tm.tqCtx}
	for {
		res := tm.data.EnqueueTaskAndWait(ctxs, forwarderTask)
		if res.ctxErr != nil {
			return // task queue closing
		}
		if !softassert.That(tm.logger, res.poller != nil, "expected a poller from match") {
			continue
		}

		poller := res.poller
		// We need to use the real source poller context since it has the poller id and
		// identity, plus the right deadline.
		task, err := tm.fwdr.ForwardPoll(poller.forwardCtx, poller.pollMetadata)
		if err == nil {
			tm.data.FinishMatchAfterPollForward(poller, task)
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
func (tm *priTaskMatcher) Offer(ctx context.Context, task *internalTask) (bool, error) {
	finish := func() (bool, error) {
		res, ok := task.getResponse()
		if !softassert.That(tm.logger, ok, "expected a sync match task") {
			return false, nil
		}
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
	if canMatch, gotMatch := tm.data.MatchTaskImmediately(task); gotMatch {
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

	res := tm.data.EnqueueTaskAndWait([]context.Context{ctx, tm.tqCtx}, task)
	if res.ctxErr != nil {
		return false, res.ctxErr
	}
	if !softassert.That(tm.logger, res.poller != nil, "expeced poller from match") {
		return false, nil
	}

	return finish()
}

func (tm *priTaskMatcher) syncOfferTask(
	ctx context.Context,
	task *internalTask,
	returnNoPollerErr bool,
) (any, error) {
	ctxs := []context.Context{ctx, tm.tqCtx}

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
	if !softassert.That(tm.logger, res.poller != nil, "expected poller from match") {
		return nil, errInternalMatchError
	}
	response, ok := task.getResponse()
	if !softassert.That(tm.logger, ok, "expected a sync match task") {
		return nil, errInternalMatchError
	}
	// Note: if task was not forwarded, this will just be the zero value and nil.
	// That's intended: the query/nexus handler in matchingEngine will wait for the real
	// result separately.
	return response.forwardRes, response.forwardErr
}

// OfferQuery will either match task to local poller or will forward query task.
// Local match is always attempted before forwarding is attempted. If local match occurs
// response and error are both nil, if forwarding occurs then response or error is returned.
func (tm *priTaskMatcher) OfferQuery(ctx context.Context, task *internalTask) (*matchingservice.QueryWorkflowResponse, error) {
	res, err := tm.syncOfferTask(ctx, task, true)
	if res != nil { // note res may be non-nil "any" containing nil pointer
		return res.(*matchingservice.QueryWorkflowResponse), err // nolint:revive
	}
	return nil, err
}

// OfferNexusTask either matchs a task to a local poller or forwards it if no local pollers available.
// Local match is always attempted before forwarding. If local match occurs response and error are both nil, if
// forwarding occurs then response or error is returned.
func (tm *priTaskMatcher) OfferNexusTask(ctx context.Context, task *internalTask) (*matchingservice.DispatchNexusTaskResponse, error) {
	res, err := tm.syncOfferTask(ctx, task, true)
	if res != nil { // note res may be non-nil "any" containing nil pointer
		return res.(*matchingservice.DispatchNexusTaskResponse), err // nolint:revive
	}
	return nil, err
}

func (tm *priTaskMatcher) AddTask(task *internalTask) {
	if !task.setRemoveFunc(func() { tm.data.RemoveTask(task) }) {
		return // handle race where task is evicted from reader before being added
	}
	tm.data.EnqueueTaskNoWait(task)
}

func (tm *priTaskMatcher) emitDispatchLatency(task *internalTask, forwarded bool) {
	if task.event.Data.CreateTime == nil {
		return // should not happen but for safety
	}

	metrics.TaskDispatchLatencyPerTaskQueue.With(tm.metricsHandler).Record(
		time.Since(timestamp.TimeValue(task.event.Data.CreateTime)),
		metrics.StringTag("source", task.source.String()),
		metrics.StringTag("forwarded", strconv.FormatBool(forwarded)),
		metrics.MatchingTaskPriorityTag(task.getPriority().GetPriorityKey()),
	)
}

// Poll blocks until a task is found or context deadline is exceeded
// On success, the returned task could be a query task or a regular task
// Returns errNoTasks when context deadline is exceeded
func (tm *priTaskMatcher) Poll(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, error) {
	return tm.poll(ctx, pollMetadata, false)
}

// PollForQuery blocks until a *query* task is found or context deadline is exceeded
// Returns errNoTasks when context deadline is exceeded
func (tm *priTaskMatcher) PollForQuery(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, error) {
	return tm.poll(ctx, pollMetadata, true)
}

func (tm *priTaskMatcher) ReprocessAllTasks() {
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

func (tm *priTaskMatcher) poll(
	ctx context.Context, pollMetadata *pollMetadata, queryOnly bool,
) (*internalTask, error) {
	start := time.Now()
	pollWasForwarded := false
	var priority int32

	defer func() {
		// TODO(pri): can we consolidate all the metrics code below?
		if pollMetadata.forwardedFrom == "" {
			// Only recording for original polls (i.e. on child if forwarded)
			metrics.PollLatencyPerTaskQueue.With(tm.metricsHandler).Record(
				time.Since(start),
				metrics.StringTag("forwarded", strconv.FormatBool(pollWasForwarded)),
				metrics.MatchingTaskPriorityTag(priority),
			)
		}
	}()

	poller := &waitingPoller{
		startTime:    start,
		queryOnly:    queryOnly,
		forwardCtx:   ctx,
		pollMetadata: pollMetadata,
	}

	var res *matchResult
	if pollMetadata.conditions.GetNoWait() {
		res = tm.data.MatchPollerImmediately(poller)
	} else {
		ctxs := []context.Context{ctx, tm.tqCtx}
		res = tm.data.EnqueuePollerAndWait(ctxs, poller)
	}

	if res == nil {
		return nil, errNoTasks // only possible for MatchPollerImmediately
	} else if res.ctxErr != nil {
		if res.ctxErrIdx == 0 {
			metrics.PollTimeoutPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
		}
		return nil, errNoTasks
	}

	if !softassert.That(tm.logger, res.task != nil, "expected task from match") {
		return nil, errInternalMatchError
	}

	task := res.task
	pollWasForwarded = task.isStarted() // true if this poll was forwarded _from_ this matcher
	priority = task.getPriority().GetPriorityKey()

	if !pollWasForwarded {
		// Only record these metrics on the parent for forwarded polls
		if !task.isQuery() {
			if task.isSyncMatchTask() {
				metrics.PollSuccessWithSyncPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
			}
			metrics.PollSuccessPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
		} else {
			metrics.PollSuccessWithSyncPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
			metrics.PollSuccessPerTaskQueueCounter.With(tm.metricsHandler).Record(1)
		}
		tm.emitForwardedSourceStats(task.isForwarded(), pollMetadata.forwardedFrom)
	}

	return task, nil
}

func (tm *priTaskMatcher) isForwardingAllowed() bool {
	return tm.fwdr != nil
}

func (tm *priTaskMatcher) emitForwardedSourceStats(
	isTaskForwarded bool,
	pollForwardedSource string,
) {
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

func getInvalidTaskTag(task *internalTask) metrics.Tag {
	if IsTaskExpired(task.event.AllocatedTaskInfo) {
		return metrics.TaskExpireStageMemoryTag
	}
	return metrics.TaskInvalidTag
}

func (p *waitingPoller) minPriority() priorityKey {
	if p.pollMetadata == nil || p.pollMetadata.conditions == nil {
		return 0
	}
	return priorityKey(p.pollMetadata.conditions.MinPriority)
}
