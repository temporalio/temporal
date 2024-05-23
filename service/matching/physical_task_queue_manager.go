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
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/types/known/durationpb"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"

	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
)

const (
	// Time budget for empty task to propagate through the function stack and be returned to
	// pollForActivityTask or pollForWorkflowTask handler.
	returnEmptyTaskTimeBudget = time.Second

	// Fake Task ID to wrap a task for syncmatch
	syncMatchTaskId = -137

	ioTimeout = 5 * time.Second * debug.TimeoutMultiplier

	// Threshold for counting a AddTask call as a no recent poller call
	noPollerThreshold = time.Minute * 2

	// denotes the duration of each mini-interval in the circularTaskBuffer
	intervalSize = 5
	// denotes the total duration which shall be used to calculate the rate of tasks added/dispatched
	totalIntervalSize = 30
)

type (
	addTaskParams struct {
		taskInfo      *persistencespb.TaskInfo
		source        enumsspb.TaskSource
		forwardedFrom string
	}

	physicalTaskQueueManager interface {
		Start()
		Stop()
		WaitUntilInitialized(context.Context) error
		// AddTask adds a task to the task queue. This method will first attempt a synchronous
		// match with a poller. When that fails, task will be written to database and later
		// asynchronously matched with a poller
		AddTask(ctx context.Context, params addTaskParams) (syncMatch bool, err error)
		// PollTask blocks waiting for a task Returns error when context deadline is exceeded
		// maxDispatchPerSecond is the max rate at which tasks are allowed to be dispatched
		// from this task queue to pollers
		PollTask(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, error)
		// MarkAlive updates the liveness timer to keep this physicalTaskQueueManager alive.
		MarkAlive()
		// SpoolTask spools a task to persistence to be matched asynchronously when a poller is available.
		SpoolTask(params addTaskParams) error
		ProcessSpooledTask(ctx context.Context, task *internalTask) error
		// DispatchSpooledTask dispatches a task to a poller. When there are no pollers to pick
		// up the task, this method will return error. Task will not be persisted to db
		DispatchSpooledTask(ctx context.Context, task *internalTask, userDataChanged <-chan struct{}) error
		// DispatchQueryTask will dispatch query to local or remote poller. If forwarded then result or error is returned,
		// if dispatched to local poller then nil and nil is returned.
		DispatchQueryTask(ctx context.Context, taskId string, request *matchingservice.QueryWorkflowRequest) (*matchingservice.QueryWorkflowResponse, error)
		// DispatchNexusTask dispatches a nexus task to a local or remote poller. If forwarded then result or
		// error is returned, if dispatched to local poller then nil and nil is returned.
		DispatchNexusTask(ctx context.Context, taskId string, request *matchingservice.DispatchNexusTaskRequest) (*matchingservice.DispatchNexusTaskResponse, error)
		UpdatePollerInfo(pollerIdentity, *pollMetadata)
		GetAllPollerInfo() []*taskqueuepb.PollerInfo
		HasPollerAfter(accessTime time.Time) bool
		// LegacyDescribeTaskQueue returns pollers info and legacy TaskQueueStatus for this physical queue
		LegacyDescribeTaskQueue(includeTaskQueueStatus bool) *matchingservice.DescribeTaskQueueResponse
		GetBacklogInfo(ctx context.Context) (*taskqueuepb.BacklogInfo, error)
		UnloadFromPartitionManager()
		String() string
		QueueKey() *PhysicalTaskQueueKey
		Matcher() *TaskMatcher
	}

	// physicalTaskQueueManagerImpl manages a single DB-level (aka physical) task queue in memory
	physicalTaskQueueManagerImpl struct {
		status               int32
		partitionMgr         *taskQueuePartitionManagerImpl
		queue                *PhysicalTaskQueueKey
		config               *taskQueueConfig
		backlogMgr           *backlogManagerImpl
		liveness             *liveness
		matcher              *TaskMatcher // for matching a task producer with a poller
		namespaceRegistry    namespace.Registry
		logger               log.Logger
		throttledLogger      log.ThrottledLogger
		matchingClient       matchingservice.MatchingServiceClient
		metricsHandler       metrics.Handler
		clusterMeta          cluster.Metadata
		taggedMetricsHandler metrics.Handler // namespace/taskqueue tagged metric scope
		// pollerHistory stores poller which poll from this taskqueue in last few minutes
		pollerHistory              *pollerHistory
		currentPolls               atomic.Int64
		taskValidator              taskValidator
		TasksAddedInIntervals      *taskTracker
		TasksDispatchedInIntervals *taskTracker
	}
)

// a circular array of a fixed size which shall have it's pointer for tracking tasks
type circularTaskBuffer struct {
	buffer     []int
	currentPos int
}

func newCircularTaskBuffer(size int) *circularTaskBuffer {
	return &circularTaskBuffer{
		buffer:     make([]int, size), // Initialize the buffer with the given size
		currentPos: 0,
	}
}

func (cb *circularTaskBuffer) incrementTaskCount() {
	cb.buffer[cb.currentPos]++
}

func (cb *circularTaskBuffer) advance() {
	cb.currentPos = (cb.currentPos + 1) % len(cb.buffer)
	cb.buffer[cb.currentPos] = 0 // Reset the task count for the new interval
}

// returns the total number of tasks in the buffer
func (cb *circularTaskBuffer) totalTasks() int {
	totalTasks := 0
	for _, count := range cb.buffer {
		totalTasks += count
	}
	return totalTasks
}

type taskTracker struct {
	mutex             sync.RWMutex
	clock             clock.TimeSource
	startTime         time.Time     // time when taskTracker was initialized
	startIntervalTime time.Time     // the starting time of a window in the buffer
	interval          time.Duration // the duration of each mini- in the buffer
	totalIntervalSize int           // the number over which rate of tasks added/dispatched would be determined
	tasksInInterval   *circularTaskBuffer
}

func newTaskTracker(timeSource clock.TimeSource, intervalSize int, totalIntervalSize int) *taskTracker {
	return &taskTracker{
		clock:             timeSource,
		startTime:         timeSource.Now(),
		startIntervalTime: timeSource.Now(),
		interval:          time.Duration(intervalSize) * time.Second, // Todo: Shivam - replace with config value
		totalIntervalSize: totalIntervalSize,
		tasksInInterval:   newCircularTaskBuffer((totalIntervalSize / intervalSize) + 1), // Todo: Shivam - replace hardcoded value with number of buckets
	}
}

// advanceAndResetTracker is a helper to advance the trackers position and clear out any expired intervals
func (s *taskTracker) advanceAndResetTracker(elapsed time.Duration) {
	// Calculate the number of intervals elapsed since the start interval time
	intervalsElapsed := int(elapsed / s.interval)

	if intervalsElapsed != 0 {
		for i := 0; i < intervalsElapsed; i++ {
			s.tasksInInterval.advance() // advancing our circular buffer's position until we land on the right interval
		}
		s.startIntervalTime = s.startIntervalTime.Add(time.Duration(intervalsElapsed) * s.interval)
	}
}

// trackTasks is responsible for adding/removing tasks from the current time that falls in the appropriate interval
func (s *taskTracker) trackTasks() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	currentTime := s.clock.Now()

	// Calculate elapsed time from the latest start interval time
	elapsed := currentTime.Sub(s.startIntervalTime)
	s.advanceAndResetTracker(elapsed)
	s.tasksInInterval.incrementTaskCount()
}

// rate is responsible for returning the rate of tasks added/dispatched in a given interval
func (s *taskTracker) rate() float32 {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	currentTime := s.clock.Now()

	// Calculate elapsed time from the latest start interval time
	elapsed := currentTime.Sub(s.startIntervalTime)
	s.advanceAndResetTracker(elapsed)
	totalTasks := s.tasksInInterval.totalTasks()

	// time passed since start of the current window + totalIntervalSize in milliseconds
	elapsedTime := currentTime.Sub(s.startIntervalTime).Milliseconds() + int64(s.totalIntervalSize*1000)
	// rate per second
	return (float32(totalTasks) / float32(elapsedTime)) * 1000
}

var _ physicalTaskQueueManager = (*physicalTaskQueueManagerImpl)(nil)

var (
	errRemoteSyncMatchFailed  = serviceerror.NewCanceled("remote sync match failed")
	errMissingNormalQueueName = errors.New("missing normal queue name")
)

func newPhysicalTaskQueueManager(
	partitionMgr *taskQueuePartitionManagerImpl,
	queue *PhysicalTaskQueueKey,
	opts ...taskQueueManagerOpt,
) (*physicalTaskQueueManagerImpl, error) {
	e := partitionMgr.engine
	config := partitionMgr.config
	logger := log.With(partitionMgr.logger, tag.WorkerBuildId(queue.VersionSet()))
	throttledLogger := log.With(partitionMgr.throttledLogger, tag.WorkerBuildId(queue.VersionSet()))
	taggedMetricsHandler := partitionMgr.taggedMetricsHandler.WithTags(
		metrics.OperationTag(metrics.MatchingTaskQueueMgrScope),
		metrics.WorkerBuildIdTag(queue.VersionSet()))
	pqMgr := &physicalTaskQueueManagerImpl{
		status:                     common.DaemonStatusInitialized,
		partitionMgr:               partitionMgr,
		namespaceRegistry:          e.namespaceRegistry,
		matchingClient:             e.matchingRawClient,
		metricsHandler:             e.metricsHandler,
		clusterMeta:                e.clusterMeta,
		queue:                      queue,
		logger:                     logger,
		throttledLogger:            throttledLogger,
		config:                     config,
		taggedMetricsHandler:       taggedMetricsHandler,
		TasksAddedInIntervals:      newTaskTracker(clock.NewRealTimeSource(), intervalSize, totalIntervalSize),
		TasksDispatchedInIntervals: newTaskTracker(clock.NewRealTimeSource(), intervalSize, totalIntervalSize),
	}
	pqMgr.pollerHistory = newPollerHistory()

	pqMgr.liveness = newLiveness(
		clock.NewRealTimeSource(),
		config.MaxTaskQueueIdleTime,
		pqMgr.UnloadFromPartitionManager,
	)

	pqMgr.taskValidator = newTaskValidator(pqMgr.newIOContext, pqMgr.clusterMeta, pqMgr.namespaceRegistry, pqMgr.partitionMgr.engine.historyClient)
	pqMgr.backlogMgr = newBacklogManager(
		pqMgr,
		config,
		e.taskManager,
		logger,
		throttledLogger,
		e.matchingRawClient,
		taggedMetricsHandler,
		partitionMgr.callerInfoContext,
	)

	var fwdr *Forwarder
	var err error
	if !queue.Partition().IsRoot() && queue.Partition().Kind() != enumspb.TASK_QUEUE_KIND_STICKY {
		// Every DB Queue needs its own forwarder so that the throttles do not interfere
		fwdr, err = newForwarder(&config.forwarderConfig, queue, e.matchingRawClient)
		if err != nil {
			return nil, err
		}
	}
	pqMgr.matcher = newTaskMatcher(config, fwdr, pqMgr.taggedMetricsHandler)
	for _, opt := range opts {
		opt(pqMgr)
	}
	return pqMgr, nil
}

func (c *physicalTaskQueueManagerImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&c.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}
	c.liveness.Start()
	c.backlogMgr.Start()
	c.logger.Info("", tag.LifeCycleStarted)
	c.taggedMetricsHandler.Counter(metrics.TaskQueueStartedCounter.Name()).Record(1)
	c.partitionMgr.engine.updatePhysicalTaskQueueGauge(c, 1)
}

// Stop does not unload the queue from its partition. It is intended to be called by the partition manager when
// unloading a queues. For stopping and unloading a queue call unloadFromPartitionManager instead.
func (c *physicalTaskQueueManagerImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&c.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}
	c.backlogMgr.Stop()
	c.liveness.Stop()
	c.logger.Info("", tag.LifeCycleStopped)
	c.taggedMetricsHandler.Counter(metrics.TaskQueueStoppedCounter.Name()).Record(1)
	c.partitionMgr.engine.updatePhysicalTaskQueueGauge(c, -1)
}

func (c *physicalTaskQueueManagerImpl) WaitUntilInitialized(ctx context.Context) error {
	return c.backlogMgr.WaitUntilInitialized(ctx)
}

// AddTask adds a task to the task queue. This method will first attempt a synchronous
// match with a poller. When there are no pollers or if ratelimit is exceeded, task will
// be written to database and later asynchronously matched with a poller
func (c *physicalTaskQueueManagerImpl) AddTask(
	ctx context.Context,
	params addTaskParams,
) (bool, error) {
	c.TasksAddedInIntervals.trackTasks()
	if params.forwardedFrom == "" {
		// request sent by history service
		c.liveness.markAlive()
	}

	taskInfo := params.taskInfo
	namespaceEntry, err := c.namespaceRegistry.GetNamespaceByID(namespace.ID(taskInfo.GetNamespaceId()))
	if err != nil {
		return false, err
	}

	if namespaceEntry.ActiveInCluster(c.clusterMeta.GetCurrentClusterName()) {
		syncMatch, err := c.trySyncMatch(ctx, params)
		if syncMatch {
			return syncMatch, err
		}
	}

	if params.forwardedFrom != "" {
		// forwarded from child partition - only do sync match
		// child partition will persist the task when sync match fails
		return false, errRemoteSyncMatchFailed
	}

	// Ensure that tasks with the "default" versioning directive get spooled in the unversioned queue as they are not
	// associated with any version set until their execution is touched by a version specific worker.
	// "compatible" tasks OTOH are associated with a specific version set and should be stored along with all tasks for
	// that version set.
	// The task queue default set is dynamic and applies only at dispatch time. Putting "default" tasks into version set
	// specific queues could cause them to get stuck behind "compatible" tasks when they should be able to progress
	// independently.
	// TODO: [old-wv-cleanup]
	if c.queue.VersionSet() != "" && taskInfo.VersionDirective.GetUseAssignmentRules() != nil {
		err = c.partitionMgr.defaultQueue.SpoolTask(params)
	} else {
		err = c.SpoolTask(params)
	}
	return false, err
}

func (c *physicalTaskQueueManagerImpl) SpoolTask(params addTaskParams) error {
	return c.backlogMgr.SpoolTask(params.taskInfo)
}

// PollTask blocks waiting for a task.
// Returns error when context deadline is exceeded
// maxDispatchPerSecond is the max rate at which tasks are allowed
// to be dispatched from this task queue to pollers
func (c *physicalTaskQueueManagerImpl) PollTask(
	ctx context.Context,
	pollMetadata *pollMetadata,
) (*internalTask, error) {
	c.liveness.markAlive()

	c.currentPolls.Add(1)
	defer c.currentPolls.Add(-1)

	namespaceEntry, err := c.namespaceRegistry.GetNamespaceByID(c.queue.NamespaceId())
	if err != nil {
		return nil, err
	}

	// the desired global rate limit for the task queue comes from the
	// poller, which lives inside the client side worker. There is
	// one rateLimiter for this entire task queue and as we get polls,
	// we update the ratelimiter rps if it has changed from the last
	// value. Last poller wins if different pollers provide different values
	c.matcher.UpdateRatelimit(pollMetadata.ratePerSecond)

	if !namespaceEntry.ActiveInCluster(c.clusterMeta.GetCurrentClusterName()) {
		return c.matcher.PollForQuery(ctx, pollMetadata)
	}

	task, err := c.matcher.Poll(ctx, pollMetadata)
	if err != nil {
		return nil, err
	}

	task.namespace = c.partitionMgr.ns.Name()
	task.backlogCountHint = c.backlogMgr.db.getApproximateBacklogCount
	c.TasksDispatchedInIntervals.trackTasks()
	return task, nil
}

func (c *physicalTaskQueueManagerImpl) MarkAlive() {
	c.liveness.markAlive()
}

// DispatchSpooledTask dispatches a task to a poller. When there are no pollers to pick
// up the task or if rate limit is exceeded, this method will return error. Task
// *will not* be persisted to db
func (c *physicalTaskQueueManagerImpl) DispatchSpooledTask(
	ctx context.Context,
	task *internalTask,
	userDataChanged <-chan struct{},
) error {
	return c.matcher.MustOffer(ctx, task, userDataChanged)
}

func (c *physicalTaskQueueManagerImpl) ProcessSpooledTask(
	ctx context.Context,
	task *internalTask,
) error {
	if !c.taskValidator.maybeValidate(task.event.AllocatedTaskInfo, c.queue.TaskType()) {
		task.finish(nil)
		c.taggedMetricsHandler.Counter(metrics.ExpiredTasksPerTaskQueueCounter.Name()).Record(1)
		// Don't try to set read level here because it may have been advanced already.
		return nil
	}
	return c.partitionMgr.ProcessSpooledTask(ctx, task, c.queue.BuildId())
}

// DispatchQueryTask will dispatch query to local or remote poller. If forwarded then result or error is returned,
// if dispatched to local poller then nil and nil is returned.
func (c *physicalTaskQueueManagerImpl) DispatchQueryTask(
	ctx context.Context,
	taskId string,
	request *matchingservice.QueryWorkflowRequest,
) (*matchingservice.QueryWorkflowResponse, error) {
	task := newInternalQueryTask(taskId, request)
	return c.matcher.OfferQuery(ctx, task)
}

func (c *physicalTaskQueueManagerImpl) DispatchNexusTask(
	ctx context.Context,
	taskId string,
	request *matchingservice.DispatchNexusTaskRequest,
) (*matchingservice.DispatchNexusTaskResponse, error) {
	task := newInternalNexusTask(taskId, request)
	return c.matcher.OfferNexusTask(ctx, task)
}

func (c *physicalTaskQueueManagerImpl) UpdatePollerInfo(id pollerIdentity, pollMetadata *pollMetadata) {
	c.pollerHistory.updatePollerInfo(id, pollMetadata)
}

// GetAllPollerInfo returns all pollers that polled from this taskqueue in last few minutes
func (c *physicalTaskQueueManagerImpl) GetAllPollerInfo() []*taskqueuepb.PollerInfo {
	if c.pollerHistory == nil {
		return nil
	}
	return c.pollerHistory.getPollerInfo(time.Time{})
}

func (c *physicalTaskQueueManagerImpl) HasPollerAfter(accessTime time.Time) bool {
	if c.currentPolls.Load() > 0 {
		return true
	}
	if c.pollerHistory == nil {
		return false
	}
	recentPollers := c.pollerHistory.getPollerInfo(accessTime)
	return len(recentPollers) > 0
}

// LegacyDescribeTaskQueue returns information about the target taskqueue, right now this API returns the
// pollers which polled this taskqueue in last few minutes and status of taskqueue's ackManager
// (readLevel, ackLevel, backlogCountHint and taskIDBlock).
func (c *physicalTaskQueueManagerImpl) LegacyDescribeTaskQueue(includeTaskQueueStatus bool) *matchingservice.DescribeTaskQueueResponse {
	response := &matchingservice.DescribeTaskQueueResponse{
		DescResponse: &workflowservice.DescribeTaskQueueResponse{
			Pollers: c.GetAllPollerInfo(),
		},
	}
	if includeTaskQueueStatus {
		response.DescResponse.TaskQueueStatus = c.backlogMgr.BacklogStatus()
		response.DescResponse.TaskQueueStatus.RatePerSecond = c.matcher.Rate()
	}
	return response
}

func (c *physicalTaskQueueManagerImpl) GetBacklogInfo(ctx context.Context) (*taskqueuepb.BacklogInfo, error) {
	approximateBacklogCount, err := c.backlogMgr.getApproximateBacklogCount(ctx)
	if err != nil {
		return nil, err
	}
	return &taskqueuepb.BacklogInfo{
		ApproximateBacklogCount: approximateBacklogCount,
		ApproximateBacklogAge:   durationpb.New(c.backlogMgr.taskReader.getBacklogHeadCreateTime()),
		TasksAddRate:            c.TasksAddedInIntervals.rate(),
		TasksDispatchRate:       c.TasksDispatchedInIntervals.rate(),
	}, nil
}

func (c *physicalTaskQueueManagerImpl) String() string {
	buf := new(bytes.Buffer)
	if c.queue.TaskType() == enumspb.TASK_QUEUE_TYPE_ACTIVITY {
		buf.WriteString("Activity")
	} else {
		buf.WriteString("Workflow")
	}
	_, _ = fmt.Fprintf(buf, "Backlog=%s\n", c.backlogMgr.String())

	return buf.String()
}

func (c *physicalTaskQueueManagerImpl) trySyncMatch(ctx context.Context, params addTaskParams) (bool, error) {
	if params.forwardedFrom == "" && c.config.TestDisableSyncMatch() {
		return false, nil
	}
	childCtx, cancel := newChildContext(ctx, c.config.SyncMatchWaitDuration(), time.Second)
	defer cancel()

	// Use fake TaskId for sync match as it hasn't been allocated yet
	fakeTaskIdWrapper := &persistencespb.AllocatedTaskInfo{
		Data:   params.taskInfo,
		TaskId: syncMatchTaskId,
	}

	task := newInternalTask(fakeTaskIdWrapper, nil, params.source, params.forwardedFrom, true)
	return c.matcher.Offer(childCtx, task)
}

// newChildContext creates a child context with desired timeout.
// if tailroom is non-zero, then child context timeout will be
// the minOf(parentCtx.Deadline()-tailroom, timeout). Use this
// method to create child context when childContext cannot use
// all of parent's deadline but instead there is a need to leave
// some time for parent to do some post-work
func newChildContext(
	parent context.Context,
	timeout time.Duration,
	tailroom time.Duration,
) (context.Context, context.CancelFunc) {
	if parent.Err() != nil {
		return parent, func() {}
	}
	deadline, ok := parent.Deadline()
	if !ok {
		return context.WithTimeout(parent, timeout)
	}
	remaining := time.Until(deadline) - tailroom
	if remaining < timeout {
		timeout = max(0, remaining)
	}
	return context.WithTimeout(parent, timeout)
}

func (c *physicalTaskQueueManagerImpl) QueueKey() *PhysicalTaskQueueKey {
	return c.queue
}

func (c *physicalTaskQueueManagerImpl) Matcher() *TaskMatcher {
	return c.matcher
}

func (c *physicalTaskQueueManagerImpl) newIOContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(context.Background(), ioTimeout)
	return c.partitionMgr.callerInfoContext(ctx), cancel
}

func (c *physicalTaskQueueManagerImpl) UnloadFromPartitionManager() {
	c.partitionMgr.unloadPhysicalQueue(c)
}
