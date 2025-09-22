package matching

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/debug"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/matching/counter"
	"go.temporal.io/server/service/worker/workerdeployment"
	"google.golang.org/protobuf/types/known/durationpb"
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

	// We avoid retrying failed deployment registration for this period.
	deploymentRegisterErrorBackoff = 5 * time.Second
)

type (
	addTaskParams struct {
		taskInfo    *persistencespb.TaskInfo
		forwardInfo *taskqueuespb.TaskForwardInfo
	}
	// physicalTaskQueueManagerImpl manages a set of physical queues that comprise one logical
	// queue, corresponding to a single versioned queue of a task queue partition.
	// TODO(pri): rename this
	physicalTaskQueueManagerImpl struct {
		status             int32
		partitionMgr       *taskQueuePartitionManagerImpl
		queue              *PhysicalTaskQueueKey
		config             *taskQueueConfig
		defaultPriorityKey priorityKey

		// This context is valid for lifetime of this physicalTaskQueueManagerImpl.
		// It can be used to notify when the task queue is closing.
		tqCtx       context.Context
		tqCtxCancel context.CancelFunc

		cancelMatcherSub  func()
		cancelFairnessSub func()
		backlogMgr        backlogManager
		liveness          *liveness
		oldMatcher        *TaskMatcher // TODO(pri): old matcher cleanup
		priMatcher        *priTaskMatcher
		matcher           matcherInterface // TODO(pri): old matcher cleanup
		namespaceRegistry namespace.Registry
		logger            log.Logger
		throttledLogger   log.ThrottledLogger
		matchingClient    matchingservice.MatchingServiceClient
		clusterMeta       cluster.Metadata
		metricsHandler    metrics.Handler // namespace/taskqueue tagged metric scope
		// pollerHistory stores poller which poll from this taskqueue in last few minutes
		pollerHistory               *pollerHistory
		currentPolls                atomic.Int64
		taskValidator               taskValidator
		deploymentRegistrationCh    chan struct{}
		deploymentVersionRegistered bool
		pollerScalingRateLimiter    quotas.RateLimiter

		taskTrackerLock sync.RWMutex
		tasksAdded      map[priorityKey]*taskTracker
		tasksDispatched map[priorityKey]*taskTracker
	}

	// TODO(pri): old matcher cleanup
	matcherInterface interface {
		Start()
		Stop()
		Poll(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, error)
		PollForQuery(ctx context.Context, pollMetadata *pollMetadata) (*internalTask, error)
		OfferQuery(ctx context.Context, task *internalTask) (*matchingservice.QueryWorkflowResponse, error)
		OfferNexusTask(ctx context.Context, task *internalTask) (*matchingservice.DispatchNexusTaskResponse, error)
		ReprocessAllTasks()
	}
)

var _ physicalTaskQueueManager = (*physicalTaskQueueManagerImpl)(nil)

var (
	errRemoteSyncMatchFailed     = serviceerror.NewCanceled("remote sync match failed")
	errMissingNormalQueueName    = errors.New("missing normal queue name")
	errDeploymentVersionNotReady = serviceerror.NewUnavailable("task queue is not ready to process polls from this deployment version, try again shortly")
	ErrBlackholedQuery           = "You are trying to query a closed Workflow that is PINNED to Worker Deployment Version %s, but %s is drained and has no pollers to answer the query. Immediately: You can re-deploy Workers in this Deployment Version to take those queries, or you can workflow update-options to change your workflow to AUTO_UPGRADE. For the future: In your infrastructure, consider waiting longer after the last queried timestamp as reported in Describe Deployment before you sunset Workers. Or mark this workflow as AUTO_UPGRADE."

	backlogTagClassic  = tag.NewStringTag("backlog", "classic")
	backlogTagPriority = tag.NewStringTag("backlog", "priority")
	backlogTagFairness = tag.NewStringTag("backlog", "fairness")
)

func newPhysicalTaskQueueManager(
	partitionMgr *taskQueuePartitionManagerImpl,
	queue *PhysicalTaskQueueKey,
) (*physicalTaskQueueManagerImpl, error) {
	e := partitionMgr.engine
	config := partitionMgr.config
	buildIdTagValue := queue.Version().MetricsTagValue()
	buildIdTag := tag.WorkerBuildId(buildIdTagValue)
	taggedMetricsHandler := partitionMgr.metricsHandler.WithTags(
		metrics.OperationTag(metrics.MatchingTaskQueueMgrScope),
		metrics.WorkerBuildIdTag(buildIdTagValue, config.BreakdownMetricsByBuildID()))

	tqCtx, tqCancel := context.WithCancel(partitionMgr.callerInfoContext(context.Background()))

	// We multiply by a big number so that we can later divide it by the number of pollers when grabbing permits,
	// to allow us to make more decisions per second when there are more pollers.
	pollerScalingRateLimitFn := func() float64 {
		return config.PollerScalingDecisionsPerSecond() * 1e6
	}
	pqMgr := &physicalTaskQueueManagerImpl{
		defaultPriorityKey:       defaultPriorityLevel(config.PriorityLevels()),
		status:                   common.DaemonStatusInitialized,
		partitionMgr:             partitionMgr,
		queue:                    queue,
		config:                   config,
		tqCtx:                    tqCtx,
		tqCtxCancel:              tqCancel,
		namespaceRegistry:        e.namespaceRegistry,
		matchingClient:           e.matchingRawClient,
		clusterMeta:              e.clusterMeta,
		metricsHandler:           taggedMetricsHandler,
		tasksAdded:               make(map[priorityKey]*taskTracker),
		tasksDispatched:          make(map[priorityKey]*taskTracker),
		pollerScalingRateLimiter: quotas.NewDefaultOutgoingRateLimiter(pollerScalingRateLimitFn),
		deploymentRegistrationCh: make(chan struct{}, 1),
	}
	pqMgr.deploymentRegistrationCh <- struct{}{} // seed

	pqMgr.pollerHistory = newPollerHistory(partitionMgr.config.PollerHistoryTTL())

	pqMgr.liveness = newLiveness(
		clock.NewRealTimeSource(),
		config.MaxTaskQueueIdleTime,
		func() { pqMgr.UnloadFromPartitionManager(unloadCauseIdle) },
	)

	pqMgr.taskValidator = newTaskValidator(
		tqCtx,
		pqMgr.clusterMeta,
		pqMgr.namespaceRegistry,
		pqMgr.partitionMgr.engine.historyClient,
	)

	isSticky := queue.Partition().Kind() == enumspb.TASK_QUEUE_KIND_STICKY
	isChild := !isSticky && !queue.Partition().IsRoot()

	var fairness, newMatcher bool
	// Fairness is disabled for sticky queues for now so that we can still use TTLs.
	if !isSticky {
		fairness, pqMgr.cancelFairnessSub = config.EnableFairness(func(bool) {
			// unload on change so that we can reload with the new setting:
			pqMgr.UnloadFromPartitionManager(unloadCauseConfigChange)
		})
	}
	if fairness {
		pqMgr.logger = log.With(partitionMgr.logger, buildIdTag, backlogTagFairness)
		pqMgr.throttledLogger = log.With(partitionMgr.throttledLogger, buildIdTag, backlogTagFairness)

		counterFactory := func() counter.Counter {
			src := rand.NewPCG(rand.Uint64(), rand.Uint64())
			return counter.NewHybridCounter(config.FairnessCounter(), src)
		}

		pqMgr.backlogMgr = newFairBacklogManager(
			tqCtx,
			pqMgr,
			config,
			e.fairTaskManager,
			pqMgr.logger,
			pqMgr.throttledLogger,
			e.matchingRawClient,
			newFairMetricsHandler(taggedMetricsHandler),
			counterFactory,
		)
		var fwdr *priForwarder
		var err error
		if isChild {
			// Every DB Queue needs its own forwarder so that the throttles do not interfere
			fwdr, err = newPriForwarder(&config.forwarderConfig, queue, e.matchingRawClient)
			if err != nil {
				return nil, err
			}
		}
		pqMgr.priMatcher = newPriTaskMatcher(
			tqCtx,
			config,
			queue.partition,
			fwdr,
			pqMgr.taskValidator,
			pqMgr.logger,
			newFairMetricsHandler(taggedMetricsHandler),
			partitionMgr.rateLimitManager,
			pqMgr.MarkAlive,
		)
		pqMgr.matcher = pqMgr.priMatcher
		return pqMgr, nil
	}

	newMatcher, pqMgr.cancelMatcherSub = config.NewMatcher(func(bool) {
		// unload on change to NewMatcher so that we can reload with the new setting:
		pqMgr.UnloadFromPartitionManager(unloadCauseConfigChange)
	})

	if newMatcher {
		pqMgr.logger = log.With(partitionMgr.logger, buildIdTag, backlogTagPriority)
		pqMgr.throttledLogger = log.With(partitionMgr.throttledLogger, buildIdTag, backlogTagPriority)

		pqMgr.backlogMgr = newPriBacklogManager(
			tqCtx,
			pqMgr,
			config,
			e.taskManager,
			pqMgr.logger,
			pqMgr.throttledLogger,
			e.matchingRawClient,
			newPriMetricsHandler(taggedMetricsHandler),
		)
		var fwdr *priForwarder
		var err error
		if isChild {
			// Every DB Queue needs its own forwarder so that the throttles do not interfere
			fwdr, err = newPriForwarder(&config.forwarderConfig, queue, e.matchingRawClient)
			if err != nil {
				return nil, err
			}
		}
		pqMgr.priMatcher = newPriTaskMatcher(
			tqCtx,
			config,
			queue.partition,
			fwdr,
			pqMgr.taskValidator,
			pqMgr.logger,
			newPriMetricsHandler(taggedMetricsHandler),
			partitionMgr.rateLimitManager,
			pqMgr.MarkAlive,
		)
		pqMgr.matcher = pqMgr.priMatcher
		return pqMgr, nil
	}

	pqMgr.logger = log.With(partitionMgr.logger, buildIdTag, backlogTagClassic)
	pqMgr.throttledLogger = log.With(partitionMgr.throttledLogger, buildIdTag, backlogTagClassic)

	pqMgr.backlogMgr = newBacklogManager(
		tqCtx,
		pqMgr,
		config,
		e.taskManager,
		pqMgr.logger,
		pqMgr.throttledLogger,
		e.matchingRawClient,
		taggedMetricsHandler,
	)
	var fwdr *Forwarder
	var err error
	if isChild {
		// Every DB Queue needs its own forwarder so that the throttles do not interfere
		fwdr, err = newForwarder(&config.forwarderConfig, queue, e.matchingRawClient)
		if err != nil {
			return nil, err
		}
	}
	pqMgr.oldMatcher = newTaskMatcher(config, fwdr, taggedMetricsHandler, pqMgr.partitionMgr.GetRateLimitManager().GetRateLimiter())
	pqMgr.matcher = pqMgr.oldMatcher
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
	c.matcher.Start()
	c.logger.Info("Started physicalTaskQueueManager", tag.LifeCycleStarted, tag.Cause(c.config.loadCause.String()))
	c.metricsHandler.Counter(metrics.TaskQueueStartedCounter.Name()).Record(1)
	c.partitionMgr.engine.updatePhysicalTaskQueueGauge(c.partitionMgr.ns, c.partitionMgr.partition, c.queue.version, 1)
}

// Stop does not unload the queue from its partition. It is intended to be called by the partition manager when
// unloading a queues. For stopping and unloading a queue call UnloadFromPartitionManager instead.
func (c *physicalTaskQueueManagerImpl) Stop(unloadCause unloadCause) {
	if !atomic.CompareAndSwapInt32(
		&c.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}
	if c.cancelMatcherSub != nil {
		c.cancelMatcherSub()
	}
	if c.cancelFairnessSub != nil {
		c.cancelFairnessSub()
	}
	// this may attempt to write one final ack update, do this before canceling tqCtx
	c.backlogMgr.Stop()
	c.matcher.Stop()
	c.liveness.Stop()
	c.tqCtxCancel()
	c.logger.Info("Stopped physicalTaskQueueManager", tag.LifeCycleStopped, tag.Cause(unloadCause.String()))
	c.metricsHandler.Counter(metrics.TaskQueueStoppedCounter.Name()).Record(1)
	c.partitionMgr.engine.updatePhysicalTaskQueueGauge(c.partitionMgr.ns, c.partitionMgr.partition, c.queue.version, -1)
}

func (c *physicalTaskQueueManagerImpl) WaitUntilInitialized(ctx context.Context) error {
	return c.backlogMgr.WaitUntilInitialized(ctx)
}

func (c *physicalTaskQueueManagerImpl) SpoolTask(taskInfo *persistencespb.TaskInfo) error {
	c.liveness.markAlive()
	return c.backlogMgr.SpoolTask(taskInfo)
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

	namespaceId := namespace.ID(c.queue.NamespaceId())
	namespaceEntry, err := c.namespaceRegistry.GetNamespaceByID(namespaceId)
	if err != nil {
		return nil, err
	}

	if c.partitionMgr.engine.config.EnableDeploymentVersions(namespaceEntry.Name().String()) {
		if err = c.ensureRegisteredInDeploymentVersion(ctx, namespaceEntry, pollMetadata); err != nil {
			return nil, err
		}
	}

	if !namespaceEntry.ActiveInCluster(c.clusterMeta.GetCurrentClusterName()) {
		return c.matcher.PollForQuery(ctx, pollMetadata)
	}

	for {
		task, err := c.matcher.Poll(ctx, pollMetadata)
		if err != nil {
			return nil, err
		}

		// It's possible to get an expired task here: taskReader checks for expiration when
		// reading from persistence, and physicalTaskQueueManager checks for expiration in
		// ProcessSpooledTask, but the one task blocked in the matcher could expire while it's
		// there. In that case, go back for another task.
		// If we didn't do this, the task would be rejected when we call RecordXTaskStarted on
		// history, but this is more efficient.

		if task.event != nil && IsTaskExpired(task.event.AllocatedTaskInfo) {
			// task is expired while polling
			c.metricsHandler.Counter(metrics.ExpiredTasksPerTaskQueueCounter.Name()).Record(1, metrics.TaskExpireStageMemoryTag)
			task.finish(nil, false)
			continue
		}

		task.namespace = c.partitionMgr.ns.Name()
		task.backlogCountHint = c.backlogCountHint

		if pollMetadata.forwardedFrom == "" && // only track the original polls, not forwarded ones.
			(!task.isStarted() || !task.started.hasEmptyResponse()) { // Need to filter out the empty "started" ones
			c.getOrCreateTaskTracker(c.tasksDispatched, priorityKey(task.getPriority().GetPriorityKey())).incrementTaskCount()
		}
		return task, nil
	}
}

func (c *physicalTaskQueueManagerImpl) backlogCountHint() int64 {
	return c.backlogMgr.BacklogCountHint()
}

func (c *physicalTaskQueueManagerImpl) MarkAlive() {
	c.liveness.markAlive()
}

// DispatchSpooledTask dispatches a task to a poller. When there are no pollers to pick
// up the task or if rate limit is exceeded, this method will return error. Task
// *will not* be persisted to db
// TODO(pri): old matcher cleanup
func (c *physicalTaskQueueManagerImpl) DispatchSpooledTask(
	ctx context.Context,
	task *internalTask,
	userDataChanged <-chan struct{},
) error {
	return c.oldMatcher.MustOffer(ctx, task, userDataChanged)
}

// TODO(pri): old matcher cleanup
func (c *physicalTaskQueueManagerImpl) ProcessSpooledTask(
	ctx context.Context,
	task *internalTask,
) error {
	if !c.taskValidator.maybeValidate(task.event.AllocatedTaskInfo, c.queue.TaskType()) {
		task.finish(nil, false)

		var invalidTaskTag = getInvalidTaskTag(task)
		c.metricsHandler.Counter(metrics.ExpiredTasksPerTaskQueueCounter.Name()).Record(1, invalidTaskTag)
		// Don't try to set read level here because it may have been advanced already.

		// Stay alive as long as we're invalidating tasks
		c.MarkAlive()

		return nil
	}
	return c.partitionMgr.ProcessSpooledTask(ctx, task, c.queue)
}

func (c *physicalTaskQueueManagerImpl) AddSpooledTask(task *internalTask) error {
	return c.partitionMgr.AddSpooledTask(c.tqCtx, task, c.queue)
}

func (c *physicalTaskQueueManagerImpl) AddSpooledTaskToMatcher(task *internalTask) {
	c.priMatcher.AddTask(task)
}

func (c *physicalTaskQueueManagerImpl) UserDataChanged() {
	c.matcher.ReprocessAllTasks()
}

// DispatchQueryTask will dispatch query to local or remote poller. If forwarded then result or error is returned,
// if dispatched to local poller then nil and nil is returned.
func (c *physicalTaskQueueManagerImpl) DispatchQueryTask(
	ctx context.Context,
	taskId string,
	request *matchingservice.QueryWorkflowRequest,
) (*matchingservice.QueryWorkflowResponse, error) {
	task := newInternalQueryTask(taskId, request)
	if !task.isForwarded() {
		c.getOrCreateTaskTracker(c.tasksAdded, priorityKey(request.GetPriority().GetPriorityKey())).incrementTaskCount()
	}
	return c.matcher.OfferQuery(ctx, task)
}

func (c *physicalTaskQueueManagerImpl) DispatchNexusTask(
	ctx context.Context,
	taskId string,
	request *matchingservice.DispatchNexusTaskRequest,
) (*matchingservice.DispatchNexusTaskResponse, error) {
	deadline, _ := ctx.Deadline() // If not set by user, our client will set a default.
	var opDeadline time.Time
	if header := nexus.Header(request.GetRequest().GetHeader()); header != nil {
		if opTimeoutHeader := header.Get(nexus.HeaderOperationTimeout); opTimeoutHeader != "" {
			opTimeout, err := time.ParseDuration(opTimeoutHeader)
			if err != nil {
				// Operation-Timeout header is not required so don't fail request on parsing errors.
				c.logger.Warn(fmt.Sprintf("unable to parse %v header: %v", nexus.HeaderOperationTimeout, opTimeoutHeader), tag.Error(err), tag.WorkflowNamespaceID(request.NamespaceId))
			} else {
				opDeadline = time.Now().Add(opTimeout)
			}
		}
	}
	task := newInternalNexusTask(taskId, deadline, opDeadline, request)
	if !task.isForwarded() {
		c.getOrCreateTaskTracker(c.tasksAdded, priorityKey(0)).incrementTaskCount() // Nexus has no priorities
	}
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
	res := c.pollerHistory.getPollerInfo(time.Time{})
	return res
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
		rps, _ := c.partitionMgr.GetRateLimitManager().GetEffectiveRPSAndSource()
		//nolint:staticcheck // SA1019: using deprecated TaskQueueStatus for legacy compatibility
		response.DescResponse.TaskQueueStatus.RatePerSecond = rps
	}
	return response
}

func (c *physicalTaskQueueManagerImpl) GetStatsByPriority() map[int32]*taskqueuepb.TaskQueueStats {
	stats := c.backlogMgr.BacklogStatsByPriority()

	c.taskTrackerLock.RLock()
	defer c.taskTrackerLock.RUnlock()

	for pri, tt := range c.tasksAdded {
		if _, ok := stats[int32(pri)]; !ok {
			stats[int32(pri)] = &taskqueuepb.TaskQueueStats{}
		}
		stats[int32(pri)].TasksAddRate = tt.rate()
	}
	for pri, tt := range c.tasksDispatched {
		if _, ok := stats[int32(pri)]; !ok {
			stats[int32(pri)] = &taskqueuepb.TaskQueueStats{}
		}
		stats[int32(pri)].TasksDispatchRate = tt.rate()
	}
	return stats
}

func (c *physicalTaskQueueManagerImpl) GetInternalTaskQueueStatus() []*taskqueuespb.InternalTaskQueueStatus {
	return c.backlogMgr.InternalStatus()
}

func (c *physicalTaskQueueManagerImpl) TrySyncMatch(ctx context.Context, task *internalTask) (bool, error) {
	if !task.isForwarded() {
		// request sent by history service
		c.liveness.markAlive()
		c.getOrCreateTaskTracker(c.tasksAdded, priorityKey(task.getPriority().GetPriorityKey())).incrementTaskCount()
		if disable, _ := testhooks.Get[bool](c.partitionMgr.engine.testHooks, testhooks.MatchingDisableSyncMatch); disable {
			return false, nil
		}
	}

	if c.priMatcher != nil {
		return c.priMatcher.Offer(ctx, task)
	}

	childCtx, cancel := contextutil.WithDeadlineBuffer(ctx, c.config.SyncMatchWaitDuration(), time.Second)
	defer cancel()

	return c.oldMatcher.Offer(childCtx, task)
}

func (c *physicalTaskQueueManagerImpl) ensureRegisteredInDeploymentVersion(
	ctx context.Context,
	namespaceEntry *namespace.Namespace,
	pollMetadata *pollMetadata,
) error {
	workerDeployment, err := worker_versioning.DeploymentFromCapabilities(pollMetadata.workerVersionCapabilities, pollMetadata.deploymentOptions)
	if err != nil {
		return err
	}
	if workerDeployment == nil {
		return nil
	}
	if !c.partitionMgr.engine.config.EnableDeploymentVersions(namespaceEntry.Name().String()) {
		return errMissingDeploymentVersion
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.deploymentRegistrationCh:
		// lock so that only one poll does the update and the rest wait for it
		// using a channel instead of mutex so we can honor the context timeout
	}

	defer func() {
		select {
		// release the lock
		case c.deploymentRegistrationCh <- struct{}{}:
		default:
			c.logger.Error("deploymentRegistrationCh is already unlocked")
		}
	}()

	if c.deploymentVersionRegistered {
		// deployment version already registered
		return nil
	}

	userData, _, err := c.partitionMgr.GetUserDataManager().GetUserData()
	if err != nil {
		return err
	}

	deploymentData := userData.GetData().GetPerType()[int32(c.queue.TaskType())].GetDeploymentData()
	if worker_versioning.FindDeploymentVersion(deploymentData, worker_versioning.DeploymentVersionFromDeployment(workerDeployment)) != -1 {
		// already registered in user data, we can assume the workflow is running.
		// TODO: consider replication scenarios where user data is replicated before
		// the deployment workflow.
		return nil
	}

	// we need to update the deployment workflow to tell it about this task queue
	// TODO: add some backoff here if we got an error last time

	err = c.partitionMgr.engine.workerDeploymentClient.RegisterTaskQueueWorker(
		ctx, namespaceEntry, workerDeployment.SeriesName, workerDeployment.BuildId, c.queue.TaskQueueFamily().Name(), c.queue.TaskType(),
		"matching service")
	if err != nil {
		if common.IsContextDeadlineExceededErr(err) || common.IsContextCanceledErr(err) {
			// error is not from registration, just return it without waiting
			return err
		}
		var errMaxTaskQueuesInVersion workerdeployment.ErrMaxTaskQueuesInVersion
		var errMaxVersionsInDeployment workerdeployment.ErrMaxVersionsInDeployment
		var errMaxDeploymentsInNamespace workerdeployment.ErrMaxDeploymentsInNamespace
		if errors.As(err, &errMaxTaskQueuesInVersion) {
			err = errMaxTaskQueuesInVersion
		} else if errors.As(err, &errMaxVersionsInDeployment) {
			err = errMaxVersionsInDeployment
		} else if errors.As(err, &errMaxDeploymentsInNamespace) {
			err = errMaxDeploymentsInNamespace
		} else {
			// Do not surface low level error to user
			c.logger.Error("error while registering version", tag.Error(err))
			err = errDeploymentVersionNotReady
		}
		// Before retrying the error, hold the poller for some time so it does not retry immediately
		// Parallel polls are already serialized using the lock.
		time.Sleep(deploymentRegisterErrorBackoff)
		return err
	}

	// the deployment workflow will register itself in this task queue's user data.
	// wait for it to propagate here.
	for {
		userData, userDataChanged, err := c.partitionMgr.GetUserDataManager().GetUserData()
		if err != nil {
			return err
		}
		deploymentData := userData.GetData().GetPerType()[int32(c.queue.TaskType())].GetDeploymentData()
		if worker_versioning.FindDeploymentVersion(deploymentData, worker_versioning.DeploymentVersionFromDeployment(workerDeployment)) >= 0 {
			break
		}
		select {
		case <-userDataChanged:
		case <-ctx.Done():
			c.logger.Error("timed out waiting for worker deployment version to appear in user data")
			return ctx.Err()
		}
	}

	c.deploymentVersionRegistered = true
	return nil
}

func (c *physicalTaskQueueManagerImpl) QueueKey() *PhysicalTaskQueueKey {
	return c.queue
}

func (c *physicalTaskQueueManagerImpl) UnloadFromPartitionManager(unloadCause unloadCause) {
	c.partitionMgr.unloadPhysicalQueue(c, unloadCause)
}

func (c *physicalTaskQueueManagerImpl) MakePollerScalingDecision(
	pollStartTime time.Time) *taskqueuepb.PollerScalingDecision {
	return c.makePollerScalingDecisionImpl(pollStartTime, func() *taskqueuepb.TaskQueueStats {
		return aggregateStats(c.GetStatsByPriority())
	})
}

func (c *physicalTaskQueueManagerImpl) makePollerScalingDecisionImpl(
	pollStartTime time.Time,
	statsFn func() *taskqueuepb.TaskQueueStats,
) *taskqueuepb.PollerScalingDecision {
	pollWaitTime := c.partitionMgr.engine.timeSource.Since(pollStartTime)
	// If a poller has waited around a while, we can always suggest a decrease.
	if pollWaitTime >= c.partitionMgr.config.PollerScalingWaitTime() {
		// Decrease if any poll matched after sitting idle for some configured period
		return &taskqueuepb.PollerScalingDecision{
			PollRequestDeltaSuggestion: -1,
		}
	}

	// Avoid spiking pollers crazy fast by limiting how frequently change decisions are issued. Be more permissive when
	// there are more recent pollers.
	numPollers := c.pollerHistory.history.Size()
	if numPollers == 0 {
		numPollers = 1
	}
	if !c.pollerScalingRateLimiter.AllowN(time.Now(), 1e6/numPollers) {
		return nil
	}

	delta := int32(0)
	stats := statsFn()
	if stats.ApproximateBacklogCount > 0 &&
		stats.ApproximateBacklogAge.AsDuration() > c.partitionMgr.config.PollerScalingBacklogAgeScaleUp() {
		// Always increase when there is a backlog, even if we're a partition. It's also important to increase for
		// sticky queues.
		delta = 1
	} else if !c.queue.Partition().IsRoot() {
		// Non-root partitions don't have an appropriate view of the data to make decisions beyond backlog.
		return nil
	} else if (stats.TasksAddRate / stats.TasksDispatchRate) > 1.2 {
		// Increase if we're adding tasks faster than we're dispatching them. Particularly useful for Nexus tasks,
		// since those (currently) don't get backlogged.
		delta = 1
	}

	if delta == 0 {
		return nil
	}
	return &taskqueuepb.PollerScalingDecision{
		PollRequestDeltaSuggestion: delta,
	}
}

func (c *physicalTaskQueueManagerImpl) getOrCreateTaskTracker(
	intervals map[priorityKey]*taskTracker,
	priorityKey priorityKey,
) *taskTracker {
	if priorityKey == 0 {
		priorityKey = c.defaultPriorityKey
	}

	// First try with read lock for the common case where tracker already exists.
	c.taskTrackerLock.RLock()
	if tracker, ok := intervals[priorityKey]; ok {
		c.taskTrackerLock.RUnlock()
		return tracker
	}
	c.taskTrackerLock.RUnlock()

	// Otherwise, we need to maybe create a new tracker with the write lock.
	c.taskTrackerLock.Lock()
	defer c.taskTrackerLock.Unlock()
	if tracker, ok := intervals[priorityKey]; ok {
		return tracker // tracker was created while we were waiting for the lock
	}

	// Initalize all task trackers together; or the timeframes won't line up.
	c.tasksAdded[priorityKey] = newTaskTracker(c.partitionMgr.engine.timeSource)
	c.tasksDispatched[priorityKey] = newTaskTracker(c.partitionMgr.engine.timeSource)

	return intervals[priorityKey]
}

func aggregateStats(stats map[int32]*taskqueuepb.TaskQueueStats) *taskqueuepb.TaskQueueStats {
	result := &taskqueuepb.TaskQueueStats{ApproximateBacklogAge: durationpb.New(0)}
	for _, s := range stats {
		mergeStats(result, s)
	}
	return result
}

func mergeStats(into, from *taskqueuepb.TaskQueueStats) {
	into.ApproximateBacklogCount += from.ApproximateBacklogCount
	into.ApproximateBacklogAge = oldestBacklogAge(into.ApproximateBacklogAge, from.ApproximateBacklogAge)
	into.TasksAddRate += from.TasksAddRate
	into.TasksDispatchRate += from.TasksDispatchRate
}

func oldestBacklogAge(left, right *durationpb.Duration) *durationpb.Duration {
	if left.AsDuration() > right.AsDuration() {
		return left
	}
	return right
}
