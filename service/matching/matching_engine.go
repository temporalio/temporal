package matching

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	deploymentpb "go.temporal.io/api/deployment/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	taskqueuespb "go.temporal.io/server/api/taskqueue/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/client/matching"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	hlc "go.temporal.io/server/common/clock/hybrid_logical_clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/contextutil"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/searchattribute"
	serviceerrors "go.temporal.io/server/common/serviceerror"
	"go.temporal.io/server/common/stream_batcher"
	"go.temporal.io/server/common/tasktoken"
	"go.temporal.io/server/common/testing/testhooks"
	"go.temporal.io/server/common/tqid"
	"go.temporal.io/server/common/util"
	"go.temporal.io/server/common/worker_versioning"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/worker/workerdeployment"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	// If sticky poller is not seem in last 10s, we treat it as sticky worker unavailable
	// This seems aggressive, but the default sticky schedule_to_start timeout is 5s, so 10s seems reasonable.
	stickyPollerUnavailableWindow = 10 * time.Second
	// If a compatible poller hasn't been seen for this time, we fail the CommitBuildId
	// Set to 70s so that it's a little over the max time a poller should be kept waiting.
	versioningPollerSeenWindow        = 70 * time.Second
	recordTaskStartedDefaultTimeout   = 10 * time.Second
	recordTaskStartedSyncMatchTimeout = 1 * time.Second
)

type (
	TaskDispatchRateLimiter quotas.RequestRateLimiter
	pollerIDCtxKey          string
	identityCtxKey          string

	taskQueueCounterKey struct {
		namespaceID   string
		taskType      enumspb.TaskQueueType
		partitionType enumspb.TaskQueueKind
		versioned     string // one of these values: "unversioned", "versionSet", "buildId"
	}

	pollMetadata struct {
		taskQueueMetadata         *taskqueuepb.TaskQueueMetadata
		workerVersionCapabilities *commonpb.WorkerVersionCapabilities
		deploymentOptions         *deploymentpb.WorkerDeploymentOptions
		conditions                *matchingservice.PollConditions
		forwardedFrom             string
		localPollStartTime        time.Time
		workerInstanceKey         string
	}

	userDataUpdate struct {
		taskQueue string
		update    persistence.SingleTaskQueueUserDataUpdate
	}

	gaugeMetrics struct {
		loadedTaskQueueFamilyCount    map[taskQueueCounterKey]int
		loadedTaskQueueCount          map[taskQueueCounterKey]int
		loadedTaskQueuePartitionCount map[taskQueueCounterKey]int
		loadedPhysicalTaskQueueCount  map[taskQueueCounterKey]int
		lock                          sync.Mutex
	}

	// workerPollerTracker tracks cancel funcs by worker instance key for bulk cancellation
	// during worker shutdown. Thread-safe via internal mutex.
	// The inner map uses a UUID key (not pollerID) because pollerID is reused when forwarded.
	workerPollerTracker struct {
		lock    sync.Mutex
		pollers map[string]map[string]context.CancelFunc // workerInstanceKey -> pollerTrackerKey -> cancel
	}

	// Implements matching.Engine
	matchingEngineImpl struct {
		status                        int32
		taskManager                   persistence.TaskManager
		fairTaskManager               persistence.FairTaskManager
		historyClient                 resource.HistoryClient
		matchingRawClient             resource.MatchingRawClient
		workerDeploymentClient        workerdeployment.Client
		tokenSerializer               *tasktoken.Serializer
		historySerializer             serialization.Serializer
		logger                        log.Logger
		throttledLogger               log.ThrottledLogger
		namespaceRegistry             namespace.Registry
		hostInfoProvider              membership.HostInfoProvider
		serviceResolver               membership.ServiceResolver
		membershipChangedCh           chan *membership.ChangedEvent
		clusterMeta                   cluster.Metadata
		timeSource                    clock.TimeSource
		visibilityManager             manager.VisibilityManager
		nexusEndpointClient           *nexusEndpointClient
		nexusEndpointsOwnershipLostCh chan struct{}
		saMapperProvider              searchattribute.MapperProvider
		saProvider                    searchattribute.Provider
		metricsHandler                metrics.Handler
		partitionsLock                sync.RWMutex // locks mutation of partitions
		partitions                    map[tqid.PartitionKey]taskQueuePartitionManager
		gaugeMetrics                  gaugeMetrics // per-namespace task queue counters
		config                        *Config
		versionChecker                headers.VersionChecker
		testHooks                     testhooks.TestHooks
		// queryResults maps query TaskID (which is a UUID generated in QueryWorkflow() call) to a channel
		// that QueryWorkflow() will block on. The channel is unblocked either by worker sending response through
		// RespondQueryTaskCompleted() or through an internal service error causing temporal to be unable to dispatch
		// query task to workflow worker.
		queryResults collection.SyncMap[string, chan *queryResult]
		// nexusResults maps nexus TaskID (which is a UUID generated in the DispatchNexusTask() call) to
		// a channel that DispatchNexusTask() blocks on. The channel is unblocked either by worker responding
		// via RespondNexusTaskCompleted() or RespondNexusTaskFailed(), or through an internal service error.
		nexusResults collection.SyncMap[string, chan *nexusResult]
		// outstandingPollers is needed to keep track of all outstanding pollers for a particular
		// taskqueue.  PollerID generated by frontend is used as the key and CancelFunc is the
		// value.  This is used to cancel the context to unblock any outstanding poller when
		// the frontend detects client connection is closed to prevent tasks being dispatched
		// to zombie pollers.
		outstandingPollers collection.SyncMap[string, context.CancelFunc]
		// workerInstancePollers tracks pollers by worker instance key for bulk cancellation during shutdown.
		workerInstancePollers workerPollerTracker
		// Only set if global namespaces are enabled on the cluster.
		namespaceReplicationQueue persistence.NamespaceReplicationQueue
		// Lock to serialize replication queue updates.
		replicationLock sync.Mutex
		// Serialize and batch user data updates by namespace.
		userDataUpdateBatchers collection.SyncMap[namespace.ID, *stream_batcher.Batcher[*userDataUpdate, error]]
		// Stores results of reachability queries to visibility
		reachabilityCache reachabilityCache
		// Rate limiter to limit the task dispatch
		rateLimiter TaskDispatchRateLimiter
	}
)

// Add registers a poller for a worker instance. Thread-safe.
func (t *workerPollerTracker) Add(workerKey, pollerID string, cancel context.CancelFunc) {
	t.lock.Lock()
	defer t.lock.Unlock()
	util.GetOrSetMap(t.pollers, workerKey)[pollerID] = cancel
}

// Remove unregisters a poller. Cleans up empty worker entries to prevent memory leak. Thread-safe.
func (t *workerPollerTracker) Remove(workerKey, pollerID string) {
	t.lock.Lock()
	defer t.lock.Unlock()
	util.DeleteFromMap(t.pollers, workerKey, pollerID)
}

// CancelAll cancels all pollers for a worker and removes the worker entry. Returns cancelled count. Thread-safe.
func (t *workerPollerTracker) CancelAll(workerKey string) int32 {
	t.lock.Lock()
	pollerCancels := t.pollers[workerKey]
	delete(t.pollers, workerKey)
	t.lock.Unlock()

	// Cancel all pollers for the worker.
	for _, cancel := range pollerCancels {
		cancel()
	}
	return int32(len(pollerCancels))
}

var (
	// EmptyPollWorkflowTaskQueueResponse is the response when there are no workflow tasks to hand out
	emptyPollWorkflowTaskQueueResponse = &matchingservice.PollWorkflowTaskQueueResponseWithRawHistory{}
	// EmptyPollActivityTaskQueueResponse is the response when there are no activity tasks to hand out
	emptyPollActivityTaskQueueResponse = &matchingservice.PollActivityTaskQueueResponse{}

	errNoTasks = errors.New("no tasks")

	pollerIDKey pollerIDCtxKey = "pollerID"
	identityKey identityCtxKey = "identity"

	// The routing key for the single partition used to route Nexus endpoints CRUD RPCs to.
	nexusEndpointsTablePartitionRoutingKey, _ = tqid.MustNormalPartitionFromRpcName("not-applicable", "not-applicable", enumspb.TASK_QUEUE_TYPE_UNSPECIFIED).RoutingKey(0)

	// Options for batching user data updates.
	userDataBatcherOptions = stream_batcher.BatcherOptions{
		MaxItems: 100,
		MinDelay: 100 * time.Millisecond,
		MaxDelay: 500 * time.Millisecond,
		IdleTime: time.Minute,
	}
)

var _ Engine = (*matchingEngineImpl)(nil) // Asserts that interface is indeed implemented

// NewEngine creates an instance of matching engine
func NewEngine(
	taskManager persistence.TaskManager,
	fairTaskManager persistence.FairTaskManager,
	historyClient resource.HistoryClient,
	matchingRawClient resource.MatchingRawClient,
	workerDeploymentClient workerdeployment.Client,
	config *Config,
	logger log.Logger,
	throttledLogger log.ThrottledLogger,
	metricsHandler metrics.Handler,
	namespaceRegistry namespace.Registry,
	hostInfoProvider membership.HostInfoProvider,
	resolver membership.ServiceResolver,
	clusterMeta cluster.Metadata,
	namespaceReplicationQueue persistence.NamespaceReplicationQueue,
	visibilityManager manager.VisibilityManager,
	nexusEndpointManager persistence.NexusEndpointManager,
	testHooks testhooks.TestHooks,
	saProvider searchattribute.Provider,
	saMapperProvider searchattribute.MapperProvider,
	rateLimiter TaskDispatchRateLimiter,
	historySerializer serialization.Serializer,
) Engine {
	scopedMetricsHandler := metricsHandler.WithTags(metrics.OperationTag(metrics.MatchingEngineScope))
	e := &matchingEngineImpl{
		status:                        common.DaemonStatusInitialized,
		taskManager:                   taskManager,
		fairTaskManager:               fairTaskManager,
		historyClient:                 historyClient,
		matchingRawClient:             matchingRawClient,
		tokenSerializer:               tasktoken.NewSerializer(),
		workerDeploymentClient:        workerDeploymentClient,
		historySerializer:             historySerializer,
		logger:                        log.With(logger, tag.ComponentMatchingEngine),
		throttledLogger:               log.With(throttledLogger, tag.ComponentMatchingEngine),
		namespaceRegistry:             namespaceRegistry,
		hostInfoProvider:              hostInfoProvider,
		serviceResolver:               resolver,
		membershipChangedCh:           make(chan *membership.ChangedEvent, 1), // allow one signal to be buffered while we're working
		clusterMeta:                   clusterMeta,
		timeSource:                    clock.NewRealTimeSource(), // No need to mock this at the moment
		visibilityManager:             visibilityManager,
		nexusEndpointClient:           newEndpointClient(config.NexusEndpointsRefreshInterval, nexusEndpointManager),
		nexusEndpointsOwnershipLostCh: make(chan struct{}),
		saProvider:                    saProvider,
		saMapperProvider:              saMapperProvider,
		metricsHandler:                scopedMetricsHandler,
		partitions:                    make(map[tqid.PartitionKey]taskQueuePartitionManager),
		gaugeMetrics: gaugeMetrics{
			loadedTaskQueueFamilyCount:    make(map[taskQueueCounterKey]int),
			loadedTaskQueueCount:          make(map[taskQueueCounterKey]int),
			loadedTaskQueuePartitionCount: make(map[taskQueueCounterKey]int),
			loadedPhysicalTaskQueueCount:  make(map[taskQueueCounterKey]int),
		},
		config:                    config,
		versionChecker:            headers.NewDefaultVersionChecker(),
		testHooks:                 testHooks,
		queryResults:              collection.NewSyncMap[string, chan *queryResult](),
		nexusResults:              collection.NewSyncMap[string, chan *nexusResult](),
		outstandingPollers:        collection.NewSyncMap[string, context.CancelFunc](),
		workerInstancePollers:     workerPollerTracker{pollers: make(map[string]map[string]context.CancelFunc)},
		namespaceReplicationQueue: namespaceReplicationQueue,
		userDataUpdateBatchers:    collection.NewSyncMap[namespace.ID, *stream_batcher.Batcher[*userDataUpdate, error]](),
		rateLimiter:               rateLimiter,
	}
	e.reachabilityCache = newReachabilityCache(
		metrics.NoopMetricsHandler,
		visibilityManager,
		e.config.ReachabilityCacheOpenWFsTTL(),
		e.config.ReachabilityCacheClosedWFsTTL())
	return e
}

func (e *matchingEngineImpl) Start() {
	if !atomic.CompareAndSwapInt32(
		&e.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	go e.watchMembership()
	_ = e.serviceResolver.AddListener(e.listenerKey(), e.membershipChangedCh)
}

func (e *matchingEngineImpl) Stop() {
	if !atomic.CompareAndSwapInt32(
		&e.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	_ = e.serviceResolver.RemoveListener(e.listenerKey())
	close(e.membershipChangedCh)

	e.nexusEndpointClient.notifyOwnershipChanged(false)

	for _, l := range e.getTaskQueuePartitions(math.MaxInt32) {
		l.Stop(unloadCauseShuttingDown)
	}
}

func (e *matchingEngineImpl) listenerKey() string {
	return fmt.Sprintf("matchingEngine[%p]", e)
}

func (e *matchingEngineImpl) watchMembership() {
	self := e.hostInfoProvider.HostInfo().Identity()
	rc, ok := e.matchingRawClient.(matching.RoutingClient)
	if !ok {
		e.logger.Warn("watchMembership found non-routing matching client")
		return // this should only happen in unit tests
	}
	ownedByOther := func(p tqid.Partition) bool {
		addr, err := rc.Route(p)
		// don't take action on lookup error
		return err == nil && addr != self
	}

	for range e.membershipChangedCh {
		delay := e.config.MembershipUnloadDelay()
		if delay == 0 {
			continue
		}

		e.notifyNexusEndpointsOwnershipChange()

		// Check all our loaded partitions to see if we lost ownership of any of them.
		e.partitionsLock.RLock()
		partitions := make([]tqid.Partition, 0, len(e.partitions))
		for _, pm := range e.partitions {
			partitions = append(partitions, pm.Partition())
		}
		e.partitionsLock.RUnlock()

		partitions = util.FilterSlice(partitions, ownedByOther)

		const batchSize = 100
		for i := 0; i < len(partitions); i += batchSize {
			// We don't own these anymore, but don't unload them immediately, wait a few seconds to ensure
			// the membership update has propagated everywhere so that they won't get immediately re-loaded.
			// Note that we don't verify ownership at load time, so this is the only guard against a task
			// queue bouncing back and forth due to long membership propagation time.
			batch := partitions[i:min(len(partitions), i+batchSize)]
			wait := backoff.Jitter(delay, 0.1)
			time.AfterFunc(wait, func() {
				// maybe the whole engine stopped
				if atomic.LoadInt32(&e.status) != common.DaemonStatusStarted {
					return
				}
				for _, p := range batch {
					// maybe ownership changed again
					if !ownedByOther(p) {
						return
					}
					// now we can unload
					e.unloadTaskQueuePartitionByKey(p, nil, unloadCauseMembership)
				}
			})
		}
	}
}

func (e *matchingEngineImpl) getTaskQueuePartitions(maxCount int) (lists []taskQueuePartitionManager) {
	e.partitionsLock.RLock()
	defer e.partitionsLock.RUnlock()
	lists = make([]taskQueuePartitionManager, 0, len(e.partitions))
	count := 0
	for _, tlMgr := range e.partitions {
		lists = append(lists, tlMgr)
		count++
		if count >= maxCount {
			break
		}
	}
	return
}

// Returns taskQueuePartitionManager for a task queue. If not already cached, and create is true, tries
// to get new range from DB and create one. This blocks (up to the context deadline) for the
// task queue to be initialized.
//
// Note that task queue kind (sticky vs normal) and normal name for sticky task queues is not used as
// part of the task queue identity. That means that if getTaskQueuePartitionManager
// is called twice with the same task queue but different sticky info, the
// properties of the taskQueuePartitionManager will depend on which call came first. In general, we can
// rely on kind being the same for all calls now, but normalName was a later addition to the
// protocol and is not always set consistently. normalName is only required when using
// versioning, and SDKs that support versioning will always set it. The current server version
// will also set it when adding tasks from history. So that particular inconsistency is okay.
func (e *matchingEngineImpl) getTaskQueuePartitionManager(
	ctx context.Context,
	partition tqid.Partition,
	create bool,
	loadCause loadCause,
) (retPM taskQueuePartitionManager, retCreated bool, retErr error) {
	var newPM *taskQueuePartitionManagerImpl

	defer func() {
		if retErr != nil || retPM == nil {
			return
		}

		if retErr = retPM.WaitUntilInitialized(ctx); retErr != nil {
			e.unloadTaskQueuePartition(retPM, unloadCauseInitError)
			return
		}

		if retCreated {
			// Whenever a root partition is loaded, we need to force all child partitions to load.
			// If there is a backlog of tasks on any child partitions, force loading will ensure
			// that they can forward their tasks the poller which caused the root partition to be
			// loaded. These partitions could be managed by this matchingEngineImpl, but are most
			// likely not. We skip checking and just make gRPC requests to force loading them all.
			// Note that if retCreated is true, retPM must be newPM, so we can use newPM here.
			newPM.ForceLoadAllChildPartitions()
		}
	}()

	key := partition.Key()
	e.partitionsLock.RLock()
	pm, ok := e.partitions[key]
	e.partitionsLock.RUnlock()
	if ok {
		return pm, false, nil
	}

	if !create {
		return nil, false, nil
	}

	namespaceEntry, err := e.namespaceRegistry.GetNamespaceByID(namespace.ID(partition.NamespaceId()))
	if err != nil {
		return nil, false, err
	}

	tqConfig := newTaskQueueConfig(partition.TaskQueue(), e.config, namespaceEntry.Name())
	tqConfig.loadCause = loadCause
	logger, throttledLogger, metricsHandler := e.loggerAndMetricsForPartition(namespaceEntry, partition, tqConfig)
	onFatalErr := func(cause unloadCause) { newPM.unloadFromEngine(cause) }
	onUserDataChanged := func(to *persistencespb.VersionedTaskQueueUserData) { newPM.userDataChanged(to) }
	onEphemeralDataChanged := func(data *taskqueuespb.EphemeralData) { newPM.ephemeralDataChanged(data) }
	userDataManager := newUserDataManager(
		e.taskManager,
		e.matchingRawClient,
		onFatalErr,
		onUserDataChanged,
		onEphemeralDataChanged,
		partition,
		tqConfig,
		logger,
		e.namespaceRegistry,
	)
	newPM, err = newTaskQueuePartitionManager(
		e,
		namespaceEntry,
		partition,
		tqConfig,
		logger,
		throttledLogger,
		metricsHandler,
		userDataManager,
	)
	if err != nil {
		return nil, false, err
	}

	// If it gets here, write lock and check again in case a task queue is created between the two locks
	e.partitionsLock.Lock()
	pm, ok = e.partitions[key]
	if ok {
		e.partitionsLock.Unlock()
		return pm, false, nil
	}

	e.partitions[key] = newPM
	e.partitionsLock.Unlock()

	newPM.Start()
	return newPM, true, nil
}

func (e *matchingEngineImpl) loggerAndMetricsForPartition(
	nsEntry *namespace.Namespace,
	partition tqid.Partition,
	tqConfig *taskQueueConfig,
) (log.Logger, log.Logger, metrics.Handler) {
	nsName := nsEntry.Name().String()
	var nsState string
	if nsEntry.ActiveInCluster(e.clusterMeta.GetCurrentClusterName()) {
		nsState = metrics.ActiveNamespaceStateTagValue
	} else {
		nsState = metrics.PassiveNamespaceStateTagValue
	}
	logger := log.With(e.logger,
		tag.WorkflowTaskQueueName(partition.RpcName()),
		tag.WorkflowTaskQueueType(partition.TaskType()),
		tag.WorkflowNamespace(nsName))
	throttledLogger := log.With(e.throttledLogger,
		tag.WorkflowTaskQueueName(partition.RpcName()),
		tag.WorkflowTaskQueueType(partition.TaskType()),
		tag.WorkflowNamespace(nsName))
	metricsHandler := metrics.GetPerTaskQueuePartitionIDScope(
		e.metricsHandler,
		nsName,
		partition,
		tqConfig.BreakdownMetricsByTaskQueue(),
		tqConfig.BreakdownMetricsByPartition(),
		metrics.OperationTag(metrics.MatchingTaskQueuePartitionManagerScope),
	).WithTags(metrics.NamespaceStateTag(nsState))
	return logger, throttledLogger, metricsHandler
}

// For use in tests
func (e *matchingEngineImpl) updateTaskQueue(partition tqid.Partition, mgr taskQueuePartitionManager) {
	e.partitionsLock.Lock()
	defer e.partitionsLock.Unlock()
	e.partitions[partition.Key()] = mgr
}

func (e *matchingEngineImpl) AddWorkflowTask(
	ctx context.Context,
	addRequest *matchingservice.AddWorkflowTaskRequest,
) (buildId string, syncMatch bool, err error) {
	partition, err := tqid.PartitionFromProto(addRequest.TaskQueue, addRequest.NamespaceId, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return "", false, err
	}

	sticky := partition.Kind() == enumspb.TASK_QUEUE_KIND_STICKY
	// do not load sticky task queue if it is not already loaded, which means it has no poller.
	pm, _, err := e.getTaskQueuePartitionManager(ctx, partition, !sticky, loadCauseTask)
	if err != nil {
		return "", false, err
	} else if sticky && !stickyWorkerAvailable(pm) {
		return "", false, serviceerrors.NewStickyWorkerUnavailable()
	}

	// This needs to move to history see - https://go.temporal.io/server/issues/181
	var expirationTime *timestamppb.Timestamp
	now := time.Now().UTC()
	expirationDuration := addRequest.GetScheduleToStartTimeout().AsDuration()
	if expirationDuration != 0 {
		expirationTime = timestamppb.New(now.Add(expirationDuration))
	}
	taskInfo := &persistencespb.TaskInfo{
		NamespaceId:      addRequest.NamespaceId,
		RunId:            addRequest.Execution.GetRunId(),
		WorkflowId:       addRequest.Execution.GetWorkflowId(),
		ScheduledEventId: addRequest.GetScheduledEventId(),
		Clock:            addRequest.GetClock(),
		ExpiryTime:       expirationTime,
		CreateTime:       timestamppb.New(now),
		VersionDirective: addRequest.VersionDirective,
		Stamp:            addRequest.Stamp,
		Priority:         addRequest.Priority,
	}

	return pm.AddTask(ctx, addTaskParams{
		taskInfo:    taskInfo,
		forwardInfo: addRequest.ForwardInfo,
	})
}

// AddActivityTask either delivers task directly to waiting poller or save it into task queue persistence.
func (e *matchingEngineImpl) AddActivityTask(
	ctx context.Context,
	addRequest *matchingservice.AddActivityTaskRequest,
) (buildId string, syncMatch bool, err error) {
	partition, err := tqid.PartitionFromProto(addRequest.TaskQueue, addRequest.GetNamespaceId(), enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	if err != nil {
		return "", false, err
	}
	pm, _, err := e.getTaskQueuePartitionManager(ctx, partition, true, loadCauseTask)
	if err != nil {
		return "", false, err
	}

	var expirationTime *timestamppb.Timestamp
	now := time.Now().UTC()
	expirationDuration := timestamp.DurationValue(addRequest.GetScheduleToStartTimeout())
	if expirationDuration != 0 {
		expirationTime = timestamppb.New(now.Add(expirationDuration))
	}
	taskInfo := &persistencespb.TaskInfo{
		NamespaceId:      addRequest.NamespaceId,
		RunId:            addRequest.Execution.GetRunId(),
		WorkflowId:       addRequest.Execution.GetWorkflowId(),
		ScheduledEventId: addRequest.GetScheduledEventId(),
		Clock:            addRequest.GetClock(),
		CreateTime:       timestamppb.New(now),
		ExpiryTime:       expirationTime,
		VersionDirective: addRequest.VersionDirective,
		Stamp:            addRequest.Stamp,
		Priority:         addRequest.Priority,
		ComponentRef:     addRequest.ComponentRef,
	}

	return pm.AddTask(ctx, addTaskParams{
		taskInfo:    taskInfo,
		forwardInfo: addRequest.ForwardInfo,
	})
}

// PollWorkflowTaskQueue tries to get the workflow task using exponential backoff.
func (e *matchingEngineImpl) PollWorkflowTaskQueue(
	ctx context.Context,
	req *matchingservice.PollWorkflowTaskQueueRequest,
	opMetrics metrics.Handler,
) (*matchingservice.PollWorkflowTaskQueueResponseWithRawHistory, error) {
	namespaceID := namespace.ID(req.GetNamespaceId())
	pollerID := req.GetPollerId()
	request := req.PollRequest
	taskQueueName := request.TaskQueue.GetName()

	// Namespace field is not populated for forwarded requests.
	if len(request.Namespace) == 0 {
		ns, err := e.namespaceRegistry.GetNamespaceName(namespace.ID(req.GetNamespaceId()))
		if err != nil {
			return nil, err
		}
		request.Namespace = ns.String()
	}

pollLoop:
	for {
		err := common.IsValidContext(ctx)
		if err != nil {
			return nil, err
		}
		// Add frontend generated pollerID to context so taskqueueMgr can support cancellation of
		// long-poll when frontend calls CancelOutstandingPoll API
		pollerCtx := context.WithValue(ctx, pollerIDKey, pollerID)
		pollerCtx = context.WithValue(pollerCtx, identityKey, request.GetIdentity())
		partition, err := tqid.PartitionFromProto(request.TaskQueue, req.NamespaceId, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
		if err != nil {
			return nil, err
		}
		pollMetadata := &pollMetadata{
			workerVersionCapabilities: request.WorkerVersionCapabilities,
			deploymentOptions:         request.DeploymentOptions,
			forwardedFrom:             req.ForwardedSource,
			conditions:                req.Conditions,
			workerInstanceKey:         request.WorkerInstanceKey,
		}
		task, versionSetUsed, err := e.pollTask(pollerCtx, partition, pollMetadata)
		if err != nil {
			if errors.Is(err, errNoTasks) {
				return emptyPollWorkflowTaskQueueResponse, nil
			}
			return nil, err
		}
		if task.isStarted() {
			// tasks received from remote are already started. So, simply forward the response
			return e.convertPollWorkflowTaskQueueResponse(task.pollWorkflowTaskQueueResponse(), task.namespace)
		}

		if task.isQuery() {
			task.finish(nil, true) // this only means query task sync match succeed.

			// for query task, we don't need to update history to record workflow task started. but we need to know
			// the NextEventID and the currently set sticky task queue.
			// TODO: in theory we only need this lookup for non-sticky queries (to get NextEventID for populating
			//		partial history in the response), but we need a new history API to determine whether the query
			//		is sticky or not without this call
			mutableStateResp, err := e.historyClient.GetMutableState(ctx, &historyservice.GetMutableStateRequest{
				NamespaceId: req.GetNamespaceId(),
				Execution:   task.workflowExecution(),
			})
			if err != nil {
				// will notify query client that the query task failed
				_ = e.deliverQueryResult(task.query.taskID, &queryResult{internalError: err})
				return emptyPollWorkflowTaskQueueResponse, nil
			}

			// A non-sticky poll may get task for a workflow that has sticky still set in its mutable state after
			// their sticky worker is dead for longer than 10s. In such case, we should set this to false so that
			// we return full history.
			isStickyEnabled := taskQueueName == mutableStateResp.StickyTaskQueue.GetName()

			hist, rawHistoryBytes, nextPageToken, err := e.getHistoryForQueryTask(ctx, namespaceID, task, isStickyEnabled)

			if err != nil {
				// will notify query client that the query task failed
				_ = e.deliverQueryResult(task.query.taskID, &queryResult{internalError: err})
				return emptyPollWorkflowTaskQueueResponse, nil
			}

			resp := &historyservice.RecordWorkflowTaskStartedResponse{
				PreviousStartedEventId:     mutableStateResp.PreviousStartedEventId,
				NextEventId:                mutableStateResp.NextEventId,
				WorkflowType:               mutableStateResp.WorkflowType,
				StickyExecutionEnabled:     isStickyEnabled,
				WorkflowExecutionTaskQueue: mutableStateResp.TaskQueue,
				BranchToken:                mutableStateResp.CurrentBranchToken,
				StartedEventId:             common.EmptyEventID,
				Attempt:                    1,
				History:                    hist,
				RawHistoryBytes:            rawHistoryBytes,
				NextPageToken:              nextPageToken,
			}

			return e.createPollWorkflowTaskQueueResponse(task, resp, opMetrics), nil
		}

		requestClone := request
		if versionSetUsed {
			// We remove build ID from workerVersionCapabilities so History can differentiate between
			// old and new versioning in Record*TaskStart.
			// TODO: remove this block after old versioning cleanup. [cleanup-old-wv]
			requestClone = common.CloneProto(request)
			requestClone.WorkerVersionCapabilities.BuildId = ""
		}
		resp, err := e.recordWorkflowTaskStarted(ctx, requestClone, task)
		if err != nil {
			switch err := err.(type) {
			case *serviceerror.Internal, *serviceerror.DataLoss:
				e.nonRetryableErrorsDropTask(task, taskQueueName, err)
				// drop the task as otherwise task would be stuck in a retry-loop
				task.finish(nil, false)
			case *serviceerror.NotFound: // mutable state not found, workflow not running or workflow task not found
				e.logger.Info("Workflow task not found",
					tag.WorkflowTaskQueueName(taskQueueName),
					tag.WorkflowNamespaceID(task.event.Data.GetNamespaceId()),
					tag.WorkflowID(task.event.Data.GetWorkflowId()),
					tag.WorkflowRunID(task.event.Data.GetRunId()),
					tag.TaskID(task.event.GetTaskId()),
					tag.TaskVisibilityTimestamp(timestamp.TimeValue(task.event.Data.GetCreateTime())),
					tag.WorkflowEventID(task.event.Data.GetScheduledEventId()),
					tag.Error(err),
				)
				task.finish(nil, false)
			case *serviceerrors.TaskAlreadyStarted:
				e.logger.Debug("Duplicated workflow task", tag.WorkflowTaskQueueName(taskQueueName), tag.TaskID(task.event.GetTaskId()))
				task.finish(nil, false)
			case *serviceerrors.ObsoleteDispatchBuildId:
				// history should've scheduled another task on the right build ID. dropping this one.
				e.logger.Info("dropping workflow task due to invalid build ID",
					tag.WorkflowTaskQueueName(taskQueueName),
					tag.WorkflowNamespaceID(task.event.Data.GetNamespaceId()),
					tag.WorkflowID(task.event.Data.GetWorkflowId()),
					tag.WorkflowRunID(task.event.Data.GetRunId()),
					tag.TaskID(task.event.GetTaskId()),
					tag.TaskVisibilityTimestamp(timestamp.TimeValue(task.event.Data.GetCreateTime())),
					tag.BuildId(requestClone.WorkerVersionCapabilities.GetBuildId()),
				)
				task.finish(nil, false)
			case *serviceerrors.ObsoleteMatchingTask:
				// History should've scheduled another task on the right task queue and deployment.
				// Dropping this one.
				e.logger.Info("dropping obsolete workflow task",
					tag.WorkflowTaskQueueName(taskQueueName),
					tag.WorkflowNamespaceID(task.event.Data.GetNamespaceId()),
					tag.WorkflowID(task.event.Data.GetWorkflowId()),
					tag.WorkflowRunID(task.event.Data.GetRunId()),
					tag.TaskID(task.event.GetTaskId()),
					tag.WorkflowScheduledEventID(task.event.Data.GetScheduledEventId()),
					tag.TaskVisibilityTimestamp(timestamp.TimeValue(task.event.Data.GetCreateTime())),
					tag.VersioningBehavior(task.event.Data.VersionDirective.GetBehavior()),
					//nolint:staticcheck // SA1019 deprecated WorkerVersionCapabilities will clean up later
					tag.Deployment(worker_versioning.DeploymentNameFromCapabilities(requestClone.WorkerVersionCapabilities, requestClone.DeploymentOptions)),
					//nolint:staticcheck // SA1019 deprecated WorkerVersionCapabilities will clean up later
					tag.BuildId(worker_versioning.BuildIdFromCapabilities(requestClone.WorkerVersionCapabilities, requestClone.DeploymentOptions)),
					tag.Error(err),
				)
				task.finish(nil, false)
			case *serviceerror.ResourceExhausted:
				// If history returns one ResourceExhausted, it's likely to return more if we retry
				// immediately. Instead, return the error to the client which will back off.
				// BUSY_WORKFLOW is limited to one workflow and is okay to retry.
				task.finish(err, false)
				if err.Cause != enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW {
					return nil, err
				}
			default:
				task.finish(err, false)
				if err.Error() == common.ErrNamespaceHandover.Error() {
					// do not keep polling new tasks when namespace is in handover state
					// as record start request will be rejected by history service
					return nil, err
				}
			}

			continue pollLoop
		}

		task.finish(nil, true)
		return e.createPollWorkflowTaskQueueResponse(task, resp, opMetrics), nil
	}
}

// getHistoryForQueryTask retrieves history associated with a query task returned
// by PollWorkflowTaskQueue. Returns empty history for sticky query and full history for non-sticky.
// Returns either deserialized history OR raw history bytes (not both).
// When SendRawHistoryBytesToMatchingService is enabled, it uses GetWorkflowExecutionRawHistory API
// to get raw bytes and passes them through to frontend without deserializing.
func (e *matchingEngineImpl) getHistoryForQueryTask(
	ctx context.Context,
	nsID namespace.ID,
	task *internalTask,
	isStickyEnabled bool,
) (*historypb.History, [][]byte, []byte, error) {
	if isStickyEnabled {
		return &historypb.History{Events: []*historypb.HistoryEvent{}}, nil, nil, nil
	}

	maxPageSize := int32(e.config.HistoryMaxPageSize(task.namespace.String()))

	// When SendRawHistoryBytesToMatchingService is enabled, use GetWorkflowExecutionRawHistory API
	// to get raw bytes and pass them through to frontend without deserializing in matching service.
	if e.config.SendRawHistoryBytesToMatchingService() {
		resp, err := e.historyClient.GetWorkflowExecutionRawHistory(ctx,
			&historyservice.GetWorkflowExecutionRawHistoryRequest{
				NamespaceId: nsID.String(),
				Request: &adminservice.GetWorkflowExecutionRawHistoryRequest{
					NamespaceId:       nsID.String(),
					Execution:         task.workflowExecution(),
					StartEventId:      common.FirstEventID,
					StartEventVersion: common.EmptyVersion,
					EndEventId:        common.EmptyEventID,
					EndEventVersion:   common.EmptyVersion,
					MaximumPageSize:   maxPageSize,
				},
			})
		if err != nil {
			return nil, nil, nil, err
		}

		// Extract raw bytes from HistoryBatches
		historyBatches := resp.GetResponse().GetHistoryBatches()
		rawHistoryBytes := make([][]byte, len(historyBatches))
		for i, blob := range historyBatches {
			rawHistoryBytes[i] = blob.Data
		}
		return nil, rawHistoryBytes, resp.GetResponse().GetNextPageToken(), nil
	}

	// Use regular GetWorkflowExecutionHistory API
	resp, err := e.historyClient.GetWorkflowExecutionHistory(ctx,
		&historyservice.GetWorkflowExecutionHistoryRequest{
			NamespaceId: nsID.String(),
			Request: &workflowservice.GetWorkflowExecutionHistoryRequest{
				Namespace:       task.namespace.String(),
				Execution:       task.workflowExecution(),
				MaximumPageSize: maxPageSize,
				WaitNewEvent:    false,
				SkipArchival:    true,
			},
		})
	if err != nil {
		return nil, nil, nil, err
	}

	// History service can send history events in response.History.Events. In that case use that directly.
	// This happens when history.sendRawHistoryBetweenInternalServices is enabled.
	ns, err := e.namespaceRegistry.GetNamespaceName(nsID)
	if err != nil {
		return nil, nil, nil, err
	}
	err = api.ProcessInternalRawHistory(
		ctx,
		e.saProvider,
		e.saMapperProvider,
		resp,
		e.visibilityManager,
		e.versionChecker,
		ns,
		false,
	)
	if err != nil {
		return nil, nil, nil, err
	}

	hist := resp.GetResponse().GetHistory()
	if resp.GetResponse().GetRawHistory() != nil {
		historyEvents := make([]*historypb.HistoryEvent, 0, maxPageSize)
		for _, blob := range resp.GetResponse().GetRawHistory() {
			events, err := e.historySerializer.DeserializeEvents(blob)
			if err != nil {
				return nil, nil, nil, err
			}
			historyEvents = append(historyEvents, events...)
		}
		hist = &historypb.History{Events: historyEvents}
	}

	return hist, nil, resp.GetResponse().GetNextPageToken(), nil
}

func (e *matchingEngineImpl) nonRetryableErrorsDropTask(task *internalTask, taskQueueName string, err error) {
	e.logger.Error("dropping task due to non-nonretryable errors",
		tag.WorkflowNamespace(task.namespace.String()),
		tag.WorkflowNamespaceID(task.event.Data.GetNamespaceId()),
		tag.WorkflowID(task.event.Data.GetWorkflowId()),
		tag.WorkflowRunID(task.event.Data.GetRunId()),
		tag.WorkflowTaskQueueName(taskQueueName),
		tag.TaskID(task.event.GetTaskId()),
		tag.WorkflowScheduledEventID(task.event.Data.GetScheduledEventId()),
		tag.Error(err),
		tag.ErrorType(err),
	)

	metrics.NonRetryableTasks.With(e.metricsHandler).Record(1, metrics.ServiceErrorTypeTag(err))
}

// PollActivityTaskQueue takes one task from the task manager, update workflow execution history, mark task as
// completed and return it to user. If a task from task manager is already started, return an empty response, without
// error. Timeouts handled by the timer queue.
func (e *matchingEngineImpl) PollActivityTaskQueue(
	ctx context.Context,
	req *matchingservice.PollActivityTaskQueueRequest,
	opMetrics metrics.Handler,
) (*matchingservice.PollActivityTaskQueueResponse, error) {
	pollerID := req.GetPollerId()
	request := req.PollRequest
	taskQueueName := request.TaskQueue.GetName()

	// Namespace field is not populated for forwarded requests.
	if len(request.Namespace) == 0 {
		ns, err := e.namespaceRegistry.GetNamespaceName(namespace.ID(req.GetNamespaceId()))
		if err != nil {
			return nil, err
		}
		request.Namespace = ns.String()
	}

pollLoop:
	for {
		err := common.IsValidContext(ctx)
		if err != nil {
			return nil, err
		}

		partition, err := tqid.PartitionFromProto(request.TaskQueue, req.NamespaceId, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
		if err != nil {
			return nil, err
		}

		// Add frontend generated pollerID to context so taskqueueMgr can support cancellation of
		// long-poll when frontend calls CancelOutstandingPoll API
		pollerCtx := context.WithValue(ctx, pollerIDKey, pollerID)
		pollerCtx = context.WithValue(pollerCtx, identityKey, request.GetIdentity())
		pollMetadata := &pollMetadata{
			taskQueueMetadata:         request.TaskQueueMetadata,
			workerVersionCapabilities: request.WorkerVersionCapabilities,
			deploymentOptions:         request.DeploymentOptions,
			forwardedFrom:             req.ForwardedSource,
			conditions:                req.Conditions,
			workerInstanceKey:         request.WorkerInstanceKey,
		}
		task, versionSetUsed, err := e.pollTask(pollerCtx, partition, pollMetadata)
		if err != nil {
			if errors.Is(err, errNoTasks) {
				return emptyPollActivityTaskQueueResponse, nil
			}
			return nil, err
		}

		if task.isStarted() {
			// tasks received from remote are already started. So, simply forward the response
			return task.pollActivityTaskQueueResponse(), nil
		}
		requestClone := request
		if versionSetUsed {
			// We remove build ID from workerVersionCapabilities so History can differentiate between
			// old and new versioning in Record*TaskStart.
			// TODO: remove this block after old versioning cleanup. [cleanup-old-wv]
			requestClone = common.CloneProto(request)
			requestClone.WorkerVersionCapabilities.BuildId = ""
		}
		resp, err := e.recordActivityTaskStarted(ctx, requestClone, task)
		if err != nil {
			switch err := err.(type) {
			case *serviceerror.Internal, *serviceerror.DataLoss:
				e.nonRetryableErrorsDropTask(task, taskQueueName, err)
				// drop the task as otherwise task would be stuck in a retry-loop
				task.finish(nil, false)
			case *serviceerror.NotFound: // mutable state not found, workflow not running or activity info not found
				e.logger.Info("Activity task not found",
					tag.WorkflowNamespaceID(task.event.Data.GetNamespaceId()),
					tag.WorkflowID(task.event.Data.GetWorkflowId()),
					tag.WorkflowRunID(task.event.Data.GetRunId()),
					tag.WorkflowTaskQueueName(taskQueueName),
					tag.TaskID(task.event.GetTaskId()),
					tag.TaskVisibilityTimestamp(timestamp.TimeValue(task.event.Data.GetCreateTime())),
					tag.WorkflowEventID(task.event.Data.GetScheduledEventId()),
					tag.Error(err),
				)
				task.finish(nil, false)
			case *serviceerrors.TaskAlreadyStarted:
				e.logger.Debug("Duplicated activity task", tag.WorkflowTaskQueueName(taskQueueName), tag.TaskID(task.event.GetTaskId()))
				task.finish(nil, false)
			case *serviceerrors.ObsoleteDispatchBuildId:
				// history should've scheduled another task on the right build ID. dropping this one.
				e.logger.Info("dropping activity task due to invalid build ID",
					tag.WorkflowTaskQueueName(taskQueueName),
					tag.WorkflowNamespaceID(task.event.Data.GetNamespaceId()),
					tag.WorkflowID(task.event.Data.GetWorkflowId()),
					tag.WorkflowRunID(task.event.Data.GetRunId()),
					tag.TaskID(task.event.GetTaskId()),
					tag.TaskVisibilityTimestamp(timestamp.TimeValue(task.event.Data.GetCreateTime())),
					tag.BuildId(requestClone.WorkerVersionCapabilities.GetBuildId()),
				)
				task.finish(nil, false)
			case *serviceerrors.ObsoleteMatchingTask:
				// History should've scheduled another task on the right task queue and deployment.
				// Dropping this one.
				e.logger.Info("dropping obsolete activity task",
					tag.WorkflowTaskQueueName(taskQueueName),
					tag.WorkflowNamespaceID(task.event.Data.GetNamespaceId()),
					tag.WorkflowID(task.event.Data.GetWorkflowId()),
					tag.WorkflowRunID(task.event.Data.GetRunId()),
					tag.TaskID(task.event.GetTaskId()),
					tag.WorkflowScheduledEventID(task.event.Data.GetScheduledEventId()),
					tag.TaskVisibilityTimestamp(timestamp.TimeValue(task.event.Data.GetCreateTime())),
					tag.VersioningBehavior(task.event.Data.VersionDirective.GetBehavior()),
					//nolint:staticcheck // SA1019 deprecated WorkerVersionCapabilities will clean up later
					tag.Deployment(worker_versioning.DeploymentNameFromCapabilities(requestClone.WorkerVersionCapabilities, requestClone.DeploymentOptions)),
					//nolint:staticcheck // SA1019 deprecated WorkerVersionCapabilities will clean up later
					tag.BuildId(worker_versioning.BuildIdFromCapabilities(requestClone.WorkerVersionCapabilities, requestClone.DeploymentOptions)),
					tag.Error(err),
				)
				task.finish(nil, false)
			case *serviceerrors.ActivityStartDuringTransition:
				// History will schedule another task once transition ends. Dropping this one.
				e.logger.Info("dropping activity task during transition",
					tag.WorkflowTaskQueueName(taskQueueName),
					tag.WorkflowNamespaceID(task.event.Data.GetNamespaceId()),
					tag.WorkflowID(task.event.Data.GetWorkflowId()),
					tag.WorkflowRunID(task.event.Data.GetRunId()),
					tag.TaskID(task.event.GetTaskId()),
					tag.WorkflowScheduledEventID(task.event.Data.GetScheduledEventId()),
					tag.TaskVisibilityTimestamp(timestamp.TimeValue(task.event.Data.GetCreateTime())),
					tag.VersioningBehavior(task.event.Data.VersionDirective.GetBehavior()),
					//nolint:staticcheck // SA1019 deprecated WorkerVersionCapabilities will clean up later
					tag.Deployment(worker_versioning.DeploymentNameFromCapabilities(requestClone.WorkerVersionCapabilities, requestClone.DeploymentOptions)),
					//nolint:staticcheck // SA1019 deprecated WorkerVersionCapabilities will clean up later
					tag.BuildId(worker_versioning.BuildIdFromCapabilities(requestClone.WorkerVersionCapabilities, requestClone.DeploymentOptions)),
				)
				task.finish(nil, false)
			case *serviceerror.ResourceExhausted:
				// If history returns one ResourceExhausted, it's likely to return more if we retry
				// immediately. Instead, return the error to the client which will back off.
				// BUSY_WORKFLOW is limited to one workflow and is okay to retry.
				task.finish(err, false)
				if err.Cause != enumspb.RESOURCE_EXHAUSTED_CAUSE_BUSY_WORKFLOW {
					return nil, err
				}
			default:
				task.finish(err, false)
				if err.Error() == common.ErrNamespaceHandover.Error() {
					// do not keep polling new tasks when namespace is in handover state
					// as record start request will be rejected by history service
					return nil, err
				}
			}

			continue pollLoop
		}
		task.finish(nil, true)
		return e.createPollActivityTaskQueueResponse(task, resp, opMetrics), nil
	}
}

type queryResult struct {
	workerResponse *matchingservice.RespondQueryTaskCompletedRequest
	internalError  error
}

// QueryWorkflow creates a WorkflowTask with query data, send it through sync match channel, wait for that WorkflowTask
// to be processed by worker, and then return the query result.
func (e *matchingEngineImpl) QueryWorkflow(
	ctx context.Context,
	queryRequest *matchingservice.QueryWorkflowRequest,
) (*matchingservice.QueryWorkflowResponse, error) {
	partition, err := tqid.PartitionFromProto(queryRequest.TaskQueue, queryRequest.GetNamespaceId(), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}
	sticky := partition.Kind() == enumspb.TASK_QUEUE_KIND_STICKY
	// do not load sticky task queue if it is not already loaded, which means it has no poller.
	pm, _, err := e.getTaskQueuePartitionManager(ctx, partition, !sticky, loadCauseQuery)
	if err != nil {
		return nil, err
	} else if sticky && !stickyWorkerAvailable(pm) {
		return nil, serviceerrors.NewStickyWorkerUnavailable()
	}

	taskID := uuid.NewString()
	queryResultCh := make(chan *queryResult, 1)
	e.queryResults.Set(taskID, queryResultCh)
	defer e.queryResults.Delete(taskID)

	resp, err := pm.DispatchQueryTask(ctx, taskID, queryRequest)

	// if we get a response or error it means that query task was handled by forwarding to another matching host
	// this remote host's result can be returned directly
	if resp != nil || err != nil {
		return resp, err
	}

	// if we get here it means that dispatch of query task has occurred locally
	// must wait on result channel to get query result
	select {
	case result := <-queryResultCh:
		if result.internalError != nil {
			return nil, result.internalError
		}

		workerResponse := result.workerResponse
		switch workerResponse.GetCompletedRequest().GetCompletedType() {
		case enumspb.QUERY_RESULT_TYPE_ANSWERED:
			return &matchingservice.QueryWorkflowResponse{QueryResult: workerResponse.GetCompletedRequest().GetQueryResult()}, nil
		case enumspb.QUERY_RESULT_TYPE_FAILED:
			return nil, serviceerror.NewQueryFailedWithFailure(workerResponse.GetCompletedRequest().GetErrorMessage(), workerResponse.GetCompletedRequest().GetFailure())
		default:
			return nil, serviceerror.NewInternal("unknown query completed type")
		}
	case <-ctx.Done():
		// task timed out. log (optionally) and return the timeout error
		ns, err := e.namespaceRegistry.GetNamespaceByID(namespace.ID(partition.NamespaceId()))
		if err != nil {
			e.logger.Error("Failed to get the namespace by ID",
				tag.WorkflowNamespaceID(partition.NamespaceId()),
				tag.Error(err))
		} else {
			sampleRate := e.config.QueryWorkflowTaskTimeoutLogRate(ns.Name().String(), partition.TaskQueue().Name(), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
			if rand.Float64() < sampleRate {
				e.logger.Info("Workflow Query Task timed out",
					tag.WorkflowNamespaceID(ns.ID().String()),
					tag.WorkflowNamespace(ns.Name().String()),
					tag.WorkflowID(queryRequest.GetQueryRequest().GetExecution().GetWorkflowId()),
					tag.WorkflowRunID(queryRequest.GetQueryRequest().GetExecution().GetRunId()),
					tag.WorkflowTaskRequestId(taskID),
					tag.WorkflowTaskQueueName(partition.TaskQueue().Name()))
			}
		}
		return nil, ctx.Err()
	}
}

func (e *matchingEngineImpl) RespondQueryTaskCompleted(
	_ context.Context,
	request *matchingservice.RespondQueryTaskCompletedRequest,
	opMetrics metrics.Handler,
) error {
	if err := e.deliverQueryResult(request.GetTaskId(), &queryResult{workerResponse: request}); err != nil {
		metrics.RespondQueryTaskFailedPerTaskQueueCounter.With(opMetrics).Record(1)
		return err
	}
	return nil
}

func (e *matchingEngineImpl) deliverQueryResult(taskID string, queryResult *queryResult) error {
	queryResultCh, ok := e.queryResults.Pop(taskID)
	if !ok {
		return serviceerror.NewNotFound("query task not found, or already expired")
	}
	queryResultCh <- queryResult
	return nil
}

func (e *matchingEngineImpl) CancelOutstandingPoll(
	_ context.Context,
	request *matchingservice.CancelOutstandingPollRequest,
) error {
	cancel, ok := e.outstandingPollers.Pop(request.PollerId)
	if ok {
		cancel()
	}
	return nil
}

func (e *matchingEngineImpl) CancelOutstandingWorkerPolls(
	ctx context.Context,
	request *matchingservice.CancelOutstandingWorkerPollsRequest,
) (*matchingservice.CancelOutstandingWorkerPollsResponse, error) {
	cancelledCount := e.workerInstancePollers.CancelAll(request.WorkerInstanceKey)
	e.removePollerFromHistory(ctx, request)
	return &matchingservice.CancelOutstandingWorkerPollsResponse{CancelledCount: cancelledCount}, nil
}

// removePollerFromHistory eagerly removes the worker from pollerHistory so
// DescribeTaskQueue doesn't show stale pollers after worker shutdown.
func (e *matchingEngineImpl) removePollerFromHistory(
	ctx context.Context,
	request *matchingservice.CancelOutstandingWorkerPollsRequest,
) {
	workerIdentity := request.GetWorkerIdentity()
	if workerIdentity == "" {
		return
	}

	taskQueueName := request.GetTaskQueue().GetName()
	partition, err := tqid.PartitionFromProto(request.GetTaskQueue(), request.GetNamespaceId(), request.GetTaskQueueType())
	if err != nil {
		e.logger.Warn("Invalid task queue for poller history cleanup",
			tag.WorkflowTaskQueueName(taskQueueName),
			tag.Error(err))
		return
	}

	pm, _, err := e.getTaskQueuePartitionManager(ctx, partition, false, loadCauseOtherWrite)
	if err != nil {
		e.logger.Warn("Failed to get task queue partition manager for poller history cleanup",
			tag.WorkflowTaskQueueName(taskQueueName),
			tag.Error(err))
		return
	}
	if pm == nil {
		e.logger.Debug("Partition manager not loaded, skipping poller history cleanup",
			tag.WorkflowTaskQueueName(taskQueueName))
		return
	}

	pm.RemovePoller(pollerIdentity(workerIdentity))
}

func (e *matchingEngineImpl) DescribeTaskQueue(
	ctx context.Context,
	request *matchingservice.DescribeTaskQueueRequest,
) (*matchingservice.DescribeTaskQueueResponse, error) {
	req := request.GetDescRequest()

	// This has been deprecated.
	if req.ApiMode == enumspb.DESCRIBE_TASK_QUEUE_MODE_ENHANCED {
		rootPartition, err := tqid.PartitionFromProto(req.GetTaskQueue(), request.GetNamespaceId(), req.GetTaskQueueType())
		if err != nil {
			return nil, err
		}
		tqConfig := newTaskQueueConfig(rootPartition.TaskQueue(), e.config, namespace.Name(req.Namespace))
		if !rootPartition.IsRoot() || rootPartition.Kind() == enumspb.TASK_QUEUE_KIND_STICKY || rootPartition.TaskType() != enumspb.TASK_QUEUE_TYPE_WORKFLOW {
			return nil, serviceerror.NewInvalidArgument("DescribeTaskQueue must be called on the root partition of workflow task queue if api mode is DESCRIBE_TASK_QUEUE_MODE_ENHANCED")
		}
		userData, err := e.getUserDataClone(ctx, rootPartition, loadCauseDescribe)
		if err != nil {
			return nil, err
		}
		if req.GetVersions() == nil {
			defaultBuildId := getDefaultBuildId(userData.GetVersioningData().GetAssignmentRules())
			req.Versions = &taskqueuepb.TaskQueueVersionSelection{BuildIds: []string{defaultBuildId}}
		}
		rootPM, _, err := e.getTaskQueuePartitionManager(ctx, rootPartition, true, loadCauseDescribe)
		if err != nil {
			return nil, err
		}

		// TODO bug fix: We cache the last response for each build ID. timeSinceLastFanOut is the last fan out time, that means some enteries in the cache can be more stale if
		// user is calling this API back-to-back but with different version selection.
		cacheKeyFunc := func(buildId string, taskQueueType enumspb.TaskQueueType) string {
			return fmt.Sprintf("dtq_enhanced:%s.%s", buildId, taskQueueType.String())
		}
		missingItemsInCache := false
		physicalTqInfos := make(map[string]map[enumspb.TaskQueueType]*taskqueuespb.PhysicalTaskQueueInfo)
		//nolint:staticcheck // SA1019 deprecated
		requestedBuildIds, err := e.getBuildIds(req.Versions)
		if err != nil {
			return nil, err
		}

		for buildId := range requestedBuildIds {
			physicalTqInfos[buildId] = make(map[enumspb.TaskQueueType]*taskqueuespb.PhysicalTaskQueueInfo)
			//nolint:staticcheck // SA1019 deprecated
			for _, taskQueueType := range req.TaskQueueTypes {
				cacheKey := cacheKeyFunc(buildId, taskQueueType)
				cachedInfo := rootPM.GetCache(cacheKey) // any expired cache entry will return nil
				if cachedInfo == nil {
					missingItemsInCache = true
					break // once we find a missing item, we can stop checking the cache
				}
				//revive:disable-next-line:unchecked-type-assertion
				physicalTqInfos[buildId][taskQueueType] = cachedInfo.(*taskqueuespb.PhysicalTaskQueueInfo)
			}
			if missingItemsInCache {
				break // stop checking other build IDs if we already found missing items
			}
		}

		if missingItemsInCache {
			// Fan out to partitions to get the needed info
			var foundItems []struct {
				buildId       string
				taskQueueType enumspb.TaskQueueType
			}
			numPartitions := max(tqConfig.NumWritePartitions(), tqConfig.NumReadPartitions())
			for _, taskQueueType := range req.TaskQueueTypes {
				for i := range numPartitions {
					partitionResp, err := e.matchingRawClient.DescribeTaskQueuePartition(ctx, &matchingservice.DescribeTaskQueuePartitionRequest{
						NamespaceId: request.GetNamespaceId(),
						TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
							TaskQueue:     req.TaskQueue.Name,
							TaskQueueType: taskQueueType,
							PartitionId:   &taskqueuespb.TaskQueuePartition_NormalPartitionId{NormalPartitionId: int32(i)},
						},
						Versions:      req.GetVersions(),
						ReportStats:   req.GetReportStats(),
						ReportPollers: req.GetReportPollers(),
					})
					if err != nil {
						return nil, err
					}
					for buildId, vii := range partitionResp.VersionsInfoInternal {
						foundItems = append(foundItems, struct {
							buildId       string
							taskQueueType enumspb.TaskQueueType
						}{buildId, taskQueueType})

						if _, ok := physicalTqInfos[buildId]; !ok {
							physicalTqInfos[buildId] = make(map[enumspb.TaskQueueType]*taskqueuespb.PhysicalTaskQueueInfo)
						}
						if physInfo, ok := physicalTqInfos[buildId][taskQueueType]; !ok {
							physicalTqInfos[buildId][taskQueueType] = vii.PhysicalTaskQueueInfo
						} else {
							var mergedStats *taskqueuepb.TaskQueueStats

							if req.GetReportStats() {
								totalStats := physicalTqInfos[buildId][taskQueueType].TaskQueueStats
								partitionStats := vii.PhysicalTaskQueueInfo.TaskQueueStats
								mergedStats = &taskqueuepb.TaskQueueStats{
									ApproximateBacklogCount: totalStats.ApproximateBacklogCount + partitionStats.ApproximateBacklogCount,
									ApproximateBacklogAge:   oldestBacklogAge(totalStats.ApproximateBacklogAge, partitionStats.ApproximateBacklogAge),
									TasksAddRate:            totalStats.TasksAddRate + partitionStats.TasksAddRate,
									TasksDispatchRate:       totalStats.TasksDispatchRate + partitionStats.TasksDispatchRate,
								}
							}

							physicalTqInfos[buildId][taskQueueType] = &taskqueuespb.PhysicalTaskQueueInfo{
								Pollers:        dedupPollers(append(physInfo.GetPollers(), vii.PhysicalTaskQueueInfo.GetPollers()...)),
								TaskQueueStats: mergedStats,
							}
						}
					}
				}
			}

			// put the found items into cache
			for _, item := range foundItems {
				physicalTqInfo := physicalTqInfos[item.buildId][item.taskQueueType]
				rootPM.PutCache(cacheKeyFunc(item.buildId, item.taskQueueType), physicalTqInfo)
			}
		}

		// smush internal info into versions info
		versionsInfo := make(map[string]*taskqueuepb.TaskQueueVersionInfo, 0)
		for bid, typeMap := range physicalTqInfos {
			typesInfo := make(map[int32]*taskqueuepb.TaskQueueTypeInfo, 0)
			for taskQueueType, physicalInfo := range typeMap {
				typesInfo[int32(taskQueueType)] = &taskqueuepb.TaskQueueTypeInfo{
					Pollers: physicalInfo.Pollers,
					Stats:   physicalInfo.TaskQueueStats,
				}
			}
			var reachability enumspb.BuildIdTaskReachability
			if req.GetReportTaskReachability() {
				reachability, err = getBuildIdTaskReachability(ctx,
					newReachabilityCalculator(
						userData.GetVersioningData(),
						e.reachabilityCache,
						request.GetNamespaceId(),
						req.GetNamespace(),
						rootPartition.TaskQueue().Family(),
						e.config.ReachabilityBuildIdVisibilityGracePeriod(req.GetNamespace()),
						tqConfig,
					),
					e.metricsHandler,
					e.logger,
					bid,
				)
				if err != nil {
					return nil, err
				}
			}
			versionsInfo[bid] = &taskqueuepb.TaskQueueVersionInfo{
				TypesInfo:        typesInfo,
				TaskReachability: reachability,
			}
		}
		return &matchingservice.DescribeTaskQueueResponse{
			DescResponse: &workflowservice.DescribeTaskQueueResponse{
				VersionsInfo: versionsInfo,
			},
		}, nil
	}

	partition, err := tqid.PartitionFromProto(req.TaskQueue, request.GetNamespaceId(), req.TaskQueueType)
	if err != nil {
		return nil, err
	}
	pm, _, err := e.getTaskQueuePartitionManager(ctx, partition, true, loadCauseDescribe)
	if err != nil {
		return nil, err
	}
	//nolint:staticcheck // SA1019 deprecated
	descrResp, err := pm.LegacyDescribeTaskQueue(req.GetIncludeTaskQueueStatus())
	if err != nil {
		return nil, err
	}

	if req.ReportStats {
		if !pm.Partition().IsRoot() {
			return nil, serviceerror.NewInvalidArgument("DescribeTaskQueue stats are only supported for the root partition")
		}

		var buildIds []string
		var reportUnversioned bool

		if request.Version != nil {
			// A particular version was requested. This is only available internally; not user-facing.
			buildIds = []string{worker_versioning.WorkerDeploymentVersionToStringV32(request.Version)}
		}

		// TODO(stephan): cache each version separately to allow re-use of cached stats
		cacheKey := "dtq_default:" + strings.Join(buildIds, ",")
		if ts := pm.GetCache(cacheKey); ts != nil {
			//revive:disable-next-line:unchecked-type-assertion
			cachedResp := ts.(*workflowservice.DescribeTaskQueueResponse)
			descrResp.DescResponse.Stats = cachedResp.Stats
			descrResp.DescResponse.StatsByPriorityKey = cachedResp.StatsByPriorityKey
		} else {
			taskQueueStats := &taskqueuepb.TaskQueueStats{}
			taskQueueStatsByPriority := make(map[int32]*taskqueuepb.TaskQueueStats)

			// No version was requested, so we need to query all versions.
			if len(buildIds) == 0 {
				userData, _, err := pm.GetUserDataManager().GetUserData()
				if err != nil {
					return nil, err
				}
				typedUserData := userData.GetData().GetPerType()[int32(pm.Partition().TaskType())]

				// Fetch buildIDs from old deploymentData format
				for _, v := range typedUserData.GetDeploymentData().GetVersions() {
					if v.GetVersion() == nil || v.GetVersion().GetDeploymentName() == "" || v.GetVersion().GetBuildId() == "" {
						continue
					}
					deploymentVersion := worker_versioning.WorkerDeploymentVersionToStringV32(v.GetVersion())
					buildIds = append(buildIds, deploymentVersion)
				}

				// Fetch buildIDs from new deploymentData format
				for deploymentName, v := range typedUserData.GetDeploymentData().GetDeploymentsData() {
					if v.GetVersions() == nil {
						continue
					}
					for buildID := range v.GetVersions() {
						deploymentVersion := worker_versioning.BuildIDToStringV32(deploymentName, buildID)
						buildIds = append(buildIds, deploymentVersion)
					}
				}

				// Report stats from the unversioned queue here
				reportUnversioned = true
			}

			if reportUnversioned {
				buildIds = append(buildIds, "")
			}

			// query each partition for stats
			// TODO(stephanos): don't query root partition again
			for i := 0; i < pm.PartitionCount(); i++ {
				partitionResp, err := e.matchingRawClient.DescribeTaskQueuePartition(ctx,
					&matchingservice.DescribeTaskQueuePartitionRequest{
						NamespaceId: request.GetNamespaceId(),
						TaskQueuePartition: &taskqueuespb.TaskQueuePartition{
							TaskQueue:     req.TaskQueue.Name,
							TaskQueueType: req.TaskQueueType,
							PartitionId:   &taskqueuespb.TaskQueuePartition_NormalPartitionId{NormalPartitionId: int32(i)},
						},
						Versions: &taskqueuepb.TaskQueueVersionSelection{
							BuildIds:    buildIds,
							Unversioned: reportUnversioned,
						},
						ReportStats: true,
					})
				if err != nil {
					return nil, err
				}
				for _, vii := range partitionResp.VersionsInfoInternal {
					partitionStats := vii.PhysicalTaskQueueInfo.TaskQueueStatsByPriorityKey
					for pri, priorityStats := range partitionStats {
						if _, ok := taskQueueStatsByPriority[pri]; !ok {
							taskQueueStatsByPriority[pri] = &taskqueuepb.TaskQueueStats{}
						}
						mergeStats(taskQueueStats, priorityStats)
						mergeStats(taskQueueStatsByPriority[pri], priorityStats)
					}
				}
			}
			pm.PutCache(cacheKey, &workflowservice.DescribeTaskQueueResponse{
				Stats:              taskQueueStats,
				StatsByPriorityKey: taskQueueStatsByPriority,
			})
			descrResp.DescResponse.Stats = taskQueueStats
			descrResp.DescResponse.StatsByPriorityKey = taskQueueStatsByPriority
		}
	}

	if req.GetReportConfig() {
		userData, _, err := pm.GetUserDataManager().GetUserData()
		if err != nil {
			return nil, err
		}
		descrResp.DescResponse.Config = userData.GetData().GetPerType()[int32(req.GetTaskQueueType())].GetConfig()
	}

	effectiveRPS, sourceForEffectiveRPS := pm.GetRateLimitManager().GetEffectiveRPSAndSource()
	descrResp.DescResponse.EffectiveRateLimit = &workflowservice.DescribeTaskQueueResponse_EffectiveRateLimit{
		RequestsPerSecond: float32(effectiveRPS),
		RateLimitSource:   sourceForEffectiveRPS,
	}

	return descrResp, nil
}

func (e *matchingEngineImpl) DescribeVersionedTaskQueues(
	ctx context.Context,
	request *matchingservice.DescribeVersionedTaskQueuesRequest,
) (*matchingservice.DescribeVersionedTaskQueuesResponse, error) {
	partition, err := tqid.PartitionFromProto(request.TaskQueue, request.GetNamespaceId(), request.TaskQueueType)
	if err != nil {
		return nil, err
	}

	pm, _, err := e.getTaskQueuePartitionManager(ctx, partition, true, loadCauseDescribe)
	if err != nil {
		return nil, err
	}

	cacheKey := fmt.Sprintf("dvtq:%s", worker_versioning.WorkerDeploymentVersionToStringV31(request.Version))
	if cached := pm.GetCache(cacheKey); cached != nil {
		//revive:disable-next-line:unchecked-type-assertion
		return cached.(*matchingservice.DescribeVersionedTaskQueuesResponse), nil
	}

	resp := &matchingservice.DescribeVersionedTaskQueuesResponse{}
	for _, tq := range request.VersionTaskQueues {
		tqResp, err := e.matchingRawClient.DescribeTaskQueue(ctx,
			&matchingservice.DescribeTaskQueueRequest{
				NamespaceId: request.GetNamespaceId(),
				DescRequest: &workflowservice.DescribeTaskQueueRequest{
					TaskQueue: &taskqueuepb.TaskQueue{
						Name: tq.Name,
						Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
					},
					TaskQueueType: tq.Type,
					ReportStats:   true,
				},
				Version: request.Version,
			})
		if err != nil {
			return nil, err
		}
		resp.VersionTaskQueues = append(resp.VersionTaskQueues,
			&matchingservice.DescribeVersionedTaskQueuesResponse_VersionTaskQueue{
				Name:               tq.Name,
				Type:               tq.Type,
				Stats:              tqResp.DescResponse.Stats,
				StatsByPriorityKey: tqResp.DescResponse.StatsByPriorityKey,
			})
	}

	pm.PutCache(cacheKey, resp)
	return resp, nil
}

func dedupPollers(pollerInfos []*taskqueuepb.PollerInfo) []*taskqueuepb.PollerInfo {
	allKeys := make(map[string]bool)
	var list []*taskqueuepb.PollerInfo
	for _, item := range pollerInfos {
		if _, value := allKeys[item.GetIdentity()]; !value {
			allKeys[item.GetIdentity()] = true
			list = append(list, item)
		}
	}
	return list
}

func (e *matchingEngineImpl) DescribeTaskQueuePartition(
	ctx context.Context,
	request *matchingservice.DescribeTaskQueuePartitionRequest,
) (*matchingservice.DescribeTaskQueuePartitionResponse, error) {
	if request.GetVersions() == nil {
		return nil, serviceerror.NewInvalidArgument("versions must not be nil, to describe the default queue, pass the default build ID as a member of the BuildIds list")
	}
	pm, _, err := e.getTaskQueuePartitionManager(ctx, tqid.PartitionFromPartitionProto(request.GetTaskQueuePartition(), request.GetNamespaceId()), true, loadCauseDescribe)
	if err != nil {
		return nil, err
	}
	buildIds, err := e.getBuildIds(request.GetVersions())
	if err != nil {
		return nil, err
	}
	return pm.Describe(ctx, buildIds, request.GetVersions().GetAllActive(), request.GetReportStats(), request.GetReportPollers(), request.GetReportInternalTaskQueueStatus())
}

func (e *matchingEngineImpl) getBuildIds(versions *taskqueuepb.TaskQueueVersionSelection) (map[string]bool, error) {
	buildIds := make(map[string]bool)
	if versions != nil {
		for _, bid := range versions.GetBuildIds() {
			buildIds[bid] = true
		}
		if versions.GetUnversioned() {
			buildIds[""] = true
		}
	}
	return buildIds, nil
}

func (e *matchingEngineImpl) ListTaskQueuePartitions(
	_ context.Context,
	request *matchingservice.ListTaskQueuePartitionsRequest,
) (*matchingservice.ListTaskQueuePartitionsResponse, error) {
	activityTaskQueueInfo, err := e.listTaskQueuePartitions(request, enumspb.TASK_QUEUE_TYPE_ACTIVITY)
	if err != nil {
		return nil, err
	}
	workflowTaskQueueInfo, err := e.listTaskQueuePartitions(request, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}
	resp := matchingservice.ListTaskQueuePartitionsResponse{
		ActivityTaskQueuePartitions: activityTaskQueueInfo,
		WorkflowTaskQueuePartitions: workflowTaskQueueInfo,
	}
	return &resp, nil
}

func (e *matchingEngineImpl) listTaskQueuePartitions(request *matchingservice.ListTaskQueuePartitionsRequest, taskQueueType enumspb.TaskQueueType) ([]*taskqueuepb.TaskQueuePartitionMetadata, error) {
	partitions, err := e.getAllPartitionRpcNames(
		namespace.Name(request.GetNamespace()),
		request.TaskQueue,
		taskQueueType,
	)

	if err != nil {
		return nil, err
	}

	partitionHostInfo := make([]*taskqueuepb.TaskQueuePartitionMetadata, len(partitions))
	for i, partition := range partitions {
		host, err := e.getHostInfo(partition)
		if err != nil {
			return nil, err
		}

		partitionHostInfo[i] = &taskqueuepb.TaskQueuePartitionMetadata{
			Key:           partition,
			OwnerHostName: host,
		}
	}

	return partitionHostInfo, nil
}

func (e *matchingEngineImpl) UpdateWorkerVersioningRules(
	ctx context.Context,
	request *matchingservice.UpdateWorkerVersioningRulesRequest,
) (*matchingservice.UpdateWorkerVersioningRulesResponse, error) {
	req := request.GetRequest()
	ns, err := e.namespaceRegistry.GetNamespace(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}
	if ns.ID().String() != request.GetNamespaceId() {
		return nil, serviceerror.NewInternal("Namespace ID does not match Namespace in wrapped command")
	}
	if req.GetTaskQueue() != request.GetTaskQueue() {
		return nil, serviceerror.NewInternal("Task Queue does not match Task Queue in wrapped command")
	}

	// We only expect to receive task queue family name (root partition) here.
	taskQueueFamily, err := tqid.NewTaskQueueFamily(ns.ID().String(), req.GetTaskQueue())
	if err != nil {
		return nil, err
	}
	tqMgr, _, err := e.getTaskQueuePartitionManager(ctx, taskQueueFamily.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition(), true, loadCauseOtherWrite)
	if err != nil {
		return nil, err
	}

	// we don't set updateOptions.TaskQueueLimitPerBuildId, because the Versioning Rule limits will be checked separately
	// we don't set updateOptions.KnownVersion, because we handle external API call ordering with conflictToken
	updateOptions := UserDataUpdateOptions{Source: "UpdateWorkerVersioningRules"}
	cT := req.GetConflictToken()
	var getResp *matchingservice.GetWorkerVersioningRulesResponse
	var maxUpstreamBuildIDs int

	_, err = tqMgr.GetUserDataManager().UpdateUserData(ctx, updateOptions, func(data *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
		clk := data.GetClock()
		if clk == nil {
			clk = hlc.Zero(e.clusterMeta.GetClusterID())
		}

		prevCT, err := clk.Marshal()
		if err != nil {
			return nil, false, err
		}
		if !bytes.Equal(cT, prevCT) {
			return nil, false, serviceerror.NewFailedPreconditionf(
				"provided conflict token '%v' does not match existing one '%v'", cT, prevCT,
			)
		}

		updatedClock := hlc.Next(clk, e.timeSource)
		var versioningData *persistencespb.VersioningData
		switch req.GetOperation().(type) {
		case *workflowservice.UpdateWorkerVersioningRulesRequest_InsertAssignmentRule:
			versioningData, err = InsertAssignmentRule(
				updatedClock,
				data.GetVersioningData(),
				req.GetInsertAssignmentRule(),
				e.config.AssignmentRuleLimitPerQueue(ns.Name().String()),
			)
		case *workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceAssignmentRule:
			versioningData, err = ReplaceAssignmentRule(
				updatedClock,
				data.GetVersioningData(),
				req.GetReplaceAssignmentRule(),
			)
		case *workflowservice.UpdateWorkerVersioningRulesRequest_DeleteAssignmentRule:
			versioningData, err = DeleteAssignmentRule(
				updatedClock,
				data.GetVersioningData(),
				req.GetDeleteAssignmentRule(),
			)
		case *workflowservice.UpdateWorkerVersioningRulesRequest_AddCompatibleRedirectRule:
			versioningData, err = AddCompatibleRedirectRule(
				updatedClock,
				data.GetVersioningData(),
				req.GetAddCompatibleRedirectRule(),
				e.config.RedirectRuleLimitPerQueue(ns.Name().String()),
				e.config.RedirectRuleMaxUpstreamBuildIDsPerQueue(ns.Name().String()),
			)
		case *workflowservice.UpdateWorkerVersioningRulesRequest_ReplaceCompatibleRedirectRule:
			versioningData, err = ReplaceCompatibleRedirectRule(
				updatedClock,
				data.GetVersioningData(),
				req.GetReplaceCompatibleRedirectRule(),
				e.config.RedirectRuleMaxUpstreamBuildIDsPerQueue(ns.Name().String()),
			)
		case *workflowservice.UpdateWorkerVersioningRulesRequest_DeleteCompatibleRedirectRule:
			versioningData, err = DeleteCompatibleRedirectRule(
				updatedClock,
				data.GetVersioningData(),
				req.GetDeleteCompatibleRedirectRule(),
			)
		case *workflowservice.UpdateWorkerVersioningRulesRequest_CommitBuildId_:
			versioningData, err = CommitBuildID(
				updatedClock,
				data.GetVersioningData(),
				req.GetCommitBuildId(),
				tqMgr.HasPollerAfter(req.GetCommitBuildId().GetTargetBuildId(), time.Now().Add(-versioningPollerSeenWindow)),
				e.config.AssignmentRuleLimitPerQueue(ns.Name().String()),
			)
		}
		if err != nil {
			// operation can't be completed due to failed validation. no action, do not replicate, report error
			return nil, false, err
		}

		// Get versioning data formatted for response
		getResp, err = GetTimestampedWorkerVersioningRules(versioningData, updatedClock)
		if err != nil {
			return nil, false, err
		}
		// Get max upstream build IDs (min is 0, because we count number of upstream nodes)
		activeRedirectRules := getActiveRedirectRules(versioningData.GetRedirectRules())
		for _, rule := range activeRedirectRules {
			upstream := getUpstreamBuildIds(rule.GetRule().GetTargetBuildId(), activeRedirectRules)
			if len(upstream)+1 > maxUpstreamBuildIDs {
				maxUpstreamBuildIDs = len(upstream) + 1
			}
		}

		// Clean up tombstones after all fallible tasks are complete, once we know we are committing and replicating the changes.
		// We can replicate tombstone cleanup, because it's just based on DeletionTimestamp, so no need to only do it locally.
		versioningData = CleanupRuleTombstones(versioningData, e.config.DeletedRuleRetentionTime(ns.Name().String()))

		// Avoid mutation
		ret := common.CloneProto(data)
		ret.Clock = updatedClock
		ret.VersioningData = versioningData
		return ret, true, nil
	})

	if err != nil {
		return nil, err
	}

	// log resulting rule counts
	assignmentRules := getResp.GetResponse().GetAssignmentRules()
	redirectRules := getResp.GetResponse().GetCompatibleRedirectRules()

	e.logger.Info("UpdateWorkerVersioningRules completed",
		tag.WorkerVersioningRedirectRuleCount(len(redirectRules)),
		tag.WorkerVersioningAssignmentRuleCount(len(assignmentRules)),
		tag.WorkerVersioningMaxUpstreamBuildIDs(maxUpstreamBuildIDs))

	return &matchingservice.UpdateWorkerVersioningRulesResponse{Response: &workflowservice.UpdateWorkerVersioningRulesResponse{
		AssignmentRules:         assignmentRules,
		CompatibleRedirectRules: redirectRules,
		ConflictToken:           getResp.GetResponse().GetConflictToken(),
	}}, nil
}

func (e *matchingEngineImpl) GetWorkerVersioningRules(
	ctx context.Context,
	request *matchingservice.GetWorkerVersioningRulesRequest,
) (*matchingservice.GetWorkerVersioningRulesResponse, error) {
	req := request.GetRequest()
	ns, err := e.namespaceRegistry.GetNamespace(namespace.Name(req.GetNamespace()))
	if err != nil {
		return nil, err
	}
	if ns.ID().String() != request.GetNamespaceId() {
		return nil, serviceerror.NewInternal("Namespace ID does not match Namespace in wrapped command")
	}
	if req.GetTaskQueue() != request.GetTaskQueue() {
		return nil, serviceerror.NewInternal("Task Queue does not match Task Queue in wrapped command")
	}

	// We only expect to receive task queue family name (root partition) here.
	taskQueueFamily, err := tqid.NewTaskQueueFamily(ns.ID().String(), req.GetTaskQueue())
	if err != nil {
		return nil, err
	}
	userData, err := e.getUserDataClone(ctx, taskQueueFamily.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition(), loadCauseOtherRead)
	if err != nil {
		return nil, err
	}
	clk := userData.GetClock()
	if clk == nil {
		clk = hlc.Zero(e.clusterMeta.GetClusterID())
	}
	return GetTimestampedWorkerVersioningRules(userData.GetVersioningData(), clk)
}

func (e *matchingEngineImpl) getUserDataClone(
	ctx context.Context,
	rootPartition tqid.Partition,
	loadCause loadCause,
) (*persistencespb.TaskQueueUserData, error) {
	rootPartitionMgr, _, err := e.getTaskQueuePartitionManager(ctx, rootPartition, true, loadCause)
	if err != nil {
		return nil, err
	}
	userData, _, err := rootPartitionMgr.GetUserDataManager().GetUserData()
	if err != nil {
		return nil, err
	}
	if userData == nil {
		userData = &persistencespb.VersionedTaskQueueUserData{Data: &persistencespb.TaskQueueUserData{}}
	} else {
		userData = common.CloneProto(userData)
	}
	return userData.GetData(), nil
}

func (e *matchingEngineImpl) UpdateWorkerBuildIdCompatibility(
	ctx context.Context,
	req *matchingservice.UpdateWorkerBuildIdCompatibilityRequest,
) (*matchingservice.UpdateWorkerBuildIdCompatibilityResponse, error) {
	namespaceID := namespace.ID(req.GetNamespaceId())
	ns, err := e.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}
	taskQueue, err := tqid.NewTaskQueueFamily(req.NamespaceId, req.GetTaskQueue())
	if err != nil {
		return nil, err
	}
	pm, _, err := e.getTaskQueuePartitionManager(ctx, taskQueue.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition(), true, loadCauseOtherWrite)
	if err != nil {
		return nil, err
	}
	updateOptions := UserDataUpdateOptions{Source: "UpdateWorkerBuildIdCompatibility"}
	operationCreatedTombstones := false
	switch req.GetOperation().(type) {
	case *matchingservice.UpdateWorkerBuildIdCompatibilityRequest_ApplyPublicRequest_:
		// Only apply the limit when request is initiated by a user.
		updateOptions.TaskQueueLimitPerBuildId = e.config.TaskQueueLimitPerBuildId(ns.Name().String())
	case *matchingservice.UpdateWorkerBuildIdCompatibilityRequest_RemoveBuildIds_:
		updateOptions.KnownVersion = req.GetRemoveBuildIds().GetKnownUserDataVersion()
	}

	_, err = pm.GetUserDataManager().UpdateUserData(ctx, updateOptions, func(data *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
		clk := data.GetClock()
		if clk == nil {
			tmp := hlc.Zero(e.clusterMeta.GetClusterID())
			clk = tmp
		}
		updatedClock := hlc.Next(clk, e.timeSource)
		var versioningData *persistencespb.VersioningData
		switch req.GetOperation().(type) {
		case *matchingservice.UpdateWorkerBuildIdCompatibilityRequest_ApplyPublicRequest_:
			var err error
			versioningData, err = UpdateVersionSets(
				updatedClock,
				data.GetVersioningData(),
				req.GetApplyPublicRequest().GetRequest(),
				e.config.VersionCompatibleSetLimitPerQueue(ns.Name().String()),
				e.config.VersionBuildIdLimitPerQueue(ns.Name().String()),
			)
			if err != nil {
				return nil, false, err
			}
		case *matchingservice.UpdateWorkerBuildIdCompatibilityRequest_RemoveBuildIds_:
			versioningData = RemoveBuildIds(
				updatedClock,
				data.GetVersioningData(),
				req.GetRemoveBuildIds().GetBuildIds(),
			)
			if ns.ReplicationPolicy() == namespace.ReplicationPolicyMultiCluster {
				operationCreatedTombstones = true
			} else {
				// We don't need to keep the tombstones around if we're not replicating them.
				versioningData = ClearTombstones(versioningData)
			}
		case *matchingservice.UpdateWorkerBuildIdCompatibilityRequest_PersistUnknownBuildId:
			versioningData = PersistUnknownBuildId(
				updatedClock,
				data.GetVersioningData(),
				req.GetPersistUnknownBuildId(),
			)
		default:
			return nil, false, serviceerror.NewInvalidArgumentf("invalid operation: %v", req.GetOperation())
		}
		// Avoid mutation
		ret := common.CloneProto(data)
		ret.Clock = updatedClock
		ret.VersioningData = versioningData
		return ret, true, nil
	})
	if err != nil {
		return nil, err
	}

	// Only clear tombstones after they have been replicated.
	if operationCreatedTombstones {
		opts := UserDataUpdateOptions{Source: "UpdateWorkerBuildIdCompatibility/clear-tombstones"}
		_, err = pm.GetUserDataManager().UpdateUserData(ctx, opts, func(data *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
			updatedClock := hlc.Next(data.GetClock(), e.timeSource)
			// Avoid mutation
			ret := common.CloneProto(data)
			ret.Clock = updatedClock
			ret.VersioningData = ClearTombstones(data.VersioningData)
			return ret, false, nil // Do not replicate the deletion of tombstones
		})
		if err != nil {
			return nil, err
		}
	}
	return &matchingservice.UpdateWorkerBuildIdCompatibilityResponse{}, nil
}

func (e *matchingEngineImpl) GetWorkerBuildIdCompatibility(
	ctx context.Context,
	req *matchingservice.GetWorkerBuildIdCompatibilityRequest,
) (*matchingservice.GetWorkerBuildIdCompatibilityResponse, error) {
	taskQueueFamily, err := tqid.NewTaskQueueFamily(req.NamespaceId, req.Request.GetTaskQueue())
	if err != nil {
		return nil, err
	}
	pm, _, err := e.getTaskQueuePartitionManager(ctx, taskQueueFamily.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition(), true, loadCauseOtherRead)
	if err != nil {
		if _, ok := err.(*serviceerror.NotFound); ok {
			return &matchingservice.GetWorkerBuildIdCompatibilityResponse{}, nil
		}
		return nil, err
	}
	userData, _, err := pm.GetUserDataManager().GetUserData()
	if err != nil {
		return nil, err
	}
	return &matchingservice.GetWorkerBuildIdCompatibilityResponse{
		Response: ToBuildIdOrderingResponse(userData.GetData().GetVersioningData(), int(req.GetRequest().GetMaxSets())),
	}, nil
}

func (e *matchingEngineImpl) GetTaskQueueUserData(
	ctx context.Context,
	req *matchingservice.GetTaskQueueUserDataRequest,
) (*matchingservice.GetTaskQueueUserDataResponse, error) {
	partition, err := tqid.PartitionFromProto(&taskqueuepb.TaskQueue{Name: req.GetTaskQueue()}, req.NamespaceId, req.TaskQueueType)
	if err != nil {
		return nil, err
	}
	pm, _, err := e.getTaskQueuePartitionManager(ctx, partition, !req.OnlyIfLoaded, loadCauseUserData)
	if err != nil {
		return nil, err
	} else if pm == nil {
		return nil, serviceerror.NewFailedPrecondition("partition was not loaded")
	}
	if req.WaitNewData {
		// mark alive so that it doesn't unload while a child partition is doing a long poll
		pm.MarkAlive()
	}
	return pm.GetUserDataManager().HandleGetUserDataRequest(ctx, req)
}

func (e *matchingEngineImpl) SyncDeploymentUserData(
	ctx context.Context,
	req *matchingservice.SyncDeploymentUserDataRequest,
) (*matchingservice.SyncDeploymentUserDataResponse, error) {
	taskQueueFamily, err := tqid.NewTaskQueueFamily(req.NamespaceId, req.GetTaskQueue())
	applyUpdatesToRoutingConfig := false

	if err != nil {
		return nil, err
	}
	if req.GetOperation() == nil && req.GetDeploymentName() == "" {
		return nil, errMissingDeploymentVersion
	}

	tqMgr, _, err := e.getTaskQueuePartitionManager(ctx, taskQueueFamily.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition(), true, loadCauseOtherWrite)
	if err != nil {
		return nil, err
	}

	updateOptions := UserDataUpdateOptions{Source: "SyncDeploymentUserData"}

	version, err := tqMgr.GetUserDataManager().UpdateUserData(ctx, updateOptions, func(data *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
		clk := data.GetClock()
		if clk == nil {
			clk = hlc.Zero(e.clusterMeta.GetClusterID())
		}
		now := hlc.Next(clk, e.timeSource)
		// clone the whole thing so we can just mutate
		data = common.CloneProto(data)

		// fill in enough structure so that we can set/append the new deployment data
		if data == nil {
			data = &persistencespb.TaskQueueUserData{}
		}
		if data.PerType == nil {
			data.PerType = make(map[int32]*persistencespb.TaskQueueTypeUserData)
		}

		changed := false
		for _, t := range req.TaskQueueTypes {
			if data.PerType[int32(t)] == nil {
				data.PerType[int32(t)] = &persistencespb.TaskQueueTypeUserData{}
			}
			if data.PerType[int32(t)].DeploymentData == nil {
				data.PerType[int32(t)].DeploymentData = &persistencespb.DeploymentData{}
			}

			// set/append the new data
			deploymentData := data.PerType[int32(t)].DeploymentData

			//nolint:staticcheck // SA1019
			if vd := req.GetUpdateVersionData(); vd != nil {
				// [cleanup-public-preview-versioning]
				if vd.GetVersion() == nil { // unversioned ramp
					if deploymentData.GetUnversionedRampData().GetRoutingUpdateTime().AsTime().After(vd.GetRoutingUpdateTime().AsTime()) {
						continue
					}
					workerDeploymentData := deploymentData.GetDeploymentsData()[req.GetDeploymentName()]
					changed = true
					// only update if the timestamp is more recent
					if vd.GetRampingSinceTime() == nil { // unset
						deploymentData.UnversionedRampData = nil

						// Also have to unset the ramp, if present, from the new deployment data format.
						unsetRampingFromRoutingConfig(workerDeploymentData)

					} else { // set or update
						deploymentData.UnversionedRampData = vd

					}
				} else if idx := worker_versioning.FindDeploymentVersion(deploymentData, vd.GetVersion()); idx >= 0 {
					old := deploymentData.Versions[idx]
					if old.GetRoutingUpdateTime().AsTime().After(vd.GetRoutingUpdateTime().AsTime()) {
						continue
					}
					changed = true
					// only update if the timestamp is more recent
					deploymentData.Versions[idx] = vd

					// Go through the new deployment data format for this deployment.
					workerDeploymentData := deploymentData.GetDeploymentsData()[vd.GetVersion().GetDeploymentName()]
					clearVersionFromRoutingConfig(workerDeploymentData, old, vd)

				} else {
					changed = true
					deploymentData.Versions = append(deploymentData.Versions, vd)

					// Go through the new deployment data format for this deployment.
					workerDeploymentData := deploymentData.GetDeploymentsData()[vd.GetVersion().GetDeploymentName()]
					clearVersionFromRoutingConfig(workerDeploymentData, nil, vd)
				}
			} else if v := req.GetForgetVersion(); v != nil {
				if idx := worker_versioning.FindDeploymentVersion(deploymentData, v); idx >= 0 {
					changed = true
					deploymentData.Versions = append(deploymentData.Versions[:idx], deploymentData.Versions[idx+1:]...)

					// Go through the new deployment data format for this deployment and remove the version if present.
					workerDeploymentData := deploymentData.GetDeploymentsData()[v.GetDeploymentName()]
					_ = removeDeploymentVersions(
						deploymentData,
						v.GetDeploymentName(),
						workerDeploymentData,
						[]string{v.GetBuildId()},
						/* removeOldFormat */ false,
					)
				}
			} else {

				// Only initialize DeploymentsData if we're using the new format
				if deploymentData.GetDeploymentsData() == nil {
					deploymentData.DeploymentsData = make(map[string]*persistencespb.WorkerDeploymentData)
				}
				if deploymentData.GetDeploymentsData()[req.GetDeploymentName()] == nil {
					deploymentData.GetDeploymentsData()[req.GetDeploymentName()] = &persistencespb.WorkerDeploymentData{}
				}

				rc := req.GetUpdateRoutingConfig()
				tqWorkerDeploymentData := deploymentData.GetDeploymentsData()[req.GetDeploymentName()]

				ignoreRevCheck, _ := testhooks.Get(e.testHooks, testhooks.MatchingIgnoreRoutingConfigRevisionCheck, namespace.ID(req.NamespaceId))
				if ignoreRevCheck || rc.GetRevisionNumber() > tqWorkerDeploymentData.GetRoutingConfig().GetRevisionNumber() {
					changed = true
					// Update routing config when newer or equal revision is provided
					tqWorkerDeploymentData.RoutingConfig = rc
					applyUpdatesToRoutingConfig = true
				}

				if tqWorkerDeploymentData.Versions == nil {
					tqWorkerDeploymentData.Versions = make(map[string]*deploymentspb.WorkerDeploymentVersionData)
				}
				for buildID, versionData := range req.GetUpsertVersionsData() {
					existing := tqWorkerDeploymentData.Versions[buildID]
					// Skip if existing version data has a higher revision number to avoid stale writes.
					// Equal revision number is accepted for now because we may roll back the workflow version
					// and stop incrementing the revision number.
					if existing != nil && existing.GetRevisionNumber() > versionData.GetRevisionNumber() {
						continue
					}
					tqWorkerDeploymentData.Versions[buildID] = versionData
					changed = true
					if versionData.GetDeleted() {
						// Remove the version from the old deployment data format if present.
						//nolint:staticcheck // SA1019 deprecated versions will clean up later
						for idx, oldVersions := range deploymentData.GetVersions() {
							if oldVersions.GetVersion().GetDeploymentName() == req.GetDeploymentName() &&
								oldVersions.GetVersion().GetBuildId() == buildID {
								//nolint:staticcheck // SA1019 deprecated versions will clean up later
								deploymentData.Versions = append(deploymentData.Versions[:idx], deploymentData.Versions[idx+1:]...)
								changed = true
								break
							}
						}
					}
				}

				if removed := removeDeploymentVersions(
					deploymentData,
					req.GetDeploymentName(),
					tqWorkerDeploymentData,
					req.GetForgetVersions(),
					/* removeOldFormat */ true,
				); removed {
					changed = true
				}

				/* Migrate all the versions from the old deployment if present. This shall prevent the following scenario:

				Assume all of this is in the same deployment "foo":

					t0: Current version is A with old deployment format.
					t1: Current version is B with new deployment format.
					t2: User unsets current version B.

				The right behaviour is that after unsetting, the current version should be unversioned and not version A.

				However, if the following were present the behaviour would be different:

				Assume all of this is are in different deployments "foo" and "bar":

					t0: Current version is foo.A with old deployment format.
					t1: Current version is bar.B with new deployment format.
					t2: User unsets current version bar.B.

				The right behaviour is that after unsetting, the current version should be foo.A and not unversioned as the task-queue
				still belongs to a versioned deployment.

				So, the idea is that if there are updates to the routing config of a worker-deployment, remove versions present in the
				old deployment data format under the same deployment.
				*/

				if applyUpdatesToRoutingConfig {
					migrateOldFormatVersions(
						deploymentData,
						req.GetDeploymentName(),
						tqWorkerDeploymentData,
					)
				}

				if worker_versioning.CleanupOldDeletedVersions(tqWorkerDeploymentData, e.config.MaxVersionsInTaskQueue(tqMgr.Namespace().Name().String())) {
					changed = true
				}
			}
		}
		if !changed {
			return nil, false, errUserDataUnmodified
		}

		data.Clock = now
		return data, true, nil
	})
	if err != nil {
		return nil, err
	}
	return &matchingservice.SyncDeploymentUserDataResponse{Version: version, RoutingConfigChanged: applyUpdatesToRoutingConfig}, nil
}

func (e *matchingEngineImpl) ApplyTaskQueueUserDataReplicationEvent(
	ctx context.Context,
	req *matchingservice.ApplyTaskQueueUserDataReplicationEventRequest,
) (*matchingservice.ApplyTaskQueueUserDataReplicationEventResponse, error) {
	namespaceID := namespace.ID(req.GetNamespaceId())
	ns, err := e.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}
	taskQueueFamily, err := tqid.NewTaskQueueFamily(req.NamespaceId, req.GetTaskQueue())
	if err != nil {
		return nil, err
	}
	pm, _, err := e.getTaskQueuePartitionManager(ctx, taskQueueFamily.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition(), true, loadCauseUserData)
	if err != nil {
		return nil, err
	}
	updateOptions := UserDataUpdateOptions{
		// Avoid setting a limit to allow the replication event to always be applied
		TaskQueueLimitPerBuildId: 0,
		Source:                   "ApplyTaskQueueUserDataReplicationEvent",
	}
	_, err = pm.GetUserDataManager().UpdateUserData(ctx, updateOptions, func(current *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
		mergedUserData := common.CloneProto(current)
		currentVersioningData := current.GetVersioningData()
		newVersioningData := req.GetUserData().GetVersioningData()
		_, buildIdsRemoved := GetBuildIdDeltas(currentVersioningData, newVersioningData)
		var buildIdsToRevive []string
		for _, buildId := range buildIdsRemoved {
			// We accept that the user data is locked for updates while running these visibility queries.
			// Nothing else is _supposed_ to update it on follower (standby) clusters.
			exists, err := worker_versioning.WorkflowsExistForBuildId(ctx, e.visibilityManager, ns, req.TaskQueue, buildId)
			if err != nil {
				return nil, false, err
			}
			if exists {
				buildIdsToRevive = append(buildIdsToRevive, buildId)
			}
		}

		// merge v1 sets
		mergedData := MergeVersioningData(currentVersioningData, newVersioningData)

		// take last writer for V2 rules and V3 data
		if req.GetUserData().GetClock() == nil || current.GetClock() != nil && hlc.Greater(current.GetClock(), req.GetUserData().GetClock()) {
			if mergedData != nil {
				// v2 rules
				mergedData.AssignmentRules = currentVersioningData.GetAssignmentRules()
				mergedData.RedirectRules = currentVersioningData.GetRedirectRules()
			}
			mergedUserData.PerType = current.GetPerType()
		} else {
			if mergedData != nil {
				// v2 rules
				mergedData.AssignmentRules = newVersioningData.GetAssignmentRules()
				mergedData.RedirectRules = newVersioningData.GetRedirectRules()
			}
			mergedUserData.PerType = req.GetUserData().GetPerType()
		}

		for _, buildId := range buildIdsToRevive {
			setIdx, buildIdIdx := worker_versioning.FindBuildId(mergedData, buildId)
			if setIdx == -1 {
				continue
			}
			set := mergedData.VersionSets[setIdx]
			set.BuildIds[buildIdIdx] = e.reviveBuildId(ns, req.GetTaskQueue(), set.GetBuildIds()[buildIdIdx])
			mergedUserData.Clock = hlc.Max(mergedUserData.Clock, set.BuildIds[buildIdIdx].StateUpdateTimestamp)

			setDefault := set.BuildIds[len(set.BuildIds)-1]
			if setDefault.State == persistencespb.STATE_DELETED {
				// We merged an update which removed (at least) two build ids: the default for set x and another one for set
				// x. We discovered we're still using the other one, so we revive it. now we also have to revive the default
				// for set x, or it will be left with the wrong default.
				set.BuildIds[len(set.BuildIds)-1] = e.reviveBuildId(ns, req.GetTaskQueue(), setDefault)
				mergedUserData.Clock = hlc.Max(mergedUserData.Clock, setDefault.StateUpdateTimestamp)
			}
		}

		if mergedData != nil {
			// No need to keep the v1 tombstones around after replication.
			mergedUserData.VersioningData = ClearTombstones(mergedData)
		}
		return mergedUserData, len(buildIdsToRevive) > 0, nil
	})
	return &matchingservice.ApplyTaskQueueUserDataReplicationEventResponse{}, err
}

func (e *matchingEngineImpl) GetBuildIdTaskQueueMapping(
	ctx context.Context,
	req *matchingservice.GetBuildIdTaskQueueMappingRequest,
) (*matchingservice.GetBuildIdTaskQueueMappingResponse, error) {
	taskQueues, err := e.taskManager.GetTaskQueuesByBuildId(ctx, &persistence.GetTaskQueuesByBuildIdRequest{
		NamespaceID: req.NamespaceId,
		BuildID:     req.BuildId,
	})
	if err != nil {
		return nil, err
	}
	return &matchingservice.GetBuildIdTaskQueueMappingResponse{TaskQueues: taskQueues}, nil
}

// TODO Shivam - remove this in 123
func (e *matchingEngineImpl) ForceUnloadTaskQueue(
	ctx context.Context,
	req *matchingservice.ForceUnloadTaskQueueRequest,
) (*matchingservice.ForceUnloadTaskQueueResponse, error) {
	p, err := tqid.NormalPartitionFromRpcName(req.GetTaskQueue(), req.GetNamespaceId(), req.GetTaskQueueType())
	if err != nil {
		return nil, err
	}

	wasLoaded := e.unloadTaskQueuePartitionByKey(p, nil, unloadCauseForce)
	return &matchingservice.ForceUnloadTaskQueueResponse{WasLoaded: wasLoaded}, nil
}

func (e *matchingEngineImpl) ForceLoadTaskQueuePartition(
	ctx context.Context,
	req *matchingservice.ForceLoadTaskQueuePartitionRequest,
) (*matchingservice.ForceLoadTaskQueuePartitionResponse, error) {
	partition := tqid.PartitionFromPartitionProto(req.GetTaskQueuePartition(), req.GetNamespaceId())
	// Leverage getTaskQueuePartitionManager to check and then create the partition
	_, wasUnloaded, err := e.getTaskQueuePartitionManager(ctx, partition, true, loadCauseForce)
	if err != nil {
		return nil, err
	}
	return &matchingservice.ForceLoadTaskQueuePartitionResponse{WasUnloaded: wasUnloaded}, nil
}

func (e *matchingEngineImpl) ForceUnloadTaskQueuePartition(
	ctx context.Context,
	req *matchingservice.ForceUnloadTaskQueuePartitionRequest,
) (*matchingservice.ForceUnloadTaskQueuePartitionResponse, error) {
	partition := tqid.PartitionFromPartitionProto(req.GetTaskQueuePartition(), req.GetNamespaceId())

	wasLoaded := e.unloadTaskQueuePartitionByKey(partition, nil, unloadCauseForce)
	return &matchingservice.ForceUnloadTaskQueuePartitionResponse{WasLoaded: wasLoaded}, nil
}

func (e *matchingEngineImpl) UpdateTaskQueueUserData(ctx context.Context, request *matchingservice.UpdateTaskQueueUserDataRequest) (*matchingservice.UpdateTaskQueueUserDataResponse, error) {
	namespaceId := namespace.ID(request.NamespaceId)
	var applied, conflicting bool
	persistenceErr, ctxErr := e.getUserDataBatcher(namespaceId).Add(ctx, &userDataUpdate{
		taskQueue: request.GetTaskQueue(),
		update: persistence.SingleTaskQueueUserDataUpdate{
			UserData:        request.UserData,
			BuildIdsAdded:   request.BuildIdsAdded,
			BuildIdsRemoved: request.BuildIdsRemoved,
			Applied:         &applied,
			Conflicting:     &conflicting,
		},
	})
	if ctxErr != nil {
		// Return context errors as-is.
		return nil, ctxErr
	}
	// If applied is true, this one succeeded even though others in the batch failed.
	if persistenceErr != nil && !applied {
		if persistence.IsConflictErr(persistenceErr) {
			if conflicting {
				// This specific update was the conflicting one. Use InvalidArgument so the
				// caller does not retry.
				return nil, serviceerror.NewInvalidArgument(persistenceErr.Error())
			}
			// This update may or may not be conflicting. Use Unavailable to allow retries.
			return nil, serviceerror.NewUnavailable(persistenceErr.Error())
		}
		// Other errors from persistence get returned as-is.
		return nil, persistenceErr
	}
	return &matchingservice.UpdateTaskQueueUserDataResponse{}, nil
}

func (e *matchingEngineImpl) ReplicateTaskQueueUserData(ctx context.Context, request *matchingservice.ReplicateTaskQueueUserDataRequest) (*matchingservice.ReplicateTaskQueueUserDataResponse, error) {
	if e.namespaceReplicationQueue == nil {
		return &matchingservice.ReplicateTaskQueueUserDataResponse{}, nil
	}

	e.replicationLock.Lock()
	defer e.replicationLock.Unlock()

	err := e.namespaceReplicationQueue.Publish(ctx, &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_TASK_QUEUE_USER_DATA,
		Attributes: &replicationspb.ReplicationTask_TaskQueueUserDataAttributes{
			TaskQueueUserDataAttributes: &replicationspb.TaskQueueUserDataAttributes{
				NamespaceId:   request.GetNamespaceId(),
				TaskQueueName: request.GetTaskQueue(),
				UserData:      request.GetUserData(),
			},
		},
	})
	return &matchingservice.ReplicateTaskQueueUserDataResponse{}, err

}

func (e *matchingEngineImpl) CheckTaskQueueUserDataPropagation(ctx context.Context, req *matchingservice.CheckTaskQueueUserDataPropagationRequest) (*matchingservice.CheckTaskQueueUserDataPropagationResponse, error) {
	rootPartition, err := tqid.NormalPartitionFromRpcName(req.TaskQueue, req.NamespaceId, enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}
	pm, _, err := e.getTaskQueuePartitionManager(ctx, rootPartition, true, loadCauseOtherRead)
	if err != nil {
		return nil, err
	}

	nsName := pm.Namespace().Name().String()
	tqName := rootPartition.TaskQueue().Name()
	wfPartitions := max(
		e.config.NumTaskqueueReadPartitions(nsName, tqName, enumspb.TASK_QUEUE_TYPE_WORKFLOW),
		e.config.NumTaskqueueWritePartitions(nsName, tqName, enumspb.TASK_QUEUE_TYPE_WORKFLOW),
	)
	actPartitions := max(
		e.config.NumTaskqueueReadPartitions(nsName, tqName, enumspb.TASK_QUEUE_TYPE_ACTIVITY),
		e.config.NumTaskqueueWritePartitions(nsName, tqName, enumspb.TASK_QUEUE_TYPE_ACTIVITY),
	)

	err = pm.GetUserDataManager().CheckTaskQueueUserDataPropagation(ctx, req.Version, wfPartitions, actPartitions)
	if err != nil {
		return nil, err
	}
	return &matchingservice.CheckTaskQueueUserDataPropagationResponse{}, nil
}

// nexusResult is container for a response or error.
// Only one field may be set at a time.
type nexusResult struct {
	successfulWorkerResponse *matchingservice.RespondNexusTaskCompletedRequest
	failedWorkerResponse     *matchingservice.RespondNexusTaskFailedRequest
	internalError            error
}

func (e *matchingEngineImpl) DispatchNexusTask(ctx context.Context, request *matchingservice.DispatchNexusTaskRequest) (*matchingservice.DispatchNexusTaskResponse, error) {
	partition, err := tqid.PartitionFromProto(request.GetTaskQueue(), request.GetNamespaceId(), enumspb.TASK_QUEUE_TYPE_NEXUS)
	if err != nil {
		return nil, err
	}
	pm, _, err := e.getTaskQueuePartitionManager(ctx, partition, true, loadCauseNexusTask)
	if err != nil {
		return nil, err
	}

	taskID := uuid.NewString()

	namespaceID := namespace.ID(request.GetNamespaceId())
	ns, err := e.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}

	// Buffer the deadline so we can still respond with timeout if we hit the deadline while dispatching
	ctx, cancel := contextutil.WithDeadlineBuffer(ctx, matching.DefaultTimeout, e.config.MinDispatchTaskTimeout(ns.Name().String()))
	defer cancel()

	// First allocate a result channel and register it so that when the task is completed locally (without forwarding) the
	// result can be sent on this channel.
	resultCh := make(chan *nexusResult, 1)
	e.nexusResults.Set(taskID, resultCh)
	defer e.nexusResults.Delete(taskID)

	resp, err := pm.DispatchNexusTask(ctx, taskID, request)

	if err != nil {
		if ctx.Err() != nil {
			// The context deadline has expired if it reaches here; return an explicit timeout response to the caller.
			return &matchingservice.DispatchNexusTaskResponse{Outcome: &matchingservice.DispatchNexusTaskResponse_RequestTimeout{
				RequestTimeout: &matchingservice.DispatchNexusTaskResponse_Timeout{},
			}}, nil
		}
		return resp, err
	}

	// If we get a response it means that the Nexus task was handled by forwarding to another matching host this remote
	// host's result can be returned directly.
	if resp != nil {
		return resp, nil
	}

	// If we get here it means that task dispatch has occurred locally.
	// Must wait on result channel to get query result.
	select {
	case result := <-resultCh:
		if result.internalError != nil {
			return nil, result.internalError
		}
		if result.failedWorkerResponse != nil {
			if result.failedWorkerResponse.GetRequest().GetError() != nil { // nolint:staticcheck // checking deprecated field for backwards compatibility
				// Deprecated case. Kept for backwards-compatibility with older SDKs that are sending errors instead of failures.
				return &matchingservice.DispatchNexusTaskResponse{Outcome: &matchingservice.DispatchNexusTaskResponse_HandlerError{
					HandlerError: result.failedWorkerResponse.GetRequest().GetError(), // nolint:staticcheck // checking deprecated field for backwards compatibility
				}}, nil
			}
			return &matchingservice.DispatchNexusTaskResponse{Outcome: &matchingservice.DispatchNexusTaskResponse_Failure{
				Failure: result.failedWorkerResponse.GetRequest().GetFailure(),
			}}, nil
		}

		return &matchingservice.DispatchNexusTaskResponse{Outcome: &matchingservice.DispatchNexusTaskResponse_Response{
			Response: result.successfulWorkerResponse.GetRequest().GetResponse(),
		}}, nil
	case <-ctx.Done():
		// The context deadline has expired if it reaches here; return an explicit timeout response to the caller.
		return &matchingservice.DispatchNexusTaskResponse{Outcome: &matchingservice.DispatchNexusTaskResponse_RequestTimeout{
			RequestTimeout: &matchingservice.DispatchNexusTaskResponse_Timeout{},
		}}, nil
	}
}

func (e *matchingEngineImpl) PollNexusTaskQueue(
	ctx context.Context,
	req *matchingservice.PollNexusTaskQueueRequest,
	opMetrics metrics.Handler,
) (*matchingservice.PollNexusTaskQueueResponse, error) {
	namespaceID := namespace.ID(req.GetNamespaceId())
	pollerID := req.GetPollerId()
	request := req.Request
	taskQueueName := request.TaskQueue.GetName()
	e.logger.Debug("Received PollNexusTaskQueue for taskQueue", tag.Name(taskQueueName))
pollLoop:
	for {
		err := common.IsValidContext(ctx)
		if err != nil {
			return nil, err
		}
		// Add frontend generated pollerID to context so taskqueueMgr can support cancellation of
		// long-poll when frontend calls CancelOutstandingPoll API
		pollerCtx := context.WithValue(ctx, pollerIDKey, pollerID)
		pollerCtx = context.WithValue(pollerCtx, identityKey, request.GetIdentity())
		partition, err := tqid.PartitionFromProto(request.TaskQueue, req.NamespaceId, enumspb.TASK_QUEUE_TYPE_NEXUS)
		if err != nil {
			return nil, err
		}
		pollMetadata := &pollMetadata{
			workerVersionCapabilities: request.WorkerVersionCapabilities,
			deploymentOptions:         request.DeploymentOptions,
			forwardedFrom:             req.ForwardedSource,
			conditions:                req.Conditions,
			workerInstanceKey:         request.WorkerInstanceKey,
		}
		task, _, err := e.pollTask(pollerCtx, partition, pollMetadata)
		if err != nil {
			if errors.Is(err, errNoTasks) {
				return &matchingservice.PollNexusTaskQueueResponse{}, nil
			}
			return nil, err
		}

		if task.isStarted() {
			// tasks received from remote are already started. So, simply forward the response
			return task.pollNexusTaskQueueResponse(), nil
		}

		task.finish(err, true)
		if err != nil {
			continue pollLoop
		}

		taskToken := &tokenspb.NexusTask{
			NamespaceId: string(namespaceID),
			TaskQueue:   taskQueueName,
			TaskId:      task.nexus.taskID,
		}
		serializedToken, _ := e.tokenSerializer.SerializeNexusTaskToken(taskToken)

		nexusReq := task.nexus.request.GetRequest()
		if nexusReq.Header == nil {
			nexusReq.Header = make(map[string]string)
		}
		nexusReq.Header[nexus.HeaderRequestTimeout] = time.Until(task.nexus.deadline).String()
		// Java SDK currently expects the header in this form. We should be able to remove this duplication sometime mid 2025.
		nexusReq.Header["Request-Timeout"] = time.Until(task.nexus.deadline).String()
		if !task.nexus.operationDeadline.IsZero() {
			nexusReq.Header[nexus.HeaderOperationTimeout] = commonnexus.FormatDuration(time.Until(task.nexus.operationDeadline))
		}

		return &matchingservice.PollNexusTaskQueueResponse{
			Response: &workflowservice.PollNexusTaskQueueResponse{
				TaskToken:             serializedToken,
				Request:               nexusReq,
				PollerScalingDecision: task.pollerScalingDecision,
			},
		}, nil
	}
}

func (e *matchingEngineImpl) RespondNexusTaskCompleted(ctx context.Context, request *matchingservice.RespondNexusTaskCompletedRequest, opMetrics metrics.Handler) (*matchingservice.RespondNexusTaskCompletedResponse, error) {
	resultCh, ok := e.nexusResults.Pop(request.GetTaskId())
	if !ok {
		opMetrics.Counter(metrics.RespondNexusTaskFailedPerTaskQueueCounter.Name()).Record(1)
		return nil, serviceerror.NewNotFound("Nexus task not found or already expired")
	}
	resultCh <- &nexusResult{
		successfulWorkerResponse: request,
		internalError:            nil,
	}
	return &matchingservice.RespondNexusTaskCompletedResponse{}, nil
}

func (e *matchingEngineImpl) RespondNexusTaskFailed(ctx context.Context, request *matchingservice.RespondNexusTaskFailedRequest, opMetrics metrics.Handler) (*matchingservice.RespondNexusTaskFailedResponse, error) {
	resultCh, ok := e.nexusResults.Pop(request.GetTaskId())
	if !ok {
		opMetrics.Counter(metrics.RespondNexusTaskFailedPerTaskQueueCounter.Name()).Record(1)
		return nil, serviceerror.NewNotFound("Nexus task not found or already expired")
	}
	resultCh <- &nexusResult{
		failedWorkerResponse: request,
		internalError:        nil,
	}
	return &matchingservice.RespondNexusTaskFailedResponse{}, nil
}

func (e *matchingEngineImpl) CreateNexusEndpoint(ctx context.Context, request *matchingservice.CreateNexusEndpointRequest) (*matchingservice.CreateNexusEndpointResponse, error) {
	// Write API, let persistence verify table ownership.
	res, err := e.nexusEndpointClient.CreateNexusEndpoint(ctx, &internalCreateNexusEndpointRequest{
		spec:       request.GetSpec(),
		clusterID:  e.clusterMeta.GetClusterID(),
		timeSource: e.timeSource,
	})
	if err != nil {
		e.logger.Error("Failed to create Nexus endpoint", tag.Error(err), tag.Endpoint(request.GetSpec().GetName()))
	} else {
		e.logger.Info("Created Nexus endpoint", tag.Endpoint(request.GetSpec().GetName()))
	}
	return res, err
}

func (e *matchingEngineImpl) UpdateNexusEndpoint(ctx context.Context, request *matchingservice.UpdateNexusEndpointRequest) (*matchingservice.UpdateNexusEndpointResponse, error) {
	// Write API, let persistence verify table ownership.
	res, err := e.nexusEndpointClient.UpdateNexusEndpoint(ctx, &internalUpdateNexusEndpointRequest{
		endpointID: request.GetId(),
		version:    request.GetVersion(),
		spec:       request.GetSpec(),
		clusterID:  e.clusterMeta.GetClusterID(),
		timeSource: e.timeSource,
	})
	if err != nil {
		e.logger.Error("Failed to update Nexus endpoint", tag.Error(err), tag.Endpoint(request.GetSpec().GetName()))
	} else {
		e.logger.Info("Updated Nexus endpoint", tag.Endpoint(request.GetSpec().GetName()))
	}
	return res, err
}

func (e *matchingEngineImpl) DeleteNexusEndpoint(ctx context.Context, request *matchingservice.DeleteNexusEndpointRequest) (*matchingservice.DeleteNexusEndpointResponse, error) {
	// Write API, let persistence verify table ownership.
	res, err := e.nexusEndpointClient.DeleteNexusEndpoint(ctx, request)
	if err != nil {
		e.logger.Error("Failed to delete Nexus endpoint", tag.Error(err), tag.Endpoint(request.GetId()))
	} else {
		e.logger.Info("Deleted Nexus endpoint", tag.Endpoint(request.GetId()))
	}
	return res, err
}

func (e *matchingEngineImpl) ListNexusEndpoints(ctx context.Context, request *matchingservice.ListNexusEndpointsRequest) (*matchingservice.ListNexusEndpointsResponse, error) {
	lastKnownVersion := request.LastKnownTableVersion
	// Read API, verify table ownership via membership.
	isOwner, ownershipLostCh, err := e.checkNexusEndpointsOwnership()
	if err != nil {
		e.logger.Error("Failed to check Nexus endpoints ownership", tag.Error(err))
		return nil, serviceerror.NewAbortedf("cannot verify ownership of Nexus endpoints table: %v", err)
	}
	if !isOwner {
		e.logger.Error("Matching node doesn't think it's the Nexus endpoints table owner", tag.Error(err))
		return nil, serviceerror.NewAborted("matching node doesn't think it's the Nexus endpoints table owner")
	}

	if request.Wait {
		if request.NextPageToken != nil {
			return nil, serviceerror.NewInvalidArgument("request Wait=true and NextPageToken!=nil on ListNexusEndpoints request. waiting is only allowed on first page")
		}

		// if waiting, send request with unknown table version so we get the newest view of the table
		request.LastKnownTableVersion = 0

		var cancel context.CancelFunc
		ctx, cancel = contextutil.WithDeadlineBuffer(ctx, e.config.ListNexusEndpointsLongPollTimeout(), returnEmptyTaskTimeBudget)
		defer cancel()
	}

	for {
		resp, tableVersionChanged, err := e.nexusEndpointClient.ListNexusEndpoints(ctx, request)
		if err != nil {
			return resp, err
		}

		if request.Wait && lastKnownVersion == resp.TableVersion {
			// long-poll: wait for data to change/appear
			select {
			case <-ownershipLostCh:
				return nil, serviceerror.NewAborted("Nexus endpoints table ownership lost")
			case <-ctx.Done():
				return resp, nil
			case <-tableVersionChanged:
				continue
			}
		}

		return resp, err
	}
}

func (e *matchingEngineImpl) checkNexusEndpointsOwnership() (bool, <-chan struct{}, error) {
	// Get the channel before checking the condition to prevent the channel from being closed while we're running this
	// check.
	ch := e.nexusEndpointsOwnershipLostCh
	self := e.hostInfoProvider.HostInfo().Identity()
	owner, err := e.serviceResolver.Lookup(nexusEndpointsTablePartitionRoutingKey)
	if err != nil {
		return false, nil, fmt.Errorf("cannot resolve Nexus endpoints partition owner: %w", err)
	}
	return owner.Identity() == self, ch, nil
}

func (e *matchingEngineImpl) notifyNexusEndpointsOwnershipChange() {
	// We don't care about the channel returned here. This method is ensured to only be called from the single
	// watchMembership method and is the only way the channel may be replaced.
	isOwner, _, err := e.checkNexusEndpointsOwnership()
	if err != nil {
		e.logger.Error("Failed to check Nexus endpoints ownership", tag.Error(err))
		return
	}
	if !isOwner {
		close(e.nexusEndpointsOwnershipLostCh)
		e.nexusEndpointsOwnershipLostCh = make(chan struct{})
	}
	e.nexusEndpointClient.notifyOwnershipChanged(isOwner)
}

func (e *matchingEngineImpl) getUserDataBatcher(namespaceId namespace.ID) *stream_batcher.Batcher[*userDataUpdate, error] {
	// Note that values are never removed from this map. The batcher's goroutine will exit
	// after the idle time, though, which gets most of the desired resource savings.
	if batcher, ok := e.userDataUpdateBatchers.Get(namespaceId); ok {
		return batcher
	}
	fn := func(batch []*userDataUpdate) error {
		return e.applyUserDataUpdateBatch(namespaceId, batch)
	}
	newBatcher := stream_batcher.NewBatcher[*userDataUpdate, error](fn, userDataBatcherOptions, e.timeSource)
	batcher, _ := e.userDataUpdateBatchers.GetOrSet(namespaceId, newBatcher)
	return batcher
}

func (e *matchingEngineImpl) applyUserDataUpdateBatch(namespaceId namespace.ID, batch []*userDataUpdate) error {
	ctx, cancel := context.WithTimeout(context.Background(), ioTimeout)
	// TODO: should use namespace name here
	ctx = headers.SetCallerInfo(ctx, headers.NewBackgroundHighCallerInfo(namespaceId.String()))
	defer cancel()

	// convert to map
	updatesMap := make(map[string]*persistence.SingleTaskQueueUserDataUpdate)
	for _, update := range batch {
		updatesMap[update.taskQueue] = &update.update
	}

	// now apply the batch of updates
	return e.taskManager.UpdateTaskQueueUserData(ctx, &persistence.UpdateTaskQueueUserDataRequest{
		NamespaceID: namespaceId.String(),
		Updates:     updatesMap,
	})
}

func (e *matchingEngineImpl) getHostInfo(partitionKey string) (string, error) {
	host, err := e.serviceResolver.Lookup(partitionKey)
	if err != nil {
		return "", err
	}
	return host.GetAddress(), nil
}

func (e *matchingEngineImpl) getAllPartitionRpcNames(
	ns namespace.Name,
	taskQueue *taskqueuepb.TaskQueue,
	taskQueueType enumspb.TaskQueueType,
) ([]string, error) {
	var partitionKeys []string
	namespaceID, err := e.namespaceRegistry.GetNamespaceID(ns)
	if err != nil {
		return partitionKeys, err
	}
	taskQueueFamily, err := tqid.NewTaskQueueFamily(namespaceID.String(), taskQueue.GetName())
	if err != nil {
		return partitionKeys, err
	}

	n := e.config.NumTaskqueueWritePartitions(ns.String(), taskQueueFamily.Name(), taskQueueType)
	for i := range n {
		partitionKeys = append(partitionKeys, taskQueueFamily.TaskQueue(taskQueueType).NormalPartition(i).RpcName())
	}
	return partitionKeys, nil
}

func (e *matchingEngineImpl) pollTask(
	ctx context.Context,
	partition tqid.Partition,
	pollMetadata *pollMetadata,
) (*internalTask, bool, error) {
	pm, _, err := e.getTaskQueuePartitionManager(ctx, partition, true, loadCausePoll)
	if err != nil {
		return nil, false, err
	}

	pollMetadata.localPollStartTime = e.timeSource.Now()

	// We need to set a shorter timeout than the original ctx; otherwise, by the time ctx deadline is
	// reached, instead of emptyTask, context timeout error is returned to the frontend by the rpc stack,
	// which counts against our SLO. By shortening the timeout by a very small amount, the emptyTask can be
	// returned to the handler before a context timeout error is generated.
	ctx, cancel := contextutil.WithDeadlineBuffer(ctx, pm.LongPollExpirationInterval(), returnEmptyTaskTimeBudget)
	defer cancel()

	if pollerID, ok := ctx.Value(pollerIDKey).(string); ok && pollerID != "" {
		e.outstandingPollers.Set(pollerID, cancel)

		// Also track by worker instance key for bulk cancellation during shutdown.
		// Use UUID (not pollerID) because pollerID is reused when forwarded.
		workerInstanceKey := pollMetadata.workerInstanceKey
		pollerTrackerKey := uuid.NewString()
		if workerInstanceKey != "" {
			e.workerInstancePollers.Add(workerInstanceKey, pollerTrackerKey, cancel)
		}

		defer func() {
			e.outstandingPollers.Delete(pollerID)
			if workerInstanceKey != "" {
				e.workerInstancePollers.Remove(workerInstanceKey, pollerTrackerKey)
			}
		}()
	}
	return pm.PollTask(ctx, pollMetadata)
}

// Unloads the given task queue partition. If it has already been unloaded (i.e. it's not present in the loaded
// partitions map), then does nothing.
// partitions map), unloadPM.Stop(...) is still called.
func (e *matchingEngineImpl) unloadTaskQueuePartition(unloadPM taskQueuePartitionManager, unloadCause unloadCause) {
	e.unloadTaskQueuePartitionByKey(unloadPM.Partition(), unloadPM, unloadCause)
}

// Unloads a task queue partition by id. If unloadPM is given and the loaded partition for queueID does not match
// unloadPM, then nothing is unloaded from matching engine (but unloadPM will be stopped).
// Returns true if it unloaded a partition and false if not.
func (e *matchingEngineImpl) unloadTaskQueuePartitionByKey(
	partition tqid.Partition,
	unloadPM taskQueuePartitionManager,
	unloadCause unloadCause,
) bool {
	key := partition.Key()
	e.partitionsLock.Lock()
	foundTQM, ok := e.partitions[key]
	if !ok || (unloadPM != nil && foundTQM != unloadPM) {
		e.partitionsLock.Unlock()
		return false
	}
	delete(e.partitions, key)
	e.partitionsLock.Unlock()
	foundTQM.Stop(unloadCause)
	return true
}

// Responsible for emitting and updating loaded_physical_task_queue_count metric
func (e *matchingEngineImpl) updatePhysicalTaskQueueGauge(
	ns *namespace.Namespace,
	partition tqid.Partition,
	version PhysicalTaskQueueVersion,
	delta int,
) {
	// calculating versioned to be one of: “unversioned” or "buildId” or “versionSet”
	versioned := "unversioned"
	if dep := version.Deployment(); dep != nil {
		versioned = "deployment"
	} else if buildID := version.BuildId(); buildID != "" {
		versioned = "buildId"
	} else if versionSet := version.VersionSet(); versionSet != "" {
		versioned = "versionSet"
	}

	physicalTaskQueueParameters := taskQueueCounterKey{
		namespaceID:   partition.NamespaceId(),
		taskType:      partition.TaskType(),
		partitionType: partition.Kind(),
		versioned:     versioned,
	}

	e.gaugeMetrics.lock.Lock()
	e.gaugeMetrics.loadedPhysicalTaskQueueCount[physicalTaskQueueParameters] += delta
	loadedPhysicalTaskQueueCounter := e.gaugeMetrics.loadedPhysicalTaskQueueCount[physicalTaskQueueParameters]
	e.gaugeMetrics.lock.Unlock()

	metrics.LoadedPhysicalTaskQueueGauge.With(
		metrics.GetPerTaskQueuePartitionTypeScope(
			e.metricsHandler,
			ns.Name().String(),
			partition,
			// TODO: Track counters per TQ name so we can honor pm.config.BreakdownMetricsByTaskQueue(),
			false,
		)).Record(
		float64(loadedPhysicalTaskQueueCounter),
		metrics.VersionedTag(versioned),
	)
}

// Responsible for emitting and updating loaded_task_queue_family_count, loaded_task_queue_count and
// loaded_task_queue_partition_count metrics
func (e *matchingEngineImpl) updateTaskQueuePartitionGauge(
	ns *namespace.Namespace,
	partition tqid.Partition,
	delta int,
) {
	// each metric shall be accessed based on the mentioned parameters
	taskQueueFamilyParameters := taskQueueCounterKey{
		namespaceID: partition.NamespaceId(),
	}

	taskQueueParameters := taskQueueCounterKey{
		namespaceID: partition.NamespaceId(),
		taskType:    partition.TaskType(),
	}

	taskQueuePartitionParameters := taskQueueCounterKey{
		namespaceID:   partition.NamespaceId(),
		taskType:      partition.TaskType(),
		partitionType: partition.Kind(),
	}

	rootPartition := partition.IsRoot()
	e.gaugeMetrics.lock.Lock()

	loadedTaskQueueFamilyCounter, loadedTaskQueueCounter, loadedTaskQueuePartitionCounter :=
		e.gaugeMetrics.loadedTaskQueueFamilyCount[taskQueueFamilyParameters], e.gaugeMetrics.loadedTaskQueueCount[taskQueueParameters],
		e.gaugeMetrics.loadedTaskQueuePartitionCount[taskQueuePartitionParameters]

	loadedTaskQueuePartitionCounter += delta
	e.gaugeMetrics.loadedTaskQueuePartitionCount[taskQueuePartitionParameters] = loadedTaskQueuePartitionCounter
	if rootPartition {
		loadedTaskQueueCounter += delta
		e.gaugeMetrics.loadedTaskQueueCount[taskQueueParameters] = loadedTaskQueueCounter
		if partition.TaskType() == enumspb.TASK_QUEUE_TYPE_WORKFLOW {
			loadedTaskQueueFamilyCounter += delta
			e.gaugeMetrics.loadedTaskQueueFamilyCount[taskQueueFamilyParameters] = loadedTaskQueueFamilyCounter
		}
	}
	e.gaugeMetrics.lock.Unlock()

	nsName := ns.Name().String()

	e.metricsHandler.Gauge(metrics.LoadedTaskQueueFamilyGauge.Name()).Record(
		float64(loadedTaskQueueFamilyCounter),
		metrics.NamespaceTag(nsName),
	)

	metrics.LoadedTaskQueueGauge.With(e.metricsHandler).Record(
		float64(loadedTaskQueueCounter),
		metrics.NamespaceTag(nsName),
		metrics.TaskQueueTypeTag(taskQueueParameters.taskType),
	)

	taggedHandler := metrics.GetPerTaskQueuePartitionTypeScope(
		e.metricsHandler,
		nsName,
		partition,
		// TODO: Track counters per TQ name so we can honor pm.config.BreakdownMetricsByTaskQueue(),
		false,
	)
	metrics.LoadedTaskQueuePartitionGauge.With(taggedHandler).Record(float64(loadedTaskQueuePartitionCounter))
}

// Populate the workflow task response based on context and scheduled/started events.
func (e *matchingEngineImpl) createPollWorkflowTaskQueueResponse(
	task *internalTask,
	recordStartResp *historyservice.RecordWorkflowTaskStartedResponse,
	metricsHandler metrics.Handler,
) *matchingservice.PollWorkflowTaskQueueResponseWithRawHistory {

	var serializedToken []byte
	if task.isQuery() {
		// for a query task
		queryRequest := task.query.request
		queryTaskToken := &tokenspb.QueryTask{
			NamespaceId: queryRequest.GetNamespaceId(),
			TaskQueue:   queryRequest.TaskQueue.Name,
			TaskId:      task.query.taskID,
		}
		serializedToken, _ = e.tokenSerializer.SerializeQueryTaskToken(queryTaskToken)
	} else {
		taskToken := tasktoken.NewWorkflowTaskToken(
			task.event.Data.GetNamespaceId(),
			task.event.Data.GetWorkflowId(),
			task.event.Data.GetRunId(),
			recordStartResp.GetScheduledEventId(),
			recordStartResp.GetStartedEventId(),
			recordStartResp.GetStartedTime(),
			recordStartResp.GetAttempt(),
			recordStartResp.GetClock(),
			recordStartResp.GetVersion(),
		)
		serializedToken, _ = e.tokenSerializer.Serialize(taskToken)
		if task.responseC == nil {
			ct := timestamp.TimeValue(task.event.Data.CreateTime)
			metrics.AsyncMatchLatencyPerTaskQueue.With(metricsHandler).Record(time.Since(ct))
		}
	}

	response := common.CreateMatchingPollWorkflowTaskQueueResponse(
		recordStartResp,
		task.workflowExecution(),
		serializedToken)

	if task.query != nil {
		response.Query = task.query.request.QueryRequest.Query
	}
	if task.backlogCountHint != nil {
		response.BacklogCountHint = task.backlogCountHint()
	}
	response.PollerScalingDecision = task.pollerScalingDecision
	return response
}

// convertPollWorkflowTaskQueueResponse converts a PollWorkflowTaskQueueResponse to
// PollWorkflowTaskQueueResponseWithRawHistory. This is used when forwarding tasks
// from remote matching nodes where the client has already deserialized the response.
// This function processes search attributes if the history came from raw bytes
// (RawHistory field), since raw history hasn't been processed yet.
// If history came from the History field, it's already been processed by history service.
func (e *matchingEngineImpl) convertPollWorkflowTaskQueueResponse(
	resp *matchingservice.PollWorkflowTaskQueueResponse,
	ns namespace.Name,
) (*matchingservice.PollWorkflowTaskQueueResponseWithRawHistory, error) {
	if resp == nil {
		return nil, nil
	}
	// RawHistory from forwarded response contains deserialized History (auto-deserialized by gRPC).
	// We don't re-serialize it back to bytes. The History field is used instead.
	// Use RawHistory only if History is not available (backward compat with old matching services).
	history := resp.History
	if history == nil && resp.RawHistory != nil { //nolint:staticcheck
		history = resp.RawHistory //nolint:staticcheck
		// Process search attributes only when using RawHistory.
		// RawHistory contains auto-deserialized raw bytes that bypass history service's SA processing.
		// History field means it was already processed by history service.
		if err := api.ProcessOutgoingSearchAttributes(e.saProvider, e.saMapperProvider, history.Events, ns, e.visibilityManager); err != nil {
			return nil, err
		}
	}
	newResp := &matchingservice.PollWorkflowTaskQueueResponseWithRawHistory{
		TaskToken:                  resp.TaskToken,
		WorkflowExecution:          resp.WorkflowExecution,
		WorkflowType:               resp.WorkflowType,
		PreviousStartedEventId:     resp.PreviousStartedEventId,
		StartedEventId:             resp.StartedEventId,
		Attempt:                    resp.Attempt,
		NextEventId:                resp.NextEventId,
		BacklogCountHint:           resp.BacklogCountHint,
		StickyExecutionEnabled:     resp.StickyExecutionEnabled,
		Query:                      resp.Query,
		TransientWorkflowTask:      resp.TransientWorkflowTask,
		WorkflowExecutionTaskQueue: resp.WorkflowExecutionTaskQueue,
		BranchToken:                resp.BranchToken,
		ScheduledTime:              resp.ScheduledTime,
		StartedTime:                resp.StartedTime,
		Queries:                    resp.Queries,
		Messages:                   resp.Messages,
		History:                    history,
		NextPageToken:              resp.NextPageToken,
		PollerScalingDecision:      resp.PollerScalingDecision,
	}
	return newResp, nil
}

// Populate the activity task response based on context and scheduled/started events.
func (e *matchingEngineImpl) createPollActivityTaskQueueResponse(
	task *internalTask,
	historyResponse *historyservice.RecordActivityTaskStartedResponse,
	metricsHandler metrics.Handler,
) *matchingservice.PollActivityTaskQueueResponse {
	scheduledEvent := historyResponse.ScheduledEvent
	if scheduledEvent.GetActivityTaskScheduledEventAttributes() == nil {
		panic("GetActivityTaskScheduledEventAttributes is not set")
	}
	attributes := scheduledEvent.GetActivityTaskScheduledEventAttributes()
	if attributes.ActivityId == "" {
		panic("ActivityTaskScheduledEventAttributes.ActivityID is not set")
	}
	if task.responseC == nil {
		ct := timestamp.TimeValue(task.event.Data.CreateTime)
		metrics.AsyncMatchLatencyPerTaskQueue.With(metricsHandler).Record(time.Since(ct))
	}

	taskToken := tasktoken.NewActivityTaskToken(
		task.event.Data.GetNamespaceId(),
		task.event.Data.GetWorkflowId(),
		task.event.Data.GetRunId(),
		task.event.Data.GetScheduledEventId(),
		attributes.GetActivityId(),
		attributes.GetActivityType().GetName(),
		historyResponse.GetAttempt(),
		historyResponse.GetClock(),
		historyResponse.GetVersion(),
		historyResponse.GetStartVersion(),
		task.event.GetData().GetComponentRef(),
	)
	serializedToken, _ := e.tokenSerializer.Serialize(taskToken)

	// This is here to ensure that this field is never nil as expected by the TS SDK.
	// This may happen if ScheduleActivityExecution was recorded in version 1.23.
	scheduleToCloseTimeout := attributes.ScheduleToCloseTimeout
	if scheduleToCloseTimeout == nil {
		scheduleToCloseTimeout = timestamp.DurationPtr(0)
	}

	return &matchingservice.PollActivityTaskQueueResponse{
		ActivityId:                  attributes.ActivityId,
		ActivityType:                attributes.ActivityType,
		ActivityRunId:               historyResponse.GetActivityRunId(),
		Header:                      attributes.Header,
		Input:                       attributes.Input,
		WorkflowExecution:           task.workflowExecution(),
		CurrentAttemptScheduledTime: historyResponse.CurrentAttemptScheduledTime,
		ScheduledTime:               scheduledEvent.EventTime,
		ScheduleToCloseTimeout:      scheduleToCloseTimeout,
		StartedTime:                 historyResponse.StartedTime,
		StartToCloseTimeout:         attributes.StartToCloseTimeout,
		HeartbeatTimeout:            attributes.HeartbeatTimeout,
		TaskToken:                   serializedToken,
		Attempt:                     taskToken.Attempt,
		HeartbeatDetails:            historyResponse.HeartbeatDetails,
		WorkflowType:                historyResponse.WorkflowType,
		WorkflowNamespace:           historyResponse.WorkflowNamespace,
		PollerScalingDecision:       task.pollerScalingDecision,
		Priority:                    historyResponse.Priority,
		RetryPolicy:                 historyResponse.RetryPolicy,
	}
}

func (e *matchingEngineImpl) recordWorkflowTaskStarted(
	ctx context.Context,
	pollReq *workflowservice.PollWorkflowTaskQueueRequest,
	task *internalTask,
) (*historyservice.RecordWorkflowTaskStartedResponse, error) {

	metrics.OperationCounter.With(e.metricsHandler).Record(
		1,
		metrics.OperationTag("RecordWorkflowTaskStarted"),
		metrics.NamespaceTag(pollReq.Namespace),
		metrics.TaskTypeTag(""), // Added to make tags consistent with history task executor.
	)
	if e.rateLimiter != nil {
		err := e.rateLimiter.Wait(ctx, quotas.Request{
			API:        "RecordWorkflowTaskStarted",
			Token:      1,
			Caller:     pollReq.Namespace,
			CallerType: headers.CallerTypeAPI,
		})
		if err != nil {
			return nil, err
		}
	}

	ctx, cancel := newRecordTaskStartedContext(ctx, task)
	defer cancel()

	// Only send the target Deployment Version if it is different from the poller's version.
	// If the poller is not versioned, we still send the target version because the workflow may be moving from an
	// unversioned worker to a versioned worker.
	var sentTargetVersion *deploymentpb.WorkerDeploymentVersion
	if task.targetWorkerDeploymentVersion.GetBuildId() != pollReq.DeploymentOptions.GetBuildId() ||
		task.targetWorkerDeploymentVersion.GetDeploymentName() != pollReq.DeploymentOptions.GetDeploymentName() {
		sentTargetVersion = worker_versioning.ExternalWorkerDeploymentVersionFromVersion(task.targetWorkerDeploymentVersion)
	}

	recordStartedRequest := &historyservice.RecordWorkflowTaskStartedRequest{
		NamespaceId:         task.event.Data.GetNamespaceId(),
		WorkflowExecution:   task.workflowExecution(),
		ScheduledEventId:    task.event.Data.GetScheduledEventId(),
		Clock:               task.event.Data.GetClock(),
		RequestId:           uuid.NewString(),
		PollRequest:         pollReq,
		BuildIdRedirectInfo: task.redirectInfo,
		// TODO: stop sending ScheduledDeployment. [cleanup-old-wv]
		ScheduledDeployment:        worker_versioning.DirectiveDeployment(task.event.Data.VersionDirective),
		VersionDirective:           task.event.Data.VersionDirective,
		Stamp:                      task.event.Data.GetStamp(),
		TaskDispatchRevisionNumber: task.taskDispatchRevisionNumber,
		TargetDeploymentVersion:    sentTargetVersion,
	}

	resp, err := e.historyClient.RecordWorkflowTaskStarted(ctx, recordStartedRequest)
	if err != nil {
		return nil, err
	}

	// History service returns RecordWorkflowTaskStartedResponseWithRawHistory on the wire,
	// but the gRPC client deserializes it as RecordWorkflowTaskStartedResponse.
	// Due to wire compatibility:
	// - Server's RawHistory (repeated bytes, field 20) -> Client's RawHistory (*History, auto-deserialized)
	// - Server's RawHistoryBytes (repeated bytes, field 21) -> Client's RawHistoryBytes ([][]byte, stays as raw)
	//
	// Handle history fields - check which one has data:
	// 1. RawHistoryBytes (new path) - raw bytes, pass through to frontend
	// 2. RawHistory (old path) - auto-deserialized to *History by gRPC wire compatibility
	// 3. History - use directly (raw history disabled)
	if len(resp.RawHistoryBytes) > 0 {
		// New path: raw bytes in field 21, pass through to frontend without processing.
		// Search attributes will be processed by frontend.
	} else if resp.RawHistory != nil { //nolint:staticcheck
		// Old path: history service using deprecated RawHistory field (field 20).
		// The gRPC client auto-deserializes repeated bytes into *History via wire compatibility.
		// Since this came from raw bytes, search attributes haven't been processed yet.
		// Process them here before moving to History field.
		ns := namespace.Name(pollReq.Namespace)
		if err := api.ProcessOutgoingSearchAttributes(e.saProvider, e.saMapperProvider, resp.RawHistory.Events, ns, e.visibilityManager); err != nil { //nolint:staticcheck
			return nil, err
		}
		// Move to History field for consistent handling downstream.
		resp.History = resp.RawHistory //nolint:staticcheck
		resp.RawHistory = nil          //nolint:staticcheck
	}
	// If neither RawHistoryBytes nor RawHistory is set, resp.History should already have the data.

	return resp, nil
}

func (e *matchingEngineImpl) recordActivityTaskStarted(
	ctx context.Context,
	pollReq *workflowservice.PollActivityTaskQueueRequest,
	task *internalTask,
) (*historyservice.RecordActivityTaskStartedResponse, error) {

	metrics.OperationCounter.With(e.metricsHandler).Record(
		1,
		metrics.OperationTag("RecordActivityTaskStarted"),
		metrics.NamespaceTag(pollReq.Namespace),
		metrics.TaskTypeTag(""), // Added to make tags consistent with history task executor.
	)
	if e.rateLimiter != nil {
		err := e.rateLimiter.Wait(ctx, quotas.Request{
			API:        "RecordActivityTaskStarted",
			Token:      1,
			Caller:     pollReq.Namespace,
			CallerType: headers.CallerTypeAPI,
		})
		if err != nil {
			return nil, err
		}
	}

	ctx, cancel := newRecordTaskStartedContext(ctx, task)
	defer cancel()

	recordStartedRequest := &historyservice.RecordActivityTaskStartedRequest{
		NamespaceId:         task.event.Data.GetNamespaceId(),
		WorkflowExecution:   task.workflowExecution(),
		ScheduledEventId:    task.event.Data.GetScheduledEventId(),
		Clock:               task.event.Data.GetClock(),
		RequestId:           uuid.NewString(),
		PollRequest:         pollReq,
		BuildIdRedirectInfo: task.redirectInfo,
		Stamp:               task.event.Data.GetStamp(),
		// TODO: stop sending ScheduledDeployment. [cleanup-old-wv]
		ScheduledDeployment:        worker_versioning.DirectiveDeployment(task.event.Data.VersionDirective),
		VersionDirective:           task.event.Data.VersionDirective,
		TaskDispatchRevisionNumber: task.taskDispatchRevisionNumber,
		ComponentRef:               task.event.Data.GetComponentRef(),
	}

	return e.historyClient.RecordActivityTaskStarted(ctx, recordStartedRequest)
}

// newRecordTaskStartedContext creates a context for recording
// activity or workflow task started. The parentCtx from
// pollActivity/WorkflowTaskQueue endpoint (which is a long poll
// API) has long timeout and unsuitable for recording task started,
// especially if the task is doing sync match and has caller
// (history transfer queue) waiting for response.
func newRecordTaskStartedContext(
	parentCtx context.Context,
	task *internalTask,
) (context.Context, context.CancelFunc) {
	timeout := recordTaskStartedDefaultTimeout
	if task.isSyncMatchTask() {
		timeout = recordTaskStartedSyncMatchTimeout
	}

	return context.WithTimeout(parentCtx, timeout)
}

// Revives a deleted build ID updating its HLC timestamp.
// Returns a new build ID leaving the provided one untouched.
func (e *matchingEngineImpl) reviveBuildId(ns *namespace.Namespace, taskQueue string, buildId *persistencespb.BuildId) *persistencespb.BuildId {
	// Bump the stamp and ensure it's newer than the deletion stamp.
	prevStamp := common.CloneProto(buildId.StateUpdateTimestamp)
	stamp := hlc.Next(prevStamp, e.timeSource)
	stamp.ClusterId = e.clusterMeta.GetClusterID()
	e.logger.Info("Revived build ID while applying replication event",
		tag.WorkflowNamespace(ns.Name().String()),
		tag.WorkflowTaskQueueName(taskQueue),
		tag.BuildId(buildId.Id))
	return &persistencespb.BuildId{
		Id:                     buildId.GetId(),
		State:                  persistencespb.STATE_ACTIVE,
		StateUpdateTimestamp:   stamp,
		BecameDefaultTimestamp: buildId.BecameDefaultTimestamp,
	}
}

// We use a very short timeout for considering a sticky worker available, since tasks can also
// be processed on the normal queue.
func stickyWorkerAvailable(pm taskQueuePartitionManager) bool {
	return pm != nil && pm.HasPollerAfter("", time.Now().Add(-stickyPollerUnavailableWindow))
}

func buildRateLimitConfig(update *workflowservice.UpdateTaskQueueConfigRequest_RateLimitUpdate, updateTime *timestamppb.Timestamp, updateIdentity string) *taskqueuepb.RateLimitConfig {
	var rateLimit *taskqueuepb.RateLimit
	if r := update.GetRateLimit(); r != nil {
		rateLimit = &taskqueuepb.RateLimit{RequestsPerSecond: r.RequestsPerSecond}
	}
	return &taskqueuepb.RateLimitConfig{
		RateLimit: rateLimit,
		Metadata: &taskqueuepb.ConfigMetadata{
			Reason:         update.GetReason(),
			UpdateTime:     updateTime,
			UpdateIdentity: updateIdentity,
		},
	}
}

func prepareTaskQueueUserData(
	tqud *persistencespb.TaskQueueUserData,
	taskQueueType enumspb.TaskQueueType,
) *persistencespb.TaskQueueUserData {
	data := common.CloneProto(tqud)
	if data == nil {
		data = &persistencespb.TaskQueueUserData{}
	}
	if data.PerType == nil {
		data.PerType = make(map[int32]*persistencespb.TaskQueueTypeUserData)
	}
	tqType := int32(taskQueueType)
	if data.PerType[tqType] == nil {
		data.PerType[tqType] = &persistencespb.TaskQueueTypeUserData{}
	}
	if data.PerType[tqType].Config == nil {
		data.PerType[tqType].Config = &taskqueuepb.TaskQueueConfig{}
	}
	return data
}

func (e *matchingEngineImpl) CheckTaskQueueVersionMembership(
	ctx context.Context,
	request *matchingservice.CheckTaskQueueVersionMembershipRequest,
) (*matchingservice.CheckTaskQueueVersionMembershipResponse, error) {
	partition, err := tqid.PartitionFromProto(&taskqueuepb.TaskQueue{
		Name: request.GetTaskQueue(),
		Kind: enumspb.TASK_QUEUE_KIND_NORMAL,
	}, request.GetNamespaceId(), request.GetTaskQueueType())
	if err != nil {
		return nil, err
	}
	pm, _, err := e.getTaskQueuePartitionManager(ctx, partition, true, loadCauseOtherRead)
	if err != nil {
		return nil, err
	}

	userData, _, err := pm.GetUserDataManager().GetUserData()
	if err != nil {
		return nil, err
	}

	typedUserData := userData.GetData().GetPerType()[int32(request.GetTaskQueueType())]
	present := worker_versioning.HasDeploymentVersion(typedUserData.GetDeploymentData(), request.GetVersion())
	return &matchingservice.CheckTaskQueueVersionMembershipResponse{IsMember: present}, nil
}

func (e *matchingEngineImpl) UpdateTaskQueueConfig(
	ctx context.Context,
	request *matchingservice.UpdateTaskQueueConfigRequest,
) (*matchingservice.UpdateTaskQueueConfigResponse, error) {
	taskQueueFamily, err := tqid.NewTaskQueueFamily(request.NamespaceId, request.UpdateTaskqueueConfig.GetTaskQueue())
	if err != nil {
		return nil, err
	}
	taskQueueType := request.UpdateTaskqueueConfig.GetTaskQueueType()
	// Get the partition manager for the root workflow partition of the task queue family.
	// Configuration updates are applied here and eventually propagate,
	// to all partitions and associated activity task queues of the same task queue family.
	tqm, _, err := e.getTaskQueuePartitionManager(ctx,
		taskQueueFamily.TaskQueue(enumspb.TASK_QUEUE_TYPE_WORKFLOW).RootPartition(),
		true, loadCauseOtherWrite)
	if err != nil {
		return nil, err
	}
	if request.GetUpdateTaskqueueConfig() == nil {
		tqud, _, err := tqm.GetUserDataManager().GetUserData()
		if err != nil {
			return nil, err
		}
		// If no update is requested, return the current config.
		return &matchingservice.UpdateTaskQueueConfigResponse{
			UpdatedTaskqueueConfig: tqud.GetData().GetPerType()[int32(taskQueueType)].GetConfig(),
		}, nil
	}
	updateOptions := UserDataUpdateOptions{Source: "UpdateTaskQueueConfig"}
	_, err = tqm.GetUserDataManager().UpdateUserData(ctx, updateOptions,
		func(tqud *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
			data := prepareTaskQueueUserData(tqud, taskQueueType)
			// Update timestamp from hlc clock
			existingClock := data.Clock
			if existingClock == nil {
				existingClock = hlc.Zero(e.clusterMeta.GetClusterID())
			}
			now := hlc.Next(existingClock, e.timeSource)
			protoTs := hlc.ProtoTimestamp(now)

			// Update relevant config fields
			cfg := data.PerType[int32(taskQueueType)].Config
			updateTaskQueueConfig := request.GetUpdateTaskqueueConfig()
			updateIdentity := updateTaskQueueConfig.GetIdentity()

			// Queue Rate Limit
			if qrl := updateTaskQueueConfig.GetUpdateQueueRateLimit(); qrl != nil {
				cfg.QueueRateLimit = buildRateLimitConfig(qrl, protoTs, updateIdentity)
			}

			// Fairness Queue Rate Limit
			if fkrl := updateTaskQueueConfig.GetUpdateFairnessKeyRateLimitDefault(); fkrl != nil {
				cfg.FairnessKeysRateLimitDefault = buildRateLimitConfig(fkrl, protoTs, updateIdentity)
			}

			// Fairness Weight Overrides
			if len(updateTaskQueueConfig.GetSetFairnessWeightOverrides()) > 0 ||
				len(updateTaskQueueConfig.GetUnsetFairnessWeightOverrides()) > 0 {
				cfg.FairnessWeightOverrides, err = mergeFairnessWeightOverrides(
					cfg.FairnessWeightOverrides,
					updateTaskQueueConfig.GetSetFairnessWeightOverrides(),
					updateTaskQueueConfig.GetUnsetFairnessWeightOverrides(),
					tqm.GetConfig().MaxFairnessKeyWeightOverrides(),
				)
				if err != nil {
					return nil, false, err
				}
			}

			// Update the clock on TaskQueueUserData to enforce LWW on config updates
			data.Clock = now
			return data, true, nil
		},
	)
	if err != nil {
		return nil, err
	}
	userData, _, err := tqm.GetUserDataManager().GetUserData()
	if err != nil {
		return nil, err
	}
	return &matchingservice.UpdateTaskQueueConfigResponse{
		UpdatedTaskqueueConfig: userData.GetData().GetPerType()[int32(taskQueueType)].GetConfig(),
	}, nil
}

func (e *matchingEngineImpl) UpdateFairnessState(
	ctx context.Context,
	req *matchingservice.UpdateFairnessStateRequest,
) (*matchingservice.UpdateFairnessStateResponse, error) {
	partition, err := tqid.NormalPartitionFromRpcName(req.GetTaskQueue(), req.GetNamespaceId(), enumspb.TASK_QUEUE_TYPE_WORKFLOW)
	if err != nil {
		return nil, err
	}

	pm, _, err := e.getTaskQueuePartitionManager(ctx, partition, true, loadCauseOtherWrite)
	if err != nil {
		return nil, err
	}

	updateFn := func(old *persistencespb.TaskQueueUserData) (*persistencespb.TaskQueueUserData, bool, error) {
		data := old
		if data != nil {
			data = common.CloneProto(old)
		} else {
			data = &persistencespb.TaskQueueUserData{}
		}
		if data.PerType == nil {
			data.PerType = make(map[int32]*persistencespb.TaskQueueTypeUserData)
		}
		typ := int32(req.GetTaskQueueType())
		perType := data.PerType[typ]
		if perType == nil {
			data.PerType[typ] = &persistencespb.TaskQueueTypeUserData{}
			perType = data.PerType[typ]
		}
		perType.FairnessState = req.FairnessState
		return data, true, nil
	}
	_, err = pm.GetUserDataManager().UpdateUserData(ctx, UserDataUpdateOptions{Source: "Matching auto enable"}, updateFn)
	if err != nil {
		return nil, err
	}
	return &matchingservice.UpdateFairnessStateResponse{}, nil
}

// migrateOldFormatVersions moves versions present in the given deployment from the
// deprecated old-format slice into the new per-deployment map.
//
//nolint:staticcheck // SA1019 deprecated versions will clean up later
func migrateOldFormatVersions(
	deploymentData *persistencespb.DeploymentData,
	deploymentName string,
	workerDeploymentData *persistencespb.WorkerDeploymentData,
) {

	oldVersions := deploymentData.GetVersions()
	dst := make([]*deploymentspb.DeploymentVersionData, 0, len(oldVersions))
	for _, dv := range oldVersions {
		if dv.GetVersion().GetDeploymentName() == deploymentName {
			// Move membership from old format into the per-deployment new-format map.
			buildID := dv.GetVersion().GetBuildId()
			if _, exists := workerDeploymentData.Versions[buildID]; !exists {
				workerDeploymentData.Versions[buildID] = &deploymentspb.WorkerDeploymentVersionData{
					Status: dv.GetStatus(),
				}
			}
			continue
		}
		dst = append(dst, dv)
	}
	deploymentData.Versions = dst
}

// removeDeploymentVersions removes provided build IDs from the new-format per-deployment map and,
// when requested, the corresponding entries from the deprecated old-format slice for the same deployment.
// It returns true if any change was made (either format).
//
//nolint:staticcheck // SA1019 deprecated versions will clean up later
func removeDeploymentVersions(
	deploymentData *persistencespb.DeploymentData,
	deploymentName string,
	workerDeploymentData *persistencespb.WorkerDeploymentData,
	buildIDs []string,
	removeOldFormat bool,
) bool {
	if workerDeploymentData == nil {
		return false
	}
	changed := false
	deletedInNew := false

	for _, buildID := range buildIDs {
		if _, exists := workerDeploymentData.Versions[buildID]; exists {
			delete(workerDeploymentData.Versions, buildID)
			deletedInNew = true
			changed = true
		}
		if removeOldFormat {
			// Remove the version from the old deployment data format if present.
			for idx, oldVersions := range deploymentData.GetVersions() {
				if oldVersions.GetVersion().GetDeploymentName() == deploymentName &&
					oldVersions.GetVersion().GetBuildId() == buildID {
					//nolint:staticcheck // SA1019 deprecated versions will clean up later
					deploymentData.Versions = append(deploymentData.Versions[:idx], deploymentData.Versions[idx+1:]...)
					changed = true
					break
				}
			}
		}
	}

	// Only remove the deployment entry if versions were actually deleted from the new-format map.
	if deletedInNew && len(workerDeploymentData.Versions) == 0 {
		delete(deploymentData.GetDeploymentsData(), deploymentName)
	}
	return changed
}

// clearVersionFromRoutingConfig clears current/ramping fields in new-format routing config
// when an old-format DeploymentVersionData's roles change.
func clearVersionFromRoutingConfig(
	workerDeploymentData *persistencespb.WorkerDeploymentData,
	oldVd *deploymentspb.DeploymentVersionData,
	newVd *deploymentspb.DeploymentVersionData,
) {
	if workerDeploymentData == nil || workerDeploymentData.RoutingConfig == nil || newVd == nil {
		return
	}
	rc := workerDeploymentData.GetRoutingConfig()

	if newVd.GetRampingSinceTime() != nil {
		// Ramping version is cleared from the RoutingConfig. Note: When the ramping version is being set to unversioned,
		// the code takes a different path. See SyncDeploymentUserData for more details.
		rc.RampingDeploymentVersion = nil
		rc.RampingVersionPercentage = 0
		rc.RampingVersionPercentageChangedTime = nil
		rc.RampingVersionChangedTime = nil
	}

	// Check if current role changed. If it did, clear current from RC.
	oldCurrent := oldVd.GetCurrentSinceTime() != nil
	newCurrent := newVd.GetCurrentSinceTime() != nil
	if oldCurrent != newCurrent || newCurrent {
		//nolint:staticcheck // SA1019
		rc.CurrentVersion = ""
		rc.CurrentDeploymentVersion = nil
		rc.CurrentVersionChangedTime = nil
	}
}

func unsetRampingFromRoutingConfig(
	workerDeploymentData *persistencespb.WorkerDeploymentData,
) {
	if workerDeploymentData == nil || workerDeploymentData.RoutingConfig == nil {
		return
	}

	rc := workerDeploymentData.GetRoutingConfig()
	rc.RampingDeploymentVersion = nil
	rc.RampingVersionPercentage = 0
	rc.RampingVersionPercentageChangedTime = nil
	rc.RampingVersionChangedTime = nil
}
