package worker

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/util"
	workercommon "go.temporal.io/server/service/worker/common"
	expmaps "golang.org/x/exp/maps"
)

const (
	// Always refresh workers after this time even if there were no membership or namespace
	// state changes. This is to pick up dynamic config changes in component enabled status,
	// and heal anything that may have gotten into a bad state.
	refreshInterval = 10 * time.Minute
)

type (
	PerNamespaceWorkerManager struct {
		status int32

		// from init params or Start
		logger            log.Logger
		sdkClientFactory  sdk.ClientFactory
		namespaceRegistry namespace.Registry
		self              membership.HostInfo
		hostName          resource.HostName
		config            *Config
		taskQueueName     string
		serviceResolver   membership.ServiceResolver
		components        []workercommon.PerNSWorkerComponent
		initialRetry      time.Duration
		thisClusterName   string
		startLimiter      quotas.RateLimiter

		membershipChangedCh chan *membership.ChangedEvent

		lock    sync.Mutex
		workers map[namespace.ID]*perNamespaceWorker
	}

	perNamespaceWorker struct {
		wm     *PerNamespaceWorkerManager
		logger log.Logger
		cancel func()

		lock sync.Mutex // protects below fields
		refreshArgs
		retrier      backoff.Retrier
		retryTimer   *time.Timer
		reserved     bool // whether startLimiter.Reserve was already called
		componentSet string
		client       sdkclient.Client
		worker       sdkworker.Worker
		cleanup      []func()
	}

	// mutable parts of perNamespaceWorker that we want to pass around as a copy to use without
	// holding the lock.
	refreshArgs struct {
		ns    *namespace.Namespace // can change on namespace change notification
		count int                  // pushed from dynamic config
		opts  sdkworker.Options    // pushed from dynamic config
	}

	workerAllocation struct {
		total, local int
	}

	errRetryAfter time.Duration
)

var (
	errNoWorkerNeeded = errors.New("no worker needed") // sentinel value, not a real error
	// errInvalidConfiguration indicates that the value provided by dynamic config is not legal
	errInvalidConfiguration = errors.New("invalid dynamic configuration")
)

func NewPerNamespaceWorkerManager(
	logger log.Logger,
	sdkClientFactory sdk.ClientFactory,
	namespaceRegistry namespace.Registry,
	hostName resource.HostName,
	config *Config,
	clusterMetadata cluster.Metadata,
	components []workercommon.PerNSWorkerComponent,
	taskQueueName string,
) *PerNamespaceWorkerManager {
	return &PerNamespaceWorkerManager{
		logger:              log.With(logger, tag.ComponentPerNSWorkerManager),
		sdkClientFactory:    sdkClientFactory,
		namespaceRegistry:   namespaceRegistry,
		hostName:            hostName,
		taskQueueName:       taskQueueName,
		config:              config,
		components:          components,
		initialRetry:        1 * time.Second,
		thisClusterName:     clusterMetadata.GetCurrentClusterName(),
		startLimiter:        quotas.NewDefaultOutgoingRateLimiter(quotas.RateFn(config.PerNamespaceWorkerStartRate)),
		membershipChangedCh: make(chan *membership.ChangedEvent),
		workers:             make(map[namespace.ID]*perNamespaceWorker),
	}
}

func (wm *PerNamespaceWorkerManager) Running() bool {
	return atomic.LoadInt32(&wm.status) == common.DaemonStatusStarted
}

func (wm *PerNamespaceWorkerManager) Start(
	self membership.HostInfo,
	serviceResolver membership.ServiceResolver,
) {
	if !atomic.CompareAndSwapInt32(
		&wm.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	wm.self = self
	wm.serviceResolver = serviceResolver

	wm.logger.Info("", tag.LifeCycleStarting)

	// this will call namespaceCallback with current namespaces
	wm.namespaceRegistry.RegisterStateChangeCallback(wm, wm.namespaceCallback)

	err := wm.serviceResolver.AddListener(fmt.Sprintf("%p", wm), wm.membershipChangedCh)
	if err != nil {
		wm.logger.Fatal("Unable to register membership listener", tag.Error(err))
	}
	go wm.membershipChangedListener()
	go wm.periodicRefresh()

	wm.logger.Info("", tag.LifeCycleStarted)
}

func (wm *PerNamespaceWorkerManager) Stop() {
	if !atomic.CompareAndSwapInt32(
		&wm.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	wm.logger.Info("", tag.LifeCycleStopping)

	wm.namespaceRegistry.UnregisterStateChangeCallback(wm)
	err := wm.serviceResolver.RemoveListener(fmt.Sprintf("%p", wm))
	if err != nil {
		wm.logger.Error("Unable to unregister membership listener", tag.Error(err))
	}
	close(wm.membershipChangedCh)

	wm.lock.Lock()
	workers := expmaps.Values(wm.workers)
	maps.DeleteFunc(wm.workers, func(_ namespace.ID, _ *perNamespaceWorker) bool { return true })
	wm.lock.Unlock()

	for _, worker := range workers {
		worker.stopWorkerAndResetTimer()
		worker.cancel()
	}

	wm.logger.Info("", tag.LifeCycleStopped)
}

func (wm *PerNamespaceWorkerManager) namespaceCallback(ns *namespace.Namespace, nsDeleted bool) {
	go wm.getWorkerByNamespace(ns).update(ns, nsDeleted, nil, nil)
}

func (wm *PerNamespaceWorkerManager) refreshAll() {
	wm.lock.Lock()
	defer wm.lock.Unlock()
	for _, worker := range wm.workers {
		go worker.update(nil, false, nil, nil)
	}
}

func (wm *PerNamespaceWorkerManager) membershipChangedListener() {
	for range wm.membershipChangedCh {
		wm.refreshAll()
	}
}

func (wm *PerNamespaceWorkerManager) periodicRefresh() {
	for range time.NewTicker(refreshInterval).C {
		if atomic.LoadInt32(&wm.status) != common.DaemonStatusStarted {
			return
		}
		wm.refreshAll()
	}
}

func (wm *PerNamespaceWorkerManager) getWorkerByNamespace(ns *namespace.Namespace) *perNamespaceWorker {
	wm.lock.Lock()
	defer wm.lock.Unlock()

	if worker, ok := wm.workers[ns.ID()]; ok {
		return worker
	}

	worker := &perNamespaceWorker{
		wm:      wm,
		logger:  log.With(wm.logger, tag.WorkflowNamespace(ns.Name().String())),
		retrier: backoff.NewRetrier(backoff.NewExponentialRetryPolicy(wm.initialRetry), clock.NewRealTimeSource()),
	}
	count, c1 := wm.config.PerNamespaceWorkerCount(ns.Name().String(), worker.setWorkerCount)
	opts, c2 := wm.config.PerNamespaceWorkerOptions(ns.Name().String(), worker.setWorkerOptions)
	worker.ns = ns
	worker.count = count
	worker.opts = opts
	worker.cancel = func() { c1(); c2() }

	wm.workers[ns.ID()] = worker
	return worker
}

func (wm *PerNamespaceWorkerManager) removeWorker(ns *namespace.Namespace) {
	wm.lock.Lock()
	defer wm.lock.Unlock()
	prev := wm.workers[ns.ID()]
	delete(wm.workers, ns.ID())
	if prev != nil {
		prev.cancel()
	}
}

func (w *perNamespaceWorker) getWorkerAllocation(args refreshArgs) (workerAllocation, error) {
	if args.count < 0 {
		return workerAllocation{}, errInvalidConfiguration
	} else if args.count == 0 {
		return workerAllocation{0, 0}, nil
	}
	localCount, err := w.getLocallyDesiredWorkers(args)
	if err != nil {
		return workerAllocation{}, err
	}
	return workerAllocation{total: args.count, local: localCount}, nil
}

func (w *perNamespaceWorker) getLocallyDesiredWorkers(args refreshArgs) (int, error) {
	key := args.ns.ID().String()
	availableHosts := w.wm.serviceResolver.LookupN(key, args.count)
	hostsCount := len(availableHosts)
	if hostsCount == 0 {
		return 0, membership.ErrInsufficientHosts
	}
	maxWorkersPerHost := args.count/hostsCount + 1
	desiredDistribution := util.RepeatSlice(availableHosts, maxWorkersPerHost)[:args.count]

	isLocal := func(info membership.HostInfo) bool { return info.Identity() == w.wm.self.Identity() }
	result := len(util.FilterSlice(desiredDistribution, isLocal))
	return result, nil
}

func (w *perNamespaceWorker) setWorkerCount(count int) {
	w.update(nil, false, &count, nil)
}

func (w *perNamespaceWorker) setWorkerOptions(opts sdkworker.Options) {
	w.update(nil, false, nil, &opts)
}

// called on namespace state change callback, membership change, and dynamic config change
func (w *perNamespaceWorker) update(ns *namespace.Namespace, nsDeleted bool, newCount *int, newOpts *sdkworker.Options) {
	w.lock.Lock()

	if ns != nil {
		w.ns = ns
		// The name inside of *ns, which was used to initialize the logger, can change, but
		// don't update w.logger here, otherwise we'd have to hold w.lock just to log.
	}
	if newCount != nil {
		w.count = *newCount
	}
	if newOpts != nil {
		w.opts = *newOpts
	}

	refreshArgs := w.refreshArgs // copy before releasing lock
	isRetrying := w.retryTimer != nil
	w.lock.Unlock()

	if nsDeleted {
		w.stopWorkerAndResetTimer()
		// if namespace is fully deleted from db, we can remove from our map also
		w.wm.removeWorker(ns)
		return
	}

	if !isRetrying {
		w.refresh(refreshArgs)
	}
}

// handleError should be called on errors from worker creation or run. it will attempt to
// refresh the worker again at a later time.
func (w *perNamespaceWorker) handleError(err error) {
	if err == nil {
		return
	} else if err == errNoWorkerNeeded {
		w.stopWorkerAndResetTimer()
		return
	}

	w.lock.Lock()
	defer w.lock.Unlock()

	if w.retryTimer != nil {
		// this shouldn't ever happen
		w.logger.Error("bug: handleError found existing timer")
		return
	}

	var sleep time.Duration

	if retryAfter, ok := err.(errRetryAfter); ok {
		// asked for an explicit delay due to rate limit, use that
		sleep = time.Duration(retryAfter)
	} else {
		sleep = w.retrier.NextBackOff(err)
		if sleep < 0 {
			w.logger.Error("Failed to start sdk worker, out of retries", tag.Error(err))
			return
		}
		w.logger.Warn("Failed to start sdk worker", tag.Error(err), tag.Duration("sleep", sleep))
	}

	w.retryTimer = time.AfterFunc(sleep, func() {
		w.lock.Lock()
		w.retryTimer = nil
		args := w.refreshArgs // copy before releasing lock
		w.lock.Unlock()
		w.refresh(args)
	})
}

// Returning an error from here means that we should retry creating/starting the worker.
// Returning noWorkerNeeded means any existing worker should be stopped.
func (w *perNamespaceWorker) refresh(args refreshArgs) (retErr error) {
	defer func() {
		w.handleError(retErr)
	}()

	// note w.lock is not locked until we're about to start/stop a worker

	if !w.wm.Running() ||
		args.ns.State() == enumspb.NAMESPACE_STATE_DELETED ||
		!args.ns.ActiveInCluster(w.wm.thisClusterName) {
		return errNoWorkerNeeded
	}

	// figure out which components are enabled at all for this namespace
	var enabledComponents []workercommon.PerNSWorkerComponent
	var componentSet strings.Builder
	for _, cmp := range w.wm.components {
		options := cmp.DedicatedWorkerOptions(args.ns)
		if options.Enabled {
			enabledComponents = append(enabledComponents, cmp)
			fmt.Fprintf(&componentSet, "%p,", cmp)
		}
	}

	if len(enabledComponents) == 0 {
		// no components enabled, we don't need a worker
		return errNoWorkerNeeded
	}

	// check if we are responsible for this namespace at all
	workerAllocation, err := w.getWorkerAllocation(args)
	if err != nil {
		w.logger.Error("Failed to look up hosts", tag.Error(err))
		// TODO: add metric also
		return err
	}
	if workerAllocation.local == 0 {
		// not ours, don't need a worker
		return errNoWorkerNeeded
	}
	// ensure this changes if multiplicity changes
	fmt.Fprintf(&componentSet, "%d,", workerAllocation.local)

	// get sdk worker options
	fmt.Fprintf(&componentSet, "%+v,", w.opts)

	// we do need a worker, but maybe we have one already
	w.lock.Lock()
	defer w.lock.Unlock()

	if args.ns != w.ns {
		// stale refresh goroutine, do nothing
		return nil
	}

	if componentSet.String() == w.componentSet {
		// no change in set of components enabled, leave existing running
		return nil
	}

	// ask rate limiter if we can start now
	if !w.reserved {
		w.reserved = true
		if delay := w.wm.startLimiter.Reserve().Delay(); delay > 0 {
			return errRetryAfter(delay)
		}
	}

	// set of components changed, need to recreate worker. first stop old one
	w.stopWorkerLocked()

	// create new one. note that even before startWorker returns, the worker may have started
	// and already called the fatal error handler. we need to set w.client+worker+componentSet
	// before releasing the lock to keep our state consistent.
	client, worker, err := w.startWorker(enabledComponents, workerAllocation)
	if err != nil {
		// TODO: add metric also
		w.stopWorkerLocked() // for calling cleanup
		return err
	}

	w.client = client
	w.worker = worker
	w.componentSet = componentSet.String()
	return nil
}

func (w *perNamespaceWorker) startWorker(
	components []workercommon.PerNSWorkerComponent,
	allocation workerAllocation,
) (sdkclient.Client, sdkworker.Worker, error) {
	nsName := w.ns.Name().String()
	// this should not block because it uses an existing grpc connection
	client := w.wm.sdkClientFactory.NewClient(sdkclient.Options{
		Namespace:     nsName,
		DataConverter: sdk.PreferProtoDataConverter,
	})

	var sdkoptions sdkworker.Options

	// copy from dynamic config. apply explicit defaults for some instead of using the sdk
	// defaults so that we can multiply below.
	sdkoptions.MaxConcurrentActivityExecutionSize = cmp.Or(w.opts.MaxConcurrentActivityExecutionSize, 1000)
	sdkoptions.WorkerActivitiesPerSecond = w.opts.WorkerActivitiesPerSecond
	sdkoptions.MaxConcurrentLocalActivityExecutionSize = cmp.Or(w.opts.MaxConcurrentLocalActivityExecutionSize, 1000)
	sdkoptions.WorkerLocalActivitiesPerSecond = w.opts.WorkerLocalActivitiesPerSecond
	sdkoptions.MaxConcurrentActivityTaskPollers = max(cmp.Or(w.opts.MaxConcurrentActivityTaskPollers, 2), 2)
	sdkoptions.MaxConcurrentWorkflowTaskExecutionSize = cmp.Or(w.opts.MaxConcurrentWorkflowTaskExecutionSize, 1000)
	sdkoptions.MaxConcurrentWorkflowTaskPollers = max(cmp.Or(w.opts.MaxConcurrentWorkflowTaskPollers, 2), 2)
	sdkoptions.StickyScheduleToStartTimeout = w.opts.StickyScheduleToStartTimeout

	sdkoptions.BackgroundActivityContext = headers.SetCallerInfo(context.Background(), headers.NewBackgroundHighCallerInfo(nsName))
	sdkoptions.Identity = fmt.Sprintf("temporal-system@%s@%s", w.wm.hostName, nsName)
	// increase these if we're supposed to run with more allocation
	sdkoptions.MaxConcurrentWorkflowTaskPollers *= allocation.local
	sdkoptions.MaxConcurrentActivityTaskPollers *= allocation.local
	sdkoptions.MaxConcurrentLocalActivityExecutionSize *= allocation.local
	sdkoptions.MaxConcurrentWorkflowTaskExecutionSize *= allocation.local
	sdkoptions.MaxConcurrentActivityExecutionSize *= allocation.local
	sdkoptions.OnFatalError = w.onFatalError

	// this should not block because the client already has server capabilities
	worker := w.wm.sdkClientFactory.NewWorker(client, w.wm.taskQueueName, sdkoptions)
	details := workercommon.RegistrationDetails{
		TotalWorkers: allocation.total,
		Multiplicity: allocation.local,
	}
	for _, cmp := range components {
		cleanup := cmp.Register(worker, w.ns, details)
		if cleanup != nil {
			w.cleanup = append(w.cleanup, cleanup)
		}
	}

	// this blocks by calling DescribeNamespace a few times (with a 10s timeout)
	err := worker.Start()
	if err != nil {
		client.Close()
		return nil, nil, err
	}

	return client, worker, nil
}

func (w *perNamespaceWorker) onFatalError(err error) {
	// clean up worker and client
	w.stopWorker()

	switch err.(type) {
	case *serviceerror.NamespaceNotFound:
		// if this is a NamespaceNotFound, we should retry
		w.handleError(err)
	default:
		// other sdk fatal errors:
		// serviceerror.InvalidArgument
		// serviceerror.ClientVersionNotSupported
		w.logger.Error("sdk worker got non-retryable error, not restarting", tag.Error(err))
	}
}

func (w *perNamespaceWorker) stopWorker() {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.stopWorkerLocked()
}

func (w *perNamespaceWorker) stopWorkerAndResetTimer() {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.stopWorkerLocked()
	w.retrier.Reset()
	// Note that we only reset reserved here, not in stopWorkerLocked: if we did it there, we
	// would take a rate limiter token on each retry after failure. Failure to start the worker
	// means we probably didn't do any polls yet, which is the main reason for the rate limit,
	// so this it's okay to use only the backoff timer in that case.
	w.reserved = false
	if w.retryTimer != nil {
		w.retryTimer.Stop()
		w.retryTimer = nil
	}
}

func (w *perNamespaceWorker) stopWorkerLocked() {
	for _, cleanup := range w.cleanup {
		cleanup()
	}
	w.cleanup = nil
	if w.worker != nil {
		w.worker.Stop()
		w.worker = nil
	}
	if w.client != nil {
		w.client.Close()
		w.client = nil
	}
	w.componentSet = ""
}

func (e errRetryAfter) Error() string {
	return fmt.Sprintf("<delay %v>", time.Duration(e))
}
