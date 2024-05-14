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

package worker

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/server/common/clock"
	"go.uber.org/fx"
	"golang.org/x/exp/maps"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/util"
	workercommon "go.temporal.io/server/service/worker/common"
)

const (
	perNamespaceWorkerManagerListenerKey = "perNamespaceWorkerManager"

	// Always refresh workers after this time even if there were no membership or namespace
	// state changes. This is to pick up dynamic config changes (which we can't subscribe to).
	refreshInterval = 10 * time.Minute
)

type (
	perNamespaceWorkerManagerInitParams struct {
		fx.In
		Logger            log.Logger
		SdkClientFactory  sdk.ClientFactory
		NamespaceRegistry namespace.Registry
		HostName          resource.HostName
		Config            *Config
		ClusterMetadata   cluster.Metadata
		Components        []workercommon.PerNSWorkerComponent `group:"perNamespaceWorkerComponent"`
	}

	perNamespaceWorkerManager struct {
		status int32

		// from init params or Start
		logger            log.Logger
		sdkClientFactory  sdk.ClientFactory
		namespaceRegistry namespace.Registry
		self              membership.HostInfo
		hostName          resource.HostName
		config            *Config
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
		wm     *perNamespaceWorkerManager
		logger log.Logger

		lock         sync.Mutex // protects below fields
		ns           *namespace.Namespace
		retrier      backoff.Retrier
		retryTimer   *time.Timer
		reserved     bool // whether startLimiter.Reserve was already called
		componentSet string
		client       sdkclient.Client
		worker       sdkworker.Worker
	}

	sdkWorkerOptions struct {
		// Copy of relevant fields from sdkworker.Options
		MaxConcurrentActivityExecutionSize      int
		WorkerActivitiesPerSecond               float64
		MaxConcurrentLocalActivityExecutionSize int
		WorkerLocalActivitiesPerSecond          float64
		MaxConcurrentActivityTaskPollers        int
		MaxConcurrentWorkflowTaskExecutionSize  int
		MaxConcurrentWorkflowTaskPollers        int
		StickyScheduleToStartTimeout            string // parse into time.Duration
		StickyScheduleToStartTimeoutDuration    time.Duration
	}

	workerAllocation struct {
		Total int
		Local int
	}

	errRetryAfter time.Duration
)

var (
	errNoWorkerNeeded = errors.New("no worker needed") // sentinel value, not a real error
	// errInvalidConfiguration indicates that the value provided by dynamic config is not legal
	errInvalidConfiguration = errors.New("invalid dynamic configuration")
)

func NewPerNamespaceWorkerManager(params perNamespaceWorkerManagerInitParams) *perNamespaceWorkerManager {
	return &perNamespaceWorkerManager{
		logger:              log.With(params.Logger, tag.ComponentPerNSWorkerManager),
		sdkClientFactory:    params.SdkClientFactory,
		namespaceRegistry:   params.NamespaceRegistry,
		hostName:            params.HostName,
		config:              params.Config,
		components:          params.Components,
		initialRetry:        1 * time.Second,
		thisClusterName:     params.ClusterMetadata.GetCurrentClusterName(),
		startLimiter:        quotas.NewDefaultOutgoingRateLimiter(quotas.RateFn(params.Config.PerNamespaceWorkerStartRate)),
		membershipChangedCh: make(chan *membership.ChangedEvent),
		workers:             make(map[namespace.ID]*perNamespaceWorker),
	}
}

func (wm *perNamespaceWorkerManager) Running() bool {
	return atomic.LoadInt32(&wm.status) == common.DaemonStatusStarted
}

func (wm *perNamespaceWorkerManager) Start(
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

	err := wm.serviceResolver.AddListener(perNamespaceWorkerManagerListenerKey, wm.membershipChangedCh)
	if err != nil {
		wm.logger.Fatal("Unable to register membership listener", tag.Error(err))
	}
	go wm.membershipChangedListener()

	wm.logger.Info("", tag.LifeCycleStarted)
}

func (wm *perNamespaceWorkerManager) Stop() {
	if !atomic.CompareAndSwapInt32(
		&wm.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	wm.logger.Info("", tag.LifeCycleStopping)

	wm.namespaceRegistry.UnregisterStateChangeCallback(wm)
	err := wm.serviceResolver.RemoveListener(perNamespaceWorkerManagerListenerKey)
	if err != nil {
		wm.logger.Error("Unable to unregister membership listener", tag.Error(err))
	}
	close(wm.membershipChangedCh)

	wm.lock.Lock()
	workers := maps.Values(wm.workers)
	maps.Clear(wm.workers)
	wm.lock.Unlock()

	for _, worker := range workers {
		worker.stopWorkerAndResetTimer()
	}

	wm.logger.Info("", tag.LifeCycleStopped)
}

func (wm *perNamespaceWorkerManager) namespaceCallback(ns *namespace.Namespace, deleted bool) {
	go wm.getWorkerByNamespace(ns).refreshWithNewNamespace(ns, deleted)
}

func (wm *perNamespaceWorkerManager) refreshAll() {
	wm.lock.Lock()
	defer wm.lock.Unlock()
	for _, worker := range wm.workers {
		go worker.refreshWithExistingNamespace()
	}
}

func (wm *perNamespaceWorkerManager) membershipChangedListener() {
loop:
	for {
		timer := time.NewTimer(refreshInterval)
		select {
		case _, ok := <-wm.membershipChangedCh:
			timer.Stop()
			if !ok {
				break loop
			}
		case <-timer.C:
		}
		wm.refreshAll()
	}
}

func (wm *perNamespaceWorkerManager) getWorkerByNamespace(ns *namespace.Namespace) *perNamespaceWorker {
	wm.lock.Lock()
	defer wm.lock.Unlock()

	if worker, ok := wm.workers[ns.ID()]; ok {
		return worker
	}

	worker := &perNamespaceWorker{
		wm:      wm,
		logger:  log.With(wm.logger, tag.WorkflowNamespace(ns.Name().String())),
		retrier: backoff.NewRetrier(backoff.NewExponentialRetryPolicy(wm.initialRetry), clock.NewRealTimeSource()),
		ns:      ns,
	}

	wm.workers[ns.ID()] = worker
	return worker
}

func (wm *perNamespaceWorkerManager) removeWorker(ns *namespace.Namespace) {
	wm.lock.Lock()
	defer wm.lock.Unlock()
	delete(wm.workers, ns.ID())
}

func (wm *perNamespaceWorkerManager) getWorkerAllocation(ns *namespace.Namespace) (*workerAllocation, error) {
	desiredWorkersCount, err := wm.getConfiguredWorkersCountFor(ns)
	if err != nil {
		return nil, err
	}
	if desiredWorkersCount == 0 {
		return &workerAllocation{0, 0}, nil
	}
	localCount, err := wm.getLocallyDesiredWorkersCount(ns, desiredWorkersCount)
	if err != nil {
		return nil, err
	}
	return &workerAllocation{desiredWorkersCount, localCount}, nil
}

func (wm *perNamespaceWorkerManager) getConfiguredWorkersCountFor(ns *namespace.Namespace) (int, error) {
	totalWorkers := wm.config.PerNamespaceWorkerCount(ns.Name().String())
	if totalWorkers < 0 {
		err := fmt.Errorf("%w namespace %s, workers count %d", errInvalidConfiguration, ns.Name(), totalWorkers)
		return 0, err
	}
	return totalWorkers, nil
}

func (wm *perNamespaceWorkerManager) getLocallyDesiredWorkersCount(ns *namespace.Namespace, desiredNumberOfWorkers int) (int, error) {
	key := ns.ID().String()
	availableHosts := wm.serviceResolver.LookupN(key, desiredNumberOfWorkers)
	hostsCount := len(availableHosts)
	if hostsCount == 0 {
		return 0, membership.ErrInsufficientHosts
	}
	maxWorkersPerHost := desiredNumberOfWorkers/hostsCount + 1
	desiredDistribution := util.RepeatSlice(availableHosts, maxWorkersPerHost)[:desiredNumberOfWorkers]

	isLocal := func(info membership.HostInfo) bool { return info.Identity() == wm.self.Identity() }
	result := len(util.FilterSlice(desiredDistribution, isLocal))
	return result, nil
}

func (wm *perNamespaceWorkerManager) getWorkerOptions(ns *namespace.Namespace) sdkWorkerOptions {
	optionsMap := wm.config.PerNamespaceWorkerOptions(ns.Name().String())
	var options sdkWorkerOptions
	b, err := json.Marshal(optionsMap)
	if err != nil {
		return options
	}
	_ = json.Unmarshal(b, &options) // ignore errors, just use the zero value anyway
	if len(options.StickyScheduleToStartTimeout) > 0 {
		if options.StickyScheduleToStartTimeoutDuration, err = timestamp.ParseDuration(options.StickyScheduleToStartTimeout); err != nil {
			wm.logger.Warn("invalid StickyScheduleToStartTimeout", tag.Error(err))
		}
	}
	return options
}

// called on namespace state change callback
func (w *perNamespaceWorker) refreshWithNewNamespace(ns *namespace.Namespace, deleted bool) {
	w.lock.Lock()
	w.ns = ns
	// namespace name can change, but don't update w.logger, otherwise we'd have to hold w.lock
	// just to log.
	isRetrying := w.retryTimer != nil
	w.lock.Unlock()

	if deleted {
		w.stopWorkerAndResetTimer()
		// if namespace is fully deleted from db, we can remove from our map also
		w.wm.removeWorker(ns)
		return
	}

	if !isRetrying {
		w.refresh(ns)
	}
}

// called on all namespaces on membership change in worker ring
func (w *perNamespaceWorker) refreshWithExistingNamespace() {
	w.lock.Lock()
	ns := w.ns
	isRetrying := w.retryTimer != nil
	w.lock.Unlock()

	if !isRetrying {
		w.refresh(ns)
	}
}

func (w *perNamespaceWorker) refresh(ns *namespace.Namespace) {
	w.handleError(w.tryRefresh(ns))
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
		sleep = w.retrier.NextBackOff()
		if sleep < 0 {
			w.logger.Error("Failed to start sdk worker, out of retries", tag.Error(err))
			return
		}
		w.logger.Warn("Failed to start sdk worker", tag.Error(err), tag.NewDurationTag("sleep", sleep))
	}

	w.retryTimer = time.AfterFunc(sleep, func() {
		w.lock.Lock()
		w.retryTimer = nil
		ns := w.ns
		w.lock.Unlock()
		w.refresh(ns)
	})
}

// Only call from refresh so that errors are handled properly. Returning an error from here
// means that we should retry creating/starting the worker. Returning noWorkerNeeded means any
// existing worker should be stopped.
func (w *perNamespaceWorker) tryRefresh(ns *namespace.Namespace) error {
	if !w.wm.Running() ||
		ns.State() == enumspb.NAMESPACE_STATE_DELETED ||
		!ns.ActiveInCluster(w.wm.thisClusterName) {
		return errNoWorkerNeeded
	}

	// figure out which components are enabled at all for this namespace
	var enabledComponents []workercommon.PerNSWorkerComponent
	var componentSet string
	for _, cmp := range w.wm.components {
		options := cmp.DedicatedWorkerOptions(ns)
		if options.Enabled {
			enabledComponents = append(enabledComponents, cmp)
			componentSet += fmt.Sprintf("%p,", cmp)
		}
	}

	if len(enabledComponents) == 0 {
		// no components enabled, we don't need a worker
		return errNoWorkerNeeded
	}

	// check if we are responsible for this namespace at all
	workerAllocation, err := w.wm.getWorkerAllocation(ns)
	if err != nil {
		w.logger.Error("Failed to look up hosts", tag.Error(err))
		// TODO: add metric also
		return err
	}
	if workerAllocation.Local == 0 {
		// not ours, don't need a worker
		return errNoWorkerNeeded
	}
	// ensure this changes if multiplicity changes
	componentSet += fmt.Sprintf(",%d", workerAllocation.Local)

	// get sdk worker options
	dcOptions := w.wm.getWorkerOptions(ns)
	componentSet += fmt.Sprintf(",%+v", dcOptions)

	// we do need a worker, but maybe we have one already
	w.lock.Lock()
	defer w.lock.Unlock()

	if componentSet == w.componentSet {
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
	client, worker, err := w.startWorker(ns, enabledComponents, workerAllocation, dcOptions)
	if err != nil {
		// TODO: add metric also
		return err
	}

	w.client = client
	w.worker = worker
	w.componentSet = componentSet
	return nil
}

func (w *perNamespaceWorker) startWorker(
	ns *namespace.Namespace,
	components []workercommon.PerNSWorkerComponent,
	allocation *workerAllocation,
	dcOptions sdkWorkerOptions,
) (sdkclient.Client, sdkworker.Worker, error) {
	nsName := ns.Name().String()
	// this should not block because it uses an existing grpc connection
	client := w.wm.sdkClientFactory.NewClient(sdkclient.Options{
		Namespace:     nsName,
		DataConverter: sdk.PreferProtoDataConverter,
	})

	var sdkoptions sdkworker.Options

	// copy from dynamic config. apply explicit defaults for some instead of using the sdk
	// defaults so that we can multiply below.
	sdkoptions.MaxConcurrentActivityExecutionSize = cmp.Or(dcOptions.MaxConcurrentActivityExecutionSize, 1000)
	sdkoptions.WorkerActivitiesPerSecond = dcOptions.WorkerActivitiesPerSecond
	sdkoptions.MaxConcurrentLocalActivityExecutionSize = cmp.Or(dcOptions.MaxConcurrentLocalActivityExecutionSize, 1000)
	sdkoptions.WorkerLocalActivitiesPerSecond = dcOptions.WorkerLocalActivitiesPerSecond
	sdkoptions.MaxConcurrentActivityTaskPollers = max(cmp.Or(dcOptions.MaxConcurrentActivityTaskPollers, 2), 2)
	sdkoptions.MaxConcurrentWorkflowTaskExecutionSize = cmp.Or(dcOptions.MaxConcurrentWorkflowTaskExecutionSize, 1000)
	sdkoptions.MaxConcurrentWorkflowTaskPollers = max(cmp.Or(dcOptions.MaxConcurrentWorkflowTaskPollers, 2), 2)
	sdkoptions.StickyScheduleToStartTimeout = dcOptions.StickyScheduleToStartTimeoutDuration

	sdkoptions.BackgroundActivityContext = headers.SetCallerInfo(context.Background(), headers.NewBackgroundCallerInfo(ns.Name().String()))
	sdkoptions.Identity = fmt.Sprintf("server-worker@%d@%s@%s", os.Getpid(), w.wm.hostName, nsName)
	// increase these if we're supposed to run with more allocation
	sdkoptions.MaxConcurrentWorkflowTaskPollers *= allocation.Local
	sdkoptions.MaxConcurrentActivityTaskPollers *= allocation.Local
	sdkoptions.MaxConcurrentLocalActivityExecutionSize *= allocation.Local
	sdkoptions.MaxConcurrentWorkflowTaskExecutionSize *= allocation.Local
	sdkoptions.MaxConcurrentActivityExecutionSize *= allocation.Local
	sdkoptions.OnFatalError = w.onFatalError

	// this should not block because the client already has server capabilities
	worker := w.wm.sdkClientFactory.NewWorker(client, primitives.PerNSWorkerTaskQueue, sdkoptions)
	details := workercommon.RegistrationDetails{
		TotalWorkers: allocation.Total,
		Multiplicity: allocation.Local,
	}
	for _, cmp := range components {
		cmp.Register(worker, ns, details)
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
