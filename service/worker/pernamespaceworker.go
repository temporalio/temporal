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
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.uber.org/fx"
	"golang.org/x/exp/maps"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	workercommon "go.temporal.io/server/service/worker/common"
)

const (
	perNamespaceWorkerManagerListenerKey = "perNamespaceWorkerManager"
)

type (
	perNamespaceWorkerManagerInitParams struct {
		fx.In
		Logger            log.Logger
		SdkClientFactory  sdk.ClientFactory
		SdkWorkerFactory  sdk.WorkerFactory
		NamespaceRegistry namespace.Registry
		HostName          resource.HostName
		Config            *Config
		Components        []workercommon.PerNSWorkerComponent `group:"perNamespaceWorkerComponent"`
	}

	perNamespaceWorkerManager struct {
		status int32

		// from init params or Start
		logger            log.Logger
		sdkClientFactory  sdk.ClientFactory
		sdkWorkerFactory  sdk.WorkerFactory
		namespaceRegistry namespace.Registry
		self              *membership.HostInfo
		hostName          resource.HostName
		config            *Config
		serviceResolver   membership.ServiceResolver
		components        []workercommon.PerNSWorkerComponent
		initialRetry      time.Duration

		membershipChangedCh chan *membership.ChangedEvent

		lock    sync.Mutex
		workers map[namespace.ID]*perNamespaceWorker
	}

	perNamespaceWorker struct {
		wm *perNamespaceWorkerManager

		lock         sync.Mutex // protects below fields
		ns           *namespace.Namespace
		componentSet string
		client       sdkclient.Client
		worker       sdkworker.Worker
	}
)

func NewPerNamespaceWorkerManager(params perNamespaceWorkerManagerInitParams) *perNamespaceWorkerManager {
	return &perNamespaceWorkerManager{
		logger:              log.With(params.Logger, tag.ComponentPerNSWorkerManager),
		sdkClientFactory:    params.SdkClientFactory,
		sdkWorkerFactory:    params.SdkWorkerFactory,
		namespaceRegistry:   params.NamespaceRegistry,
		hostName:            params.HostName,
		config:              params.Config,
		components:          params.Components,
		initialRetry:        1 * time.Second,
		membershipChangedCh: make(chan *membership.ChangedEvent),
		workers:             make(map[namespace.ID]*perNamespaceWorker),
	}
}

func (wm *perNamespaceWorkerManager) Running() bool {
	return atomic.LoadInt32(&wm.status) == common.DaemonStatusStarted
}

func (wm *perNamespaceWorkerManager) Start(
	self *membership.HostInfo,
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

	wm.serviceResolver.AddListener(perNamespaceWorkerManagerListenerKey, wm.membershipChangedCh)
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
	wm.serviceResolver.RemoveListener(perNamespaceWorkerManagerListenerKey)
	close(wm.membershipChangedCh)

	wm.lock.Lock()
	workers := maps.Values(wm.workers)
	maps.Clear(wm.workers)
	wm.lock.Unlock()

	for _, worker := range workers {
		worker.stopWorker()
	}

	wm.logger.Info("", tag.LifeCycleStopped)
}

func (wm *perNamespaceWorkerManager) namespaceCallback(ns *namespace.Namespace, deleted bool) {
	go wm.getWorkerByNamespace(ns).refreshWithNewNamespace(ns, deleted)
}

func (wm *perNamespaceWorkerManager) membershipChangedListener() {
	for range wm.membershipChangedCh {
		wm.lock.Lock()
		for _, worker := range wm.workers {
			go worker.refreshWithExistingNamespace()
		}
		wm.lock.Unlock()
	}
}

func (wm *perNamespaceWorkerManager) getWorkerByNamespace(ns *namespace.Namespace) *perNamespaceWorker {
	wm.lock.Lock()
	defer wm.lock.Unlock()

	if worker, ok := wm.workers[ns.ID()]; ok {
		return worker
	}

	worker := &perNamespaceWorker{
		wm: wm,
		ns: ns,
	}

	wm.workers[ns.ID()] = worker
	return worker
}

func (wm *perNamespaceWorkerManager) removeWorker(ns *namespace.Namespace) {
	wm.lock.Lock()
	defer wm.lock.Unlock()
	delete(wm.workers, ns.ID())
}

func (wm *perNamespaceWorkerManager) getWorkerMultiplicity(ns *namespace.Namespace) (int, error) {
	workerCount := wm.config.PerNamespaceWorkerCount(ns.Name().String())
	// This can result in fewer than the intended number of workers if numWorkers > 1, because
	// multiple lookups might land on the same node. To compensate, we increase the number of
	// pollers in that case, but it would be better to try to spread them across our nodes.
	// TODO: implement this properly using LookupN in serviceResolver
	multiplicity := 0
	for i := 0; i < workerCount; i++ {
		key := fmt.Sprintf("%s/%d", ns.ID().String(), i)
		target, err := wm.serviceResolver.Lookup(key)
		if err != nil {
			return 0, err
		}
		if target.Identity() == wm.self.Identity() {
			multiplicity++
		}
	}
	return multiplicity, nil
}

func (w *perNamespaceWorker) refreshWithNewNamespace(ns *namespace.Namespace, deleted bool) {
	if deleted {
		w.stopWorker()
		// if namespace is fully deleted from db, we can remove from our map also
		w.wm.removeWorker(ns)
		return
	}
	w.lock.Lock()
	w.ns = ns
	w.lock.Unlock()
	w.refresh(ns)
}

func (w *perNamespaceWorker) refreshWithExistingNamespace() {
	w.lock.Lock()
	ns := w.ns
	w.lock.Unlock()
	w.refresh(ns)
}

// This is called after change to this namespace state _or_ any membership change in the server
// worker ring. It runs in its own goroutine (except for server shutdown), and multiple
// goroutines for the same namespace may be running at once. That's okay because they should
// eventually converge on the same state (running or not running, set of components) and exit.
func (w *perNamespaceWorker) refresh(ns *namespace.Namespace) {
	op := func() error {
		if !w.wm.Running() || ns.State() == enumspb.NAMESPACE_STATE_DELETED {
			w.stopWorker()
			return nil
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
			w.stopWorker()
			return nil
		}

		// check if we are responsible for this namespace at all
		multiplicity, err := w.wm.getWorkerMultiplicity(ns)
		if err != nil {
			w.wm.logger.Error("Failed to look up hosts", tag.WorkflowNamespace(ns.Name().String()), tag.Error(err))
			// TODO: add metric also
			return err
		}
		if multiplicity == 0 {
			// not ours, don't need a worker
			w.stopWorker()
			return nil
		}
		// ensure this changes if multiplicity changes
		componentSet += fmt.Sprintf("%d", multiplicity)

		// we do need a worker, but maybe we have one already
		w.lock.Lock()
		if componentSet == w.componentSet {
			// no change in set of components enabled
			w.lock.Unlock()
			return nil
		}
		// set of components changed, need to recreate worker. first stop old one
		w.stopWorkerLocked()
		w.lock.Unlock()

		// create worker outside of lock
		client, worker, err := w.startWorker(ns, enabledComponents, multiplicity)
		if err != nil {
			w.wm.logger.Error("Failed to start sdk worker", tag.WorkflowNamespace(ns.Name().String()), tag.Error(err))
			// TODO: add metric also
			return err
		}

		w.lock.Lock()
		defer w.lock.Unlock()
		// maybe there was a race and someone else created a client already. stop ours
		if !w.wm.Running() || w.client != nil || w.worker != nil {
			worker.Stop()
			client.Close()
			return nil
		}
		w.client = client
		w.worker = worker
		w.componentSet = componentSet
		return nil
	}
	policy := backoff.NewExponentialRetryPolicy(w.wm.initialRetry).
		WithMaximumInterval(1 * time.Minute).
		WithExpirationInterval(backoff.NoInterval)
	backoff.ThrottleRetry(op, policy, nil)
}

func (w *perNamespaceWorker) startWorker(
	ns *namespace.Namespace,
	components []workercommon.PerNSWorkerComponent,
	multiplicity int,
) (sdkclient.Client, sdkworker.Worker, error) {
	nsName := ns.Name().String()
	// TODO: after sdk supports cloning clients to share connections, use that here
	client, err := w.wm.sdkClientFactory.NewClient(nsName, w.wm.logger)
	if err != nil {
		return nil, nil, err
	}

	var sdkoptions sdkworker.Options
	sdkoptions.BackgroundActivityContext = headers.SetCallerInfo(context.Background(), headers.NewBackgroundCallerInfo(ns.Name().String()))
	sdkoptions.Identity = fmt.Sprintf("server-worker@%d@%s@%s", os.Getpid(), w.wm.hostName, nsName)
	// sdk default is 2, we increase it if we're supposed to run with more multiplicity.
	// other defaults are already large enough.
	sdkoptions.MaxConcurrentWorkflowTaskPollers = 2 * multiplicity
	sdkoptions.MaxConcurrentActivityTaskPollers = 2 * multiplicity
	sdkoptions.OnFatalError = func(error) {
		// if the sdk sees a fatal error (e.g. namespace does not exist), it will log it and
		// Stop() the worker. that means we should not call Stop() ourself.
		w.lock.Lock()
		defer w.lock.Unlock()
		w.worker = nil
	}

	sdkworker := w.wm.sdkWorkerFactory.New(client, primitives.PerNSWorkerTaskQueue, sdkoptions)
	for _, cmp := range components {
		cmp.Register(sdkworker, ns)
	}
	// TODO: use Run() and handle post-startup errors by recreating worker
	err = sdkworker.Start()
	if err != nil {
		client.Close()
		return nil, nil, err
	}

	return client, sdkworker, nil
}

func (w *perNamespaceWorker) stopWorker() {
	w.lock.Lock()
	defer w.lock.Unlock()
	w.stopWorkerLocked()
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
