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
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	enumspb "go.temporal.io/api/enums/v1"
	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.uber.org/fx"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/namespace"
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
		serviceResolver   membership.ServiceResolver
		components        []workercommon.PerNSWorkerComponent
		initialRetry      time.Duration

		membershipChangedCh chan *membership.ChangedEvent

		lock       sync.Mutex
		workerSets map[namespace.ID]*workerSet
	}

	workerSet struct {
		wm *perNamespaceWorkerManager

		lock    sync.Mutex // protects below fields
		ns      *namespace.Namespace
		deleted bool
		workers map[workercommon.PerNSWorkerComponent]*worker
	}

	worker struct {
		client sdkclient.Client
		worker sdkworker.Worker
	}
)

func NewPerNamespaceWorkerManager(params perNamespaceWorkerManagerInitParams) *perNamespaceWorkerManager {
	return &perNamespaceWorkerManager{
		logger:              params.Logger,
		sdkClientFactory:    params.SdkClientFactory,
		sdkWorkerFactory:    params.SdkWorkerFactory,
		namespaceRegistry:   params.NamespaceRegistry,
		hostName:            params.HostName,
		components:          params.Components,
		initialRetry:        1 * time.Second,
		membershipChangedCh: make(chan *membership.ChangedEvent),
		workerSets:          make(map[namespace.ID]*workerSet),
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

	wm.logger.Info("", tag.ComponentPerNSWorkerManager, tag.LifeCycleStarting)

	// this will call namespaceCallback with current namespaces
	wm.namespaceRegistry.RegisterStateChangeCallback(wm, wm.namespaceCallback)

	wm.serviceResolver.AddListener(perNamespaceWorkerManagerListenerKey, wm.membershipChangedCh)
	go wm.membershipChangedListener()

	wm.logger.Info("", tag.ComponentPerNSWorkerManager, tag.LifeCycleStarted)
}

func (wm *perNamespaceWorkerManager) Stop() {
	if !atomic.CompareAndSwapInt32(
		&wm.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	wm.logger.Info("", tag.ComponentPerNSWorkerManager, tag.LifeCycleStopping)

	wm.namespaceRegistry.UnregisterStateChangeCallback(wm)
	wm.serviceResolver.RemoveListener(perNamespaceWorkerManagerListenerKey)
	close(wm.membershipChangedCh)

	wm.lock.Lock()
	defer wm.lock.Unlock()

	for _, ws := range wm.workerSets {
		// this will see that the perNamespaceWorkerManager is not running
		// anymore and stop all workers
		ws.refresh(nil, false)
	}

	wm.logger.Info("", tag.ComponentPerNSWorkerManager, tag.LifeCycleStopped)
}

func (wm *perNamespaceWorkerManager) namespaceCallback(ns *namespace.Namespace, deleted bool) {
	go wm.getWorkerSet(ns).refresh(ns, deleted)
}

func (wm *perNamespaceWorkerManager) membershipChangedListener() {
	for range wm.membershipChangedCh {
		wm.lock.Lock()
		for _, ws := range wm.workerSets {
			go ws.refresh(nil, false)
		}
		wm.lock.Unlock()
	}
}

func (wm *perNamespaceWorkerManager) getWorkerSet(ns *namespace.Namespace) *workerSet {
	wm.lock.Lock()
	defer wm.lock.Unlock()

	if ws, ok := wm.workerSets[ns.ID()]; ok {
		return ws
	}

	ws := &workerSet{
		wm:      wm,
		ns:      ns,
		workers: make(map[workercommon.PerNSWorkerComponent]*worker),
	}

	wm.workerSets[ns.ID()] = ws
	return ws
}

func (wm *perNamespaceWorkerManager) removeWorkerSet(ns *namespace.Namespace) {
	wm.lock.Lock()
	defer wm.lock.Unlock()
	delete(wm.workerSets, ns.ID())
}

func (wm *perNamespaceWorkerManager) responsibleForNamespace(ns *namespace.Namespace, queueName string, num int) (int, error) {
	// This can result in fewer than the intended number of workers if num > 1, because
	// multiple lookups might land on the same node. To compensate, we increase the number of
	// pollers in that case, but it would be better to try to spread them across our nodes.
	// TODO: implement this properly using LookupN in serviceResolver
	multiplicity := 0
	for i := 0; i < num; i++ {
		key := fmt.Sprintf("%s/%s/%d", ns.ID().String(), queueName, i)
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

// called after change to this namespace state _or_ any membership change in the
// server worker ring
func (ws *workerSet) refresh(newNs *namespace.Namespace, newDeleted bool) {
	ws.lock.Lock()
	if newNs != nil {
		ws.ns = newNs
		ws.deleted = newDeleted
	}
	ns, deleted := ws.ns, ws.deleted
	ws.lock.Unlock()

	for _, wc := range ws.wm.components {
		ws.refreshComponent(wc, ns, deleted)
	}

	if deleted {
		// if fully deleted from db, we can remove from our map also
		ws.wm.removeWorkerSet(ns)
	}
}

func (ws *workerSet) refreshComponent(
	cmp workercommon.PerNSWorkerComponent,
	ns *namespace.Namespace,
	deleted bool,
) {
	op := func() error {
		// we should run only if all four are true:
		// 1. perNamespaceWorkerManager is running
		// 2. this namespace is not deleted
		// 3. the component says we should be (can filter by namespace)
		// 4. we are responsible for this namespace
		multiplicity := 0
		var options *workercommon.PerNSDedicatedWorkerOptions
		if ws.wm.Running() && ns.State() != enumspb.NAMESPACE_STATE_DELETED && !deleted {
			options = cmp.DedicatedWorkerOptions(ns)
			if options.Enabled {
				var err error
				multiplicity, err = ws.wm.responsibleForNamespace(ns, options.TaskQueue, options.NumWorkers)
				if err != nil {
					return err
				}
			}
		}

		if multiplicity > 0 {
			ws.lock.Lock()
			if _, ok := ws.workers[cmp]; ok {
				// worker is already running. it's possible that it started with a different
				// multiplicity. we don't bother changing it in that case.
				ws.lock.Unlock()
				return nil
			}
			ws.lock.Unlock()

			worker, err := ws.startWorker(cmp, ns, options, multiplicity)
			if err != nil {
				return err
			}

			ws.lock.Lock()
			defer ws.lock.Unlock()

			// check again in case we had a race
			if _, ok := ws.workers[cmp]; ok || !ws.wm.Running() {
				worker.stop()
				return nil
			}

			ws.workers[cmp] = worker

			return nil
		} else {
			ws.lock.Lock()
			defer ws.lock.Unlock()

			if worker, ok := ws.workers[cmp]; ok {
				worker.stop()
				delete(ws.workers, cmp)
			}

			return nil
		}
	}
	policy := backoff.NewExponentialRetryPolicy(ws.wm.initialRetry)
	backoff.ThrottleRetry(op, policy, nil)
}

func (ws *workerSet) startWorker(
	wc workercommon.PerNSWorkerComponent,
	ns *namespace.Namespace,
	options *workercommon.PerNSDedicatedWorkerOptions,
	multiplicity int,
) (*worker, error) {
	nsName := ns.Name().String()
	client, err := ws.wm.sdkClientFactory.NewClient(nsName, ws.wm.logger)
	if err != nil {
		return nil, err
	}

	sdkoptions := options.Options
	sdkoptions.Identity = fmt.Sprintf("%d@%s@%s@%s@%T", os.Getpid(), ws.wm.hostName, nsName, options.TaskQueue, wc)
	// sdk default is 2, we increase it if we're supposed to run with more multiplicity.
	// other defaults are already large enough.
	sdkoptions.MaxConcurrentWorkflowTaskPollers = 2 * multiplicity
	sdkoptions.MaxConcurrentActivityTaskPollers = 2 * multiplicity

	sdkworker := ws.wm.sdkWorkerFactory.New(client, options.TaskQueue, sdkoptions)
	wc.Register(sdkworker, ns)
	// TODO: use Run() and handle post-startup errors by recreating worker
	// (after sdk supports returning post-startup errors from Run)
	err = sdkworker.Start()
	if err != nil {
		client.Close()
		return nil, err
	}

	return &worker{client: client, worker: sdkworker}, nil
}

func (w *worker) stop() {
	w.worker.Stop()
	w.client.Close()
}
