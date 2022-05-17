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
		NamespaceRegistry namespace.Registry
		Components        []workercommon.PerNSWorkerComponent `group:"perNamespaceWorkerComponent"`
	}

	perNamespaceWorkerManager struct {
		status int32

		// from init params or Start
		logger            log.Logger
		sdkClientFactory  sdk.ClientFactory
		namespaceRegistry namespace.Registry
		self              *membership.HostInfo
		serviceResolver   membership.ServiceResolver
		components        []workercommon.PerNSWorkerComponent

		membershipChangedCh chan *membership.ChangedEvent

		lock       sync.Mutex
		workerSets map[namespace.ID]*workerSet
	}

	workerSet struct {
		ns *namespace.Namespace
		wm *perNamespaceWorkerManager

		lock    sync.Mutex
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
		namespaceRegistry:   params.NamespaceRegistry,
		components:          params.Components,
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
		ws.refresh()
	}

	wm.logger.Info("", tag.ComponentPerNSWorkerManager, tag.LifeCycleStopped)
}

func (wm *perNamespaceWorkerManager) namespaceCallback(ns *namespace.Namespace) {
	go wm.getWorkerSet(ns).refresh()
}

func (wm *perNamespaceWorkerManager) membershipChangedListener() {
	for range wm.membershipChangedCh {
		for _, ws := range wm.workerSets {
			go ws.refresh()
		}
	}
}

func (wm *perNamespaceWorkerManager) getWorkerSet(ns *namespace.Namespace) *workerSet {
	wm.lock.Lock()
	defer wm.lock.Unlock()

	if ws, ok := wm.workerSets[ns.ID()]; ok {
		return ws
	}

	ws := &workerSet{
		ns:      ns,
		wm:      wm,
		workers: make(map[workercommon.PerNSWorkerComponent]*worker),
	}

	wm.workerSets[ns.ID()] = ws
	return ws
}

func (wm *perNamespaceWorkerManager) responsibleForNamespace(ns *namespace.Namespace, queueName string, num int) (bool, error) {
	// TODO: implement num > 1 (with LookupN in serviceResolver)
	key := ns.ID().String() + "-" + queueName
	target, err := wm.serviceResolver.Lookup(key)
	if err != nil {
		return false, err
	}
	return target.Identity() == wm.self.Identity(), nil
}

// called after change to this namespace state _or_ any membership change in the
// server worker ring
func (ws *workerSet) refresh() {
	nsExists := ws.ns.State() != enumspb.NAMESPACE_STATE_DELETED

	for _, wc := range ws.wm.components {
		ws.refreshComponent(wc, nsExists)
	}
}

func (ws *workerSet) refreshComponent(
	cmp workercommon.PerNSWorkerComponent,
	nsExists bool,
) {
	op := func() error {
		// we should run only if all four are true:
		// 1. perNamespaceWorkerManager is running
		// 2. this namespace exists
		// 3. the component says we should be (can filter by namespace)
		// 4. we are responsible for this namespace
		shouldBeRunning := ws.wm.Running() && nsExists
		if shouldBeRunning {
			options := cmp.DedicatedWorkerOptions(ws.ns)
			if !options.Enabled {
				shouldBeRunning = false
			} else {
				var err error
				shouldBeRunning, err = ws.wm.responsibleForNamespace(ws.ns, options.TaskQueue, options.NumWorkers)
				if err != nil {
					return err
				}
			}
		}

		if shouldBeRunning {
			ws.lock.Lock()
			if _, ok := ws.workers[cmp]; ok {
				ws.lock.Unlock()
				return nil
			}
			ws.lock.Unlock()

			worker, err := ws.startWorker(cmp)
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
			ws.wm.logger.Info("", tag.ComponentPerNSWorkerManager, tag.LifeCycleStarted, tag.WorkflowNamespace(ws.ns.Name().String()), tag.WorkerComponent(cmp))

			return nil
		} else {
			ws.lock.Lock()
			defer ws.lock.Unlock()

			if worker, ok := ws.workers[cmp]; ok {
				worker.stop()
				delete(ws.workers, cmp)
				ws.wm.logger.Info("", tag.ComponentPerNSWorkerManager, tag.LifeCycleStopped, tag.WorkflowNamespace(ws.ns.Name().String()), tag.WorkerComponent(cmp))
			}

			return nil
		}
	}
	policy := backoff.NewExponentialRetryPolicy(1 * time.Second)
	backoff.Retry(op, policy, nil)
}

func (ws *workerSet) startWorker(wc workercommon.PerNSWorkerComponent) (*worker, error) {
	client, err := ws.wm.sdkClientFactory.NewClient(ws.ns.Name().String(), ws.wm.logger)
	if err != nil {
		return nil, err
	}

	options := wc.DedicatedWorkerOptions(ws.ns)
	sdkworker := sdkworker.New(client, options.TaskQueue, options.Options)
	wc.Register(sdkworker, ws.ns)
	// TODO: use Run() and handle post-startup errors by recreating worker
	// (after sdk supports returning post-startup errors from Run)
	err = sdkworker.Start()
	if err != nil {
		return nil, err
	}

	return &worker{client: client, worker: sdkworker}, nil
}

func (w *worker) stop() {
	w.worker.Stop()
	w.client.Close()
}
