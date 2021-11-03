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
	"sync/atomic"

	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.uber.org/fx"
)

const DefaultWorkerTaskQueue = "default-worker-tq"

type (
	// WorkerComponent represent a type of work needed for worker role
	WorkerComponent interface {
		// Register registers Workflow and Activity types provided by this worker component
		Register(sdkworker.Worker)
		// DedicatedWorkerOptions returns a DedicatedWorkerOptions for this worker component. Return nil to use
		// default worker instance.
		DedicatedWorkerOptions() *DedicatedWorkerOptions
	}

	DedicatedWorkerOptions struct {
		TaskQueue string
		Options   sdkworker.Options
	}

	// workerManager maintains list of SDK workers.
	workerManager struct {
		status           int32
		logger           log.Logger
		sdkClient        sdkclient.Client
		workers          []sdkworker.Worker
		workerComponents []WorkerComponent
	}

	initParams struct {
		fx.In
		Logger           log.Logger
		SdkClient        sdkclient.Client
		WorkerComponents []WorkerComponent `group:"workerComponent"`
	}
)

func NewWorkerManager(params initParams) *workerManager {
	return &workerManager{
		logger:           params.Logger,
		sdkClient:        params.SdkClient,
		workerComponents: params.WorkerComponents,
	}
}

func (wm *workerManager) Start() {
	if !atomic.CompareAndSwapInt32(
		&wm.status,
		common.DaemonStatusInitialized,
		common.DaemonStatusStarted,
	) {
		return
	}

	defaultWorkerOptions := sdkworker.Options{
		// TODO: add dynamic config for this
	}
	defaultWorker := sdkworker.New(wm.sdkClient, DefaultWorkerTaskQueue, defaultWorkerOptions)
	wm.workers = []sdkworker.Worker{defaultWorker}

	for _, wc := range wm.workerComponents {
		workerOptions := wc.DedicatedWorkerOptions()
		if workerOptions == nil {
			// use default worker
			wc.Register(defaultWorker)
		} else {
			// this worker component requires a dedicated worker
			dedicatedWorker := sdkworker.New(wm.sdkClient, workerOptions.TaskQueue, workerOptions.Options)
			wc.Register(dedicatedWorker)
			wm.workers = append(wm.workers, dedicatedWorker)
		}
	}

	wm.logger.Info("", tag.ComponentWorkerManager, tag.LifeCycleStarted)
}

func (wm *workerManager) Stop() {
	if !atomic.CompareAndSwapInt32(
		&wm.status,
		common.DaemonStatusStarted,
		common.DaemonStatusStopped,
	) {
		return
	}

	for _, w := range wm.workers {
		w.Stop()
	}
	wm.logger.Info("", tag.ComponentWorkerManager, tag.LifeCycleStopped)
}
