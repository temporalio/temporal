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

	sdkworker "go.temporal.io/sdk/worker"
	"go.uber.org/fx"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/sdk"
	workercommon "go.temporal.io/server/service/worker/common"
)

const DefaultWorkerTaskQueue = "default-worker-tq"

type (
	// workerManager maintains list of SDK workers.
	workerManager struct {
		status           int32
		logger           log.Logger
		sdkClientFactory sdk.ClientFactory
		workers          []sdkworker.Worker
		workerComponents []workercommon.WorkerComponent
	}

	initParams struct {
		fx.In
		Logger           log.Logger
		SdkClientFactory sdk.ClientFactory
		WorkerComponents []workercommon.WorkerComponent `group:"workerComponent"`
	}
)

func NewWorkerManager(params initParams) *workerManager {
	return &workerManager{
		logger:           params.Logger,
		sdkClientFactory: params.SdkClientFactory,
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
		// TODO: add dynamic config for worker options
	}
	sdkClient := wm.sdkClientFactory.GetSystemClient(wm.logger)
	defaultWorker := sdkworker.New(sdkClient, DefaultWorkerTaskQueue, defaultWorkerOptions)
	wm.workers = []sdkworker.Worker{defaultWorker}

	for _, wc := range wm.workerComponents {
		workerOptions := wc.DedicatedWorkerOptions()
		if workerOptions == nil {
			// use default worker
			wc.Register(defaultWorker)
		} else {
			// this worker component requires a dedicated worker
			dedicatedWorker := sdkworker.New(sdkClient, workerOptions.TaskQueue, workerOptions.Options)
			wc.Register(dedicatedWorker)
			wm.workers = append(wm.workers, dedicatedWorker)
		}
	}

	for _, w := range wm.workers {
		w.Start()
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
