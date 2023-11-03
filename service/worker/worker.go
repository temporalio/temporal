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
	"sync/atomic"

	sdkworker "go.temporal.io/sdk/worker"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/sdk"
	workercommon "go.temporal.io/server/service/worker/common"
)

type (
	// workerManager maintains list of SDK workers.
	workerManager struct {
		status           int32
		logger           log.Logger
		sdkClientFactory sdk.ClientFactory
		workers          []sdkworker.Worker
		workerComponents []workercommon.WorkerComponent
	}
)

// NewWorkerManager creates a new worker manager. The workerComponents argument must be first in order for the fx param
// tag to work correctly.
func NewWorkerManager(
	workerComponents []workercommon.WorkerComponent,
	logger log.Logger,
	sdkClientFactory sdk.ClientFactory,
) *workerManager {
	return &workerManager{
		logger:           logger,
		sdkClientFactory: sdkClientFactory,
		workerComponents: workerComponents,
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
		BackgroundActivityContext: headers.SetCallerType(context.Background(), headers.CallerTypeBackground),
	}
	sdkClient := wm.sdkClientFactory.GetSystemClient()
	defaultWorker := wm.sdkClientFactory.NewWorker(sdkClient, primitives.DefaultWorkerTaskQueue, defaultWorkerOptions)
	wm.workers = []sdkworker.Worker{defaultWorker}

	for _, wc := range wm.workerComponents {
		wfWorkerOptions := wc.DedicatedWorkflowWorkerOptions()
		if wfWorkerOptions == nil {
			// use default worker
			wc.RegisterWorkflow(defaultWorker)
		} else {
			// this worker component requires a dedicated worker
			dedicatedWorker := wm.sdkClientFactory.NewWorker(sdkClient, wfWorkerOptions.TaskQueue, wfWorkerOptions.Options)
			wc.RegisterWorkflow(dedicatedWorker)
			wm.workers = append(wm.workers, dedicatedWorker)
		}

		activityWorkerOptions := wc.DedicatedActivityWorkerOptions()
		if activityWorkerOptions == nil {
			// use default worker
			wc.RegisterActivities(defaultWorker)
		} else {
			// TODO: This is to prevent issues during upgrade/downgrade. Remove in 1.24 release.
			wc.RegisterActivities(defaultWorker)

			// this worker component requires a dedicated worker for activities
			activityWorkerOptions.Options.DisableWorkflowWorker = true
			activityWorker := wm.sdkClientFactory.NewWorker(sdkClient, activityWorkerOptions.TaskQueue, activityWorkerOptions.Options)
			wc.RegisterActivities(activityWorker)
			wm.workers = append(wm.workers, activityWorker)
		}
	}

	for _, w := range wm.workers {
		if err := w.Start(); err != nil {
			wm.logger.Fatal("Unable to start worker", tag.Error(err))
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
