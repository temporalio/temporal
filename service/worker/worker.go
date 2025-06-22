package worker

import (
	"context"
	"sync/atomic"

	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/sdk"
	workercommon "go.temporal.io/server/service/worker/common"
)

type (
	// workerManager maintains list of SDK workers.
	workerManager struct {
		status           int32
		hostInfo         membership.HostInfo
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
	hostInfo membership.HostInfo,
) *workerManager {
	return &workerManager{
		hostInfo:         hostInfo,
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
		Identity: "temporal-system@" + wm.hostInfo.Identity(),
		// TODO: add dynamic config for worker options
		BackgroundActivityContext: headers.SetCallerType(context.Background(), headers.CallerTypeBackgroundHigh),
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
			wfWorkerOptions.Options.Identity = "temporal-system@" + wm.hostInfo.Identity()
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
			activityWorkerOptions.Options.Identity = "temporal-system@" + wm.hostInfo.Identity()
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
