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

package deletenamespace

import (
	"context"

	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resource"
	workercommon "go.temporal.io/server/service/worker/common"
	"go.temporal.io/server/service/worker/deletenamespace/deleteexecutions"
	"go.temporal.io/server/service/worker/deletenamespace/reclaimresources"
)

type (
	// deleteNamespaceComponent represent background work needed for delete namespace.
	deleteNamespaceComponent struct {
		atWorkerCfg       workercommon.ActivityWorkerLimitsConfig
		visibilityManager manager.VisibilityManager
		metadataManager   persistence.MetadataManager
		historyClient     resource.HistoryClient
		metricsHandler    metrics.Handler
		logger            log.Logger
	}
	componentParams struct {
		fx.In
		DynamicCollection *dynamicconfig.Collection
		VisibilityManager manager.VisibilityManager
		MetadataManager   persistence.MetadataManager
		HistoryClient     resource.HistoryClient
		MetricsHandler    metrics.Handler
		Logger            log.Logger
	}
)

var Module = workercommon.AnnotateWorkerComponentProvider(newComponent)

func newComponent(
	params componentParams,
) workercommon.WorkerComponent {
	return &deleteNamespaceComponent{
		atWorkerCfg: workercommon.NewActivityWorkerConcurrencyConfig(
			params.DynamicCollection,
			dynamicconfig.WorkerDeleteNamespaceActivityLimitsConfig,
		),
		visibilityManager: params.VisibilityManager,
		metadataManager:   params.MetadataManager,
		historyClient:     params.HistoryClient,
		metricsHandler:    params.MetricsHandler,
		logger:            params.Logger,
	}
}

func (wc *deleteNamespaceComponent) RegisterWorkflow(registry sdkworker.Registry) {
	registry.RegisterWorkflowWithOptions(DeleteNamespaceWorkflow, workflow.RegisterOptions{Name: WorkflowName})
	registry.RegisterActivity(wc.deleteNamespaceLocalActivities())

	registry.RegisterWorkflowWithOptions(reclaimresources.ReclaimResourcesWorkflow, workflow.RegisterOptions{Name: reclaimresources.WorkflowName})
	registry.RegisterActivity(wc.reclaimResourcesLocalActivities())

	registry.RegisterWorkflowWithOptions(deleteexecutions.DeleteExecutionsWorkflow, workflow.RegisterOptions{Name: deleteexecutions.WorkflowName})
	registry.RegisterActivity(wc.deleteExecutionsLocalActivities())
}

func (wc *deleteNamespaceComponent) DedicatedWorkflowWorkerOptions() *workercommon.DedicatedWorkerOptions {
	// use default worker
	return nil
}

func (wc *deleteNamespaceComponent) RegisterActivities(registry sdkworker.Registry) {
	registry.RegisterActivity(wc.reclaimResourcesActivities())
	registry.RegisterActivity(wc.deleteExecutionsActivities())
}

func (wc *deleteNamespaceComponent) DedicatedActivityWorkerOptions() *workercommon.DedicatedWorkerOptions {
	return &workercommon.DedicatedWorkerOptions{
		TaskQueue: primitives.DeleteNamespaceActivityTQ,
		Options: sdkworker.Options{
			BackgroundActivityContext:          headers.SetCallerType(context.Background(), headers.CallerTypePreemptable),
			MaxConcurrentActivityExecutionSize: wc.atWorkerCfg.MaxConcurrentActivityExecutionSize,
			TaskQueueActivitiesPerSecond:       wc.atWorkerCfg.TaskQueueActivitiesPerSecond,
			WorkerActivitiesPerSecond:          wc.atWorkerCfg.WorkerActivitiesPerSecond,
			MaxConcurrentActivityTaskPollers:   wc.atWorkerCfg.MaxConcurrentActivityTaskPollers,
		},
	}
}

func (wc *deleteNamespaceComponent) deleteNamespaceLocalActivities() *localActivities {
	return NewLocalActivities(wc.metadataManager, wc.metricsHandler, wc.logger)
}

func (wc *deleteNamespaceComponent) reclaimResourcesActivities() *reclaimresources.Activities {
	return reclaimresources.NewActivities(wc.visibilityManager, wc.metricsHandler, wc.logger)
}

func (wc *deleteNamespaceComponent) reclaimResourcesLocalActivities() *reclaimresources.LocalActivities {
	return reclaimresources.NewLocalActivities(wc.visibilityManager, wc.metadataManager, wc.metricsHandler, wc.logger)
}

func (wc *deleteNamespaceComponent) deleteExecutionsActivities() *deleteexecutions.Activities {
	return deleteexecutions.NewActivities(wc.visibilityManager, wc.historyClient, wc.metricsHandler, wc.logger)
}

func (wc *deleteNamespaceComponent) deleteExecutionsLocalActivities() *deleteexecutions.LocalActivities {
	return deleteexecutions.NewLocalActivities(wc.visibilityManager, wc.metricsHandler, wc.logger)
}
