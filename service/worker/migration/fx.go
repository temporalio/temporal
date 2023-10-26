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

package migration

import (
	"context"

	"go.temporal.io/api/workflowservice/v1"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"

	serverClient "go.temporal.io/server/client"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resource"
	workercommon "go.temporal.io/server/service/worker/common"
)

type (
	initParams struct {
		fx.In
		PersistenceConfig         *config.Persistence
		ExecutionManager          persistence.ExecutionManager
		NamespaceRegistry         namespace.Registry
		HistoryClient             resource.HistoryClient
		FrontendClient            workflowservice.WorkflowServiceClient
		ClientFactory             serverClient.Factory
		ClientBean                serverClient.Bean
		NamespaceReplicationQueue persistence.NamespaceReplicationQueue
		TaskManager               persistence.TaskManager
		Logger                    log.Logger
		MetricsHandler            metrics.Handler
	}

	replicationWorkerComponent struct {
		initParams
	}
)

var Module = workercommon.AnnotateWorkerComponentProvider(newComponent)

func newComponent(params initParams) workercommon.WorkerComponent {
	return &replicationWorkerComponent{initParams: params}
}

func (wc *replicationWorkerComponent) RegisterWorkflow(registry sdkworker.Registry) {
	registry.RegisterWorkflowWithOptions(ForceReplicationWorkflow, workflow.RegisterOptions{Name: forceReplicationWorkflowName})
	registry.RegisterWorkflowWithOptions(NamespaceHandoverWorkflow, workflow.RegisterOptions{Name: namespaceHandoverWorkflowName})
	registry.RegisterWorkflow(ForceTaskQueueUserDataReplicationWorkflow)
}

func (wc *replicationWorkerComponent) DedicatedWorkflowWorkerOptions() *workercommon.DedicatedWorkerOptions {
	// use default worker
	return nil
}

func (wc *replicationWorkerComponent) RegisterActivities(registry sdkworker.Registry) {
	registry.RegisterActivity(wc.activities())
}

func (wc *replicationWorkerComponent) DedicatedActivityWorkerOptions() *workercommon.DedicatedWorkerOptions {
	return &workercommon.DedicatedWorkerOptions{
		TaskQueue: primitives.MigrationActivityTQ,
		Options: sdkworker.Options{
			BackgroundActivityContext: headers.SetCallerType(context.Background(), headers.CallerTypePreemptable),
		},
	}
}

func (wc *replicationWorkerComponent) activities() *activities {
	return &activities{
		historyShardCount:              wc.PersistenceConfig.NumHistoryShards,
		executionManager:               wc.ExecutionManager,
		namespaceRegistry:              wc.NamespaceRegistry,
		historyClient:                  wc.HistoryClient,
		frontendClient:                 wc.FrontendClient,
		clientFactory:                  wc.ClientFactory,
		clientBean:                     wc.ClientBean,
		namespaceReplicationQueue:      wc.NamespaceReplicationQueue,
		taskManager:                    wc.TaskManager,
		logger:                         wc.Logger,
		metricsHandler:                 wc.MetricsHandler,
		forceReplicationMetricsHandler: wc.MetricsHandler.WithTags(metrics.WorkflowTypeTag(forceReplicationWorkflowName)),
	}
}
