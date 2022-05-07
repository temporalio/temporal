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
	"go.temporal.io/api/workflowservice/v1"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	workercommon "go.temporal.io/server/service/worker/common"
)

type (
	initParams struct {
		fx.In
		PersistenceConfig *config.Persistence
		ExecutionManager  persistence.ExecutionManager
		NamespaceRegistry namespace.Registry
		HistoryClient     historyservice.HistoryServiceClient
		FrontendClient    workflowservice.WorkflowServiceClient
		Logger            log.Logger
		MetricsClient     metrics.Client
	}

	fxResult struct {
		fx.Out
		Component workercommon.WorkerComponent `group:"workerComponent"`
	}

	replicationWorkerComponent struct {
		initParams
	}
)

var Module = fx.Options(
	fx.Provide(NewResult),
)

func NewResult(params initParams) fxResult {
	component := &replicationWorkerComponent{
		initParams: params,
	}
	return fxResult{
		Component: component,
	}
}

func (wc *replicationWorkerComponent) Register(worker sdkworker.Worker) {
	worker.RegisterWorkflowWithOptions(ForceReplicationWorkflow, workflow.RegisterOptions{Name: forceReplicationWorkflowName})
	worker.RegisterWorkflowWithOptions(NamespaceHandoverWorkflow, workflow.RegisterOptions{Name: namespaceHandoverWorkflowName})
	worker.RegisterActivity(wc.activities())
}

func (wc *replicationWorkerComponent) DedicatedWorkerOptions() *workercommon.DedicatedWorkerOptions {
	// use default worker
	return nil
}

func (wc *replicationWorkerComponent) activities() *activities {
	return &activities{
		historyShardCount: wc.PersistenceConfig.NumHistoryShards,
		executionManager:  wc.ExecutionManager,
		namespaceRegistry: wc.NamespaceRegistry,
		historyClient:     wc.HistoryClient,
		frontendClient:    wc.FrontendClient,
		logger:            wc.Logger,
		metricsClient:     wc.MetricsClient,
	}
}
