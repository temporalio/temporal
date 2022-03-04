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
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/sdk"
	workercommon "go.temporal.io/server/service/worker/common"
	"go.temporal.io/server/service/worker/deletenamespace/deleteexecutions"
)

type (
	// deleteNamespaceComponent represent background work needed for delete namespace.
	deleteNamespaceComponent struct {
		sdkClientFactory sdk.ClientFactory
		metadataManager  persistence.MetadataManager
		historyClient    historyservice.HistoryServiceClient
		metricsClient    metrics.Client
		logger           log.Logger
	}

	component struct {
		fx.Out
		DeleteNamespaceComponent workercommon.WorkerComponent `group:"workerComponent"`
	}
)

var Module = fx.Options(
	fx.Provide(newComponent),
)

func newComponent(
	sdkClientFactory sdk.ClientFactory,
	metadataManager persistence.MetadataManager,
	historyClient historyservice.HistoryServiceClient,
	metricsClient metrics.Client,
	logger log.Logger,
) component {
	return component{
		DeleteNamespaceComponent: &deleteNamespaceComponent{
			sdkClientFactory: sdkClientFactory,
			metadataManager:  metadataManager,
			historyClient:    historyClient,
			metricsClient:    metricsClient,
			logger:           logger,
		}}
}

func (wc *deleteNamespaceComponent) Register(worker sdkworker.Worker) {
	worker.RegisterWorkflowWithOptions(DeleteNamespaceWorkflow, workflow.RegisterOptions{Name: WorkflowName})
	worker.RegisterActivity(wc.deleteNamespaceActivities())

	worker.RegisterWorkflow(deleteexecutions.DeleteExecutionsWorkflow)
	worker.RegisterActivity(wc.deleteExecutionsActivities())
}

func (wc *deleteNamespaceComponent) DedicatedWorkerOptions() *workercommon.DedicatedWorkerOptions {
	// use default worker
	return nil
}

func (wc *deleteNamespaceComponent) deleteNamespaceActivities() *activities {
	return NewActivities(wc.sdkClientFactory, wc.metadataManager, wc.metricsClient, wc.logger)
}

func (wc *deleteNamespaceComponent) deleteExecutionsActivities() *deleteexecutions.Activities {
	return deleteexecutions.NewActivities(wc.sdkClientFactory, wc.historyClient, wc.metricsClient, wc.logger)
}
