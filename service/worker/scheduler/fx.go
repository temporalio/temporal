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

package scheduler

import (
	"go.uber.org/fx"

	"go.temporal.io/api/workflowservice/v1"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	workercommon "go.temporal.io/server/service/worker/common"
)

const (
	WorkflowType  = "temporal-sys-scheduler-workflow"
	TaskQueueName = "temporal-sys-scheduler-tq"
)

type (
	workerComponent struct {
		activityDeps activityDeps
		enabledForNs dynamicconfig.BoolPropertyFnWithNamespaceFilter
		numWorkers   dynamicconfig.IntPropertyFnWithNamespaceFilter
	}

	activityDeps struct {
		fx.In
		MetricsClient  metrics.Client
		Logger         log.Logger
		HistoryClient  historyservice.HistoryServiceClient
		FrontendClient workflowservice.WorkflowServiceClient
	}

	fxResult struct {
		fx.Out
		Component workercommon.PerNSWorkerComponent `group:"perNamespaceWorkerComponent"`
	}
)

var Module = fx.Options(
	fx.Provide(NewResult),
)

func NewResult(
	dcCollection *dynamicconfig.Collection,
	params activityDeps,
) fxResult {
	return fxResult{
		Component: &workerComponent{
			activityDeps: params,
			enabledForNs: dcCollection.GetBoolPropertyFnWithNamespaceFilter(
				dynamicconfig.WorkerEnableScheduler, false),
			numWorkers: dcCollection.GetIntPropertyFilteredByNamespace(
				dynamicconfig.WorkerSchedulerNumWorkers, 1),
		},
	}
}

func (s *workerComponent) DedicatedWorkerOptions(ns *namespace.Namespace) *workercommon.PerNSDedicatedWorkerOptions {
	return &workercommon.PerNSDedicatedWorkerOptions{
		Enabled:    s.enabledForNs(ns.Name().String()),
		TaskQueue:  TaskQueueName,
		NumWorkers: s.numWorkers(ns.Name().String()),
	}
}

func (s *workerComponent) Register(worker sdkworker.Worker, ns *namespace.Namespace) {
	worker.RegisterWorkflowWithOptions(SchedulerWorkflow, workflow.RegisterOptions{Name: WorkflowType})
	worker.RegisterActivity(s.activities(ns.Name(), ns.ID()))
}

func (s *workerComponent) activities(name namespace.Name, id namespace.ID) *activities {
	return &activities{
		activityDeps: s.activityDeps,
		namespace:    name,
		namespaceID:  id,
	}
}
