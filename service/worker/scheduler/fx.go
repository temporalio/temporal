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
	"fmt"
	"time"

	"go.uber.org/fx"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	schedspb "go.temporal.io/server/api/schedule/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/searchattribute"
	workercommon "go.temporal.io/server/service/worker/common"
)

const (
	WorkflowType      = "temporal-sys-scheduler-workflow"
	NamespaceDivision = "TemporalScheduler"
)

var (
	VisibilityBaseListQuery = fmt.Sprintf(
		"%s = '%s' AND %s = '%s' AND %s = '%s'",
		searchattribute.WorkflowType,
		WorkflowType,
		searchattribute.TemporalNamespaceDivision,
		NamespaceDivision,
		searchattribute.ExecutionStatus,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String(),
	)
)

type (
	workerComponent struct {
		specBuilder              *SpecBuilder // workflow dep
		activityDeps             activityDeps
		enabledForNs             dynamicconfig.BoolPropertyFnWithNamespaceFilter
		globalNSStartWorkflowRPS dynamicconfig.FloatPropertyFnWithNamespaceFilter
		maxBlobSize              dynamicconfig.IntPropertyFnWithNamespaceFilter
		localActivitySleepLimit  dynamicconfig.DurationPropertyFnWithNamespaceFilter
	}

	activityDeps struct {
		fx.In
		MetricsHandler metrics.Handler
		Logger         log.Logger
		HistoryClient  resource.HistoryClient
		FrontendClient workflowservice.WorkflowServiceClient
	}

	fxResult struct {
		fx.Out
		Component workercommon.PerNSWorkerComponent `group:"perNamespaceWorkerComponent"`
	}
)

var Module = fx.Options(
	fx.Provide(NewResult),
	fx.Provide(NewSpecBuilder),
)

func NewResult(
	dc *dynamicconfig.Collection,
	specBuilder *SpecBuilder,
	params activityDeps,
) fxResult {
	return fxResult{
		Component: &workerComponent{
			specBuilder:              specBuilder,
			activityDeps:             params,
			enabledForNs:             dynamicconfig.WorkerEnableScheduler.Get(dc),
			globalNSStartWorkflowRPS: dynamicconfig.SchedulerNamespaceStartWorkflowRPS.Get(dc),
			maxBlobSize:              dynamicconfig.BlobSizeLimitError.Get(dc),
			localActivitySleepLimit:  dynamicconfig.SchedulerLocalActivitySleepLimit.Get(dc),
		},
	}
}

func (s *workerComponent) DedicatedWorkerOptions(ns *namespace.Namespace) *workercommon.PerNSDedicatedWorkerOptions {
	return &workercommon.PerNSDedicatedWorkerOptions{
		Enabled: s.enabledForNs(ns.Name().String()),
	}
}

func (s *workerComponent) Register(registry sdkworker.Registry, ns *namespace.Namespace, details workercommon.RegistrationDetails) {
	wfFunc := func(ctx workflow.Context, args *schedspb.StartScheduleArgs) error {
		return schedulerWorkflowWithSpecBuilder(ctx, args, s.specBuilder)
	}
	registry.RegisterWorkflowWithOptions(wfFunc, workflow.RegisterOptions{Name: WorkflowType})
	registry.RegisterActivity(s.activities(ns.Name(), ns.ID(), details))
}

func (s *workerComponent) activities(name namespace.Name, id namespace.ID, details workercommon.RegistrationDetails) *activities {
	localRPS := func() float64 {
		return float64(details.Multiplicity) * s.globalNSStartWorkflowRPS(name.String()) / float64(details.TotalWorkers)
	}
	return &activities{
		activityDeps:             s.activityDeps,
		namespace:                name,
		namespaceID:              id,
		startWorkflowRateLimiter: quotas.NewDefaultOutgoingRateLimiter(localRPS),
		maxBlobSize:              func() int { return s.maxBlobSize(name.String()) },
		localActivitySleepLimit:  func() time.Duration { return s.localActivitySleepLimit(name.String()) },
	}
}
