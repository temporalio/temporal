// The MIT License
//
// Copyright (c) 2024 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2024 Uber Technologies, Inc.
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

package deployment

import (
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	workercommon "go.temporal.io/server/service/worker/common"
	"go.uber.org/fx"
)

const (
	DeploymentWorkflowType       = "temporal-sys-deployment-workflow"
	DeploymentSeriesWorkflowType = "temporal-sys-deployment-series-workflow"
	DeploymentNamespaceDivision  = "TemporalDeployment"
)

var (
	DeploymentVisibilityBaseListQuery = fmt.Sprintf(
		"%s = '%s' AND %s = '%s' AND %s = '%s'",
		searchattribute.WorkflowType,
		DeploymentWorkflowType,
		searchattribute.TemporalNamespaceDivision,
		DeploymentNamespaceDivision,
		searchattribute.ExecutionStatus,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String(),
	)

	DeploymentSeriesVisibilityBaseListQuery = fmt.Sprintf(
		"%s = '%s' AND %s = '%s' AND %s = '%s'",
		searchattribute.WorkflowType,
		DeploymentSeriesWorkflowType,
		searchattribute.TemporalNamespaceDivision,
		DeploymentNamespaceDivision,
		searchattribute.ExecutionStatus,
		enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING.String(),
	)
)

type (
	workerComponent struct {
		activityDeps activityDeps
		enabledForNs dynamicconfig.BoolPropertyFnWithNamespaceFilter
	}

	activityDeps struct {
		fx.In
		MetricsHandler metrics.Handler
		Logger         log.Logger
		ClientFactory  sdk.ClientFactory
		MatchingClient resource.MatchingClient
	}

	fxResult struct {
		fx.Out
		Component workercommon.PerNSWorkerComponent `group:"perNamespaceWorkerComponent"`
	}
)

var Module = fx.Options(
	fx.Provide(NewResult),
	fx.Provide(DeploymentStoreClientProvider),
)

func DeploymentStoreClientProvider(historyClient resource.HistoryClient, visibilityManager manager.VisibilityManager, dc *dynamicconfig.Collection) DeploymentStoreClient {
	return &DeploymentClientImpl{
		HistoryClient:         historyClient,
		VisibilityManager:     visibilityManager,
		MaxIDLengthLimit:      dynamicconfig.MaxIDLengthLimit.Get(dc),
		VisibilityMaxPageSize: dynamicconfig.FrontendVisibilityMaxPageSize.Get(dc),
		reachabilityCache: newReachabilityCache(
			metrics.NoopMetricsHandler,
			visibilityManager,
			reachabilityCacheOpenWFsTTL,   // TODO (carly) use dc (ie. config.ReachabilityCacheOpenWFsTTL)
			reachabilityCacheClosedWFsTTL, // TODO (carly) use dc (ie. config.ReachabilityCacheClosedWFsTTL)
		),
	}
}

func NewResult(
	dc *dynamicconfig.Collection,
	params activityDeps,
) fxResult {
	return fxResult{
		Component: &workerComponent{
			activityDeps: params,
			enabledForNs: dynamicconfig.WorkerEnableDeployment.Get(dc),
		},
	}
}

func (s *workerComponent) DedicatedWorkerOptions(ns *namespace.Namespace) *workercommon.PerNSDedicatedWorkerOptions {
	return &workercommon.PerNSDedicatedWorkerOptions{
		Enabled: s.enabledForNs(ns.Name().String()),
	}
}

func (s *workerComponent) Register(registry sdkworker.Registry, ns *namespace.Namespace, details workercommon.RegistrationDetails) func() {
	registry.RegisterWorkflowWithOptions(DeploymentWorkflow, workflow.RegisterOptions{Name: DeploymentWorkflowType})
	registry.RegisterWorkflowWithOptions(DeploymentSeriesWorkflow, workflow.RegisterOptions{Name: DeploymentSeriesWorkflowType})

	// TODO Shivam: Might need a cleanup function upon activity registration
	deploymentActivities := s.newDeploymentActivities(ns.Name(), ns.ID())
	// deploymentSeriesActivities := s.newDeploymentSeriesActivities(ns.Name(), ns.ID())
	registry.RegisterActivity(deploymentActivities)
	// registry.RegisterActivity(deploymentSeriesActivities)
	return nil
}

// TODO Shivam - place holder for now but will initialize activity rate limits (if any) amongst other things
func (s *workerComponent) newDeploymentActivities(name namespace.Name, id namespace.ID) *DeploymentActivities {
	return &DeploymentActivities{
		activityDeps:  s.activityDeps,
		namespaceName: name,
		namespaceID:   id,
	}
}

// // TODO Shivam - place holder for now but will initialize activity rate limits (if any) amongst other things
// func (s *workerComponent) newDeploymentSeriesActivities(name namespace.Name, id namespace.ID) *DeploymentSeriesActivities {
// 	return &DeploymentSeriesActivities{
// 		activityDeps: s.activityDeps,
// 		namespace:    name,
// 		namespaceID:  id,
// 	}
// }
