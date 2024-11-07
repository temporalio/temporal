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

package versioning

import (
	"fmt"

	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/searchattribute"
	workercommon "go.temporal.io/server/service/worker/common"
	"go.uber.org/fx"
)

const (
	DeploymentWorkflowType      = "temporal-sys-deployment-workflow"
	DeploymentNamespaceDivision = "TemporalDeployment"

	DeploymentNameWorkflowType      = "temporal-sys-deployment-name-workflow"
	DeploymentNameNamespaceDivision = "TemporalDeploymentName"
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

	DeploymentNameVisibilityBaseListQuery = fmt.Sprintf(
		"%s = '%s' AND %s = '%s' AND %s = '%s'",
		searchattribute.WorkflowType,
		DeploymentNameWorkflowType,
		searchattribute.TemporalNamespaceDivision,
		DeploymentNameNamespaceDivision,
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
		FrontendClient workflowservice.WorkflowServiceClient //  TODO Shivam - may not require this since deployment activities only use matching client
		MatchingClient matchingservice.MatchingServiceClient
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
	registry.RegisterWorkflowWithOptions(DeploymentNameWorkflow, workflow.RegisterOptions{Name: DeploymentNameWorkflowType})

	// TODO Shivam: Might need a cleanup function upon activity registration
	activities := s.newActivities(ns.Name(), ns.ID())
	registry.RegisterActivity(activities)
	return nil
}

// TODO Shivam - place holder for now but will initialize activity rate limits (if any) amongst other things
func (s *workerComponent) newActivities(name namespace.Name, id namespace.ID) *activities {
	return &activities{
		activityDeps: s.activityDeps,
		namespace:    name,
		namespaceID:  id,
	}
}
