// The MIT License
//
// Copyright (c) 2022 Temporal Technologies Inc.  All rights reserved.
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

package batcher

import (
	"go.temporal.io/api/workflowservice/v1"
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/sdk"
	workercommon "go.temporal.io/server/service/worker/common"
)

const (
	// BatchWFTypeName is the workflow type
	BatchWFTypeName   = "temporal-sys-batch-workflow"
	NamespaceDivision = "TemporalBatcher"
	// DefaultRPS is the default RPS
	DefaultRPS = 50
	// DefaultConcurrency is the default concurrency
	DefaultConcurrency = 5
)

type (
	workerComponent struct {
		activityDeps   activityDeps
		dc             *dynamicconfig.Collection
		enabledFeature dynamicconfig.BoolPropertyFnWithNamespaceFilter
	}

	activityDeps struct {
		fx.In
		MetricsHandler metrics.Handler
		Logger         log.Logger
		ClientFactory  sdk.ClientFactory
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
	dc *dynamicconfig.Collection,
	params activityDeps,
) fxResult {
	return fxResult{
		Component: &workerComponent{
			activityDeps:   params,
			dc:             dc,
			enabledFeature: dc.GetBoolPropertyFnWithNamespaceFilter(dynamicconfig.EnableBatcher, true),
		},
	}
}

func (s *workerComponent) DedicatedWorkerOptions(ns *namespace.Namespace) *workercommon.PerNSDedicatedWorkerOptions {
	namespaceName := ns.Name().String()
	enableFeature := s.enabledFeature(namespaceName)
	return &workercommon.PerNSDedicatedWorkerOptions{
		Enabled: enableFeature,
	}
}

func (s *workerComponent) Register(worker sdkworker.Worker, ns *namespace.Namespace, _ workercommon.RegistrationDetails) {
	worker.RegisterWorkflowWithOptions(BatchWorkflow, workflow.RegisterOptions{Name: BatchWFTypeName})
	worker.RegisterActivity(s.activities(ns.Name(), ns.ID()))
}

func (s *workerComponent) activities(name namespace.Name, id namespace.ID) *activities {
	return &activities{
		activityDeps: s.activityDeps,
		namespace:    name,
		namespaceID:  id,
		rps:          s.dc.GetIntPropertyFilteredByNamespace(dynamicconfig.BatcherRPS, DefaultRPS),
		concurrency:  s.dc.GetIntPropertyFilteredByNamespace(dynamicconfig.BatcherConcurrency, DefaultConcurrency),
	}
}
