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

package systemconfig

import (
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/persistence"
	workercommon "go.temporal.io/server/service/worker/common"
	"go.uber.org/fx"
)

type (
	initParams struct {
		fx.In
		ClusterMetadataManager persistence.ClusterMetadataManager
		ClusterMetadata        cluster.Metadata
	}

	fxResult struct {
		fx.Out
		Component workercommon.WorkerComponent `group:"workerComponent"`
	}

	clusterWorkerComponent struct {
		initParams
	}
)

var Module = fx.Options(
	fx.Provide(NewResult),
)

func NewResult(params initParams) fxResult {
	component := &clusterWorkerComponent{
		initParams: params,
	}
	return fxResult{
		Component: component,
	}
}

func (wc *clusterWorkerComponent) RegisterWorkflow(registry sdkworker.Registry) {
	registry.RegisterWorkflowWithOptions(UpdateFailoverVersionIncrementWorkflow, workflow.RegisterOptions{Name: updateFailoverVersionIncrementWorkflowName})
}

func (wc *clusterWorkerComponent) DedicatedWorkflowWorkerOptions() *workercommon.DedicatedWorkerOptions {
	// Use default worker
	return nil
}

func (wc *clusterWorkerComponent) RegisterActivities(registry sdkworker.Registry) {
	registry.RegisterActivity(wc.activities())
}

func (wc *clusterWorkerComponent) DedicatedActivityWorkerOptions() *workercommon.DedicatedWorkerOptions {
	return nil
}

func (wc *clusterWorkerComponent) activities() *activities {
	return &activities{
		currentClusterName:     wc.ClusterMetadata.GetCurrentClusterName(),
		clusterMetadataManager: wc.ClusterMetadataManager,
	}
}
