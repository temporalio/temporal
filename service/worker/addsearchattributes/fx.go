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

package addsearchattributes

import (
	sdkworker "go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"go.uber.org/fx"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/searchattribute"
	workercommon "go.temporal.io/server/service/worker/common"
)

type (
	// addSearchAttributes represent background work needed for adding search attributes
	addSearchAttributes struct {
		initParams
	}

	initParams struct {
		fx.In
		EsClient      esclient.Client
		Manager       searchattribute.Manager
		MetricsClient metrics.Client
		Logger        log.Logger
	}

	fxResult struct {
		fx.Out
		Component workercommon.WorkerComponent `group:"workerComponent"`
	}
)

var Module = fx.Options(
	fx.Provide(NewResult),
)

func NewResult(params initParams) fxResult {
	component := &addSearchAttributes{
		initParams: params,
	}
	return fxResult{
		Component: component,
	}
}

func (wc *addSearchAttributes) Register(worker sdkworker.Worker) {
	worker.RegisterWorkflowWithOptions(AddSearchAttributesWorkflow, workflow.RegisterOptions{Name: WorkflowName})
	worker.RegisterActivity(wc.activities())
}

func (wc *addSearchAttributes) DedicatedWorkerOptions() *workercommon.DedicatedWorkerOptions {
	// use default worker
	return nil
}

func (wc *addSearchAttributes) activities() *activities {
	return &activities{
		esClient:      wc.EsClient,
		saManager:     wc.Manager,
		metricsClient: wc.MetricsClient,
		logger:        wc.Logger,
	}
}
