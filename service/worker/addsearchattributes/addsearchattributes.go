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
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/searchattribute"
)

type (
	// addSearchAttributes is the background sub-system that execute workflow to add new search attributes.
	addSearchAttributes struct {
		sdkClient     sdkclient.Client
		esClient      esclient.Client
		saManager     searchattribute.Manager
		metricsClient metrics.Client
		logger        log.Logger
	}
)

// New returns a new instance of addSearchAttributes.
func New(
	sdkClient sdkclient.Client,
	esClient esclient.Client,
	saManager searchattribute.Manager,
	metricsClient metrics.Client,
	logger log.Logger,
) *addSearchAttributes {
	return &addSearchAttributes{
		sdkClient:     sdkClient,
		esClient:      esClient,
		saManager:     saManager,
		metricsClient: metricsClient,
		logger: log.With(logger,
			tag.ComponentAddSearchAttributes,
			tag.WorkflowID(WorkflowName),
		),
	}
}

// Start service.
func (s *addSearchAttributes) Start() error {
	workerOpts := worker.Options{}

	wrk := worker.New(s.sdkClient, TaskQueueName, workerOpts)

	a := newActivities(
		s.esClient,
		s.saManager,
		s.metricsClient,
		s.logger,
	)

	wrk.RegisterWorkflowWithOptions(AddSearchAttributesWorkflow, workflow.RegisterOptions{Name: WorkflowName})
	wrk.RegisterActivity(a)

	return wrk.Start()
}
