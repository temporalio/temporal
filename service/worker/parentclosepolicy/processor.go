// Copyright (c) 2017 Uber Technologies, Inc.
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

package parentclosepolicy

import (
	"context"

	"go.temporal.io/temporal/activity"
	sdkclient "go.temporal.io/temporal/client"
	"go.temporal.io/temporal/workflow"

	"go.temporal.io/temporal/worker"

	"github.com/temporalio/temporal/client"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
)

type (
	// BootstrapParams contains the set of params needed to bootstrap
	// the sub-system
	BootstrapParams struct {
		// Config contains the configuration for scanner
		// ServiceClient is an instance of cadence service client
		ServiceClient sdkclient.Client
		// MetricsClient is an instance of metrics object for emitting stats
		MetricsClient metrics.Client
		Logger        log.Logger
		// ClientBean is an instance of client.Bean for a collection of clients
		ClientBean client.Bean
	}

	// Processor is the background sub-system that execute workflow for ParentClosePolicy
	Processor struct {
		svcClient     sdkclient.Client
		clientBean    client.Bean
		metricsClient metrics.Client
		logger        log.Logger
	}
)

// New returns a new instance as daemon
func New(params *BootstrapParams) *Processor {
	return &Processor{
		svcClient:     params.ServiceClient,
		metricsClient: params.MetricsClient,
		logger:        params.Logger.WithTags(tag.ComponentBatcher),
		clientBean:    params.ClientBean,
	}
}

// Start starts the scanner
func (s *Processor) Start() error {
	ctx := context.WithValue(context.Background(), processorContextKey, s)
	workerOpts := worker.Options{
		BackgroundActivityContext: ctx,
	}
	processorWorker := worker.New(s.svcClient, processorTaskListName, workerOpts)

	processorWorker.RegisterWorkflowWithOptions(ProcessorWorkflow, workflow.RegisterOptions{Name: processorWFTypeName})
	processorWorker.RegisterActivityWithOptions(ProcessorActivity, activity.RegisterOptions{Name: processorActivityName})

	return processorWorker.Start()
}
