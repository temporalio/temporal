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

package parentclosepolicy

import (
	"context"

	"go.temporal.io/sdk/activity"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/sdk/worker"

	"go.temporal.io/server/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

type (
	// Config defines the configuration for parent close policy worker
	Config struct {
		MaxConcurrentActivityExecutionSize     dynamicconfig.IntPropertyFn
		MaxConcurrentWorkflowTaskExecutionSize dynamicconfig.IntPropertyFn
		MaxConcurrentActivityTaskPollers       dynamicconfig.IntPropertyFn
		MaxConcurrentWorkflowTaskPollers       dynamicconfig.IntPropertyFn
	}

	// BootstrapParams contains the set of params needed to bootstrap
	// the sub-system
	BootstrapParams struct {
		// SdkSystemClient is an instance of temporal service client
		SdkSystemClient sdkclient.Client
		// MetricsClient is an instance of metrics object for emitting stats
		MetricsClient metrics.Client
		// Logger is the logger
		Logger log.Logger
		// Config contains the configuration for scanner
		Config Config
		// ClientBean is an instance of client.Bean for a collection of clients
		ClientBean client.Bean
	}

	// Processor is the background sub-system that execute workflow for ParentClosePolicy
	Processor struct {
		svcClient     sdkclient.Client
		clientBean    client.Bean
		metricsClient metrics.Client
		cfg           Config
		logger        log.Logger
	}
)

// New returns a new instance as daemon
func New(params *BootstrapParams) *Processor {
	return &Processor{
		svcClient:     params.SdkSystemClient,
		metricsClient: params.MetricsClient,
		cfg:           params.Config,
		logger:        log.With(params.Logger, tag.ComponentBatcher),
		clientBean:    params.ClientBean,
	}
}

// Start starts the scanner
func (s *Processor) Start() error {
	ctx := context.WithValue(context.Background(), processorContextKey, s)
	workerOpts := worker.Options{
		MaxConcurrentActivityExecutionSize:     s.cfg.MaxConcurrentActivityExecutionSize(),
		MaxConcurrentWorkflowTaskExecutionSize: s.cfg.MaxConcurrentWorkflowTaskExecutionSize(),
		MaxConcurrentActivityTaskPollers:       s.cfg.MaxConcurrentActivityTaskPollers(),
		MaxConcurrentWorkflowTaskPollers:       s.cfg.MaxConcurrentWorkflowTaskPollers(),
		BackgroundActivityContext:              ctx,
	}
	processorWorker := worker.New(s.svcClient, processorTaskQueueName, workerOpts)

	processorWorker.RegisterWorkflowWithOptions(ProcessorWorkflow, workflow.RegisterOptions{Name: processorWFTypeName})
	processorWorker.RegisterActivityWithOptions(ProcessorActivity, activity.RegisterOptions{Name: processorActivityName})

	return processorWorker.Start()
}
