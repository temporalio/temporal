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
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/sdk/worker"

	"go.temporal.io/server/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/sdk"
)

type (
	// Config defines the configuration for parent close policy worker
	Config struct {
		MaxConcurrentActivityExecutionSize     dynamicconfig.IntPropertyFn
		MaxConcurrentWorkflowTaskExecutionSize dynamicconfig.IntPropertyFn
		MaxConcurrentActivityTaskPollers       dynamicconfig.IntPropertyFn
		MaxConcurrentWorkflowTaskPollers       dynamicconfig.IntPropertyFn
		NumParentClosePolicySystemWorkflows    dynamicconfig.IntPropertyFn
	}

	// BootstrapParams contains the set of params needed to bootstrap
	// the sub-system
	BootstrapParams struct {
		// SdkSystemClient is an instance of temporal service client
		SdkClientFactory sdk.ClientFactory
		// MetricsClient is an instance of metrics object for emitting stats
		MetricsClient metrics.Client
		// Logger is the logger
		Logger log.Logger
		// Config contains the configuration for scanner
		Config Config
		// ClientBean is an instance of client.Bean for a collection of clients
		ClientBean client.Bean
		// CurrentCluster is the name of current cluster
		CurrentCluster string
	}

	// Processor is the background sub-system that execute workflow for ParentClosePolicy
	Processor struct {
		svcClientFactory sdk.ClientFactory
		clientBean       client.Bean
		metricsClient    metrics.Client
		cfg              Config
		logger           log.Logger
		currentCluster   string
	}
)

// New returns a new instance as daemon
func New(params *BootstrapParams) *Processor {
	return &Processor{
		svcClientFactory: params.SdkClientFactory,
		metricsClient:    params.MetricsClient,
		cfg:              params.Config,
		logger:           log.With(params.Logger, tag.ComponentBatcher),
		clientBean:       params.ClientBean,
		currentCluster:   params.CurrentCluster,
	}
}

// Start starts the scanner
func (s *Processor) Start() error {
	svcClient := s.svcClientFactory.GetSystemClient(s.logger)
	processorWorker := worker.New(svcClient, processorTaskQueueName, getWorkerOptions(s))
	processorWorker.RegisterWorkflowWithOptions(ProcessorWorkflow, workflow.RegisterOptions{Name: processorWFTypeName})
	processorWorker.RegisterActivityWithOptions(ProcessorActivity, activity.RegisterOptions{Name: processorActivityName})

	return processorWorker.Start()
}

func getWorkerOptions(p *Processor) worker.Options {
	ctx := context.WithValue(context.Background(), processorContextKey, p)
	return worker.Options{
		MaxConcurrentActivityExecutionSize:     p.cfg.MaxConcurrentActivityExecutionSize(),
		MaxConcurrentWorkflowTaskExecutionSize: p.cfg.MaxConcurrentWorkflowTaskExecutionSize(),
		MaxConcurrentActivityTaskPollers:       p.cfg.MaxConcurrentActivityTaskPollers(),
		MaxConcurrentWorkflowTaskPollers:       p.cfg.MaxConcurrentWorkflowTaskPollers(),
		BackgroundActivityContext:              ctx,
	}
}
