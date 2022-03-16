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

package batcher

import (
	"context"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/sdk"
)

type (
	// Config defines the configuration for batcher
	Config struct {
		MaxConcurrentActivityExecutionSize     dynamicconfig.IntPropertyFn
		MaxConcurrentWorkflowTaskExecutionSize dynamicconfig.IntPropertyFn
		MaxConcurrentActivityTaskPollers       dynamicconfig.IntPropertyFn
		MaxConcurrentWorkflowTaskPollers       dynamicconfig.IntPropertyFn
	}

	// Batcher is the background sub-system that execute workflow for batch operations
	// It is also the context object that get's passed around within the scanner workflows / activities
	Batcher struct {
		cfg              *Config
		sdkClientFactory sdk.ClientFactory
		metricsClient    metrics.Client
		logger           log.Logger
	}
)

// New returns a new instance of batcher daemon Batcher
func New(
	cfg *Config,
	metricsClient metrics.Client,
	logger log.Logger,
	sdkClientFactory sdk.ClientFactory,
) *Batcher {
	return &Batcher{
		cfg:              cfg,
		sdkClientFactory: sdkClientFactory,
		metricsClient:    metricsClient,
		logger:           log.With(logger, tag.ComponentBatcher),
	}
}

// Start starts the scanner
func (s *Batcher) Start() error {
	// start worker for batch operation workflows
	ctx := context.WithValue(context.Background(), batcherContextKey, s)

	workerOpts := worker.Options{
		MaxConcurrentActivityExecutionSize:     s.cfg.MaxConcurrentActivityExecutionSize(),
		MaxConcurrentWorkflowTaskExecutionSize: s.cfg.MaxConcurrentWorkflowTaskExecutionSize(),
		MaxConcurrentActivityTaskPollers:       s.cfg.MaxConcurrentActivityTaskPollers(),
		MaxConcurrentWorkflowTaskPollers:       s.cfg.MaxConcurrentWorkflowTaskPollers(),

		BackgroundActivityContext: ctx,
	}

	sdkClient := s.sdkClientFactory.GetSystemClient(s.logger)
	batchWorker := worker.New(sdkClient, BatcherTaskQueueName, workerOpts)
	batchWorker.RegisterWorkflowWithOptions(BatchWorkflow, workflow.RegisterOptions{Name: BatchWFTypeName})
	batchWorker.RegisterActivityWithOptions(BatchActivity, activity.RegisterOptions{Name: batchActivityName})

	return batchWorker.Start()
}
