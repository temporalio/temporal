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

	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/sdk"
)

const (
	taskQueueName = "temporal-sys-batcher-taskqueue"
)

type (
	// Batcher is the background sub-system that executes a workflow for batch operations.
	// It is also the context object that gets passed around within the workflows / activities.
	Batcher struct {
		sdkClientFactory sdk.ClientFactory
		metricsHandler   metrics.Handler
		logger           log.Logger
		rps              dynamicconfig.IntPropertyFnWithNamespaceFilter
		concurrency      dynamicconfig.IntPropertyFnWithNamespaceFilter
	}
)

// New returns a new instance of the Batcher.
func New(
	metricsHandler metrics.Handler,
	logger log.Logger,
	sdkClientFactory sdk.ClientFactory,
	rps dynamicconfig.IntPropertyFnWithNamespaceFilter,
	concurrency dynamicconfig.IntPropertyFnWithNamespaceFilter,
) *Batcher {
	return &Batcher{
		sdkClientFactory: sdkClientFactory,
		metricsHandler:   metricsHandler,
		logger:           log.With(logger, tag.ComponentBatcher),
		rps:              rps,
		concurrency:      concurrency,
	}
}

// Start starts a worker for the Batcher's activity and workflow.
func (s *Batcher) Start() error {
	ctx := headers.SetCallerInfo(context.Background(), headers.SystemBackgroundCallerInfo)
	workerOpts := worker.Options{
		BackgroundActivityContext: ctx,
	}
	sdkClient := s.sdkClientFactory.GetSystemClient()
	batchWorker := s.sdkClientFactory.NewWorker(sdkClient, taskQueueName, workerOpts)
	batchWorker.RegisterWorkflowWithOptions(BatchWorkflow, workflow.RegisterOptions{Name: BatchWFTypeName})
	batchWorker.RegisterActivity(&activities{
		activityDeps: activityDeps{
			MetricsHandler: s.metricsHandler,
			Logger:         s.logger,
			ClientFactory:  s.sdkClientFactory,
		},
		namespace:   primitives.SystemLocalNamespace,
		namespaceID: primitives.SystemNamespaceID,
		rps:         s.rps,
		concurrency: s.concurrency,
	})

	return batchWorker.Start()
}
