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

package archiver

import (
	"context"
	"time"

	"go.temporal.io/sdk/activity"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/sdk"
)

type (
	// ClientWorker is a temporal client worker
	ClientWorker interface {
		Start() error
		Stop()
	}

	clientWorker struct {
		worker            worker.Worker
		namespaceRegistry namespace.Registry
	}

	// BootstrapContainer contains everything need for bootstrapping
	BootstrapContainer struct {
		SdkClientFactory sdk.ClientFactory
		MetricsClient    metrics.Client
		Logger           log.Logger
		HistoryV2Manager persistence.ExecutionManager
		NamespaceCache   namespace.Registry
		Config           *Config
		ArchiverProvider provider.ArchiverProvider
	}

	// Config for ClientWorker
	Config struct {
		MaxConcurrentActivityExecutionSize     dynamicconfig.IntPropertyFn
		MaxConcurrentWorkflowTaskExecutionSize dynamicconfig.IntPropertyFn
		MaxConcurrentActivityTaskPollers       dynamicconfig.IntPropertyFn
		MaxConcurrentWorkflowTaskPollers       dynamicconfig.IntPropertyFn

		ArchiverConcurrency           dynamicconfig.IntPropertyFn
		ArchivalsPerIteration         dynamicconfig.IntPropertyFn
		TimeLimitPerArchivalIteration dynamicconfig.DurationPropertyFn
	}

	contextKey int
)

const (
	workflowIDPrefix       = "temporal-archival"
	workflowTaskQueue      = "temporal-archival-tq"
	signalName             = "temporal-archival-signal"
	archivalWorkflowFnName = "archivalWorkflow"
	workflowRunTimeout     = time.Hour * 24 * 30
	workflowTaskTimeout    = time.Minute

	bootstrapContainerKey contextKey = iota
)

// these globals exist as a work around because no primitive exists to pass such objects to workflow code
// TODO: remove these and move to Fx
var (
	globalLogger        log.Logger
	globalMetricsClient metrics.Client
	globalConfig        *Config
)

// NewClientWorker returns a new ClientWorker
func NewClientWorker(container *BootstrapContainer) ClientWorker {
	globalLogger = log.With(container.Logger, tag.ComponentArchiver, tag.WorkflowNamespace(common.SystemLocalNamespace))
	globalMetricsClient = container.MetricsClient
	globalConfig = container.Config
	actCtx := context.WithValue(context.Background(), bootstrapContainerKey, container)

	sdkClient := container.SdkClientFactory.GetSystemClient(container.Logger)
	wo := worker.Options{
		MaxConcurrentActivityExecutionSize:     container.Config.MaxConcurrentActivityExecutionSize(),
		MaxConcurrentWorkflowTaskExecutionSize: container.Config.MaxConcurrentWorkflowTaskExecutionSize(),
		MaxConcurrentActivityTaskPollers:       container.Config.MaxConcurrentActivityTaskPollers(),
		MaxConcurrentWorkflowTaskPollers:       container.Config.MaxConcurrentWorkflowTaskPollers(),
		BackgroundActivityContext:              actCtx,
	}
	clientWorker := &clientWorker{
		worker:            worker.New(sdkClient, workflowTaskQueue, wo),
		namespaceRegistry: container.NamespaceCache,
	}

	clientWorker.worker.RegisterWorkflowWithOptions(archivalWorkflow, workflow.RegisterOptions{Name: archivalWorkflowFnName})
	clientWorker.worker.RegisterActivityWithOptions(uploadHistoryActivity, activity.RegisterOptions{Name: uploadHistoryActivityFnName})
	clientWorker.worker.RegisterActivityWithOptions(deleteHistoryActivity, activity.RegisterOptions{Name: deleteHistoryActivityFnName})
	clientWorker.worker.RegisterActivityWithOptions(archiveVisibilityActivity, activity.RegisterOptions{Name: archiveVisibilityActivityFnName})

	return clientWorker
}

// Start the ClientWorker
func (w *clientWorker) Start() error {
	if err := w.worker.Start(); err != nil {
		w.worker.Stop()
		return err
	}
	return nil
}

// Stop the ClientWorker
func (w *clientWorker) Stop() {
	w.worker.Stop()
	w.namespaceRegistry.Stop()
}
