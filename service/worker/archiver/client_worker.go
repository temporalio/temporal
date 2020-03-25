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

package archiver

import (
	"context"
	"time"

	"go.temporal.io/temporal/activity"
	sdkclient "go.temporal.io/temporal/client"
	"go.temporal.io/temporal/worker"
	"go.temporal.io/temporal/workflow"

	"github.com/temporalio/temporal/common"
	"github.com/temporalio/temporal/common/archiver/provider"
	"github.com/temporalio/temporal/common/cache"
	"github.com/temporalio/temporal/common/log"
	"github.com/temporalio/temporal/common/log/tag"
	"github.com/temporalio/temporal/common/metrics"
	"github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/service/dynamicconfig"
)

type (
	// ClientWorker is a cadence client worker
	ClientWorker interface {
		Start() error
		Stop()
	}

	clientWorker struct {
		worker      worker.Worker
		domainCache cache.DomainCache
	}

	// BootstrapContainer contains everything need for bootstrapping
	BootstrapContainer struct {
		PublicClient     sdkclient.Client
		MetricsClient    metrics.Client
		Logger           log.Logger
		HistoryV2Manager persistence.HistoryManager
		DomainCache      cache.DomainCache
		Config           *Config
		ArchiverProvider provider.ArchiverProvider
	}

	// Config for ClientWorker
	Config struct {
		ArchiverConcurrency           dynamicconfig.IntPropertyFn
		ArchivalsPerIteration         dynamicconfig.IntPropertyFn
		TimeLimitPerArchivalIteration dynamicconfig.DurationPropertyFn
	}

	contextKey int
)

const (
	workflowIDPrefix                = "cadence-archival"
	decisionTaskList                = "cadence-archival-tl"
	signalName                      = "cadence-archival-signal"
	archivalWorkflowFnName          = "archivalWorkflow"
	workflowStartToCloseTimeout     = time.Hour * 24 * 30
	workflowTaskStartToCloseTimeout = time.Minute

	bootstrapContainerKey contextKey = iota
)

// these globals exist as a work around because no primitive exists to pass such objects to workflow code
var (
	globalLogger        log.Logger
	globalMetricsClient metrics.Client
	globalConfig        *Config
)

// NewClientWorker returns a new ClientWorker
func NewClientWorker(container *BootstrapContainer) ClientWorker {
	globalLogger = container.Logger.WithTags(tag.ComponentArchiver, tag.WorkflowDomainName(common.SystemLocalDomainName))
	globalMetricsClient = container.MetricsClient
	globalConfig = container.Config
	actCtx := context.WithValue(context.Background(), bootstrapContainerKey, container)
	wo := worker.Options{
		BackgroundActivityContext: actCtx,
	}
	clientWorker := &clientWorker{
		worker:      worker.New(container.PublicClient, decisionTaskList, wo),
		domainCache: container.DomainCache,
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
	w.domainCache.Stop()
}
