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

package sysworkflow

import (
	"context"
	"github.com/uber-common/bark"
	"github.com/uber/cadence/client/public"
	"github.com/uber/cadence/common/blobstore"
	"github.com/uber/cadence/common/cache"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/logging"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"go.uber.org/cadence/activity"
	"go.uber.org/cadence/worker"
	"go.uber.org/cadence/workflow"
)

type (
	// SysWorker is a cadence client worker which enables running arbitrary system workflows
	SysWorker interface {
		Start() error
		Stop()
	}

	sysworker struct {
		worker      worker.Worker
		domainCache cache.DomainCache
	}

	// SysWorkerContainer contains everything needed to bootstrap SysWorker and all hosted system workflows
	SysWorkerContainer struct {
		PublicClient     public.Client
		MetricsClient    metrics.Client
		Logger           bark.Logger
		ClusterMetadata  cluster.Metadata
		HistoryManager   persistence.HistoryManager
		HistoryV2Manager persistence.HistoryV2Manager
		Blobstore        blobstore.Client
		DomainCache      cache.DomainCache
		Config           *Config

		HistoryBlobIterator HistoryBlobIterator // this is only set in testing code
	}

	// Config for SysWorker
	Config struct {
		EnableArchivalCompression dynamicconfig.BoolPropertyFnWithDomainFilter
		HistoryPageSize           dynamicconfig.IntPropertyFnWithDomainFilter
		TargetArchivalBlobSize    dynamicconfig.IntPropertyFnWithDomainFilter
	}
)

// these globals exist as a work around because no primitive exists to pass such objects to workflow code
var (
	globalLogger        bark.Logger
	globalMetricsClient metrics.Client
)

func init() {
	workflow.RegisterWithOptions(ArchiveSystemWorkflow, workflow.RegisterOptions{Name: archiveSystemWorkflowFnName})
	activity.RegisterWithOptions(ArchivalUploadActivity, activity.RegisterOptions{Name: archivalUploadActivityFnName})
	activity.RegisterWithOptions(ArchivalDeleteHistoryActivity, activity.RegisterOptions{Name: archivalDeleteHistoryActivityFnName})
}

// NewSysWorker returns a new SysWorker
func NewSysWorker(container *SysWorkerContainer) SysWorker {
	logger := container.Logger.WithFields(bark.Fields{
		logging.TagWorkflowComponent: logging.TagValueArchiveSystemWorkflowComponent,
	})
	globalLogger = logger
	globalMetricsClient = container.MetricsClient
	actCtx := context.WithValue(context.Background(), sysWorkerContainerKey, container)
	wo := worker.Options{
		BackgroundActivityContext: actCtx,
	}
	return &sysworker{
		worker:      worker.New(container.PublicClient, SystemDomainName, decisionTaskList, wo),
		domainCache: container.DomainCache,
	}
}

// Start the SysWorker
func (w *sysworker) Start() error {
	if err := w.worker.Start(); err != nil {
		w.worker.Stop()
		return err
	}
	return nil
}

// Stop the SysWorker
func (w *sysworker) Stop() {
	w.worker.Stop()
	w.domainCache.Stop()
}
