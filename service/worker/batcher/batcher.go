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

package batcher

import (
	"context"

	"github.com/opentracing/opentracing-go"
	"github.com/uber-go/tally"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/worker"

	"github.com/uber/cadence/client"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

type (
	// Config defines the configuration for batcher
	Config struct {
		AdminOperationToken dynamicconfig.StringPropertyFn
		// ClusterMetadata contains the metadata for this cluster
		ClusterMetadata cluster.Metadata
	}

	// BootstrapParams contains the set of params needed to bootstrap
	// the batcher sub-system
	BootstrapParams struct {
		// Config contains the configuration for scanner
		Config Config
		// ServiceClient is an instance of cadence service client
		ServiceClient workflowserviceclient.Interface
		// MetricsClient is an instance of metrics object for emitting stats
		MetricsClient metrics.Client
		Logger        log.Logger
		// TallyScope is an instance of tally metrics scope
		TallyScope tally.Scope
		// ClientBean is an instance of client.Bean for a collection of clients
		ClientBean client.Bean
	}

	// Batcher is the background sub-system that execute workflow for batch operations
	// It is also the context object that get's passed around within the scanner workflows / activities
	Batcher struct {
		cfg           Config
		svcClient     workflowserviceclient.Interface
		clientBean    client.Bean
		metricsClient metrics.Client
		tallyScope    tally.Scope
		logger        log.Logger
	}
)

// New returns a new instance of batcher daemon Batcher
func New(params *BootstrapParams) *Batcher {
	cfg := params.Config
	return &Batcher{
		cfg:           cfg,
		svcClient:     params.ServiceClient,
		metricsClient: params.MetricsClient,
		tallyScope:    params.TallyScope,
		logger:        params.Logger.WithTags(tag.ComponentBatcher),
		clientBean:    params.ClientBean,
	}
}

// Start starts the scanner
func (s *Batcher) Start() error {
	// start worker for batch operation workflows
	ctx := context.WithValue(context.Background(), batcherContextKey, s)
	workerOpts := worker.Options{
		MetricsScope:              s.tallyScope,
		BackgroundActivityContext: ctx,
		Tracer:                    opentracing.GlobalTracer(),
	}
	batchWorker := worker.New(s.svcClient, common.SystemLocalDomainName, BatcherTaskListName, workerOpts)
	return batchWorker.Start()
}
