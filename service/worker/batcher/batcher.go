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
	"fmt"
	"time"

	"github.com/opentracing/opentracing-go"
	"github.com/uber-go/tally"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/backoff"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/service/dynamicconfig"
	"go.uber.org/cadence/.gen/go/cadence/workflowserviceclient"
	"go.uber.org/cadence/.gen/go/shared"
	"go.uber.org/cadence/worker"
)

const (
	// maximum time waiting for this batcher to start before giving up
	maxStartupTime         = time.Minute * 3
	backOffInitialInterval = time.Second
	backOffMaxInterval     = time.Minute
	rpcTimeout             = time.Second

	// TODO remove this logic after this issue is resolved: https://github.com/uber/cadence/issues/2002
	waitingForGlobalDomainCreationRacingCondition = time.Second * 20
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
	}

	// Batcher is the background sub-system that execute workflow for batch operations
	// It is also the context object that get's passed around within the scanner workflows / activities
	Batcher struct {
		cfg           Config
		svcClient     workflowserviceclient.Interface
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
	}
}

// Start starts the scanner
func (s *Batcher) Start() error {
	//retry until making sure global system domain is there
	err := s.createGlobalSystemDomainIfNotExistsWithRetry()
	if err != nil {
		return err
	}
	time.Sleep(waitingForGlobalDomainCreationRacingCondition)

	// start worker for batch operation workflows
	workerOpts := worker.Options{
		MetricsScope:              s.tallyScope,
		BackgroundActivityContext: context.WithValue(context.Background(), batcherContextKey, s),
		Tracer:                    opentracing.GlobalTracer(),
	}
	worker := worker.New(s.svcClient, common.SystemGlobalDomainName, batcherTaskListName, workerOpts)
	return worker.Start()
}

func (s *Batcher) createGlobalSystemDomainIfNotExistsWithRetry() error {
	policy := backoff.NewExponentialRetryPolicy(backOffInitialInterval)
	policy.SetMaximumInterval(backOffMaxInterval)
	policy.SetExpirationInterval(maxStartupTime)
	return backoff.Retry(func() error {
		return s.createGlobalSystemDomainIfNotExists()
	}, policy, func(err error) bool {
		return true
	})
}

func (s *Batcher) createGlobalSystemDomainIfNotExists() error {
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	_, err := s.svcClient.DescribeDomain(ctx, &shared.DescribeDomainRequest{
		Name: common.StringPtr(common.SystemGlobalDomainName),
	})
	cancel()

	if err == nil {
		s.logger.Info("Global system domain already exists", tag.WorkflowDomainName(common.SystemGlobalDomainName))
		return nil
	}

	if s.cfg.ClusterMetadata.IsGlobalDomainEnabled() && !s.cfg.ClusterMetadata.IsMasterCluster() {
		return fmt.Errorf("not master of the global cluster, retry on describe domain only")
	}

	ctx, cancel = context.WithTimeout(context.Background(), rpcTimeout)
	err = s.svcClient.RegisterDomain(ctx, s.getDomainCreationRequest())
	cancel()
	if err != nil {
		s.logger.Error("Error creating global system domain", tag.Error(err))
		return err
	}
	s.logger.Info("Domain created successfully")
	return nil
}

func (s *Batcher) getDomainCreationRequest() *shared.RegisterDomainRequest {
	req := &shared.RegisterDomainRequest{
		Name:                                   common.StringPtr(common.SystemGlobalDomainName),
		WorkflowExecutionRetentionPeriodInDays: common.Int32Ptr(common.SystemDomainRetentionDays),
		EmitMetric:                             common.BoolPtr(true),
		SecurityToken:                          common.StringPtr(s.cfg.AdminOperationToken()),
		IsGlobalDomain:                         common.BoolPtr(false),
	}

	if s.cfg.ClusterMetadata.IsGlobalDomainEnabled() {
		req.IsGlobalDomain = common.BoolPtr(true)
		req.ActiveClusterName = common.StringPtr(s.cfg.ClusterMetadata.GetMasterClusterName())
		var clusters []*shared.ClusterReplicationConfiguration
		for name, c := range s.cfg.ClusterMetadata.GetAllClusterInfo() {
			if !c.Enabled {
				continue
			}
			clusters = append(clusters, &shared.ClusterReplicationConfiguration{
				ClusterName: common.StringPtr(name),
			})
		}
		req.Clusters = clusters
	}

	return req
}
