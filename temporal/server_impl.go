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

package temporal

import (
	"context"
	"fmt"
	"sync"

	"go.uber.org/multierr"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/util"
)

type (
	// ServerImpl is temporal server.
	ServerImpl struct {
		so               *serverOptions
		servicesMetadata []*ServicesMetadata
		stoppedCh        chan interface{}
		logger           log.Logger
		namespaceLogger  resource.NamespaceLogger

		persistenceConfig          config.Persistence
		clusterMetadata            *cluster.Config
		persistenceFactoryProvider persistenceClient.FactoryProviderFn
		metricsHandler             metrics.Handler
	}
)

// NewServerFxImpl returns a new instance of server that serves one or many services.
func NewServerFxImpl(
	opts *serverOptions,
	logger log.Logger,
	namespaceLogger resource.NamespaceLogger,
	stoppedCh chan interface{},
	servicesGroup ServicesGroupIn,
	persistenceConfig config.Persistence,
	clusterMetadata *cluster.Config,
	persistenceFactoryProvider persistenceClient.FactoryProviderFn,
	metricsHandler metrics.Handler,
) *ServerImpl {
	s := &ServerImpl{
		so:                         opts,
		stoppedCh:                  stoppedCh,
		logger:                     logger,
		namespaceLogger:            namespaceLogger,
		persistenceConfig:          persistenceConfig,
		clusterMetadata:            clusterMetadata,
		persistenceFactoryProvider: persistenceFactoryProvider,
		metricsHandler:             metricsHandler,
	}
	for _, svcMeta := range servicesGroup.Services {
		if svcMeta != nil {
			s.servicesMetadata = append(s.servicesMetadata, svcMeta)
		}
	}
	return s
}

func (s *ServerImpl) Start(ctx context.Context) error {
	s.logger.Info("Starting server for services", tag.Value(s.so.serviceNames))
	s.logger.Debug(s.so.config.String())

	if err := initSystemNamespaces(
		ctx,
		&s.persistenceConfig,
		s.clusterMetadata.CurrentClusterName,
		s.so.persistenceServiceResolver,
		s.persistenceFactoryProvider,
		s.logger,
		s.so.customDataStoreFactory,
		s.metricsHandler,
	); err != nil {
		return fmt.Errorf("unable to initialize system namespace: %w", err)
	}

	return s.startServices()
}

func (s *ServerImpl) Stop(ctx context.Context) error {
	var wg sync.WaitGroup
	wg.Add(len(s.servicesMetadata))
	close(s.stoppedCh)

	for _, svcMeta := range s.servicesMetadata {
		go func(svc *ServicesMetadata) {
			svc.Stop(ctx)
			wg.Done()
		}(svcMeta)
	}

	wg.Wait()

	if s.so.metricHandler != nil {
		s.so.metricHandler.Stop(s.logger)
	}
	return nil
}

func (s *ServerImpl) startServices() error {
	// The membership join time may exceed the configured max join duration.
	// Double the service start timeout to make sure there is enough time for start logic.
	timeout := util.Max(serviceStartTimeout, 2*s.so.config.Global.Membership.MaxJoinDuration)
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	results := make(chan startServiceResult, len(s.servicesMetadata))
	for _, svcMeta := range s.servicesMetadata {
		go func(svcMeta *ServicesMetadata) {
			err := svcMeta.app.Start(ctx)
			results <- startServiceResult{
				svc: svcMeta,
				err: err,
			}
		}(svcMeta)
	}
	return s.readResults(results)
}

func (s *ServerImpl) readResults(results chan startServiceResult) (err error) {
	for range s.servicesMetadata {
		r := <-results
		if r.err != nil {
			err = multierr.Combine(err, fmt.Errorf("failed to start service %v: %w", r.svc.serviceName, r.err))
		}
	}
	return
}

type startServiceResult struct {
	svc *ServicesMetadata
	err error
}

func initSystemNamespaces(
	ctx context.Context,
	cfg *config.Persistence,
	currentClusterName string,
	persistenceServiceResolver resolver.ServiceResolver,
	persistenceFactoryProvider persistenceClient.FactoryProviderFn,
	logger log.Logger,
	customDataStoreFactory persistenceClient.AbstractDataStoreFactory,
	metricsHandler metrics.Handler,
) error {
	clusterName := persistenceClient.ClusterName(currentClusterName)
	metricsHandler = metricsHandler.WithTags(metrics.ServiceNameTag(primitives.ServerService))
	dataStoreFactory, _ := persistenceClient.DataStoreFactoryProvider(
		clusterName,
		persistenceServiceResolver,
		cfg,
		customDataStoreFactory,
		logger,
		metricsHandler,
	)
	factory := persistenceFactoryProvider(persistenceClient.NewFactoryParams{
		DataStoreFactory:           dataStoreFactory,
		Cfg:                        cfg,
		PersistenceMaxQPS:          nil,
		PersistenceNamespaceMaxQPS: nil,
		EnablePriorityRateLimiting: nil,
		ClusterName:                persistenceClient.ClusterName(currentClusterName),
		MetricsHandler:             metricsHandler,
		Logger:                     logger,
	})
	defer factory.Close()

	metadataManager, err := factory.NewMetadataManager()
	if err != nil {
		return fmt.Errorf("unable to initialize metadata manager: %w", err)
	}
	defer metadataManager.Close()
	ctx = headers.SetCallerInfo(ctx, headers.SystemBackgroundCallerInfo)
	if err = metadataManager.InitializeSystemNamespaces(ctx, currentClusterName); err != nil {
		return fmt.Errorf("unable to register system namespace: %w", err)
	}
	return nil
}
