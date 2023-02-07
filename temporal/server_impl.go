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

	"go.uber.org/fx"
	"go.uber.org/multierr"

	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/resource"
)

type (
	// ServerImpl is temporal server.
	ServerImpl struct {
		so               *serverOptions
		servicesMetadata []*ServicesMetadata
		stoppedCh        chan interface{}
		logger           log.Logger
		namespaceLogger  resource.NamespaceLogger

		dcCollection *dynamicconfig.Collection

		persistenceConfig          config.Persistence
		clusterMetadata            *cluster.Config
		persistenceFactoryProvider persistenceClient.FactoryProviderFn
	}
)

var ServerFxImplModule = fx.Options(
	fx.Provide(NewServerFxImpl),
	fx.Provide(func(src *ServerImpl) Server { return src }),
)

// NewServer returns a new instance of server that serves one or many services.
func NewServerFxImpl(
	opts *serverOptions,
	logger log.Logger,
	namespaceLogger resource.NamespaceLogger,
	stoppedCh chan interface{},
	dcCollection *dynamicconfig.Collection,
	servicesGroup ServicesGroupIn,
	persistenceConfig config.Persistence,
	clusterMetadata *cluster.Config,
	persistenceFactoryProvider persistenceClient.FactoryProviderFn,
) *ServerImpl {
	s := &ServerImpl{
		so:                         opts,
		servicesMetadata:           servicesGroup.Services,
		stoppedCh:                  stoppedCh,
		logger:                     logger,
		namespaceLogger:            namespaceLogger,
		dcCollection:               dcCollection,
		persistenceConfig:          persistenceConfig,
		clusterMetadata:            clusterMetadata,
		persistenceFactoryProvider: persistenceFactoryProvider,
	}
	return s
}

// Start temporal server.
// This function should be called only once, Server doesn't support multiple restarts.
func (s *ServerImpl) Start() error {
	s.logger.Info("Starting server for services", tag.Value(s.so.serviceNames))
	s.logger.Debug(s.so.config.String())

	ctx := context.TODO()

	if err := initSystemNamespaces(
		ctx,
		&s.persistenceConfig,
		s.clusterMetadata.CurrentClusterName,
		s.so.persistenceServiceResolver,
		s.persistenceFactoryProvider,
		s.logger,
		s.so.customDataStoreFactory,
	); err != nil {
		return fmt.Errorf("unable to initialize system namespace: %w", err)
	}

	if err := s.startServices(); err != nil {
		return err
	}

	if s.so.blockingStart {
		// If s.so.interruptCh is nil this will wait forever.
		interruptSignal := <-s.so.interruptCh
		s.logger.Info("Received interrupt signal, stopping the server.", tag.Value(interruptSignal))
		return s.Stop()
	}

	return nil
}

// Stop stops the server.
func (s *ServerImpl) Stop() error {
	var wg sync.WaitGroup
	wg.Add(len(s.servicesMetadata))
	close(s.stoppedCh)

	for _, svcMeta := range s.servicesMetadata {
		go func(svc *ServicesMetadata) {
			svc.ServiceStopFn()
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
	serviceStartMaxTimeout := 3 * s.so.config.Global.Membership.MaxJoinDuration

	if serviceStartMaxTimeout < serviceStartTimeout {
		serviceStartMaxTimeout = serviceStartTimeout
	}
	ctx, cancel := context.WithTimeout(context.Background(), serviceStartMaxTimeout)
	defer cancel()
	results := make(chan startServiceResult, len(s.servicesMetadata))
	for _, svcMeta := range s.servicesMetadata {
		go func(svcMeta *ServicesMetadata) {
			err := svcMeta.App.Start(ctx)
			results <- startServiceResult{
				svc: svcMeta,
				err: err,
			}
		}(svcMeta)
	}
	return s.readResults(results)
}

func (s *ServerImpl) readResults(results chan startServiceResult) (err error) {
	for i := 0; i < len(s.servicesMetadata); i++ {
		r := <-results
		if r.err != nil {
			err = multierr.Combine(err, fmt.Errorf("failed to start service %v: %w", r.svc.ServiceName, r.err))
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
) error {
	clusterName := persistenceClient.ClusterName(currentClusterName)
	dataStoreFactory, _ := persistenceClient.DataStoreFactoryProvider(
		clusterName,
		persistenceServiceResolver,
		cfg,
		customDataStoreFactory,
		logger,
		nil,
	)
	factory := persistenceFactoryProvider(persistenceClient.NewFactoryParams{
		DataStoreFactory:           dataStoreFactory,
		Cfg:                        cfg,
		PersistenceMaxQPS:          nil,
		PersistenceNamespaceMaxQPS: nil,
		EnablePriorityRateLimiting: nil,
		ClusterName:                persistenceClient.ClusterName(currentClusterName),
		MetricsHandler:             nil,
		Logger:                     logger,
	})
	defer factory.Close()

	metadataManager, err := factory.NewMetadataManager()
	if err != nil {
		return fmt.Errorf("unable to initialize metadata manager: %w", err)
	}
	defer metadataManager.Close()
	if err = metadataManager.InitializeSystemNamespaces(ctx, currentClusterName); err != nil {
		return fmt.Errorf("unable to register system namespace: %w", err)
	}
	return nil
}
