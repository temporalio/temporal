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

//go:build ignore

package resource

import (
	"fmt"

	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/ringpop"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/sdk"
)

type (
	// BootstrapParams holds the set of parameters
	// needed to bootstrap a service
	BootstrapParams struct {
		Name            string
		InstanceID      string
		ThrottledLogger log.Logger
		NamespaceLogger log.Logger

		MembershipFactoryInitializer membership.MembershipFactoryInitializerFunc
		RPCFactory                   common.RPCFactory
		ClientFactoryProvider        client.FactoryProvider
		PersistenceConfig            config.Persistence
		ClusterMetadataConfig        *cluster.Config
		ReplicatorConfig             config.Replicator
		ServerMetricsReporter        metrics.Reporter
		MetricsClient                metrics.Client
		DCRedirectionPolicy          config.DCRedirectionPolicy
		SdkClientFactory             sdk.ClientFactory
		ArchivalMetadata             archiver.ArchivalMetadata
		ArchiverProvider             provider.ArchiverProvider
	}
)

// Populates parameters for a service
func NewBootstrapParams(
	logger log.Logger,
	namespaceLogger NamespaceLogger,
	cfg *config.Config,
	serviceName ServiceName,
	dc *dynamicconfig.Collection,
	serverReporter ServerReporter,
	tlsConfigProvider encryption.TLSConfigProvider,
	persistenceConfig config.Persistence,
	clusterMetadata *cluster.Config,
	clientFactoryProvider client.FactoryProvider,
) (*BootstrapParams, error) {
	svcName := string(serviceName)
	params := &BootstrapParams{
		Name:                  svcName,
		NamespaceLogger:       namespaceLogger,
		PersistenceConfig:     persistenceConfig,
		ClusterMetadataConfig: clusterMetadata,
		DCRedirectionPolicy:   cfg.DCRedirectionPolicy,
		ClientFactoryProvider: clientFactoryProvider,
	}

	svcCfg := cfg.Services[svcName]
	rpcFactory := rpc.NewFactory(&svcCfg.RPC, svcName, logger, tlsConfigProvider, dc, clusterMetadata)
	params.RPCFactory = rpcFactory

	// Ringpop uses a different port to register handlers, this map is needed to resolve
	// services to correct addresses used by clients through ServiceResolver lookup API
	servicePortMap := make(map[string]int)
	for sn, sc := range cfg.Services {
		servicePortMap[sn] = sc.RPC.GRPCPort
	}

	params.MembershipFactoryInitializer =
		func(persistenceBean persistenceClient.Bean, logger log.Logger) (membership.MembershipMonitorFactory, error) {
			return ringpop.NewRingpopFactory(
				&cfg.Global.Membership,
				rpcFactory.GetRingpopChannel(),
				svcName,
				servicePortMap,
				logger,
				persistenceBean.GetClusterMetadataManager(),
			)
		}

	params.ServerMetricsReporter = serverReporter

	serviceIdx := metrics.GetMetricsServiceIdx(svcName, logger)
	metricsClient, err := serverReporter.NewClient(logger, serviceIdx)
	if err != nil {
		return nil, fmt.Errorf("unable to initialize metrics client: %w", err)
	}

	params.MetricsClient = metricsClient

	tlsFrontendConfig, err := tlsConfigProvider.GetFrontendClientConfig()
	if err != nil {
		return nil, fmt.Errorf("unable to load frontend TLS configuration: %w", err)
	}

	sdkMetricsHandler := sdk.NewMetricHandler(metricsClient.UserScope())
	params.SdkClientFactory = sdk.NewClientFactory(
		cfg.PublicClient.HostPort,
		tlsFrontendConfig,
		sdkMetricsHandler,
	)

	params.ArchivalMetadata = archiver.NewArchivalMetadata(
		dc,
		cfg.Archival.History.State,
		cfg.Archival.History.EnableRead,
		cfg.Archival.Visibility.State,
		cfg.Archival.Visibility.EnableRead,
		&cfg.NamespaceDefaults.Archival,
	)

	params.ArchiverProvider = provider.NewArchiverProvider(cfg.Archival.History.Provider, cfg.Archival.Visibility.Provider)
	params.PersistenceConfig.TransactionSizeLimit = dc.GetIntProperty(dynamicconfig.TransactionSizeLimit, common.DefaultTransactionSizeLimit)

	return params, nil
}
