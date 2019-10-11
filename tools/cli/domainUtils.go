// Copyright (c) 2019 Uber Technologies, Inc.
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

package cli

import (
	"strings"

	"github.com/stretchr/testify/mock"
	"github.com/uber-go/tally"
	"github.com/urfave/cli"

	sericeFrontend "github.com/uber/cadence/.gen/go/cadence/workflowserviceclient"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/archiver"
	"github.com/uber/cadence/common/archiver/provider"
	"github.com/uber/cadence/common/cluster"
	"github.com/uber/cadence/common/domain"
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/loggerimpl"
	"github.com/uber/cadence/common/metrics"
	"github.com/uber/cadence/common/mocks"
	"github.com/uber/cadence/common/persistence"
	persistenceFactory "github.com/uber/cadence/common/persistence/persistence-factory"
	"github.com/uber/cadence/common/service/config"
	"github.com/uber/cadence/common/service/dynamicconfig"
)

const (
	dependencyMaxQPS = 100
)

var (
	registerDomainFlags = []cli.Flag{
		cli.StringFlag{
			Name:  FlagDescriptionWithAlias,
			Usage: "Domain description",
		},
		cli.StringFlag{
			Name:  FlagOwnerEmailWithAlias,
			Usage: "Owner email",
		},
		cli.StringFlag{
			Name:  FlagRetentionDaysWithAlias,
			Usage: "Workflow execution retention in days",
		},
		cli.StringFlag{
			Name:  FlagEmitMetricWithAlias,
			Usage: "Flag to emit metric",
		},
		cli.StringFlag{
			Name:  FlagActiveClusterNameWithAlias,
			Usage: "Active cluster name",
		},
		cli.StringFlag{
			// use StringFlag instead of buggy StringSliceFlag
			// TODO when https://github.com/urfave/cli/pull/392 & v2 is released
			//  consider update urfave/cli
			Name:  FlagClustersWithAlias,
			Usage: "Clusters",
		},
		cli.StringFlag{
			Name:  FlagIsGlobalDomainWithAlias,
			Usage: "Flag to indicate whether domain is a global domain",
		},
		cli.StringFlag{
			Name:  FlagDomainDataWithAlias,
			Usage: "Domain data of key value pairs, in format of k1:v1,k2:v2,k3:v3",
		},
		cli.StringFlag{
			Name:  FlagSecurityTokenWithAlias,
			Usage: "Security token with permission",
		},
		cli.StringFlag{
			Name:  FlagHistoryArchivalStatusWithAlias,
			Usage: "Flag to set history archival status, valid values are \"disabled\" and \"enabled\"",
		},
		cli.StringFlag{
			Name:  FlagHistoryArchivalURIWithAlias,
			Usage: "Optionally specify history archival URI (cannot be changed after first time archival is enabled)",
		},
		cli.StringFlag{
			Name:  FlagVisibilityArchivalStatusWithAlias,
			Usage: "Flag to set visibility archival status, valid values are \"disabled\" and \"enabled\"",
		},
		cli.StringFlag{
			Name:  FlagVisibilityArchivalURIWithAlias,
			Usage: "Optionally specify visibility archival URI (cannot be changed after first time archival is enabled)",
		},
	}

	updateDomainFlags = []cli.Flag{
		cli.StringFlag{
			Name:  FlagDescriptionWithAlias,
			Usage: "Domain description",
		},
		cli.StringFlag{
			Name:  FlagOwnerEmailWithAlias,
			Usage: "Owner email",
		},
		cli.StringFlag{
			Name:  FlagRetentionDaysWithAlias,
			Usage: "Workflow execution retention in days",
		},
		cli.StringFlag{
			Name:  FlagEmitMetricWithAlias,
			Usage: "Flag to emit metric",
		},
		cli.StringFlag{
			Name:  FlagActiveClusterNameWithAlias,
			Usage: "Active cluster name",
		},
		cli.StringFlag{
			// use StringFlag instead of buggy StringSliceFlag
			// TODO when https://github.com/urfave/cli/pull/392 & v2 is released
			//  consider update urfave/cli
			Name:  FlagClustersWithAlias,
			Usage: "Clusters",
		},
		cli.StringFlag{
			Name:  FlagDomainDataWithAlias,
			Usage: "Domain data of key value pairs, in format of k1:v1,k2:v2,k3:v3 ",
		},
		cli.StringFlag{
			Name:  FlagSecurityTokenWithAlias,
			Usage: "Security token with permission ",
		},
		cli.StringFlag{
			Name:  FlagHistoryArchivalStatusWithAlias,
			Usage: "Flag to set history archival status, valid values are \"disabled\" and \"enabled\"",
		},
		cli.StringFlag{
			Name:  FlagHistoryArchivalURIWithAlias,
			Usage: "Optionally specify history archival URI (cannot be changed after first time archival is enabled)",
		},
		cli.StringFlag{
			Name:  FlagVisibilityArchivalStatusWithAlias,
			Usage: "Flag to set visibility archival status, valid values are \"disabled\" and \"enabled\"",
		},
		cli.StringFlag{
			Name:  FlagVisibilityArchivalURIWithAlias,
			Usage: "Optionally specify visibility archival URI (cannot be changed after first time archival is enabled)",
		},
		cli.StringFlag{
			Name:  FlagAddBadBinary,
			Usage: "Binary checksum to add for resetting workflow",
		},
		cli.StringFlag{
			Name:  FlagRemoveBadBinary,
			Usage: "Binary checksum to remove for resetting workflow",
		},
		cli.StringFlag{
			Name:  FlagReason,
			Usage: "Reason for the operation",
		},
	}

	describeDomainFlags = []cli.Flag{
		cli.StringFlag{
			Name:  FlagDomainID,
			Usage: "Domain UUID (required if not specify domainName)",
		},
	}

	adminDomainCommonFlags = []cli.Flag{
		cli.StringFlag{
			Name:  FlagServiceConfigDirWithAlias,
			Usage: "Required service configuration dir",
		},
		cli.StringFlag{
			Name:  FlagServiceEnvWithAlias,
			Usage: "Optional service env for loading service configuration",
		},
		cli.StringFlag{
			Name:  FlagServiceZoneWithAlias,
			Usage: "Optional service zone for loading service configuration",
		},
	}

	adminRegisterDomainFlags = append(
		registerDomainFlags,
		adminDomainCommonFlags...,
	)

	adminUpdateDomainFlags = append(
		updateDomainFlags,
		adminDomainCommonFlags...,
	)

	adminDescribeDomainFlags = append(
		updateDomainFlags,
		adminDomainCommonFlags...,
	)
)

func initializeFrontendClient(
	context *cli.Context,
) sericeFrontend.Interface {
	return cFactory.ServerFrontendClient(context)
}

func initializeAdminDomainHandler(
	context *cli.Context,
) domain.Handler {

	configuration := loadConfig(context)
	metricsClient := initializeMetricsClient()
	logger := initializeLogger(configuration)
	clusterMetadata := initializeClusterMetadata(
		configuration,
		logger,
	)
	metadataMgr := initializeMetadataMgr(
		configuration,
		clusterMetadata,
		metricsClient,
		logger,
	)
	dynamicConfig := initializeDynamicConfig(configuration, logger)
	return initializeDomainHandler(
		logger,
		metadataMgr,
		clusterMetadata,
		initializeArchivalMetadata(configuration, dynamicConfig),
		initializeArchivalProvider(configuration, clusterMetadata, metricsClient, logger),
	)
}

func loadConfig(
	context *cli.Context,
) *config.Config {
	env := getEnvironment(context)
	zone := getZone(context)
	configDir := getConfigDir(context)
	var cfg config.Config
	err := config.Load(env, configDir, zone, &cfg)
	if err != nil {
		ErrorAndExit("Unable to load config.", err)
	}
	return &cfg
}

func initializeDomainHandler(
	logger log.Logger,
	metadataMgr persistence.MetadataManager,
	clusterMetadata cluster.Metadata,
	archivalMetadata archiver.ArchivalMetadata,
	archiverProvider provider.ArchiverProvider,
) domain.Handler {
	return domain.NewHandler(
		domain.MinRetentionDays,
		dynamicconfig.GetIntPropertyFilteredByDomain(domain.MaxBadBinaries),
		logger,
		metadataMgr,
		clusterMetadata,
		initializeDomainReplicator(logger),
		archivalMetadata,
		archiverProvider,
	)
}

func initializeLogger(
	serviceConfig *config.Config,
) log.Logger {
	return loggerimpl.NewLogger(serviceConfig.Log.NewZapLogger())
}

func initializeMetadataMgr(
	serviceConfig *config.Config,
	clusterMetadata cluster.Metadata,
	metricsClient metrics.Client,
	logger log.Logger,
) persistence.MetadataManager {

	pConfig := serviceConfig.Persistence
	pConfig.SetMaxQPS(pConfig.DefaultStore, dependencyMaxQPS)
	pConfig.VisibilityConfig = &config.VisibilityConfig{
		VisibilityListMaxQPS:            dynamicconfig.GetIntPropertyFilteredByDomain(dependencyMaxQPS),
		EnableSampling:                  dynamicconfig.GetBoolPropertyFn(false), // not used by domain operation
		EnableReadFromClosedExecutionV2: dynamicconfig.GetBoolPropertyFn(false), // not used by domain operation
	}
	pFactory := persistenceFactory.New(
		&pConfig,
		clusterMetadata.GetCurrentClusterName(),
		metricsClient,
		logger,
	)
	metadata, err := pFactory.NewMetadataManager()
	if err != nil {
		ErrorAndExit("Unable to initialize metadata manager.", err)
	}
	return metadata
}

func initializeClusterMetadata(
	serviceConfig *config.Config,
	logger log.Logger,
) cluster.Metadata {

	clusterMetadata := serviceConfig.ClusterMetadata
	return cluster.NewMetadata(
		logger,
		dynamicconfig.GetBoolPropertyFn(clusterMetadata.EnableGlobalDomain),
		clusterMetadata.FailoverVersionIncrement,
		clusterMetadata.MasterClusterName,
		clusterMetadata.CurrentClusterName,
		clusterMetadata.ClusterInformation,
		clusterMetadata.ReplicationConsumer,
	)
}

func initializeArchivalMetadata(
	serviceConfig *config.Config,
	dynamicConfig *dynamicconfig.Collection,
) archiver.ArchivalMetadata {

	return archiver.NewArchivalMetadata(
		dynamicConfig,
		serviceConfig.Archival.History.Status,
		serviceConfig.Archival.History.EnableRead,
		serviceConfig.Archival.Visibility.Status,
		serviceConfig.Archival.Visibility.EnableRead,
		&serviceConfig.DomainDefaults.Archival,
	)
}

func initializeArchivalProvider(
	serviceConfig *config.Config,
	clusterMetadata cluster.Metadata,
	metricsClient metrics.Client,
	logger log.Logger,
) provider.ArchiverProvider {

	archiverProvider := provider.NewArchiverProvider(
		serviceConfig.Archival.History.Provider,
		serviceConfig.Archival.Visibility.Provider,
	)

	historyArchiverBootstrapContainer := &archiver.HistoryBootstrapContainer{
		HistoryV2Manager: nil, // not used
		Logger:           logger,
		MetricsClient:    metricsClient,
		ClusterMetadata:  clusterMetadata,
		DomainCache:      nil, // not used
	}
	visibilityArchiverBootstrapContainer := &archiver.VisibilityBootstrapContainer{
		Logger:          logger,
		MetricsClient:   metricsClient,
		ClusterMetadata: clusterMetadata,
		DomainCache:     nil, // not used
	}

	err := archiverProvider.RegisterBootstrapContainer(
		common.FrontendServiceName,
		historyArchiverBootstrapContainer,
		visibilityArchiverBootstrapContainer,
	)
	if err != nil {
		ErrorAndExit("Error initializing archival provider.", err)
	}
	return archiverProvider
}

func initializeDomainReplicator(
	logger log.Logger,
) domain.Replicator {

	replicationMessageSink := &mocks.KafkaProducer{}
	replicationMessageSink.On("Publish", mock.Anything).Return(nil)
	return domain.NewDomainReplicator(replicationMessageSink, logger)
}

func initializeDynamicConfig(
	serviceConfig *config.Config,
	logger log.Logger,
) *dynamicconfig.Collection {

	// the done channel is used by dynamic config to stop refreshing
	// and CLI does not need that, so just close the done channel
	doneChan := make(chan struct{})
	close(doneChan)
	dynamicConfigClient, err := dynamicconfig.NewFileBasedClient(
		&serviceConfig.DynamicConfigClient,
		logger,
		doneChan,
	)
	if err != nil {
		ErrorAndExit("Error initializing dynamic config.", err)
	}
	return dynamicconfig.NewCollection(dynamicConfigClient, logger)
}

func initializeMetricsClient() metrics.Client {
	return metrics.NewClient(tally.NoopScope, metrics.Common)
}

func getEnvironment(c *cli.Context) string {
	return strings.TrimSpace(c.String(FlagServiceEnv))
}

func getZone(c *cli.Context) string {
	return strings.TrimSpace(c.String(FlagServiceZone))
}

func getConfigDir(c *cli.Context) string {
	dirPath := c.String(FlagServiceConfigDir)
	if len(dirPath) == 0 {
		ErrorAndExit("Must provide service configuration dir path.", nil)
	}
	return dirPath
}
