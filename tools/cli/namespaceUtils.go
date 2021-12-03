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

package cli

import (
	"strings"

	"github.com/golang/mock/gomock"
	"github.com/uber-go/tally/v4"
	"github.com/urfave/cli"
	"go.temporal.io/api/workflowservice/v1"

	"go.temporal.io/server/common/config"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/resolver"
)

const (
	dependencyMaxQPS = 100
)

var (
	registerNamespaceFlags = []cli.Flag{
		cli.StringFlag{
			Name:  FlagDescriptionWithAlias,
			Usage: "Namespace description",
		},
		cli.StringFlag{
			Name:  FlagOwnerEmailWithAlias,
			Usage: "Owner email",
		},
		cli.StringFlag{
			Name:  FlagRetentionWithAlias,
			Usage: "Workflow execution retention",
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
			Name:  FlagIsGlobalNamespaceWithAlias,
			Usage: "Flag to indicate whether namespace is a global namespace",
		},
		cli.StringFlag{
			Name:  FlagNamespaceDataWithAlias,
			Usage: "Namespace data of key value pairs, in format of k1:v1,k2:v2,k3:v3",
		},
		cli.StringFlag{
			Name:  FlagHistoryArchivalStateWithAlias,
			Usage: "Flag to set history archival state, valid values are \"disabled\" and \"enabled\"",
		},
		cli.StringFlag{
			Name:  FlagHistoryArchivalURIWithAlias,
			Usage: "Optionally specify history archival URI (cannot be changed after first time archival is enabled)",
		},
		cli.StringFlag{
			Name:  FlagVisibilityArchivalStateWithAlias,
			Usage: "Flag to set visibility archival state, valid values are \"disabled\" and \"enabled\"",
		},
		cli.StringFlag{
			Name:  FlagVisibilityArchivalURIWithAlias,
			Usage: "Optionally specify visibility archival URI (cannot be changed after first time archival is enabled)",
		},
	}

	updateNamespaceFlags = []cli.Flag{
		cli.StringFlag{
			Name:  FlagDescriptionWithAlias,
			Usage: "Namespace description",
		},
		cli.StringFlag{
			Name:  FlagOwnerEmailWithAlias,
			Usage: "Owner email",
		},
		cli.StringFlag{
			Name:  FlagRetentionWithAlias,
			Usage: "Workflow execution retention",
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
			Name:  FlagNamespaceDataWithAlias,
			Usage: "Namespace data of key value pairs, in format of k1:v1,k2:v2,k3:v3 ",
		},
		cli.StringFlag{
			Name:  FlagHistoryArchivalStateWithAlias,
			Usage: "Flag to set history archival state, valid values are \"disabled\" and \"enabled\"",
		},
		cli.StringFlag{
			Name:  FlagHistoryArchivalURIWithAlias,
			Usage: "Optionally specify history archival URI (cannot be changed after first time archival is enabled)",
		},
		cli.StringFlag{
			Name:  FlagVisibilityArchivalStateWithAlias,
			Usage: "Flag to set visibility archival state, valid values are \"disabled\" and \"enabled\"",
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
		cli.BoolFlag{
			Name:  FlagPromoteNamespaceWithAlias,
			Usage: "Promote local namespace to global namespace",
		},
	}

	describeNamespaceFlags = []cli.Flag{
		cli.StringFlag{
			Name:  FlagNamespaceID,
			Usage: "Namespace Id (required if not specify namespace)",
		},
	}

	listNamespacesFlags = []cli.Flag{}

	adminNamespaceCommonFlags = []cli.Flag{
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

	adminUpdateNamespaceFlags = append(
		updateNamespaceFlags,
		adminNamespaceCommonFlags...,
	)

	adminDescribeNamespaceFlags = append(
		updateNamespaceFlags,
		adminNamespaceCommonFlags...,
	)
)

func initializeFrontendClient(
	context *cli.Context,
) workflowservice.WorkflowServiceClient {
	return cFactory.FrontendClient(context)
}

func initializeAdminNamespaceHandler(
	context *cli.Context,
) namespace.Handler {

	configuration := loadConfig(context)
	metricsClient := initializeMetricsClient()
	logger := log.NewZapLogger(log.BuildZapLogger(configuration.Log))

	factory := initializePersistenceFactory(
		configuration,
		metricsClient,
		logger,
	)

	metadataMgr, err := factory.NewMetadataManager()
	if err != nil {
		ErrorAndExit("Unable to initialize metadata manager.", err)
	}

	clusterMetadata := initializeClusterMetadata(configuration)

	dynamicConfig := initializeDynamicConfig(configuration, logger)
	return initializeNamespaceHandler(
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

func initializeNamespaceHandler(
	logger log.Logger,
	metadataMgr persistence.MetadataManager,
	clusterMetadata cluster.Metadata,
	archivalMetadata archiver.ArchivalMetadata,
	archiverProvider provider.ArchiverProvider,
) namespace.Handler {
	return namespace.NewHandler(
		dynamicconfig.GetIntPropertyFilteredByNamespace(namespace.MaxBadBinaries),
		logger,
		metadataMgr,
		clusterMetadata,
		initializeNamespaceReplicator(logger),
		archivalMetadata,
		archiverProvider,
	)
}

func initializePersistenceFactory(
	serviceConfig *config.Config,
	metricsClient metrics.Client,
	logger log.Logger,
) client.Factory {

	pConfig := serviceConfig.Persistence
	pFactory := client.NewFactory(
		&pConfig,
		resolver.NewNoopResolver(),
		dynamicconfig.GetIntPropertyFn(dependencyMaxQPS),
		nil, // TODO propagate abstract datastore factory from the CLI.
		"",
		metricsClient,
		logger,
	)
	return pFactory
}

func initializeClusterMetadata(
	serviceConfig *config.Config,
) cluster.Metadata {

	clusterMetadata := serviceConfig.ClusterMetadata
	// Initialize static metadata without cluster metadata store
	return cluster.NewMetadata(
		clusterMetadata.EnableGlobalNamespace,
		clusterMetadata.FailoverVersionIncrement,
		clusterMetadata.MasterClusterName,
		clusterMetadata.CurrentClusterName,
		clusterMetadata.ClusterInformation,
		nil,
		log.NewNoopLogger(),
	)
}

func initializeArchivalMetadata(
	serviceConfig *config.Config,
	dynamicConfig *dynamicconfig.Collection,
) archiver.ArchivalMetadata {

	return archiver.NewArchivalMetadata(
		dynamicConfig,
		serviceConfig.Archival.History.State,
		serviceConfig.Archival.History.EnableRead,
		serviceConfig.Archival.Visibility.State,
		serviceConfig.Archival.Visibility.EnableRead,
		&serviceConfig.NamespaceDefaults.Archival,
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
		ExecutionManager: nil, // not used
		Logger:           logger,
		MetricsClient:    metricsClient,
		ClusterMetadata:  clusterMetadata,
	}
	visibilityArchiverBootstrapContainer := &archiver.VisibilityBootstrapContainer{
		Logger:          logger,
		MetricsClient:   metricsClient,
		ClusterMetadata: clusterMetadata,
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

func initializeNamespaceReplicator(
	logger log.Logger,
) namespace.Replicator {

	namespaceReplicationQueue := &persistence.MockNamespaceReplicationQueue{}
	namespaceReplicationQueue.EXPECT().Publish(gomock.Any()).Return(nil)
	return namespace.NewNamespaceReplicator(namespaceReplicationQueue, logger)
}

func initializeDynamicConfig(
	serviceConfig *config.Config,
	logger log.Logger,
) *dynamicconfig.Collection {

	// the done channel is used by dynamic config to stop refreshing
	// and CLI does not need that, so just close the done channel
	doneChan := make(chan interface{})
	close(doneChan)
	dynamicConfigClient, err := dynamicconfig.NewFileBasedClient(
		serviceConfig.DynamicConfigClient,
		logger,
		doneChan,
	)
	if err != nil {
		ErrorAndExit("Error initializing dynamic config.", err)
	}
	return dynamicconfig.NewCollection(dynamicConfigClient, logger)
}

func initializeMetricsClient() metrics.Client {
	return metrics.NewClient(&metrics.ClientConfig{}, tally.NoopScope, metrics.Common)
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
