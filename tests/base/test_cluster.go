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

package base

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"go.temporal.io/server/tests"
	"os"
	"path"
	"testing"
	"time"

	"github.com/pborman/uuid"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/filestore"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	persistencetests "go.temporal.io/server/common/persistence/persistence-tests"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/mysql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/postgresql"
	"go.temporal.io/server/common/persistence/sql/sqlplugin/sqlite"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
	"go.temporal.io/server/common/pprof"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/rpc/encryption"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/temporal"
	"go.temporal.io/server/tests/testutils"
	"go.uber.org/fx"
	"go.uber.org/multierr"
)

type (
	// TestCluster is a base struct for functional tests
	TestCluster struct {
		testBase     *persistencetests.TestBase
		archiverBase *ArchiverBase
		host         *TemporalImpl
	}

	// ArchiverBase is a base struct for archiver provider being used in functional tests
	ArchiverBase struct {
		metadata                 archiver.ArchivalMetadata
		provider                 provider.ArchiverProvider
		historyStoreDirectory    string
		visibilityStoreDirectory string
		historyURI               string
		visibilityURI            string
	}

	// TestClusterConfig are config for a test cluster
	TestClusterConfig struct {
		FrontendAddress        string
		EnableArchival         bool
		IsMasterCluster        bool
		ClusterNo              int
		ClusterMetadata        cluster.Config
		Persistence            persistencetests.TestBaseOptions
		FrontendConfig         FrontendConfig
		HistoryConfig          HistoryConfig
		MatchingConfig         MatchingConfig
		WorkerConfig           WorkerConfig
		ESConfig               *esclient.Config
		MockAdminClient        map[string]adminservice.AdminServiceClient
		FaultInjection         config.FaultInjection `yaml:"faultInjection"`
		DynamicConfigOverrides map[dynamicconfig.Key]interface{}
		GenerateMTLS           bool
		EnableMetricsCapture   bool
		// ServiceFxOptions can be populated using WithFxOptionsForService.
		ServiceFxOptions map[primitives.ServiceName][]fx.Option
	}
)

type TestClusterFactory interface {
	NewCluster(t *testing.T, options *TestClusterConfig, logger log.Logger) (*TestCluster, error)
}

type defaultTestClusterFactory struct {
	tbFactory PersistenceTestBaseFactory
}

func (f *defaultTestClusterFactory) NewCluster(t *testing.T, options *TestClusterConfig, logger log.Logger) (*TestCluster, error) {
	return NewClusterWithPersistenceTestBaseFactory(t, options, logger, f.tbFactory)
}

func NewTestClusterFactory() TestClusterFactory {
	tbFactory := &defaultPersistenceTestBaseFactory{}
	return NewTestClusterFactoryWithCustomTestBaseFactory(tbFactory)
}

func NewTestClusterFactoryWithCustomTestBaseFactory(tbFactory PersistenceTestBaseFactory) TestClusterFactory {
	return &defaultTestClusterFactory{
		tbFactory: tbFactory,
	}
}

type PersistenceTestBaseFactory interface {
	NewTestBase(options *persistencetests.TestBaseOptions) *persistencetests.TestBase
}

type defaultPersistenceTestBaseFactory struct{}

func (f *defaultPersistenceTestBaseFactory) NewTestBase(options *persistencetests.TestBaseOptions) *persistencetests.TestBase {
	options.StoreType = tests.TestFlags.PersistenceType
	switch tests.TestFlags.PersistenceType {
	case config.StoreTypeSQL:
		var ops *persistencetests.TestBaseOptions
		switch tests.TestFlags.PersistenceDriver {
		case mysql.PluginName:
			ops = persistencetests.GetMySQLTestClusterOption()
		case postgresql.PluginName:
			ops = persistencetests.GetPostgreSQLTestClusterOption()
		case postgresql.PluginNamePGX:
			ops = persistencetests.GetPostgreSQLPGXTestClusterOption()
		case sqlite.PluginName:
			ops = persistencetests.GetSQLiteMemoryTestClusterOption()
		default:
			panic(fmt.Sprintf("unknown sql store driver: %v", tests.TestFlags.PersistenceDriver))
		}
		options.SQLDBPluginName = tests.TestFlags.PersistenceDriver
		options.DBUsername = ops.DBUsername
		options.DBPassword = ops.DBPassword
		options.DBHost = ops.DBHost
		options.DBPort = ops.DBPort
		options.SchemaDir = ops.SchemaDir
		options.ConnectAttributes = ops.ConnectAttributes
	case config.StoreTypeNoSQL:
		// noop for now
	default:
		panic(fmt.Sprintf("unknown store type: %v", options.StoreType))
	}

	return persistencetests.NewTestBase(options)
}

func NewClusterWithPersistenceTestBaseFactory(t *testing.T, options *TestClusterConfig, logger log.Logger, tbFactory PersistenceTestBaseFactory) (*TestCluster, error) {
	clusterMetadataConfig := cluster.NewTestClusterMetadataConfig(
		options.ClusterMetadata.EnableGlobalNamespace,
		options.IsMasterCluster,
	)
	if !options.IsMasterCluster && options.ClusterMetadata.MasterClusterName != "" { // xdc cluster metadata setup
		clusterMetadataConfig = &cluster.Config{
			EnableGlobalNamespace:    options.ClusterMetadata.EnableGlobalNamespace,
			FailoverVersionIncrement: options.ClusterMetadata.FailoverVersionIncrement,
			MasterClusterName:        options.ClusterMetadata.MasterClusterName,
			CurrentClusterName:       options.ClusterMetadata.CurrentClusterName,
			ClusterInformation:       options.ClusterMetadata.ClusterInformation,
		}
	}
	options.Persistence.Logger = logger
	options.Persistence.FaultInjection = &options.FaultInjection

	testBase := tbFactory.NewTestBase(&options.Persistence)

	testBase.Setup(clusterMetadataConfig)
	archiverBase := newArchiverBase(options.EnableArchival, logger)

	pConfig := testBase.DefaultTestCluster.Config()
	pConfig.NumHistoryShards = options.HistoryConfig.NumHistoryShards

	var (
		indexName string
		esClient  esclient.Client
	)
	if !tests.UsingSQLAdvancedVisibility() && options.ESConfig != nil {
		// Randomize index name to avoid cross tests interference.
		for k, v := range options.ESConfig.Indices {
			options.ESConfig.Indices[k] = fmt.Sprintf("%v-%v", v, uuid.New())
		}

		err := setupIndex(options.ESConfig, logger)
		if err != nil {
			return nil, err
		}

		pConfig.VisibilityStore = "test-es-visibility"
		pConfig.DataStores[pConfig.VisibilityStore] = config.DataStore{
			Elasticsearch: options.ESConfig,
		}
		indexName = options.ESConfig.GetVisibilityIndex()
		esClient, err = esclient.NewClient(options.ESConfig, nil, logger)
		if err != nil {
			return nil, err
		}
	} else {
		options.ESConfig = nil
		storeConfig := pConfig.DataStores[pConfig.VisibilityStore]
		if storeConfig.SQL != nil {
			indexName = storeConfig.SQL.DatabaseName
		}
	}

	clusterInfoMap := make(map[string]cluster.ClusterInformation)
	for clusterName, clusterInfo := range clusterMetadataConfig.ClusterInformation {
		clusterInfo.ShardCount = options.HistoryConfig.NumHistoryShards
		clusterInfo.ClusterID = uuid.New()
		clusterInfoMap[clusterName] = clusterInfo
		_, err := testBase.ClusterMetadataManager.SaveClusterMetadata(context.Background(), &persistence.SaveClusterMetadataRequest{
			ClusterMetadata: &persistencespb.ClusterMetadata{
				HistoryShardCount:        options.HistoryConfig.NumHistoryShards,
				ClusterName:              clusterName,
				ClusterId:                clusterInfo.ClusterID,
				IsConnectionEnabled:      clusterInfo.Enabled,
				IsGlobalNamespaceEnabled: clusterMetadataConfig.EnableGlobalNamespace,
				FailoverVersionIncrement: clusterMetadataConfig.FailoverVersionIncrement,
				ClusterAddress:           clusterInfo.RPCAddress,
				HttpAddress:              clusterInfo.HTTPAddress,
				InitialFailoverVersion:   clusterInfo.InitialFailoverVersion,
			}})
		if err != nil {
			return nil, err
		}
	}
	clusterMetadataConfig.ClusterInformation = clusterInfoMap

	// This will save custom test search attributes to cluster metadata.
	// Actual Elasticsearch fields are created in setupIndex.
	err := testBase.SearchAttributesManager.SaveSearchAttributes(
		context.Background(),
		indexName,
		searchattribute.TestNameTypeMap.Custom(),
	)
	if err != nil {
		return nil, err
	}

	var tlsConfigProvider *encryption.FixedTLSConfigProvider
	if options.GenerateMTLS {
		if tlsConfigProvider, err = createFixedTLSConfigProvider(); err != nil {
			return nil, err
		}
	}

	taskCategoryRegistry := temporal.TaskCategoryRegistryProvider(archiverBase.metadata)

	temporalParams := &TemporalParams{
		ClusterMetadataConfig:            clusterMetadataConfig,
		PersistenceConfig:                pConfig,
		MetadataMgr:                      testBase.MetadataManager,
		ClusterMetadataManager:           testBase.ClusterMetadataManager,
		ShardMgr:                         testBase.ShardMgr,
		ExecutionManager:                 testBase.ExecutionManager,
		NamespaceReplicationQueue:        testBase.NamespaceReplicationQueue,
		AbstractDataStoreFactory:         testBase.AbstractDataStoreFactory,
		VisibilityStoreFactory:           testBase.VisibilityStoreFactory,
		TaskMgr:                          testBase.TaskMgr,
		Logger:                           logger,
		ClusterNo:                        options.ClusterNo,
		ESConfig:                         options.ESConfig,
		ESClient:                         esClient,
		ArchiverMetadata:                 archiverBase.metadata,
		ArchiverProvider:                 archiverBase.provider,
		FrontendConfig:                   options.FrontendConfig,
		HistoryConfig:                    options.HistoryConfig,
		MatchingConfig:                   options.MatchingConfig,
		WorkerConfig:                     options.WorkerConfig,
		MockAdminClient:                  options.MockAdminClient,
		NamespaceReplicationTaskExecutor: namespace.NewReplicationTaskExecutor(options.ClusterMetadata.CurrentClusterName, testBase.MetadataManager, logger),
		DynamicConfigOverrides:           options.DynamicConfigOverrides,
		TLSConfigProvider:                tlsConfigProvider,
		ServiceFxOptions:                 options.ServiceFxOptions,
		TaskCategoryRegistry:             taskCategoryRegistry,
	}

	if options.EnableMetricsCapture {
		temporalParams.CaptureMetricsHandler = metricstest.NewCaptureHandler()
	}

	err = newPProfInitializerImpl(logger, PprofTestPort).Start()
	if err != nil {
		logger.Fatal("Failed to start pprof", tag.Error(err))
	}

	cluster := newTemporal(t, temporalParams)
	if err := cluster.Start(); err != nil {
		return nil, err
	}

	return &TestCluster{testBase: testBase, archiverBase: archiverBase, host: cluster}, nil
}

func setupIndex(esConfig *esclient.Config, logger log.Logger) error {
	ctx := context.Background()
	var esClient esclient.IntegrationTestsClient
	op := func() error {
		var err error
		esClient, err = esclient.NewFunctionalTestsClient(esConfig, logger)
		if err != nil {
			return err
		}

		return esClient.Ping(ctx)
	}

	err := backoff.ThrottleRetry(
		op,
		backoff.NewExponentialRetryPolicy(time.Second).WithExpirationInterval(time.Minute),
		nil,
	)
	if err != nil {
		logger.Fatal("Failed to connect to elasticsearch", tag.Error(err))
	}

	exists, err := esClient.IndexExists(ctx, esConfig.GetVisibilityIndex())
	if err != nil {
		return err
	}
	if exists {
		logger.Info("Index already exists.", tag.ESIndex(esConfig.GetVisibilityIndex()))
		err = deleteIndex(esConfig, logger)
		if err != nil {
			return err
		}
	}

	indexTemplateFile := path.Join(testutils.GetRepoRootDirectory(), "schema/elasticsearch/visibility/index_template_v7.json")
	logger.Info("Creating index template.", tag.NewStringTag("templatePath", indexTemplateFile))
	template, err := os.ReadFile(indexTemplateFile)
	if err != nil {
		return err
	}
	// Template name doesn't matter.
	// This operation is idempotent and won't return an error even if template already exists.
	_, err = esClient.IndexPutTemplate(ctx, "temporal_visibility_v1_template", string(template))
	if err != nil {
		return err
	}
	logger.Info("Index template created.")

	logger.Info("Creating index.", tag.ESIndex(esConfig.GetVisibilityIndex()))
	_, err = esClient.CreateIndex(
		ctx,
		esConfig.GetVisibilityIndex(),
		map[string]any{
			"settings": map[string]any{
				"index": map[string]any{
					"number_of_replicas": 0,
				},
			},
		},
	)
	if err != nil {
		return err
	}
	if err := waitForYellowStatus(esClient, esConfig.GetVisibilityIndex()); err != nil {
		return err
	}
	logger.Info("Index created.", tag.ESIndex(esConfig.GetVisibilityIndex()))

	logger.Info("Add custom search attributes for tests.")
	_, err = esClient.PutMapping(ctx, esConfig.GetVisibilityIndex(), searchattribute.TestNameTypeMap.Custom())
	if err != nil {
		return err
	}
	if err := waitForYellowStatus(esClient, esConfig.GetVisibilityIndex()); err != nil {
		return err
	}
	logger.Info("Index setup complete.", tag.ESIndex(esConfig.GetVisibilityIndex()))

	return nil
}

func waitForYellowStatus(esClient esclient.IntegrationTestsClient, index string) error {
	status, err := esClient.WaitForYellowStatus(context.Background(), index)
	if err != nil {
		return err
	}
	if status == "red" {
		return fmt.Errorf("Elasticsearch index status for %s is red", index)
	}
	return nil
}

func deleteIndex(esConfig *esclient.Config, logger log.Logger) error {
	esClient, err := esclient.NewFunctionalTestsClient(esConfig, logger)
	if err != nil {
		return err
	}

	logger.Info("Deleting index.", tag.ESIndex(esConfig.GetVisibilityIndex()))
	_, err = esClient.DeleteIndex(context.Background(), esConfig.GetVisibilityIndex())
	if err != nil {
		return err
	}
	logger.Info("Index deleted.", tag.ESIndex(esConfig.GetVisibilityIndex()))
	return nil
}

func newPProfInitializerImpl(logger log.Logger, port int) *pprof.PProfInitializerImpl {
	return &pprof.PProfInitializerImpl{
		PProf: &config.PProf{
			Port: port,
		},
		Logger: logger,
	}
}

func newArchiverBase(enabled bool, logger log.Logger) *ArchiverBase {
	dcCollection := dynamicconfig.NewNoopCollection()
	if !enabled {
		return &ArchiverBase{
			metadata: archiver.NewArchivalMetadata(dcCollection, "", false, "", false, &config.ArchivalNamespaceDefaults{}),
			provider: provider.NewArchiverProvider(nil, nil),
		}
	}

	historyStoreDirectory, err := os.MkdirTemp("", "test-history-archival")
	if err != nil {
		logger.Fatal("Failed to create temp dir for history archival", tag.Error(err))
	}
	visibilityStoreDirectory, err := os.MkdirTemp("", "test-visibility-archival")
	if err != nil {
		logger.Fatal("Failed to create temp dir for visibility archival", tag.Error(err))
	}
	cfg := &config.FilestoreArchiver{
		FileMode: "0666",
		DirMode:  "0766",
	}
	provider := provider.NewArchiverProvider(
		&config.HistoryArchiverProvider{
			Filestore: cfg,
		},
		&config.VisibilityArchiverProvider{
			Filestore: cfg,
		},
	)
	return &ArchiverBase{
		metadata: archiver.NewArchivalMetadata(dcCollection, "enabled", true, "enabled", true, &config.ArchivalNamespaceDefaults{
			History: config.HistoryArchivalNamespaceDefaults{
				State: "enabled",
				URI:   "testScheme://test/history/archive/path",
			},
			Visibility: config.VisibilityArchivalNamespaceDefaults{
				State: "enabled",
				URI:   "testScheme://test/visibility/archive/path",
			},
		}),
		provider:                 provider,
		historyStoreDirectory:    historyStoreDirectory,
		visibilityStoreDirectory: visibilityStoreDirectory,
		historyURI:               filestore.URIScheme + "://" + historyStoreDirectory,
		visibilityURI:            filestore.URIScheme + "://" + visibilityStoreDirectory,
	}
}

// TearDownCluster tears down the test cluster
func (tc *TestCluster) TearDownCluster() error {
	errs := tc.host.Stop()
	tc.testBase.TearDownWorkflowStore()
	if !tests.UsingSQLAdvancedVisibility() && tc.host.esConfig != nil {
		if err := deleteIndex(tc.host.esConfig, tc.host.logger); err != nil {
			errs = multierr.Combine(errs, err)
		}
	}
	if err := os.RemoveAll(tc.archiverBase.historyStoreDirectory); err != nil {
		errs = multierr.Combine(errs, err)
	}
	if err := os.RemoveAll(tc.archiverBase.visibilityStoreDirectory); err != nil {
		errs = multierr.Combine(errs, err)
	}
	return errs
}

func (tc *TestCluster) GetTestBase() *persistencetests.TestBase {
	return tc.testBase
}

func (tc *TestCluster) ArchivalBase() *ArchiverBase {
	return tc.archiverBase
}

// FrontendClient returns a frontend client from the test cluster
func (tc *TestCluster) FrontendClient() tests.FrontendClient {
	return tc.host.FrontendClient()
}

// AdminClient returns an admin client from the test cluster
func (tc *TestCluster) AdminClient() tests.AdminClient {
	return tc.host.AdminClient()
}

func (tc *TestCluster) OperatorClient() operatorservice.OperatorServiceClient {
	return tc.host.OperatorClient()
}

// HistoryClient returns a history client from the test cluster
func (tc *TestCluster) HistoryClient() historyservice.HistoryServiceClient {
	return tc.host.GetHistoryClient()
}

// MatchingClient returns a matching client from the test cluster
func (tc *TestCluster) MatchingClient() matchingservice.MatchingServiceClient {
	return tc.host.GetMatchingClient()
}

// ExecutionManager returns an execution manager factory from the test cluster
func (tc *TestCluster) ExecutionManager() persistence.ExecutionManager {
	return tc.host.GetExecutionManager()
}

func (tc *TestCluster) Host() *TemporalImpl {
	return tc.host
}

func (tc *TestCluster) OverrideDynamicConfig(t *testing.T, key dynamicconfig.GenericSetting, value any) (cleanup func()) {
	return tc.host.overrideDynamicConfig(t, key.Key(), value)
}

var errCannotAddCACertToPool = errors.New("failed adding CA to pool")

func createFixedTLSConfigProvider() (*encryption.FixedTLSConfigProvider, error) {
	// We use the existing cert generation utilities even though they use slow
	// RSA and use disk unnecessarily
	tempDir, err := os.MkdirTemp("", "")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tempDir)

	certChain, err := testutils.GenerateTestChain(tempDir, TlsCertCommonName)
	if err != nil {
		return nil, err
	}

	// Due to how mTLS is built in the server, we have to reuse the CA for server
	// and client, therefore we might as well reuse the cert too

	tlsCert, err := tls.LoadX509KeyPair(certChain.CertPubFile, certChain.CertKeyFile)
	if err != nil {
		return nil, err
	}
	caCertPool := x509.NewCertPool()
	if caCertBytes, err := os.ReadFile(certChain.CaPubFile); err != nil {
		return nil, err
	} else if !caCertPool.AppendCertsFromPEM(caCertBytes) {
		return nil, errCannotAddCACertToPool
	}

	serverTLSConfig := &tls.Config{
		Certificates: []tls.Certificate{tlsCert},
		ClientCAs:    caCertPool,
		ClientAuth:   tls.RequireAndVerifyClientCert,
	}
	clientTLSConfig := &tls.Config{
		ServerName:   TlsCertCommonName,
		Certificates: []tls.Certificate{tlsCert},
		RootCAs:      caCertPool,
	}

	return &encryption.FixedTLSConfigProvider{
		InternodeServerConfig: serverTLSConfig,
		InternodeClientConfig: clientTLSConfig,
		FrontendServerConfig:  serverTLSConfig,
		FrontendClientConfig:  clientTLSConfig,
	}, nil
}
