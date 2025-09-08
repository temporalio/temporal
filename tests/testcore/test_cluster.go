package testcore

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path"
	"testing"
	"time"

	"github.com/pborman/uuid"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.temporal.io/api/operatorservice/v1"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/archiver/filestore"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/membership/static"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace/nsreplication"
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
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/freeport"
	"go.temporal.io/server/temporal"
	"go.temporal.io/server/temporal/environment"
	"go.temporal.io/server/tests/testutils"
	"go.uber.org/fx"
	"go.uber.org/multierr"
)

type (
	transferProtocol string

	// TestCluster is a testcore struct for functional tests
	TestCluster struct {
		testBase     *persistencetests.TestBase
		archiverBase *ArchiverBase
		host         *TemporalImpl
	}

	// ArchiverBase is a testcore struct for archiver provider being used in functional tests
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
		EnableArchival         bool
		IsMasterCluster        bool
		ClusterMetadata        cluster.Config
		Persistence            persistencetests.TestBaseOptions
		FrontendConfig         FrontendConfig
		HistoryConfig          HistoryConfig
		MatchingConfig         MatchingConfig
		WorkerConfig           WorkerConfig
		ESConfig               *esclient.Config
		MockAdminClient        map[string]adminservice.AdminServiceClient
		FaultInjection         *config.FaultInjection
		DynamicConfigOverrides map[dynamicconfig.Key]any
		EnableMTLS             bool
		EnableMetricsCapture   bool
		SpanExporters          map[telemetry.SpanExporterType]sdktrace.SpanExporter
		// ServiceFxOptions can be populated using WithFxOptionsForService.
		ServiceFxOptions map[primitives.ServiceName][]fx.Option
	}

	TestClusterFactory interface {
		NewCluster(t *testing.T, clusterConfig *TestClusterConfig, logger log.Logger) (*TestCluster, error)
	}

	defaultTestClusterFactory struct {
		tbFactory PersistenceTestBaseFactory
	}
)

const (
	httpProtocol transferProtocol = "http"
	grpcProtocol transferProtocol = "grpc"
)

func (a *ArchiverBase) Metadata() archiver.ArchivalMetadata {
	return a.metadata
}

func (a *ArchiverBase) Provider() provider.ArchiverProvider {
	return a.provider
}

func (a *ArchiverBase) HistoryURI() string {
	return a.historyURI
}

func (a *ArchiverBase) VisibilityURI() string {
	return a.visibilityURI
}

func (f *defaultTestClusterFactory) NewCluster(t *testing.T, clusterConfig *TestClusterConfig, logger log.Logger) (*TestCluster, error) {
	return newClusterWithPersistenceTestBaseFactory(t, clusterConfig, logger, f.tbFactory)
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
	options.StoreType = cliFlags.persistenceType
	switch cliFlags.persistenceType {
	case config.StoreTypeSQL:
		var ops *persistencetests.TestBaseOptions
		switch cliFlags.persistenceDriver {
		case mysql.PluginName:
			ops = persistencetests.GetMySQLTestClusterOption()
		case postgresql.PluginName:
			ops = persistencetests.GetPostgreSQLTestClusterOption()
		case postgresql.PluginNamePGX:
			ops = persistencetests.GetPostgreSQLPGXTestClusterOption()
		case sqlite.PluginName:
			ops = persistencetests.GetSQLiteMemoryTestClusterOption()
		default:
			//nolint:forbidigo // test code
			panic(fmt.Sprintf("unknown sql store driver: %v", cliFlags.persistenceDriver))
		}
		options.SQLDBPluginName = cliFlags.persistenceDriver
		options.DBUsername = ops.DBUsername
		options.DBPassword = ops.DBPassword
		options.DBHost = ops.DBHost
		options.DBPort = ops.DBPort
		options.SchemaDir = ops.SchemaDir
		options.ConnectAttributes = ops.ConnectAttributes
	case config.StoreTypeNoSQL:
		// noop for now
	default:
		//nolint:forbidigo // test code
		panic(fmt.Sprintf("unknown store type: %v", options.StoreType))
	}

	if cliFlags.enableFaultInjection != "" && options.FaultInjection == nil {
		// If -enableFaultInjection is passed to the test runner, then default fault injection config is added to the persistence options.
		// If FaultInjectionConfig is already set by test, then it means that this test requires
		// a specific fault injection configuration that takes precedence over a default one.
		options.FaultInjection = config.DefaultFaultInjection()
	}

	return persistencetests.NewTestBase(options)
}

func newClusterWithPersistenceTestBaseFactory(t *testing.T, clusterConfig *TestClusterConfig, logger log.Logger, tbFactory PersistenceTestBaseFactory) (*TestCluster, error) {
	// determine number of hosts per service
	const minNodes = 1
	clusterConfig.FrontendConfig.NumFrontendHosts = max(minNodes, clusterConfig.FrontendConfig.NumFrontendHosts)
	clusterConfig.HistoryConfig.NumHistoryHosts = max(minNodes, clusterConfig.HistoryConfig.NumHistoryHosts)
	clusterConfig.MatchingConfig.NumMatchingHosts = max(minNodes, clusterConfig.MatchingConfig.NumMatchingHosts)
	clusterConfig.WorkerConfig.NumWorkers = max(minNodes, clusterConfig.WorkerConfig.NumWorkers)
	if clusterConfig.WorkerConfig.DisableWorker {
		clusterConfig.WorkerConfig.NumWorkers = 0
	}

	// allocate ports
	hostsByProtocolByService := map[transferProtocol]map[primitives.ServiceName]static.Hosts{
		grpcProtocol: {
			primitives.FrontendService: {All: makeAddresses(clusterConfig.FrontendConfig.NumFrontendHosts)},
			primitives.MatchingService: {All: makeAddresses(clusterConfig.MatchingConfig.NumMatchingHosts)},
			primitives.HistoryService:  {All: makeAddresses(clusterConfig.HistoryConfig.NumHistoryHosts)},
			primitives.WorkerService:   {All: makeAddresses(clusterConfig.WorkerConfig.NumWorkers)},
		},
		httpProtocol: {
			primitives.FrontendService: {All: makeAddresses(clusterConfig.FrontendConfig.NumFrontendHosts)},
		},
	}

	if len(clusterConfig.ClusterMetadata.ClusterInformation) > 0 {
		// set self-address for current cluster
		ci := clusterConfig.ClusterMetadata.ClusterInformation[clusterConfig.ClusterMetadata.CurrentClusterName]
		ci.RPCAddress = hostsByProtocolByService[grpcProtocol][primitives.FrontendService].All[0]
		ci.HTTPAddress = hostsByProtocolByService[httpProtocol][primitives.FrontendService].All[0]
		clusterConfig.ClusterMetadata.ClusterInformation[clusterConfig.ClusterMetadata.CurrentClusterName] = ci
	}

	clusterMetadataConfig := cluster.NewTestClusterMetadataConfig(
		clusterConfig.ClusterMetadata.EnableGlobalNamespace,
		clusterConfig.IsMasterCluster,
	)
	if !clusterConfig.IsMasterCluster && clusterConfig.ClusterMetadata.MasterClusterName != "" { // xdc cluster metadata setup
		clusterMetadataConfig = &cluster.Config{
			EnableGlobalNamespace:    clusterConfig.ClusterMetadata.EnableGlobalNamespace,
			FailoverVersionIncrement: clusterConfig.ClusterMetadata.FailoverVersionIncrement,
			MasterClusterName:        clusterConfig.ClusterMetadata.MasterClusterName,
			CurrentClusterName:       clusterConfig.ClusterMetadata.CurrentClusterName,
			ClusterInformation:       clusterConfig.ClusterMetadata.ClusterInformation,
		}
	}
	clusterConfig.Persistence.Logger = logger
	clusterConfig.Persistence.FaultInjection = clusterConfig.FaultInjection

	testBase := tbFactory.NewTestBase(&clusterConfig.Persistence)

	testBase.Setup(clusterMetadataConfig)
	archiverBase := newArchiverBase(clusterConfig.EnableArchival, testBase.ExecutionManager, logger)

	pConfig := testBase.DefaultTestCluster.Config()
	pConfig.NumHistoryShards = clusterConfig.HistoryConfig.NumHistoryShards

	var (
		indexName string
		esClient  esclient.Client
	)
	if !UseSQLVisibility() {
		clusterConfig.ESConfig = &esclient.Config{
			Indices: map[string]string{
				esclient.VisibilityAppName: RandomizeStr("temporal_visibility_v1_test"),
			},
			URL: url.URL{
				Host:   fmt.Sprintf("%s:%d", environment.GetESAddress(), environment.GetESPort()),
				Scheme: "http",
			},
			Version: environment.GetESVersion(),
		}

		err := setupIndex(clusterConfig.ESConfig, logger)
		if err != nil {
			return nil, err
		}

		pConfig.VisibilityStore = "test-es-visibility"
		pConfig.DataStores[pConfig.VisibilityStore] = config.DataStore{
			Elasticsearch: clusterConfig.ESConfig,
		}
		indexName = clusterConfig.ESConfig.GetVisibilityIndex()
		esClient, err = esclient.NewClient(clusterConfig.ESConfig, nil, logger)
		if err != nil {
			return nil, err
		}
	} else {
		clusterConfig.ESConfig = nil
		storeConfig := pConfig.DataStores[pConfig.VisibilityStore]
		if storeConfig.SQL != nil {
			indexName = storeConfig.SQL.DatabaseName
		}
	}

	clusterInfoMap := make(map[string]cluster.ClusterInformation)
	for clusterName, clusterInfo := range clusterMetadataConfig.ClusterInformation {
		clusterInfo.ShardCount = clusterConfig.HistoryConfig.NumHistoryShards
		clusterInfo.ClusterID = uuid.New()
		clusterInfoMap[clusterName] = clusterInfo
		_, err := testBase.ClusterMetadataManager.SaveClusterMetadata(context.Background(), &persistence.SaveClusterMetadataRequest{
			ClusterMetadata: &persistencespb.ClusterMetadata{
				HistoryShardCount:        clusterConfig.HistoryConfig.NumHistoryShards,
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
	if clusterConfig.EnableMTLS {
		if tlsConfigProvider, err = createFixedTLSConfigProvider(); err != nil {
			return nil, err
		}
	}

	chasmRegistry := chasm.NewRegistry(logger)
	if err := chasmRegistry.Register(&chasm.CoreLibrary{}); err != nil {
		return nil, err
	}

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
		ESConfig:                         clusterConfig.ESConfig,
		ESClient:                         esClient,
		ArchiverMetadata:                 archiverBase.metadata,
		ArchiverProvider:                 archiverBase.provider,
		FrontendConfig:                   clusterConfig.FrontendConfig,
		HistoryConfig:                    clusterConfig.HistoryConfig,
		MatchingConfig:                   clusterConfig.MatchingConfig,
		WorkerConfig:                     clusterConfig.WorkerConfig,
		MockAdminClient:                  clusterConfig.MockAdminClient,
		NamespaceReplicationTaskExecutor: nsreplication.NewTaskExecutor(clusterConfig.ClusterMetadata.CurrentClusterName, testBase.MetadataManager, logger),
		DynamicConfigOverrides:           clusterConfig.DynamicConfigOverrides,
		TLSConfigProvider:                tlsConfigProvider,
		ServiceFxOptions:                 clusterConfig.ServiceFxOptions,
		TaskCategoryRegistry:             temporal.TaskCategoryRegistryProvider(archiverBase.metadata),
		ChasmRegistry:                    chasmRegistry,
		HostsByProtocolByService:         hostsByProtocolByService,
		SpanExporters:                    clusterConfig.SpanExporters,
	}

	if clusterConfig.EnableMetricsCapture {
		temporalParams.CaptureMetricsHandler = metricstest.NewCaptureHandler()
	}

	err = newPProfInitializerImpl(logger, PprofTestPort).Start()
	if err != nil {
		logger.Fatal("Failed to start pprof", tag.Error(err))
	}

	cluster := newTemporal(t, temporalParams)
	if err = cluster.Start(); err != nil {
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

func newArchiverBase(enabled bool, executionManager persistence.ExecutionManager, logger log.Logger) *ArchiverBase {
	dcCollection := dynamicconfig.NewNoopCollection()
	if !enabled {
		return &ArchiverBase{
			metadata: archiver.NewArchivalMetadata(dcCollection, "", false, "", false, &config.ArchivalNamespaceDefaults{}),
			provider: provider.NewArchiverProvider(nil, nil, nil, logger, metrics.NoopMetricsHandler),
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
		executionManager,
		logger,
		metrics.NoopMetricsHandler,
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
	if !UseSQLVisibility() && tc.host.esConfig != nil {
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

// TODO (alex): remove this method. Replace usages with concrete methods.
func (tc *TestCluster) TestBase() *persistencetests.TestBase {
	return tc.testBase
}

func (tc *TestCluster) ArchiverBase() *ArchiverBase {
	return tc.archiverBase
}

func (tc *TestCluster) FrontendClient() workflowservice.WorkflowServiceClient {
	return tc.host.FrontendClient()
}

func (tc *TestCluster) AdminClient() adminservice.AdminServiceClient {
	return tc.host.AdminClient()
}

func (tc *TestCluster) OperatorClient() operatorservice.OperatorServiceClient {
	return tc.host.OperatorClient()
}

// HistoryClient returns a history client from the test cluster
func (tc *TestCluster) HistoryClient() historyservice.HistoryServiceClient {
	return tc.host.HistoryClient()
}

// MatchingClient returns a matching client from the test cluster
func (tc *TestCluster) MatchingClient() matchingservice.MatchingServiceClient {
	return tc.host.MatchingClient()
}

// ExecutionManager returns an execution manager factory from the test cluster
func (tc *TestCluster) ExecutionManager() persistence.ExecutionManager {
	return tc.host.GetExecutionManager()
}

// TODO (alex): expose only needed objects from TemporalImpl.
func (tc *TestCluster) Host() *TemporalImpl {
	return tc.host
}

func (tc *TestCluster) ClusterName() string {
	return tc.host.clusterMetadataConfig.CurrentClusterName
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

func makeAddresses(count int) []string {
	hosts := make([]string, count)
	for i := range hosts {
		hosts[i] = fmt.Sprintf("127.0.0.1:%d", freeport.MustGetFreePort())
	}
	return hosts
}
