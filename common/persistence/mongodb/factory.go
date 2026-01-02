package mongodb

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/mongo/options"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/auth"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute"
)

type (
	// Factory vends store objects backed by MongoDB
	Factory struct {
		cfg                 config.MongoDB
		client              client.Client
		logger              log.Logger
		clusterName         string
		metricsHandler      metrics.Handler
		database            client.Database
		topologyInfo        *client.TopologyInfo
		transactionsEnabled bool

		sync.RWMutex
		taskStore          p.TaskStore
		fairTaskStore      p.TaskStore
		shardStore         p.ShardStore
		metadataStore      p.MetadataStore
		executionStore     p.ExecutionStore
		queue              p.Queue
		queueV2            p.QueueV2
		clusterMDStore     p.ClusterMetadataStore
		nexusEndpointStore p.NexusEndpointStore
		visibilityStore    store.VisibilityStore
	}
)

// NewFactory returns an instance of a factory object which can be used to create
// datastores backed by MongoDB
func NewFactory(
	cfg config.MongoDB,
	clusterName string,
	logger log.Logger,
	metricsHandler metrics.Handler,
) (*Factory, error) {
	factory := &Factory{
		cfg:            cfg,
		clusterName:    clusterName,
		logger:         logger,
		metricsHandler: metricsHandler,
	}

	// Initialize MongoDB client
	if err := factory.initialize(); err != nil {
		return nil, fmt.Errorf("failed to initialize MongoDB factory: %w", err)
	}

	return factory, nil
}

// initialize establishes MongoDB connection and detects topology
func (f *Factory) initialize() error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Build connection string
	uri := f.buildConnectionString()

	// Create client options
	clientOpts := options.Client().
		ApplyURI(uri).
		SetConnectTimeout(f.cfg.ConnectTimeout).
		SetServerSelectionTimeout(f.cfg.ConnectTimeout).
		SetHeartbeatInterval(10 * time.Second).
		SetRetryReads(true).
		SetRetryWrites(true)

	if f.cfg.MaxConns > 0 {
		clientOpts.SetMaxPoolSize(uint64(f.cfg.MaxConns))
		if f.cfg.MaxConnecting > 0 {
			maxConnecting := f.cfg.MaxConnecting
			if f.cfg.MaxConns > 0 && maxConnecting > f.cfg.MaxConns {
				maxConnecting = f.cfg.MaxConns
			}
			clientOpts.SetMaxConnecting(uint64(maxConnecting))
		} else {
			clientOpts.SetMaxConnecting(uint64(f.cfg.MaxConns))
		}
	}

	if f.cfg.MinConns > 0 {
		clientOpts.SetMinPoolSize(uint64(f.cfg.MinConns))
	}

	if f.cfg.ConnIdleTime > 0 {
		clientOpts.SetMaxConnIdleTime(f.cfg.ConnIdleTime)
	}

	// Configure TLS if enabled
	if f.cfg.TLS != nil && f.cfg.TLS.Enabled {
		tlsConfig, err := auth.NewTLSConfig(f.cfg.TLS)
		if err != nil {
			return fmt.Errorf("failed to create TLS config: %w", err)
		}
		clientOpts.SetTLSConfig(tlsConfig)
	}

	// Create client
	mongoClient, err := client.NewClient(ctx, uri, clientOpts)
	if err != nil {
		return fmt.Errorf("failed to create MongoDB client: %w", err)
	}

	// Connect and verify
	if err := mongoClient.Connect(ctx); err != nil {
		return fmt.Errorf("failed to connect to MongoDB: %w", err)
	}

	f.client = mongoClient
	f.database = mongoClient.Database(f.cfg.DatabaseName)

	// Detect topology and transaction support
	f.topologyInfo, err = mongoClient.GetTopology(ctx)
	if err != nil {
		return fmt.Errorf("failed to detect MongoDB topology: %w", err)
	}

	if !f.topologyInfo.Type.SupportsTransactions() {
		return fmt.Errorf("mongo topology %q does not support transactions; Temporal requires a replica set or sharded cluster", f.topologyInfo.Type.String())
	}

	f.transactionsEnabled = true
	metrics.PersistenceMongoMaxPoolSize.With(f.metricsHandler).Record(float64(f.cfg.MaxConns))
	metrics.PersistenceMongoSessionsInProgress.With(f.metricsHandler).Record(float64(f.client.NumberSessionsInProgress()))

	f.logger.Info("MongoDB connection established",
		tag.NewStringTag("topology", f.topologyInfo.Type.String()),
		tag.NewBoolTag("transactions-enabled", f.transactionsEnabled),
		tag.NewStringTag("database", f.cfg.DatabaseName),
		tag.NewInt("max-conns", f.cfg.MaxConns),
	)

	return nil
}

// buildConnectionString constructs MongoDB connection URI
func (f *Factory) buildConnectionString() string {
	uri := "mongodb://"

	// Add credentials if provided
	if f.cfg.User != "" && f.cfg.Password != "" {
		uri += fmt.Sprintf("%s:%s@", f.cfg.User, f.cfg.Password)
	}

	// Add hosts
	uri += strings.Join(f.cfg.Hosts, ",")

	// Add database name
	uri += "/" + f.cfg.DatabaseName

	// Add options
	var opts []string

	if f.cfg.ReplicaSet != "" {
		opts = append(opts, fmt.Sprintf("replicaSet=%s", f.cfg.ReplicaSet))
	}
	if f.cfg.AuthSource != "" {
		opts = append(opts, fmt.Sprintf("authSource=%s", f.cfg.AuthSource))
	}

	if len(opts) > 0 {
		uri += "?" + strings.Join(opts, "&")
	}

	return uri
}

// Close closes the factory and underlying connections
func (f *Factory) Close() {
	if f.client != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := f.client.Close(ctx); err != nil {
			f.logger.Error("Failed to close MongoDB client", tag.Error(err))
		}
	}
}

// NewTaskStore returns a new task store
func (f *Factory) NewTaskStore() (p.TaskStore, error) {
	f.Lock()
	defer f.Unlock()

	if f.taskStore != nil {
		return f.taskStore, nil
	}

	taskStore, err := NewTaskStore(
		f.database,
		f.client,
		f.cfg,
		f.logger,
		f.transactionsEnabled,
	)
	if err != nil {
		return nil, err
	}

	f.taskStore = taskStore
	return taskStore, nil
}

// NewFairTaskStore returns a new fair task store
func (f *Factory) NewFairTaskStore() (p.TaskStore, error) {
	f.Lock()
	defer f.Unlock()

	if f.fairTaskStore != nil {
		return f.fairTaskStore, nil
	}

	fairTaskStore, err := NewFairTaskStore(
		f.database,
		f.client,
		f.cfg,
		f.logger,
		f.transactionsEnabled,
	)
	if err != nil {
		return nil, err
	}

	f.fairTaskStore = fairTaskStore
	return fairTaskStore, nil
}

// NewShardStore returns a new shard store
func (f *Factory) NewShardStore() (p.ShardStore, error) {
	f.Lock()
	defer f.Unlock()

	if f.shardStore != nil {
		return f.shardStore, nil
	}

	shardStore, err := NewShardStore(
		f.database,
		f.cfg,
		f.logger,
		f.clusterName,
		f.transactionsEnabled,
	)
	if err != nil {
		return nil, err
	}

	f.shardStore = shardStore
	return shardStore, nil
}

// NewMetadataStore returns a new metadata store
func (f *Factory) NewMetadataStore() (p.MetadataStore, error) {
	f.Lock()
	defer f.Unlock()

	if f.metadataStore != nil {
		return f.metadataStore, nil
	}

	metadataStore, err := NewMetadataStore(
		f.database,
		f.cfg,
		f.logger,
		f.transactionsEnabled,
	)
	if err != nil {
		return nil, err
	}

	f.metadataStore = metadataStore
	return metadataStore, nil
}

// NewExecutionStore returns a new execution store
func (f *Factory) NewExecutionStore() (p.ExecutionStore, error) {
	f.Lock()
	defer f.Unlock()

	if f.executionStore != nil {
		return f.executionStore, nil
	}

	executionStore, err := NewExecutionStore(
		f.database,
		f.client,
		f.cfg,
		f.logger,
		f.metricsHandler,
		f.transactionsEnabled,
	)
	if err != nil {
		return nil, err
	}

	f.executionStore = executionStore
	return executionStore, nil
}

// NewQueue returns a new queue backed by MongoDB
func (f *Factory) NewQueue(queueType p.QueueType) (p.Queue, error) {
	f.Lock()
	defer f.Unlock()

	if f.queue != nil {
		return f.queue, nil
	}

	queueStore, err := NewQueueStore(
		f.database,
		f.client,
		f.cfg,
		f.logger,
		f.metricsHandler,
		f.transactionsEnabled,
		queueType,
	)
	if err != nil {
		return nil, err
	}

	f.queue = queueStore
	return queueStore, nil
}

// NewQueueV2 returns a new QueueV2 backed by MongoDB
func (f *Factory) NewQueueV2() (p.QueueV2, error) {
	f.Lock()
	defer f.Unlock()

	if f.queueV2 != nil {
		return f.queueV2, nil
	}

	queueV2Store, err := NewQueueV2Store(
		f.database,
		f.client,
		f.cfg,
		f.logger,
		f.metricsHandler,
		f.transactionsEnabled,
	)
	if err != nil {
		return nil, err
	}

	f.queueV2 = queueV2Store
	return queueV2Store, nil
}

func (f *Factory) NewVisibilityStore(
	cfg config.CustomDatastoreConfig,
	saProvider searchattribute.Provider,
	saMapperProvider searchattribute.MapperProvider,
	_ namespace.Registry,
	chasmRegistry *chasm.Registry,
	_ resolver.ServiceResolver,
	logger log.Logger,
	_ metrics.Handler,
) (store.VisibilityStore, error) {
	// Do not cache visibility store because different services have different chasm registries
	// and caching would use the registry from the first service that calls this method.
	_ = cfg

	visStore, err := NewVisibilityStore(
		f.database,
		f.client,
		f.cfg,
		logger,
		f.metricsHandler,
		saProvider,
		saMapperProvider,
		chasmRegistry,
		f.transactionsEnabled,
	)
	if err != nil {
		return nil, err
	}

	return visStore, nil
}

// NewClusterMetadataStore returns a new cluster metadata store
func (f *Factory) NewClusterMetadataStore() (p.ClusterMetadataStore, error) {
	f.Lock()
	defer f.Unlock()

	if f.clusterMDStore != nil {
		return f.clusterMDStore, nil
	}

	clusterMetadataStore, err := NewClusterMetadataStore(f.database, f.logger)
	if err != nil {
		return nil, err
	}

	f.clusterMDStore = clusterMetadataStore
	return clusterMetadataStore, nil
}

// NewNexusEndpointStore returns a new nexus endpoint store
func (f *Factory) NewNexusEndpointStore() (p.NexusEndpointStore, error) {
	f.Lock()
	defer f.Unlock()

	if f.nexusEndpointStore != nil {
		return f.nexusEndpointStore, nil
	}

	endpointStore, err := NewNexusEndpointStore(
		f.database,
		f.client,
		f.metricsHandler,
		f.logger,
		f.transactionsEnabled,
	)
	if err != nil {
		return nil, err
	}

	f.nexusEndpointStore = endpointStore
	return endpointStore, nil
}
