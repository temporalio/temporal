package cassandra

import (
	"sync"

	"github.com/gocql/gocql"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	p "go.temporal.io/server/common/persistence"
	commongocql "go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/resolver"
)

type (
	// Factory vends datastore implementations backed by cassandra
	Factory struct {
		sync.RWMutex
		cfg         config.Cassandra
		clusterName string
		logger      log.Logger
		session     commongocql.Session
	}
)

// NewFactory returns an instance of a factory object which can be used to create
// data stores that are backed by cassandra
func NewFactory(
	cfg config.Cassandra,
	r resolver.ServiceResolver,
	clusterName string,
	logger log.Logger,
	metricsHandler metrics.Handler,
) *Factory {
	session, err := commongocql.NewSession(
		func() (*gocql.ClusterConfig, error) {
			return commongocql.NewCassandraCluster(cfg, r)
		},
		logger,
		metricsHandler,
	)
	if err != nil {
		logger.Fatal("unable to initialize cassandra session", tag.Error(err))
	}
	return NewFactoryFromSession(cfg, clusterName, logger, session)
}

// NewFactoryFromSession returns an instance of a factory object from the given session.
func NewFactoryFromSession(
	cfg config.Cassandra,
	clusterName string,
	logger log.Logger,
	session commongocql.Session,
) *Factory {
	return &Factory{
		cfg:         cfg,
		clusterName: clusterName,
		logger:      logger,
		session:     session,
	}
}

// NewTaskStore returns a new task store
func (f *Factory) NewTaskStore() (p.TaskStore, error) {
	return NewMatchingTaskStore(f.session, f.logger, false), nil
}

// NewTaskStore returns a new task store
func (f *Factory) NewFairTaskStore() (p.TaskStore, error) {
	return NewMatchingTaskStore(f.session, f.logger, true), nil
}

// NewShardStore returns a new shard store
func (f *Factory) NewShardStore() (p.ShardStore, error) {
	return NewShardStore(f.clusterName, f.session, f.logger), nil
}

// NewMetadataStore returns a metadata store
func (f *Factory) NewMetadataStore() (p.MetadataStore, error) {
	return NewMetadataStore(f.clusterName, f.session, f.logger)
}

// NewClusterMetadataStore returns a metadata store
func (f *Factory) NewClusterMetadataStore() (p.ClusterMetadataStore, error) {
	return NewClusterMetadataStore(f.session, f.logger)
}

// NewExecutionStore returns a new ExecutionStore.
func (f *Factory) NewExecutionStore() (p.ExecutionStore, error) {
	return NewExecutionStore(f.session, f.logger), nil
}

// NewQueue returns a new queue backed by cassandra
func (f *Factory) NewQueue(queueType p.QueueType) (p.Queue, error) {
	return NewQueueStore(queueType, f.session, f.logger)
}

// NewQueueV2 returns a new data-access object for queues and messages stored in Cassandra. It will never return an
// error.
func (f *Factory) NewQueueV2() (p.QueueV2, error) {
	return NewQueueV2Store(f.session, f.logger), nil
}

// NewNexusEndpointStore returns a new NexusEndpointStore
func (f *Factory) NewNexusEndpointStore() (p.NexusEndpointStore, error) {
	return NewNexusEndpointStore(f.session, f.logger), nil
}

// Close closes the factory
func (f *Factory) Close() {
	f.Lock()
	defer f.Unlock()
	f.session.Close()
}
