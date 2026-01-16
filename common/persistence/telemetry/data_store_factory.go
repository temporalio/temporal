package telemetry

import (
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/visibility"
	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/resolver"
	"go.temporal.io/server/common/searchattribute"
)

type (
	TelemetryDataStoreFactory struct {
		baseFactory persistence.DataStoreFactory
		logger      log.Logger
		tracer      trace.Tracer

		taskStore          persistence.TaskStore
		fairTaskStore      persistence.TaskStore
		shardStore         persistence.ShardStore
		metadataStore      persistence.MetadataStore
		executionStore     persistence.ExecutionStore
		queue              persistence.Queue
		queueV2            persistence.QueueV2
		clusterMDStore     persistence.ClusterMetadataStore
		nexusEndpointStore persistence.NexusEndpointStore
		visibilityStore    store.VisibilityStore
	}
)

func NewTelemetryDataStoreFactory(
	baseFactory persistence.DataStoreFactory,
	logger log.Logger,
	tracer trace.Tracer,
) *TelemetryDataStoreFactory {
	return &TelemetryDataStoreFactory{
		baseFactory: baseFactory,
		logger:      logger,
		tracer:      tracer,
	}
}

func (d *TelemetryDataStoreFactory) Close() {
	d.baseFactory.Close()
}

func (d *TelemetryDataStoreFactory) NewTaskStore() (persistence.TaskStore, error) {
	if d.taskStore == nil {
		baseStore, err := d.baseFactory.NewTaskStore()
		if err != nil {
			return nil, err
		}
		d.taskStore = newTelemetryTaskStore(baseStore, d.logger, d.tracer)
	}
	return d.taskStore, nil
}

func (d *TelemetryDataStoreFactory) NewFairTaskStore() (persistence.TaskStore, error) {
	if d.fairTaskStore == nil {
		baseStore, err := d.baseFactory.NewFairTaskStore()
		if err != nil {
			return nil, err
		}
		d.fairTaskStore = newTelemetryTaskStore(baseStore, d.logger, d.tracer)
	}
	return d.fairTaskStore, nil
}

func (d *TelemetryDataStoreFactory) NewShardStore() (persistence.ShardStore, error) {
	if d.shardStore == nil {
		baseStore, err := d.baseFactory.NewShardStore()
		if err != nil {
			return nil, err
		}
		d.shardStore = newTelemetryShardStore(baseStore, d.logger, d.tracer)
	}
	return d.shardStore, nil
}

func (d *TelemetryDataStoreFactory) NewMetadataStore() (persistence.MetadataStore, error) {
	if d.metadataStore == nil {
		baseStore, err := d.baseFactory.NewMetadataStore()
		if err != nil {
			return nil, err
		}
		d.metadataStore = newTelemetryMetadataStore(baseStore, d.logger, d.tracer)
	}
	return d.metadataStore, nil
}

func (d *TelemetryDataStoreFactory) NewExecutionStore() (persistence.ExecutionStore, error) {
	if d.executionStore == nil {
		baseStore, err := d.baseFactory.NewExecutionStore()
		if err != nil {
			return nil, err
		}
		d.executionStore = newTelemetryExecutionStore(baseStore, d.logger, d.tracer)
	}
	return d.executionStore, nil
}

func (d *TelemetryDataStoreFactory) NewQueue(queueType persistence.QueueType) (persistence.Queue, error) {
	if d.queue == nil {
		baseQueue, err := d.baseFactory.NewQueue(queueType)
		if err != nil {
			return baseQueue, err
		}
		d.queue = newTelemetryQueue(baseQueue, d.logger, d.tracer)
	}
	return d.queue, nil
}

func (d *TelemetryDataStoreFactory) NewQueueV2() (persistence.QueueV2, error) {
	if d.queueV2 == nil {
		baseQueue, err := d.baseFactory.NewQueueV2()
		if err != nil {
			return baseQueue, err
		}
		d.queueV2 = newTelemetryQueueV2(baseQueue, d.logger, d.tracer)
	}
	return d.queueV2, nil
}

func (d *TelemetryDataStoreFactory) NewClusterMetadataStore() (persistence.ClusterMetadataStore, error) {
	if d.clusterMDStore == nil {
		baseStore, err := d.baseFactory.NewClusterMetadataStore()
		if err != nil {
			return nil, err
		}
		d.clusterMDStore = newTelemetryClusterMetadataStore(baseStore, d.logger, d.tracer)
	}
	return d.clusterMDStore, nil
}

func (d *TelemetryDataStoreFactory) NewNexusEndpointStore() (persistence.NexusEndpointStore, error) {
	if d.nexusEndpointStore == nil {
		baseStore, err := d.baseFactory.NewNexusEndpointStore()
		if err != nil {
			return nil, err
		}
		d.nexusEndpointStore = newTelemetryNexusEndpointStore(baseStore, d.logger, d.tracer)
	}
	return d.nexusEndpointStore, nil
}

func (d *TelemetryDataStoreFactory) NewVisibilityStore(
	cfg config.CustomDatastoreConfig,
	saProvider searchattribute.Provider,
	saMapperProvider searchattribute.MapperProvider,
	nsRegistry namespace.Registry,
	chasmRegistry *chasm.Registry,
	r resolver.ServiceResolver,
	logger log.Logger,
	metricsHandler metrics.Handler,
) (store.VisibilityStore, error) {
	baseFactory, ok := d.baseFactory.(visibility.VisibilityStoreFactory)
	if !ok {
		return nil, nil
	}
	if d.visibilityStore != nil {
		return d.visibilityStore, nil
	}

	visStore, err := baseFactory.NewVisibilityStore(
		cfg,
		saProvider,
		saMapperProvider,
		nsRegistry,
		chasmRegistry,
		r,
		logger,
		metricsHandler,
	)
	if err != nil {
		return nil, err
	}

	d.visibilityStore = visStore
	return d.visibilityStore, nil
}

var _ visibility.VisibilityStoreFactory = (*TelemetryDataStoreFactory)(nil)
