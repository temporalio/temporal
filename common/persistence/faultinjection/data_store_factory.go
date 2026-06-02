package faultinjection

import (
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence"
)

type (
	FaultInjectionDataStoreFactory struct {
		baseFactory persistence.DataStoreFactory
		fiConfig    *config.FaultInjection

		taskStore          persistence.TaskStore
		fairTaskStore      persistence.TaskStore
		shardStore         persistence.ShardStore
		metadataStore      persistence.MetadataStore
		executionStore     persistence.ExecutionStore
		queue              persistence.Queue
		queueV2            persistence.QueueV2
		clusterMDStore     persistence.ClusterMetadataStore
		nexusEndpointStore persistence.NexusEndpointStore
		streamSegments     persistence.StreamSegmentManager
	}
)

func NewFaultInjectionDatastoreFactory(
	fiConfig *config.FaultInjection,
	baseFactory persistence.DataStoreFactory,
) *FaultInjectionDataStoreFactory {
	return &FaultInjectionDataStoreFactory{
		baseFactory: baseFactory,
		fiConfig:    fiConfig,
	}
}

func (d *FaultInjectionDataStoreFactory) Close() {
	d.baseFactory.Close()
}

func (d *FaultInjectionDataStoreFactory) NewTaskStore() (persistence.TaskStore, error) {
	if d.taskStore == nil {
		baseStore, err := d.baseFactory.NewTaskStore()
		if err != nil {
			return nil, err
		}
		if generator, ok := d.storeFaultInjector(config.TaskStoreName); ok {
			d.taskStore = newFaultInjectionTaskStore(
				baseStore,
				generator,
			)
		} else {
			d.taskStore = baseStore
		}
	}
	return d.taskStore, nil
}

func (d *FaultInjectionDataStoreFactory) NewFairTaskStore() (persistence.TaskStore, error) {
	if d.fairTaskStore == nil {
		baseStore, err := d.baseFactory.NewFairTaskStore()
		if err != nil {
			return nil, err
		}
		if generator, ok := d.storeFaultInjector(config.TaskStoreName); ok {
			d.fairTaskStore = newFaultInjectionTaskStore(
				baseStore,
				generator,
			)
		} else {
			d.fairTaskStore = baseStore
		}
	}
	return d.fairTaskStore, nil
}

func (d *FaultInjectionDataStoreFactory) NewShardStore() (persistence.ShardStore, error) {
	if d.shardStore == nil {
		baseStore, err := d.baseFactory.NewShardStore()
		if err != nil {
			return nil, err
		}
		if generator, ok := d.storeFaultInjector(config.ShardStoreName); ok {
			d.shardStore = newFaultInjectionShardStore(
				baseStore,
				generator,
			)
		} else {
			d.shardStore = baseStore
		}
	}
	return d.shardStore, nil
}
func (d *FaultInjectionDataStoreFactory) NewMetadataStore() (persistence.MetadataStore, error) {
	if d.metadataStore == nil {
		baseStore, err := d.baseFactory.NewMetadataStore()
		if err != nil {
			return nil, err
		}
		if generator, ok := d.storeFaultInjector(config.MetadataStoreName); ok {
			d.metadataStore = newFaultInjectionMetadataStore(
				baseStore,
				generator,
			)
		} else {
			d.metadataStore = baseStore
		}
	}
	return d.metadataStore, nil
}

func (d *FaultInjectionDataStoreFactory) NewExecutionStore() (persistence.ExecutionStore, error) {
	if d.executionStore == nil {
		baseStore, err := d.baseFactory.NewExecutionStore()
		if err != nil {
			return nil, err
		}
		if generator, ok := d.storeFaultInjector(config.ExecutionStoreName); ok {
			d.executionStore = newFaultInjectionExecutionStore(
				baseStore,
				generator,
			)
		} else {
			d.executionStore = baseStore
		}
	}
	return d.executionStore, nil
}

func (d *FaultInjectionDataStoreFactory) NewQueue(queueType persistence.QueueType) (persistence.Queue, error) {
	if d.queue == nil {
		baseQueue, err := d.baseFactory.NewQueue(queueType)
		if err != nil {
			return baseQueue, err
		}
		if generator, ok := d.storeFaultInjector(config.QueueName); ok {
			d.queue = newFaultInjectionQueue(
				baseQueue,
				generator,
			)
		} else {
			d.queue = baseQueue
		}
	}
	return d.queue, nil
}

func (d *FaultInjectionDataStoreFactory) NewQueueV2() (persistence.QueueV2, error) {
	if d.queueV2 == nil {
		baseQueue, err := d.baseFactory.NewQueueV2()
		if err != nil {
			return baseQueue, err
		}
		if generator, ok := d.storeFaultInjector(config.QueueV2Name); ok {
			d.queueV2 = newFaultInjectionQueueV2(
				baseQueue,
				generator,
			)
		} else {
			d.queueV2 = baseQueue
		}
	}
	return d.queueV2, nil
}

func (d *FaultInjectionDataStoreFactory) NewClusterMetadataStore() (persistence.ClusterMetadataStore, error) {
	if d.clusterMDStore == nil {
		baseStore, err := d.baseFactory.NewClusterMetadataStore()
		if err != nil {
			return nil, err
		}
		if generator, ok := d.storeFaultInjector(config.ClusterMDStoreName); ok {
			d.clusterMDStore = newFaultInjectionClusterMetadataStore(
				baseStore,
				generator,
			)
		} else {
			d.clusterMDStore = baseStore
		}
	}
	return d.clusterMDStore, nil
}

func (d *FaultInjectionDataStoreFactory) NewNexusEndpointStore() (persistence.NexusEndpointStore, error) {
	if d.nexusEndpointStore == nil {
		baseStore, err := d.baseFactory.NewNexusEndpointStore()
		if err != nil {
			return nil, err
		}
		if generator, ok := d.storeFaultInjector(config.NexusEndpointStoreName); ok {
			d.nexusEndpointStore = newFaultInjectionNexusEndpointStore(
				baseStore,
				generator,
			)
		} else {
			d.nexusEndpointStore = baseStore
		}
	}
	return d.nexusEndpointStore, nil
}

func (d *FaultInjectionDataStoreFactory) NewStreamSegmentManager() (persistence.StreamSegmentManager, error) {
	if d.streamSegments == nil {
		baseManager, err := d.baseFactory.NewStreamSegmentManager()
		if err != nil {
			return nil, err
		}
		d.streamSegments = baseManager
	}
	return d.streamSegments, nil
}

func (d *FaultInjectionDataStoreFactory) storeFaultInjector(storeName config.DataStoreName) (faultGenerator, bool) {
	storeConfig, ok := d.fiConfig.Targets.DataStores[storeName]
	if !ok {
		storeConfig = config.FaultInjectionDataStoreConfig{}
	}
	return newStoreFaultInjector(storeName, &storeConfig, d.fiConfig.Injector)
}
