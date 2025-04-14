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
		shardStore         persistence.ShardStore
		metadataStore      persistence.MetadataStore
		executionStore     persistence.ExecutionStore
		queue              persistence.Queue
		queueV2            persistence.QueueV2
		clusterMDStore     persistence.ClusterMetadataStore
		nexusEndpointStore persistence.NexusEndpointStore
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
		if storeConfig, ok := d.fiConfig.Targets.DataStores[config.TaskStoreName]; ok && len(storeConfig.Methods) > 0 {
			d.taskStore = newFaultInjectionTaskStore(
				baseStore,
				newStoreFaultGenerator(&storeConfig),
			)
		} else {
			d.taskStore = baseStore
		}
	}
	return d.taskStore, nil
}

func (d *FaultInjectionDataStoreFactory) NewShardStore() (persistence.ShardStore, error) {
	if d.shardStore == nil {
		baseStore, err := d.baseFactory.NewShardStore()
		if err != nil {
			return nil, err
		}
		if storeConfig, ok := d.fiConfig.Targets.DataStores[config.ShardStoreName]; ok && len(storeConfig.Methods) > 0 {
			d.shardStore = newFaultInjectionShardStore(
				baseStore,
				newStoreFaultGenerator(&storeConfig),
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
		if storeConfig, ok := d.fiConfig.Targets.DataStores[config.MetadataStoreName]; ok && len(storeConfig.Methods) > 0 {
			d.metadataStore = newFaultInjectionMetadataStore(
				baseStore,
				newStoreFaultGenerator(&storeConfig),
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
		if storeConfig, ok := d.fiConfig.Targets.DataStores[config.ExecutionStoreName]; ok && len(storeConfig.Methods) > 0 {
			d.executionStore = newFaultInjectionExecutionStore(
				baseStore,
				newStoreFaultGenerator(&storeConfig),
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
		if storeConfig, ok := d.fiConfig.Targets.DataStores[config.QueueName]; ok && len(storeConfig.Methods) > 0 {
			d.queue = newFaultInjectionQueue(
				baseQueue,
				newStoreFaultGenerator(&storeConfig),
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
		if storeConfig, ok := d.fiConfig.Targets.DataStores[config.QueueV2Name]; ok && len(storeConfig.Methods) > 0 {
			d.queueV2 = newFaultInjectionQueueV2(
				baseQueue,
				newStoreFaultGenerator(&storeConfig),
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
		if storeConfig, ok := d.fiConfig.Targets.DataStores[config.ClusterMDStoreName]; ok && len(storeConfig.Methods) > 0 {
			d.clusterMDStore = newFaultInjectionClusterMetadataStore(
				baseStore,
				newStoreFaultGenerator(&storeConfig),
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
		if storeConfig, ok := d.fiConfig.Targets.DataStores[config.NexusEndpointStoreName]; ok && len(storeConfig.Methods) > 0 {
			d.nexusEndpointStore = newFaultInjectionNexusEndpointStore(
				baseStore,
				newStoreFaultGenerator(&storeConfig),
			)
		} else {
			d.nexusEndpointStore = baseStore
		}
	}
	return d.nexusEndpointStore, nil
}
