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

package intercept

import (
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
)

type (
	InterceptorDataStoreFactory struct {
		baseFactory persistence.DataStoreFactory
		logger      log.Logger
		interceptor PersistenceInterceptor

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

func NewInterceptorDataStoreFactory(
	baseFactory persistence.DataStoreFactory,
	interceptor PersistenceInterceptor,
) *InterceptorDataStoreFactory {
	return &InterceptorDataStoreFactory{
		baseFactory: baseFactory,
		interceptor: interceptor,
	}
}

func (d *InterceptorDataStoreFactory) Close() {
	d.baseFactory.Close()
}

func (d *InterceptorDataStoreFactory) NewTaskStore() (persistence.TaskStore, error) {
	if d.taskStore == nil {
		baseStore, err := d.baseFactory.NewTaskStore()
		if err != nil {
			return nil, err
		}
		d.taskStore = newInterceptorTaskStore(baseStore, d.interceptor)
	}
	return d.taskStore, nil
}

func (d *InterceptorDataStoreFactory) NewShardStore() (persistence.ShardStore, error) {
	if d.shardStore == nil {
		baseStore, err := d.baseFactory.NewShardStore()
		if err != nil {
			return nil, err
		}
		d.shardStore = newInterceptorShardStore(baseStore, d.interceptor)
	}
	return d.shardStore, nil
}

func (d *InterceptorDataStoreFactory) NewMetadataStore() (persistence.MetadataStore, error) {
	if d.metadataStore == nil {
		baseStore, err := d.baseFactory.NewMetadataStore()
		if err != nil {
			return nil, err
		}
		d.metadataStore = newInterceptorMetadataStore(baseStore, d.interceptor)
	}
	return d.metadataStore, nil
}

func (d *InterceptorDataStoreFactory) NewExecutionStore() (persistence.ExecutionStore, error) {
	if d.executionStore == nil {
		baseStore, err := d.baseFactory.NewExecutionStore()
		if err != nil {
			return nil, err
		}
		d.executionStore = newInterceptorExecutionStore(baseStore, d.interceptor)
	}
	return d.executionStore, nil
}

func (d *InterceptorDataStoreFactory) NewQueue(queueType persistence.QueueType) (persistence.Queue, error) {
	if d.queue == nil {
		baseQueue, err := d.baseFactory.NewQueue(queueType)
		if err != nil {
			return baseQueue, err
		}
		d.queue = newInterceptorQueue(baseQueue, d.interceptor)
	}
	return d.queue, nil
}

func (d *InterceptorDataStoreFactory) NewQueueV2() (persistence.QueueV2, error) {
	if d.queueV2 == nil {
		baseQueue, err := d.baseFactory.NewQueueV2()
		if err != nil {
			return baseQueue, err
		}
		d.queueV2 = newInterceptorQueueV2(baseQueue, d.interceptor)
	}
	return d.queueV2, nil
}

func (d *InterceptorDataStoreFactory) NewClusterMetadataStore() (persistence.ClusterMetadataStore, error) {
	if d.clusterMDStore == nil {
		baseStore, err := d.baseFactory.NewClusterMetadataStore()
		if err != nil {
			return nil, err
		}
		d.clusterMDStore = newInterceptorClusterMetadataStore(baseStore, d.interceptor)
	}
	return d.clusterMDStore, nil
}

func (d *InterceptorDataStoreFactory) NewNexusEndpointStore() (persistence.NexusEndpointStore, error) {
	if d.nexusEndpointStore == nil {
		baseStore, err := d.baseFactory.NewNexusEndpointStore()
		if err != nil {
			return nil, err
		}
		d.nexusEndpointStore = newInterceptorNexusEndpointStore(baseStore, d.interceptor)
	}
	return d.nexusEndpointStore, nil
}
