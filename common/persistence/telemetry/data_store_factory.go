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

package telemetry

import (
	"go.opentelemetry.io/otel/trace"
	"go.temporal.io/server/common/persistence"
)

type (
	TelemetryDataStoreFactory struct {
		baseFactory persistence.DataStoreFactory
		tracer      trace.Tracer

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

func NewTelemetryDataStoreFactory(
	baseFactory persistence.DataStoreFactory,
	tracer trace.Tracer,
) *TelemetryDataStoreFactory {
	return &TelemetryDataStoreFactory{
		baseFactory: baseFactory,
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
		d.taskStore = newTelemetryTaskStore(baseStore, d.tracer)
	}
	return d.taskStore, nil
}

func (d *TelemetryDataStoreFactory) NewShardStore() (persistence.ShardStore, error) {
	if d.shardStore == nil {
		baseStore, err := d.baseFactory.NewShardStore()
		if err != nil {
			return nil, err
		}
		d.shardStore = newTelemetryShardStore(baseStore, d.tracer)
	}
	return d.shardStore, nil
}

func (d *TelemetryDataStoreFactory) NewMetadataStore() (persistence.MetadataStore, error) {
	if d.metadataStore == nil {
		baseStore, err := d.baseFactory.NewMetadataStore()
		if err != nil {
			return nil, err
		}
		d.metadataStore = newTelemetryMetadataStore(baseStore, d.tracer)
	}
	return d.metadataStore, nil
}

func (d *TelemetryDataStoreFactory) NewExecutionStore() (persistence.ExecutionStore, error) {
	if d.executionStore == nil {
		baseStore, err := d.baseFactory.NewExecutionStore()
		if err != nil {
			return nil, err
		}
		d.executionStore = newTelemetryExecutionStore(baseStore, d.tracer)
	}
	return d.executionStore, nil
}

func (d *TelemetryDataStoreFactory) NewQueue(queueType persistence.QueueType) (persistence.Queue, error) {
	if d.queue == nil {
		baseQueue, err := d.baseFactory.NewQueue(queueType)
		if err != nil {
			return baseQueue, err
		}
		d.queue = newTelemetryQueue(baseQueue, d.tracer)
	}
	return d.queue, nil
}

func (d *TelemetryDataStoreFactory) NewQueueV2() (persistence.QueueV2, error) {
	if d.queueV2 == nil {
		baseQueue, err := d.baseFactory.NewQueueV2()
		if err != nil {
			return baseQueue, err
		}
		d.queueV2 = newTelemetryQueueV2(baseQueue, d.tracer)
	}
	return d.queueV2, nil
}

func (d *TelemetryDataStoreFactory) NewClusterMetadataStore() (persistence.ClusterMetadataStore, error) {
	if d.clusterMDStore == nil {
		baseStore, err := d.baseFactory.NewClusterMetadataStore()
		if err != nil {
			return nil, err
		}
		d.clusterMDStore = newTelemetryClusterMetadataStore(baseStore, d.tracer)
	}
	return d.clusterMDStore, nil
}

func (d *TelemetryDataStoreFactory) NewNexusEndpointStore() (persistence.NexusEndpointStore, error) {
	if d.nexusEndpointStore == nil {
		baseStore, err := d.baseFactory.NewNexusEndpointStore()
		if err != nil {
			return nil, err
		}
		d.nexusEndpointStore = newTelemetryNexusEndpointStore(baseStore, d.tracer)
	}
	return d.nexusEndpointStore, nil
}
