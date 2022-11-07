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

package history

import (
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
)

func TestArchivalQueueFactory(t *testing.T) {
	ctrl := gomock.NewController(t)
	namespaceRegistry := namespace.NewMockRegistry(ctrl)
	clusterMetadata := cluster.NewMockMetadata(ctrl)
	cfg := &configs.Config{
		ArchivalProcessorMaxPollHostRPS: func() int {
			return 1
		},
		PersistenceMaxQPS: func() int {
			return 1
		},
		QueueMaxReaderCount: func() int {
			return 1
		},
	}
	timeSource := namespace.NewMockClock(ctrl)
	metricsHandler := metrics.NewMockMetricsHandler(ctrl)
	logger := log.NewNoopLogger()

	clusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName)
	metricsHandler.EXPECT().WithTags(gomock.Any()).Do(func(tags ...metrics.Tag) metrics.MetricsHandler {
		require.Len(t, tags, 1)
		assert.Equal(t, metrics.OperationTagName, tags[0].Key())
		assert.Equal(t, "ArchivalQueueProcessor", tags[0].Value())
		return metricsHandler
	}).Times(2)

	queueFactory := NewArchivalQueueFactory(archivalQueueFactoryParams{
		QueueFactoryBaseParams: QueueFactoryBaseParams{
			NamespaceRegistry: namespaceRegistry,
			ClusterMetadata:   clusterMetadata,
			Config:            cfg,
			TimeSource:        timeSource,
			MetricsHandler:    metricsHandler,
			Logger:            logger,
		},
	})
	shardContext := shard.NewMockContext(ctrl)
	workflowCache := workflow.NewMockCache(ctrl)
	println(queueFactory, shardContext, workflowCache)

	shardContext.EXPECT().GetLogger().Return(logger)
	shardContext.EXPECT().GetQueueState(tasks.CategoryArchival).Return(&persistence.QueueState{
		ReaderStates: nil,
		ExclusiveReaderHighWatermark: &persistence.TaskKey{
			FireTime: timestamp.TimeNowPtrUtc(),
		},
	}, true)
	shardContext.EXPECT().GetTimeSource().Return(timeSource).AnyTimes()

	queue := queueFactory.CreateQueue(shardContext, workflowCache)
	assert.NotNil(t, queue)
}
