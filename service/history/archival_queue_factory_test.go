package history

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestArchivalQueueFactory(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metricsHandler := metrics.NewMockHandler(ctrl)
	metricsHandler.EXPECT().WithTags(gomock.Any()).DoAndReturn(
		func(tags ...metrics.Tag) metrics.Handler {
			require.Len(t, tags, 1)
			assert.Equal(t, metrics.OperationTagName, tags[0].Key)
			assert.Equal(t, "ArchivalQueueProcessor", tags[0].Value)
			return metricsHandler
		},
	).Times(1)
	metricsHandler.EXPECT().WithTags(gomock.Any()).Return(metricsHandler).Times(1)

	mockShard := shard.NewTestContext(
		ctrl,
		&persistencespb.ShardInfo{
			ShardId: 0,
			RangeId: 1,
			QueueStates: map[int32]*persistencespb.QueueState{
				int32(tasks.CategoryIDArchival): {
					ReaderStates: nil,
					ExclusiveReaderHighWatermark: &persistencespb.TaskKey{
						FireTime: timestamp.TimeNowPtrUtc(),
					},
				},
			},
		},
		tests.NewDynamicConfig(),
	)
	mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

	queueFactory := NewArchivalQueueFactory(ArchivalQueueFactoryParams{
		QueueFactoryBaseParams: QueueFactoryBaseParams{
			NamespaceRegistry: mockShard.Resource.GetNamespaceRegistry(),
			ClusterMetadata:   mockShard.Resource.GetClusterMetadata(),
			Config:            tests.NewDynamicConfig(),
			TimeSource:        clock.NewEventTimeSource(),
			MetricsHandler:    metricsHandler,
			Logger:            log.NewNoopLogger(),
			TracerProvider:    noop.NewTracerProvider(),
		},
	})
	queue := queueFactory.CreateQueue(mockShard)

	require.NotNil(t, queue)
	assert.Equal(t, tasks.CategoryArchival, queue.Category())
}
