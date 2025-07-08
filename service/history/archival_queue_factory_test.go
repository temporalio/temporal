package history

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/service/history/queues"
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

func TestArchivalQueueFactory_RateLimiter(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("enabled=%v", enabled), func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			cfg := tests.NewDynamicConfig()
			if enabled {
				cfg.TaskSchedulerEnableRateLimiter = dynamicconfig.GetBoolPropertyFn(true)
			}

			shardCtx := shard.NewTestContext(
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
				cfg,
			)
			shardCtx.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return(cluster.TestCurrentClusterName).AnyTimes()

			rl := quotas.NewRequestRateLimiterAdapter(quotas.NewRateLimiter(1, 1))

			factory := NewArchivalQueueFactory(ArchivalQueueFactoryParams{
				QueueFactoryBaseParams: QueueFactoryBaseParams{
					NamespaceRegistry:    shardCtx.Resource.GetNamespaceRegistry(),
					ClusterMetadata:      shardCtx.Resource.GetClusterMetadata(),
					Config:               cfg,
					TimeSource:           clock.NewEventTimeSource(),
					MetricsHandler:       metrics.NoopMetricsHandler,
					Logger:               log.NewNoopLogger(),
					TracerProvider:       noop.NewTracerProvider(),
					SchedulerRateLimiter: rl,
				},
			})

			q := factory.CreateQueue(shardCtx)
			require.NotNil(t, q)

			rateLimiter, shadow := getSchedulerInfo(q)
			if enabled {
				assert.NotEqual(t, quotas.NoopRequestRateLimiter, rateLimiter)
				assert.Equal(t, cfg.TaskSchedulerEnableRateLimiterShadowMode(), shadow)
			} else {
				assert.Equal(t, quotas.NoopRequestRateLimiter, rateLimiter)
				assert.False(t, shadow)
			}
		})
	}
}

func getSchedulerInfo(q queues.Queue) (quotas.RequestRateLimiter, bool) {
	v := reflect.ValueOf(q).Elem()
	qb := v.FieldByName("queueBase")
	schedulerVal := qb.Elem().FieldByName("scheduler")
	rlScheduler := reflect.ValueOf(schedulerVal.Interface()).Elem()
	tasksScheduler := reflect.ValueOf(rlScheduler.Field(0).Interface()).Elem()
	rateLimiter := tasksScheduler.FieldByName("rateLimiter").Interface().(quotas.RequestRateLimiter)
	enableShadow := tasksScheduler.FieldByName("options").FieldByName("EnableShadowMode").Bool()
	return rateLimiter, enableShadow
}
