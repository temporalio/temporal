package replication

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	enumsspb "go.temporal.io/server/api/enums/v1"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/historyservicemock/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/quotas"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

func TestSendEventLoopShutdownCancellation(t *testing.T) {
	for _, priority := range []enumsspb.TaskPriority{enumsspb.TASK_PRIORITY_HIGH, enumsspb.TASK_PRIORITY_LOW} {
		for _, phase := range []string{"catchup", "live"} {
			t.Run(priority.String()+"/"+phase, func(t *testing.T) {
				sender, server, engine, capture := newShutdownTestSender(t, priority, phase)
				engine.EXPECT().GetReplicationTasksIter(gomock.Any(), string(sender.clientShardKey.ClusterID), int64(100), int64(102)).DoAndReturn(
					func(context.Context, string, int64, int64) (collection.Iterator[tasks.Task], error) {
						return collection.NewPagingIterator[tasks.Task](func([]byte) ([]tasks.Task, []byte, error) {
							sender.Stop()
							return []tasks.Task{&tasks.HistoryReplicationTask{TaskID: 100}}, nil, nil
						}), nil
					},
				)
				if phase == "catchup" {
					server.EXPECT().Send(gomock.Any()).Times(0)
				}
				WrapEventLoop(sender.ctx, func() error { return sender.sendEventLoop(priority) }, sender.Stop,
					sender.logger, sender.metrics, sender.clientShardKey, sender.serverShardKey, sender.config)
				require.True(t, sender.shutdownChan.IsShutdown())
				require.ErrorIs(t, sender.ctx.Err(), context.Canceled)
				require.Empty(t, capture.SnapshotMetric(metrics.ReplicationServiceError.Name()))
				require.Empty(t, capture.SnapshotMetric(metrics.ReplicationStreamError.Name()))
			})
		}
	}
}

func TestSendEventLoopPreservesFailures(t *testing.T) {
	for _, stop := range []bool{false, true} {
		name := "active cancellation"
		failure := error(context.Canceled)
		if stop {
			name = "failure during shutdown"
			failure = errors.New("persistence failure")
		}
		t.Run(name, func(t *testing.T) {
			sender, _, engine, capture := newShutdownTestSender(t, enumsspb.TASK_PRIORITY_HIGH, "catchup")
			engine.EXPECT().GetReplicationTasksIter(gomock.Any(), string(sender.clientShardKey.ClusterID), int64(100), int64(102)).DoAndReturn(
				func(context.Context, string, int64, int64) (collection.Iterator[tasks.Task], error) {
					if stop {
						sender.Stop()
					}
					return nil, failure
				},
			)
			var loopErr error
			WrapEventLoop(sender.ctx, func() error {
				loopErr = sender.sendEventLoop(enumsspb.TASK_PRIORITY_HIGH)
				return loopErr
			}, sender.Stop, sender.logger, sender.metrics, sender.clientShardKey, sender.serverShardKey, sender.config)
			require.ErrorIs(t, loopErr, failure)
			require.Len(t, capture.SnapshotMetric(metrics.ReplicationServiceError.Name()), 1)
		})
	}
}

func newShutdownTestSender(
	t *testing.T,
	priority enumsspb.TaskPriority,
	phase string,
) (*StreamSenderImpl, *historyservicemock.MockHistoryService_StreamWorkflowReplicationMessagesServer, *historyi.MockEngine, *metricstest.Capture) {
	t.Helper()
	ctrl := gomock.NewController(t)
	server := historyservicemock.NewMockHistoryService_StreamWorkflowReplicationMessagesServer(ctrl)
	engine := historyi.NewMockEngine(ctrl)
	shardContext := historyi.NewMockShardContext(ctrl)
	metricHandler := metricstest.NewCaptureHandler()
	capture := metricHandler.StartCapture()
	t.Cleanup(func() { metricHandler.StopCapture(capture) })
	server.EXPECT().Context().Return(context.Background())
	shardContext.EXPECT().GetLogger().Return(log.NewNoopLogger())
	shardContext.EXPECT().GetMetricsHandler().Return(metricHandler)
	config := tests.NewDynamicConfig()
	config.EnableReplicationTaskTieredProcessing = func() bool { return true }
	config.EnableReplicationReaderGroup = func() bool { return false }
	sender := NewStreamSender(server, shardContext, engine, quotas.NoopRequestRateLimiter,
		NewMockSourceTaskConverter(ctrl), "target_cluster", 1, NewClusterShardKey(2, 1), NewClusterShardKey(1, 1), config)
	sender.status = common.DaemonStatusStarted
	t.Cleanup(sender.cancel)
	notifications := make(chan struct{}, 1)
	engine.EXPECT().SubscribeReplicationNotification("target_cluster").Return(notifications, "subscriber")
	engine.EXPECT().UnsubscribeReplicationNotification("subscriber")
	readerID := shard.ReplicationReaderIDFromClusterShardID(2, 1)
	shardContext.EXPECT().GetQueueState(tasks.CategoryReplication).Return(&persistencespb.QueueState{
		ReaderStates: map[int64]*persistencespb.QueueReaderState{
			readerID: buildTieredReaderState(syncReplicationState(100, 100, 100)),
		},
	}, true)
	if phase == "live" {
		gomock.InOrder(
			shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(100)),
			shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(102)),
		)
		server.EXPECT().Send(gomock.Any()).DoAndReturn(func(response *historyservice.StreamWorkflowReplicationMessagesResponse) error {
			require.Equal(t, int64(100), response.GetMessages().GetExclusiveHighWatermark())
			require.Equal(t, priority, response.GetMessages().GetPriority())
			notifications <- struct{}{}
			return nil
		})
	} else {
		shardContext.EXPECT().GetQueueExclusiveHighReadWatermark(tasks.CategoryReplication).Return(tasks.NewImmediateKey(102))
	}
	return sender, server, engine, capture
}
