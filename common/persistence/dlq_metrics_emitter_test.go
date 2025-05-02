package persistence

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/service/history/tasks"
	"go.uber.org/mock/gomock"
)

func TestDLQMetricsEmitter_EmitMetrics_WhenInstanceHostsShardOne(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metricsHandler := metricstest.NewCaptureHandler()
	logger := log.NewMockLogger(ctrl)
	manager := NewMockHistoryTaskQueueManager(ctrl)
	manager.EXPECT().ListQueues(gomock.Any(), &ListQueuesRequest{
		QueueType:     QueueTypeHistoryDLQ,
		PageSize:      100,
		NextPageToken: nil,
	}).Return(&ListQueuesResponse{
		Queues: []QueueInfo{
			{QueueName: GetHistoryTaskQueueName(tasks.CategoryIDTransfer, "source", "target"), MessageCount: 1},
			{QueueName: GetHistoryTaskQueueName(tasks.CategoryIDTimer, "source", "target"), MessageCount: 2},
			{QueueName: GetHistoryTaskQueueName(tasks.CategoryIDReplication, "source", "target"), MessageCount: 3},
		},
		NextPageToken: []byte("test_page_token"),
	}, nil).Times(1)
	manager.EXPECT().ListQueues(gomock.Any(), &ListQueuesRequest{
		QueueType:     QueueTypeHistoryDLQ,
		PageSize:      100,
		NextPageToken: []byte("test_page_token"),
	}).Return(&ListQueuesResponse{
		Queues: []QueueInfo{
			{QueueName: GetHistoryTaskQueueName(tasks.CategoryIDTransfer, "source", "target"), MessageCount: 8},
			{QueueName: GetHistoryTaskQueueName(tasks.CategoryIDTimer, "source", "target"), MessageCount: 9},
			{QueueName: GetHistoryTaskQueueName(tasks.CategoryIDReplication, "source", "target"), MessageCount: 10},
		},
		NextPageToken: nil,
	}, nil).Times(1)
	resolver := membership.NewMockServiceResolver(ctrl)
	resolver.EXPECT().Lookup("1").Return(membership.NewHostInfoFromAddress("testAddress"), nil).Times(1)
	hostInfoProvider := membership.NewMockHostInfoProvider(ctrl)
	hostInfoProvider.EXPECT().HostInfo().Return(membership.NewHostInfoFromAddress("testAddress")).Times(1)
	categoryRegistry := tasks.NewDefaultTaskCategoryRegistry()
	emitter := NewDLQMetricsEmitter(metricsHandler, logger, manager, resolver, hostInfoProvider, categoryRegistry)
	emitter.emitMetricsTimer = time.NewTicker(60 * time.Millisecond)
	logger.EXPECT().Info(gomock.Any()).AnyTimes()
	logger.EXPECT().Error(gomock.Any()).AnyTimes()

	capture := metricsHandler.StartCapture()
	snapshot := capture.Snapshot()
	emitter.Start()
	assert.Eventually(t, func() bool {
		snapshot = capture.Snapshot()
		return len(snapshot[metrics.DLQMessageCount.Name()]) == len(categoryRegistry.GetCategories())
	}, 5*time.Second, 100*time.Millisecond)

	emitter.Stop()
	<-emitter.shutdownCh

	messageCount := make(map[string]float64)
	for _, recording := range snapshot[metrics.DLQMessageCount.Name()] {
		value, ok := recording.Value.(float64)
		assert.True(t, ok)
		messageCount[recording.Tags[metrics.TaskCategoryTagName]] = value
	}
	assert.Equal(t, float64(9), messageCount["transfer"])
	assert.Equal(t, float64(11), messageCount["timer"])
	assert.Equal(t, float64(13), messageCount["replication"])
}

func TestDLQMetricsEmitter_DoesNotEmitMetrics_WhenInstanceDoesNotHostShardOne(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	logger := log.NewMockLogger(ctrl)
	manager := NewMockHistoryTaskQueueManager(ctrl)

	resolver := membership.NewMockServiceResolver(ctrl)
	resolver.EXPECT().Lookup("1").Return(membership.NewHostInfoFromAddress("testAddress1"), nil).MinTimes(1)
	hostInfoProvider := membership.NewMockHostInfoProvider(ctrl)
	hostInfoProvider.EXPECT().HostInfo().Return(membership.NewHostInfoFromAddress("testAddress2")).MinTimes(1)

	emitter := NewDLQMetricsEmitter(metricsHandler, logger, manager, resolver, hostInfoProvider, tasks.NewDefaultTaskCategoryRegistry())
	emitter.emitMetricsTimer = time.NewTicker(10 * time.Millisecond)
	logger.EXPECT().Info(gomock.Any()).AnyTimes()
	logger.EXPECT().Error(gomock.Any()).AnyTimes()

	emitter.Start()
	assert.Eventually(t, func() bool {
		snapshot := capture.Snapshot()
		if len(snapshot[metrics.DLQMessageCount.Name()]) == 0 {
			return false
		}
		for _, r := range snapshot[metrics.DLQMessageCount.Name()] {
			value, ok := r.Value.(float64)
			if !ok {
				return false
			}
			if value != 0 {
				return false
			}
		}
		return true
	}, 5*time.Second, 15*time.Millisecond)
	emitter.Stop()
	<-emitter.shutdownCh
}
