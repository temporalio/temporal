package persistence

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/service/history/tasks"
)

func TestDLQMetricsEmitter_EmitMetrics(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	logger := log.NewMockLogger(ctrl)
	manager := NewMockHistoryTaskQueueManager(ctrl)
	manager.EXPECT().ListQueues(gomock.Any(), &ListQueuesRequest{
		QueueType:     QueueTypeHistoryDLQ,
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
		NextPageToken: []byte("test_page_token"),
	}).Return(&ListQueuesResponse{
		Queues: []QueueInfo{
			{QueueName: GetHistoryTaskQueueName(tasks.CategoryIDTransfer, "source", "target"), MessageCount: 8},
			{QueueName: GetHistoryTaskQueueName(tasks.CategoryIDTimer, "source", "target"), MessageCount: 9},
			{QueueName: GetHistoryTaskQueueName(tasks.CategoryIDReplication, "source", "target"), MessageCount: 10},
		},
		NextPageToken: nil,
	}, nil).Times(1)

	emitter := NewDLQMetricsEmitter(metricsHandler, logger, manager)
	emitter.emitMetricsTimer = time.NewTicker(60 * time.Millisecond)
	logger.EXPECT().Info(gomock.Any()).AnyTimes()
	logger.EXPECT().Error(gomock.Any()).AnyTimes()

	snapshot := capture.Snapshot()
	emitter.Start()
	assert.Eventually(t, func() bool {
		snapshot = capture.Snapshot()
		return len(snapshot[metrics.DLQMessageCount.Name()]) == len(tasks.CategoryIDToName)
	}, 5*time.Second, 100*time.Millisecond)

	emitter.Stop()
	<-emitter.shutdownCh

	messageCount := make(map[string]float64)
	for _, recording := range snapshot[metrics.DLQMessageCount.Name()] {
		messageCount[recording.Tags[metrics.TaskCategoryTagName]] = recording.Value.(float64)
	}
	assert.Equal(t, float64(9), messageCount[tasks.CategoryIDToName[tasks.CategoryIDTransfer]])
	assert.Equal(t, float64(11), messageCount[tasks.CategoryIDToName[tasks.CategoryIDTimer]])
	assert.Equal(t, float64(13), messageCount[tasks.CategoryIDToName[tasks.CategoryIDReplication]])
}
