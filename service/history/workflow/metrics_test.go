package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestEmitWorkflowCompletionStats_WorkflowDuration(t *testing.T) {
	logger := log.NewTestLogger()
	testHandler, _ := metricstest.NewHandler(logger, metrics.ClientConfig{})
	testNamespace := namespace.Name("test-namespace")
	config := &configs.Config{
		BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(true),
	}

	completionMetric := completionMetric{
		initialized:      true,
		isWorkflow:       true,
		taskQueue:        "test-task-queue",
		namespaceState:   "active",
		workflowTypeName: "test-workflow",
		status:           enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED,
		startTime:        timestamppb.New(time.Unix(100, 0)),
		closeTime:        timestamppb.New(time.Unix(130, 0)),
	}

	emitWorkflowCompletionStats(testHandler, testNamespace, completionMetric, config)

	snapshot, err := testHandler.Snapshot()
	require.NoError(t, err)
	buckets, err := snapshot.Histogram("workflow_schedule_to_close_latency_milliseconds",

		metrics.StringTag("namespace", "test-namespace"),
		metrics.StringTag("namespace_state", "active"),
		metrics.StringTag("workflowType", "test-workflow"),
		metrics.StringTag("operation", "CompletionStats"),
		metrics.StringTag("taskqueue", "test-task-queue"),
		metrics.StringTag("otel_scope_name", "temporal"),
		metrics.StringTag("otel_scope_version", ""),
	)
	require.NoError(t, err)
	require.NotEmpty(t, buckets)
}

func TestEmitWorkflowCompletionStats_SkipNonWorkflow(t *testing.T) {
	logger := log.NewTestLogger()
	testHandler, _ := metricstest.NewHandler(logger, metrics.ClientConfig{})
	testNamespace := namespace.Name("test-namespace")
	completionMetric := completionMetric{isWorkflow: false}
	emitWorkflowCompletionStats(testHandler, testNamespace, completionMetric, nil)
	snapshot, err := testHandler.Snapshot()
	require.NoError(t, err)
	_, err = snapshot.Histogram("workflow_schedule_to_close_latency_milliseconds")
	require.Error(t, err)
}

func TestRecordActivityCompletionMetrics_SkipsStartToCloseLatencyWhenStartedTimeMissing(t *testing.T) {
	controller := gomock.NewController(t)
	handler := metrics.NewMockHandler(controller)
	shard := newActivityMetricsTestShard(controller, handler)

	expectActivityMetricsScope(handler, metrics.HistoryRespondActivityTaskCompletedScope)

	scheduleToCloseTimer := metrics.NewMockTimerIface(controller)
	scheduleToCloseTimer.EXPECT().Record(gomock.Any()).Times(1)
	handler.EXPECT().Timer(metrics.ActivityScheduleToCloseLatency.Name()).Return(scheduleToCloseTimer)

	successCounter := metrics.NewMockCounterIface(controller)
	successCounter.EXPECT().Record(int64(1)).Times(1)
	handler.EXPECT().Counter(metrics.ActivitySuccess.Name()).Return(successCounter)

	RecordActivityCompletionMetrics(
		shard,
		namespace.Name("test-namespace"),
		"test-task-queue",
		ActivityCompletionMetrics{
			Status:             ActivityStatusSucceeded,
			AttemptStartedTime: time.Time{},
			FirstScheduledTime: time.Now().Add(-2 * time.Minute),
			Closed:             true,
		},
		testActivityMetricTags(metrics.HistoryRespondActivityTaskCompletedScope)...,
	)
}

func TestRecordActivityCompletionMetrics_SkipsFutureStartTime(t *testing.T) {
	controller := gomock.NewController(t)
	handler := metrics.NewMockHandler(controller)
	shard := newActivityMetricsTestShard(controller, handler)
	expectActivityMetricsScope(handler, metrics.HistoryRespondActivityTaskCompletedScope)

	RecordActivityCompletionMetrics(
		shard,
		namespace.Name("test-namespace"),
		"test-task-queue",
		ActivityCompletionMetrics{
			Status:             ActivityStatusUnknown,
			AttemptStartedTime: time.Now().Add(1 * time.Minute),
		},
		testActivityMetricTags(metrics.HistoryRespondActivityTaskCompletedScope)...,
	)
}

func TestRecordActivityCompletionMetrics_RecordsLargeLatency(t *testing.T) {
	controller := gomock.NewController(t)
	handler := metrics.NewMockHandler(controller)
	shard := newActivityMetricsTestShard(controller, handler)
	expectActivityMetricsScope(handler, metrics.HistoryRespondActivityTaskCompletedScope)

	e2eTimer := metrics.NewMockTimerIface(controller)
	e2eTimer.EXPECT().Record(gomock.Any()).Times(1)
	handler.EXPECT().Timer(metrics.ActivityE2ELatency.Name()).Return(e2eTimer)

	startToCloseTimer := metrics.NewMockTimerIface(controller)
	startToCloseTimer.EXPECT().Record(gomock.Any()).Times(1)
	handler.EXPECT().Timer(metrics.ActivityStartToCloseLatency.Name()).Return(startToCloseTimer)

	RecordActivityCompletionMetrics(
		shard,
		namespace.Name("test-namespace"),
		"test-task-queue",
		ActivityCompletionMetrics{
			Status:             ActivityStatusUnknown,
			AttemptStartedTime: time.Now().Add(-2 * time.Hour),
		},
		testActivityMetricTags(metrics.HistoryRespondActivityTaskCompletedScope)...,
	)
}

func TestRecordActivityCompletionMetrics_TimeoutWithStartedTimeSkipsLatency(t *testing.T) {
	controller := gomock.NewController(t)
	handler := metrics.NewMockHandler(controller)
	shard := newActivityMetricsTestShard(controller, handler)
	expectActivityMetricsScope(handler, metrics.TimerActiveTaskActivityTimeoutScope)

	timeoutCounter := metrics.NewMockCounterIface(controller)
	timeoutCounter.EXPECT().Record(int64(1), metrics.StringTag("timeout_type", enumspb.TIMEOUT_TYPE_START_TO_CLOSE.String())).Times(1)
	handler.EXPECT().Counter(metrics.ActivityTaskTimeout.Name()).Return(timeoutCounter)

	RecordActivityCompletionMetrics(
		shard,
		namespace.Name("test-namespace"),
		"test-task-queue",
		ActivityCompletionMetrics{
			Status:             ActivityStatusTimeout,
			AttemptStartedTime: time.Now().Add(-30 * time.Second),
			TimerType:          enumspb.TIMEOUT_TYPE_START_TO_CLOSE,
		},
		testActivityMetricTags(metrics.TimerActiveTaskActivityTimeoutScope)...,
	)
}

func TestRecordActivityCompletionMetrics_TimeoutWithMissingStartedTimeSkipsLatencyAndEmitsCounter(t *testing.T) {
	controller := gomock.NewController(t)
	handler := metrics.NewMockHandler(controller)
	shard := newActivityMetricsTestShard(controller, handler)
	expectActivityMetricsScope(handler, metrics.TimerActiveTaskActivityTimeoutScope)

	timeoutCounter := metrics.NewMockCounterIface(controller)
	timeoutCounter.EXPECT().Record(int64(1), metrics.StringTag("timeout_type", enumspb.TIMEOUT_TYPE_HEARTBEAT.String())).Times(1)
	handler.EXPECT().Counter(metrics.ActivityTaskTimeout.Name()).Return(timeoutCounter)

	RecordActivityCompletionMetrics(
		shard,
		namespace.Name("test-namespace"),
		"test-task-queue",
		ActivityCompletionMetrics{
			Status:    ActivityStatusTimeout,
			TimerType: enumspb.TIMEOUT_TYPE_HEARTBEAT,
		},
		testActivityMetricTags(metrics.TimerActiveTaskActivityTimeoutScope)...,
	)
}

func newActivityMetricsTestShard(
	controller *gomock.Controller,
	handler *metrics.MockHandler,
) *historyi.MockShardContext {
	shard := historyi.NewMockShardContext(controller)
	shard.EXPECT().GetMetricsHandler().Return(handler).Times(1)
	shard.EXPECT().GetConfig().Return(&configs.Config{
		BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(true),
	}).Times(1)
	return shard
}

func expectActivityMetricsScope(handler *metrics.MockHandler, operation string) {
	scopeTags := []any{
		metrics.OperationTag(operation),
		metrics.WorkflowTypeTag("test-workflow"),
		metrics.ActivityTypeTag("test-activity"),
		metrics.NamespaceTag("test-namespace"),
		metrics.UnsafeTaskQueueTag("test-task-queue"),
	}
	handler.EXPECT().WithTags(scopeTags...).Return(handler).Times(1)
}

func testActivityMetricTags(operation string) []metrics.Tag {
	return []metrics.Tag{
		metrics.OperationTag(operation),
		metrics.WorkflowTypeTag("test-workflow"),
		metrics.ActivityTypeTag("test-activity"),
	}
}
