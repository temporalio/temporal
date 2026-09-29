package workflow

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	deploymentspb "go.temporal.io/server/api/deployment/v1"
	"go.temporal.io/server/chasm"
	chasmworkflow "go.temporal.io/server/chasm/lib/workflow"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/service/history/configs"
	historyi "go.temporal.io/server/service/history/interfaces"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestRecordWorkflowTaskMetrics(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		breakdownMetricsByBuildID bool
		expectedDeploymentNameTag string
		expectedBuildIDTag        string
	}{
		{
			name:                      "breakdown enabled",
			breakdownMetricsByBuildID: true,
			expectedDeploymentNameTag: "deployment",
			expectedBuildIDTag:        "build-id",
		},
		{
			name:                      "breakdown disabled",
			breakdownMetricsByBuildID: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			handler := metricstest.NewCaptureHandler()
			capture := handler.StartCapture()
			defer handler.StopCapture(capture)

			config := &configs.Config{
				BreakdownMetricsByBuildID: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(tc.breakdownMetricsByBuildID),
			}
			completion := WorkflowTaskCompletionMetrics{
				VersioningInfo: VersioningMetricContext{
					Behavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
					DeploymentVersion: &deploymentspb.WorkerDeploymentVersion{
						DeploymentName: "deployment",
						BuildId:        "build-id",
					},
				},
				Attempt: 1,
			}

			RecordWorkflowTaskCompletedMetrics(
				config,
				handler,
				namespace.Name("test-namespace"),
				"test-task-queue",
				completion,
			)
			RecordWorkflowTaskFailedMetrics(
				config,
				handler,
				namespace.Name("test-namespace"),
				"test-task-queue",
				metrics.HistoryRespondWorkflowTaskFailedScope,
				enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE.String(),
				completion,
			)

			snapshot := capture.Snapshot()
			for _, metricName := range []string{
				metrics.WorkflowTasksCompleted.Name(),
				metrics.FailedWorkflowTasksCounter.Name(),
			} {
				recordings := snapshot[metricName]
				require.Len(t, recordings, 1)
				require.Equal(t, tc.expectedDeploymentNameTag, recordings[0].Tags["worker_deployment_name"])
				require.Equal(t, tc.expectedBuildIDTag, recordings[0].Tags["worker_build_id"])
			}

			failedRecording := snapshot[metrics.FailedWorkflowTasksCounter.Name()][0]
			require.Equal(t, metrics.HistoryRespondWorkflowTaskFailedScope, failedRecording.Tags[metrics.OperationTagName])
			require.Equal(
				t,
				enumspb.WORKFLOW_TASK_FAILED_CAUSE_WORKFLOW_WORKER_UNHANDLED_FAILURE.String(),
				failedRecording.Tags[metrics.FailureTagName],
			)
		})
	}
}

func TestRecordActivityCompletionMetrics_WorkerDeploymentTags(t *testing.T) {
	for _, tc := range []struct {
		name                      string
		breakdownMetricsByBuildID bool
		expectedDeploymentNameTag string
		expectedBuildIDTag        string
	}{
		{
			name:                      "breakdown enabled",
			breakdownMetricsByBuildID: true,
			expectedDeploymentNameTag: "deployment",
			expectedBuildIDTag:        "build-id",
		},
		{
			name:                      "breakdown disabled",
			breakdownMetricsByBuildID: false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			controller := gomock.NewController(t)
			handler := metricstest.NewCaptureHandler()
			capture := handler.StartCapture()
			defer handler.StopCapture(capture)

			config := &configs.Config{
				BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(true),
				BreakdownMetricsByBuildID: func(namespace string, taskQueue string, taskQueueType enumspb.TaskQueueType) bool {
					require.Equal(t, "test-namespace", namespace)
					require.Equal(t, "test-task-queue", taskQueue)
					require.Equal(t, enumspb.TASK_QUEUE_TYPE_ACTIVITY, taskQueueType)
					return tc.breakdownMetricsByBuildID
				},
			}
			shard := historyi.NewMockShardContext(controller)
			shard.EXPECT().GetConfig().Return(config).Times(4)
			shard.EXPECT().GetMetricsHandler().Return(handler).Times(4)
			shard.EXPECT().GetTimeSource().Return(clock.NewRealTimeSource()).Times(4)

			completion := ActivityCompletionMetrics{
				Status: ActivityStatusFailed,
				Closed: true,
				VersioningInfo: VersioningMetricContext{
					Behavior: enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE,
					DeploymentVersion: &deploymentspb.WorkerDeploymentVersion{
						DeploymentName: "deployment",
						BuildId:        "build-id",
					},
				},
				AttemptStartedTime: time.Now().Add(-time.Minute),
				FirstScheduledTime: time.Now().Add(-2 * time.Minute),
			}
			RecordActivityCompletionMetrics(
				shard,
				namespace.Name("test-namespace"),
				"test-task-queue",
				completion,
			)
			completion.Status = ActivityStatusSucceeded
			RecordActivityCompletionMetrics(
				shard,
				namespace.Name("test-namespace"),
				"test-task-queue",
				completion,
			)
			completion.Status = ActivityStatusCanceled
			RecordActivityCompletionMetrics(
				shard,
				namespace.Name("test-namespace"),
				"test-task-queue",
				completion,
			)
			completion.Status = ActivityStatusTimeout
			completion.TimerType = enumspb.TIMEOUT_TYPE_HEARTBEAT
			RecordActivityCompletionMetrics(
				shard,
				namespace.Name("test-namespace"),
				"test-task-queue",
				completion,
			)

			snapshot := capture.Snapshot()
			for _, metricName := range []string{
				metrics.ActivityTaskFail.Name(),
				metrics.ActivityFail.Name(),
				metrics.ActivitySuccess.Name(),
				metrics.ActivityCancel.Name(),
				metrics.ActivityTaskTimeout.Name(),
				metrics.ActivityTimeout.Name(),
			} {
				recordings := snapshot[metricName]
				require.Len(t, recordings, 1)
				require.Equal(t, tc.expectedDeploymentNameTag, recordings[0].Tags["worker_deployment_name"])
				require.Equal(t, tc.expectedBuildIDTag, recordings[0].Tags["worker_build_id"])
				require.Equal(
					t,
					enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE.String(),
					recordings[0].Tags["versioning_behavior"],
				)
			}
			for metricName, expectedCount := range map[string]int{
				metrics.ActivityE2ELatency.Name():             3,
				metrics.ActivityStartToCloseLatency.Name():    3,
				metrics.ActivityScheduleToCloseLatency.Name(): 4,
			} {
				recordings := snapshot[metricName]
				require.Len(t, recordings, expectedCount)
				for _, recording := range recordings {
					require.Equal(t, tc.expectedDeploymentNameTag, recording.Tags["worker_deployment_name"])
					require.Equal(t, tc.expectedBuildIDTag, recording.Tags["worker_build_id"])
					require.Equal(
						t,
						enumspb.VERSIONING_BEHAVIOR_AUTO_UPGRADE.String(),
						recording.Tags["versioning_behavior"],
					)
				}
			}
			require.Equal(
				t,
				enumspb.TIMEOUT_TYPE_HEARTBEAT.String(),
				snapshot[metrics.ActivityTaskTimeout.Name()][0].Tags["timeout_type"],
			)
			require.Equal(
				t,
				enumspb.TIMEOUT_TYPE_HEARTBEAT.String(),
				snapshot[metrics.ActivityTimeout.Name()][0].Tags["timeout_type"],
			)
		})
	}
}

func TestEmitWorkflowCompletionStats_WorkflowDuration(t *testing.T) {
	logger := log.NewTestLogger()
	testHandler, _ := metricstest.NewHandler(logger, metrics.ClientConfig{})
	testNamespace := namespace.Name("test-namespace")
	config := &configs.Config{
		BreakdownMetricsByTaskQueue: dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(true),
	}

	completionMetric := completionMetric{
		shouldRecord:     true,
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

func TestEmitMutableStateStatusArchetypeTag(t *testing.T) {
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)

	registry := chasm.NewRegistry(log.NewTestLogger())
	require.NoError(t, registry.Register(chasmworkflow.NewLibrary(chasmworkflow.NewRegistry())))

	emitMutableStateStatus(
		handler,
		registry,
		chasm.WorkflowArchetypeID,
		&persistence.MutableStateStatistics{
			TotalSize: 42,
			HistoryStatistics: &persistence.HistoryStatistics{
				SizeDiff:  100,
				CountDiff: 3,
			},
		},
	)

	snapshot := capture.Snapshot()
	recordings := snapshot[metrics.MutableStateSize.Name()]
	require.Len(t, recordings, 1)
	require.Equal(t, int64(42), recordings[0].Value)
	require.Equal(t, chasm.WorkflowComponentName, recordings[0].Tags[metrics.ArchetypeTagName])

	recordings = snapshot[metrics.HistorySize.Name()]
	require.Len(t, recordings, 1)
	require.Equal(t, int64(100), recordings[0].Value)
	require.NotContains(t, recordings[0].Tags, metrics.ArchetypeTagName)

	recordings = snapshot[metrics.HistoryCount.Name()]
	require.Len(t, recordings, 1)
	require.Equal(t, int64(3), recordings[0].Value)
	require.NotContains(t, recordings[0].Tags, metrics.ArchetypeTagName)
}

func TestGetArchetypeMetricTag(t *testing.T) {
	registry := chasm.NewRegistry(log.NewTestLogger())
	require.NoError(t, registry.Register(chasmworkflow.NewLibrary(chasmworkflow.NewRegistry())))

	tag, ok := getArchetypeMetricTag(registry, chasm.UnspecifiedArchetypeID)
	require.True(t, ok)
	require.Equal(t, metrics.ArchetypeTag(""), tag)

	tag, ok = getArchetypeMetricTag(registry, chasm.WorkflowArchetypeID)
	require.True(t, ok)
	require.Equal(t, metrics.ArchetypeTag(chasm.WorkflowComponentName), tag)

	tag, ok = getArchetypeMetricTag(registry, chasm.ArchetypeID(9999))
	require.True(t, ok)
	require.Equal(t, metrics.ArchetypeTag("9999"), tag)
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
	timeoutCounter.EXPECT().Record(
		int64(1),
		metrics.StringTag("timeout_type", enumspb.TIMEOUT_TYPE_START_TO_CLOSE.String()),
	).Times(1)
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
	timeoutCounter.EXPECT().Record(
		int64(1),
		metrics.StringTag("timeout_type", enumspb.TIMEOUT_TYPE_HEARTBEAT.String()),
	).Times(1)
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
		BreakdownMetricsByBuildID:   dynamicconfig.GetBoolPropertyFnFilteredByTaskQueue(true),
	}).Times(1)
	shard.EXPECT().GetTimeSource().Return(clock.NewRealTimeSource()).Times(1)
	return shard
}

func expectActivityMetricsScope(handler *metrics.MockHandler, operation string) {
	scopeTags := []any{
		metrics.OperationTag(operation),
		metrics.WorkflowTypeTag("test-workflow"),
		metrics.ActivityTypeTag("test-activity"),
		metrics.VersioningBehaviorTag(enumspb.VERSIONING_BEHAVIOR_UNSPECIFIED),
		metrics.WorkerDeploymentNameTag("", true),
		metrics.WorkerDeploymentBuildIDTag("", true),
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
