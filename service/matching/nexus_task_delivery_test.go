package matching

import (
	"testing"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/testing/testlogger"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
)

func TestNexusTaskDeliveryPartitionLogs(t *testing.T) {
	t.Parallel()
	for _, taskType := range []enumspb.TaskQueueType{enumspb.TASK_QUEUE_TYPE_NEXUS, enumspb.TASK_QUEUE_TYPE_WORKFLOW, enumspb.TASK_QUEUE_TYPE_ACTIVITY} {
		t.Run(taskType.String(), func(t *testing.T) {
			t.Parallel()
			logger := testlogger.NewTestLogger(t, testlogger.FailOnAnyUnexpectedError)
			logger.Expect(testlogger.Error, "partition failure")
			capture := logger.StartCapture()
			defer logger.StopCapture(capture)
			clusterMetadata := cluster.NewMockMetadata(gomock.NewController(t))
			clusterMetadata.EXPECT().GetCurrentClusterName().Return("active")
			engine := &matchingEngineImpl{
				logger: log.With(logger, tag.ComponentMatchingEngine), throttledLogger: log.With(logger, tag.ComponentMatchingEngine),
				metricsHandler: metrics.NoopMetricsHandler, clusterMeta: clusterMetadata,
			}
			ns := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Id: "namespace-id", Name: "namespace-name"}, nil, "active")
			family, err := tqid.NewTaskQueueFamily("namespace-id", "queue")
			require.NoError(t, err)
			partition := family.TaskQueue(taskType).RootPartition()
			config := &taskQueueConfig{BreakdownMetricsByTaskQueue: func() bool { return false }, BreakdownMetricsByPartition: func() bool { return false }}
			normal, throttled, _ := engine.loggerAndMetricsForPartition(ns, partition, config)
			normal.Error("partition failure")
			throttled.Error("partition failure")
			records := capture.Snapshot()
			require.Len(t, records, 2)
			for _, record := range records {
				tags := map[string]any{}
				for _, logTag := range record.Tags {
					tags[logTag.Key()] = logTag.Value()
				}
				want := map[string]any{"component": "matching-engine", "wf-namespace": "namespace-name", "wf-task-queue-name": "queue", "wf-task-queue-type": taskType.String()}
				if taskType == enumspb.TASK_QUEUE_TYPE_NEXUS {
					want["nexus-stage"] = "handler-task-delivery"
				}
				require.Equal(t, want, tags)
			}
		})
	}
}
