package matching

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/future"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/common/tqid"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestPerNamespaceWorkerBacklogMetrics(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		counts        []int64
		ages          []time.Duration
		expectedCount float64
		expectedAge   float64
	}{
		{"sums priorities and versions", []int64{5, 7, 3}, []time.Duration{8 * time.Second, 13 * time.Second, 11 * time.Second}, 15, 13},
		{"empty backlog has no age", []int64{0, 0, 0}, []time.Duration{8 * time.Second, 13 * time.Second, 11 * time.Second}, 0, 0},
		{"negative approximations do not subtract", []int64{-5, 7, 3}, []time.Duration{30 * time.Second, 13 * time.Second, 11 * time.Second}, 10, 13},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			controller := gomock.NewController(t)
			unversioned := NewMockphysicalTaskQueueManager(controller)
			versioned := NewMockphysicalTaskQueueManager(controller)
			unversioned.EXPECT().GetStatsByPriority(false).Return(map[int32]*taskqueuepb.TaskQueueStats{
				1: {ApproximateBacklogCount: tc.counts[0], ApproximateBacklogAge: durationpb.New(tc.ages[0])},
				3: {ApproximateBacklogCount: tc.counts[1], ApproximateBacklogAge: durationpb.New(tc.ages[1])},
			})
			versioned.EXPECT().GetStatsByPriority(false).Return(map[int32]*taskqueuepb.TaskQueueStats{
				1: {ApproximateBacklogCount: tc.counts[2], ApproximateBacklogAge: durationpb.New(tc.ages[2])},
			})
			handler := metricstest.NewCaptureHandler()
			capture := handler.StartCapture()
			defer handler.StopCapture(capture)
			pm := &taskQueuePartitionManagerImpl{
				defaultQueueFuture: future.NewFuture[physicalTaskQueueManager](),
				versionedQueues:    map[PhysicalTaskQueueVersion]physicalTaskQueueManager{{buildId: "version"}: versioned},
			}
			pm.defaultQueueFuture.Set(unversioned, nil)
			require.True(t, pm.emitPerNamespaceWorkerBacklogMetrics(handler))
			require.InDelta(t, tc.expectedCount, capture.SnapshotMetric(metrics.PerNamespaceWorkerBacklogCount.Name())[0].Value, 0.001)
			require.InDelta(t, tc.expectedAge, capture.SnapshotMetric(metrics.PerNamespaceWorkerBacklogAgeSeconds.Name())[0].Value, 0.001)
		})
	}
}

func TestPerNamespaceWorkerBacklogMetricsScope(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		queue    string
		taskType enumspb.TaskQueueType
		sticky   bool
		expected bool
	}{
		{"system activity queue", primitives.PerNSWorkerTaskQueue, enumspb.TASK_QUEUE_TYPE_ACTIVITY, false, true},
		{"system workflow queue", primitives.PerNSWorkerTaskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW, false, true},
		{"customer queue", "customer-queue", enumspb.TASK_QUEUE_TYPE_ACTIVITY, false, false},
		{"system sticky queue", primitives.PerNSWorkerTaskQueue, enumspb.TASK_QUEUE_TYPE_WORKFLOW, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			family, err := tqid.NewTaskQueueFamily("namespace-id", tc.queue)
			require.NoError(t, err)
			queue := family.TaskQueue(tc.taskType)
			var partition tqid.Partition = queue.NormalPartition(2)
			if tc.sticky {
				partition = queue.StickyPartition("sticky-name")
			}
			captureHandler := metricstest.NewCaptureHandler()
			capture := captureHandler.StartCapture()
			defer captureHandler.StopCapture(capture)
			pm := &taskQueuePartitionManagerImpl{
				partition:      partition,
				ns:             namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Name: "customer-namespace"}, nil, "cluster"),
				metricsHandler: captureHandler,
			}
			handler := pm.perNamespaceWorkerBacklogMetricsHandler()
			if !tc.expected {
				require.Nil(t, handler)
				return
			}
			require.NotNil(t, handler)
			recordPerNamespaceWorkerBacklog(handler, 4, 9)
			for _, metric := range []string{metrics.PerNamespaceWorkerBacklogCount.Name(), metrics.PerNamespaceWorkerBacklogAgeSeconds.Name()} {
				recording := capture.SnapshotMetric(metric)
				require.Len(t, recording, 1)
				require.Equal(t, map[string]string{"namespace": "customer-namespace", "task_type": tc.taskType.String(), "per_namespace_worker_partition": "2"}, recording[0].Tags)
			}
		})
	}
}

func TestPerNamespaceWorkerBacklogMetricsTickerAndUnload(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	controller := gomock.NewController(t)
	queue := NewMockphysicalTaskQueueManager(controller)
	queue.EXPECT().GetStatsByPriority(false).DoAndReturn(func(bool) map[int32]*taskqueuepb.TaskQueueStats {
		cancel()
		return map[int32]*taskqueuepb.TaskQueueStats{3: {ApproximateBacklogCount: 15, ApproximateBacklogAge: durationpb.New(8 * time.Second)}}
	})
	family, err := tqid.NewTaskQueueFamily("namespace-id", primitives.PerNSWorkerTaskQueue)
	require.NoError(t, err)
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)
	pm := &taskQueuePartitionManagerImpl{
		partition:          family.TaskQueue(enumspb.TASK_QUEUE_TYPE_ACTIVITY).RootPartition(),
		ns:                 namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Name: "customer-namespace"}, nil, "cluster"),
		metricsHandler:     handler,
		defaultQueueFuture: future.NewFuture[physicalTaskQueueManager](),
		config: &taskQueueConfig{
			BacklogMetricsEmitInterval:  func() time.Duration { return 5 * time.Millisecond },
			BreakdownMetricsByTaskQueue: func() bool { return false },
			BreakdownMetricsByPartition: func() bool { return false },
		},
	}
	pm.defaultQueueFuture.Set(queue, nil)
	done := make(chan error, 1)
	go func() { done <- pm.emitLogicalBacklogMetrics(ctx) }()
	require.ErrorIs(t, await.Rcv(t, done), context.Canceled)
	counts := capture.SnapshotMetric(metrics.PerNamespaceWorkerBacklogCount.Name())
	ages := capture.SnapshotMetric(metrics.PerNamespaceWorkerBacklogAgeSeconds.Name())
	require.Len(t, counts, 2)
	require.Len(t, ages, 2)
	require.Equal(t, []any{float64(15), float64(0)}, []any{counts[0].Value, counts[1].Value})
	require.Equal(t, []any{float64(8), float64(0)}, []any{ages[0].Value, ages[1].Value})
	require.Empty(t, capture.SnapshotMetric(metrics.ApproximateBacklogCount.Name()))
}

func TestPerNamespaceWorkerBacklogMetricsNotInitialized(t *testing.T) {
	t.Parallel()
	handler := metricstest.NewCaptureHandler()
	capture := handler.StartCapture()
	defer handler.StopCapture(capture)
	pm := &taskQueuePartitionManagerImpl{defaultQueueFuture: future.NewFuture[physicalTaskQueueManager]()}
	require.False(t, pm.emitPerNamespaceWorkerBacklogMetrics(handler))
	require.Empty(t, capture.Snapshot())
}
