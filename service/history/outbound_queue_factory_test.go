package history

import (
	"context"
	"testing"
	"time"

	"github.com/sony/gobreaker"
	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/circuitbreaker"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/definition"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/telemetry"
	"go.temporal.io/server/common/testing/await"
	"go.temporal.io/server/service/history/circuitbreakerpool"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/replication/eventhandler"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.temporal.io/server/service/history/workflow/cache"
	"go.uber.org/mock/gomock"
)

func TestOutboundQueueFactory_ChasmTaskGroupWiring(t *testing.T) {
	t.Parallel()

	cb := circuitbreaker.NewTwoStepCircuitBreakerWithDynamicSettings(circuitbreaker.Settings{Name: "test"})
	cb.UpdateSettings(dynamicconfig.CircuitBreakerSettings{})
	taskCh := startOutboundQueueWithChasmTask(t, metrics.NoopMetricsHandler, cb)

	select {
	case executedTask := <-taskCh:
		ct, ok := executedTask.(*tasks.ChasmTask)
		require.True(t, ok, "expected ChasmTask, got %T", executedTask)
		require.Equal(t, "my-task-group", ct.OutboundTaskGroup())
	case <-time.After(10 * time.Second):
		t.Fatal("timed out waiting for task to reach executor")
	}
}

func TestOutboundQueueFactory_CircuitBreakerBlockedMetric(t *testing.T) {
	t.Parallel()

	metricsHandler := metricstest.NewCaptureHandler()
	capture := metricsHandler.StartCapture()
	startOutboundQueueWithChasmTask(t, metricsHandler, alwaysOpenCircuitBreaker{})

	await.Require(t.Context(), t, func(c *await.T) {
		recordings := capture.SnapshotMetric(metrics.CircuitBreakerExecutableBlocked.Name())
		require.NotEmpty(c, recordings)
		require.Equal(c, &metricstest.CapturedRecording{Value: int64(1), Tags: map[string]string{
			"operation":   metrics.OperationOutboundQueueProcessorScope,
			"namespace":   "test-ns",
			"destination": "test-destination",
			"task_group":  "my-task-group",
		}}, recordings[0])
	}, 10*time.Second, 10*time.Millisecond)
}

// startOutboundQueueWithChasmTask starts an outbound queue that loads one CHASM task in task group
// "my-task-group", and returns the channel the task is sent to once it reaches the executor.
func startOutboundQueueWithChasmTask(
	t *testing.T,
	metricsHandler metrics.Handler,
	cb circuitbreaker.TwoStepCircuitBreaker,
) <-chan tasks.Task {
	ctrl := gomock.NewController(t)

	chasmRegistry := chasm.NewRegistry(log.NewTestLogger())
	lib := chasm.NewMockLibrary(ctrl)
	lib.EXPECT().Name().Return("TestLib").AnyTimes()
	lib.EXPECT().Components().Return(nil)
	lib.EXPECT().NexusServices().Return(nil)
	lib.EXPECT().NexusServiceProcessors().Return(nil)
	type testTaskType struct{}
	lib.EXPECT().Tasks().Return([]*chasm.RegistrableTask{
		chasm.NewRegistrableSideEffectTask(
			"MyTask",
			chasm.NewMockSideEffectTaskHandler[*chasm.MockComponent, testTaskType](ctrl),
			chasm.WithTaskGroup("my-task-group"),
		),
	})
	require.NoError(t, chasmRegistry.Register(lib))

	config := tests.NewDynamicConfig()
	mockShard := shard.NewTestContext(ctrl, &persistencespb.ShardInfo{
		ShardId: 1,
		RangeId: 1,
		Owner:   "test-owner",
	}, config)
	mockShard.SetChasmRegistry(chasmRegistry)
	mockShard.Resource.ClusterMetadata.EXPECT().GetCurrentClusterName().Return("active").AnyTimes()
	mockShard.Resource.ClusterMetadata.EXPECT().GetClusterID().Return(int64(1)).AnyTimes()
	mockShard.Resource.NamespaceCache.EXPECT().GetNamespaceByID(gomock.Any()).Return(tests.GlobalNamespaceEntry, nil).AnyTimes()
	mockShard.Resource.NamespaceCache.EXPECT().GetNamespaceName(gomock.Any()).Return(tests.Namespace, nil).AnyTimes()

	taskTypeID := chasm.GenerateTypeID("TestLib.MyTask")
	chasmTask := &tasks.ChasmTask{
		WorkflowKey: definition.NewWorkflowKey(tests.NamespaceID.String(), "wf-id", "run-id"),
		Category:    tasks.CategoryOutbound,
		Destination: "test-destination",
		TaskID:      1,
		Info:        &persistencespb.ChasmTaskInfo{TypeId: taskTypeID},
	}
	require.Empty(t, chasmTask.OutboundTaskGroup(), "task group should not be set before loading through queue")

	mockShard.Resource.ExecutionMgr.EXPECT().GetHistoryTasks(gomock.Any(), gomock.Any()).Return(
		&persistence.GetHistoryTasksResponse{
			Tasks: []tasks.Task{chasmTask},
		}, nil,
	).AnyTimes()

	nsRegistry := namespace.NewMockRegistry(ctrl)
	nsRegistry.EXPECT().GetNamespaceName(gomock.Any()).Return(namespace.Name("test-ns"), nil).AnyTimes()

	rateLimiter, err := queues.NewPrioritySchedulerRateLimiter(
		func(string) float64 { return 100 },
		func() float64 { return 100 },
		func(string) float64 { return 100 },
		func() float64 { return 100 },
	)
	require.NoError(t, err)

	cbPool := &circuitbreakerpool.OutboundQueueCircuitBreakerPool{
		CircuitBreakerPool: circuitbreakerpool.NewCircuitBreakerPool(
			func(tasks.TaskGroupNamespaceIDAndDestination) circuitbreaker.TwoStepCircuitBreaker {
				return cb
			},
		),
	}

	taskCh := make(chan tasks.Task, 1)
	factory := NewOutboundQueueFactory(outboundQueueFactoryParams{
		QueueFactoryBaseParams: QueueFactoryBaseParams{
			NamespaceRegistry:    nsRegistry,
			ClusterMetadata:      mockShard.Resource.ClusterMetadata,
			WorkflowCache:        cache.NewMockCache(ctrl),
			Config:               configs.NewConfig(dynamicconfig.NewNoopCollection(), 1),
			TimeSource:           clock.NewRealTimeSource(),
			MetricsHandler:       metricsHandler,
			TracerProvider:       telemetry.NoopTracerProvider,
			Logger:               log.NewTestLogger(),
			SchedulerRateLimiter: rateLimiter,
			DLQWriter:            nil,
			ExecutorWrapper:      &captureExecutorWrapper{taskCh: taskCh},
			Serializer:           serialization.NewSerializer(),
			RemoteHistoryFetcher: eventhandler.NewMockHistoryPaginatedFetcher(ctrl),
			ChasmEngine:          chasm.NewMockEngine(ctrl),
			ChasmRegistry:        chasmRegistry,
		},
		ClientBean:         client.NewMockBean(ctrl),
		CircuitBreakerPool: cbPool,
		MatchingClient:     nil,
	})

	queue := factory.CreateQueue(mockShard)
	require.NotNil(t, queue)

	factory.Start()
	t.Cleanup(factory.Stop)

	queue.Start()
	t.Cleanup(queue.Stop)

	queue.NotifyNewTasks([]tasks.Task{chasmTask})
	return taskCh
}

// alwaysOpenCircuitBreaker rejects every request and never closes.
type alwaysOpenCircuitBreaker struct{}

func (alwaysOpenCircuitBreaker) Name() string               { return "always-open" }
func (alwaysOpenCircuitBreaker) State() gobreaker.State     { return gobreaker.StateOpen }
func (alwaysOpenCircuitBreaker) Counts() gobreaker.Counts   { return gobreaker.Counts{} }
func (alwaysOpenCircuitBreaker) Allow() (func(bool), error) { return nil, gobreaker.ErrOpenState }

// captureExecutorWrapper intercepts tasks at the executor level.
type captureExecutorWrapper struct {
	taskCh chan<- tasks.Task
}

func (w *captureExecutorWrapper) Wrap(delegate queues.Executor) queues.Executor {
	return &captureExecutor{taskCh: w.taskCh}
}

// captureExecutor captures the first task it sees and sends it to the channel.
type captureExecutor struct {
	taskCh chan<- tasks.Task
}

func (e *captureExecutor) Execute(_ context.Context, executable queues.Executable) queues.ExecuteResponse {
	select {
	case e.taskCh <- executable.GetTask():
	default:
	}
	return queues.ExecuteResponse{}
}
