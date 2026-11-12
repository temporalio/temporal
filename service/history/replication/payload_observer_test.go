package replication

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/metrics/metricstest"
	ctasks "go.temporal.io/server/common/tasks"
	"go.temporal.io/server/service/history/tests"
	"go.uber.org/mock/gomock"
)

type recordingPayloadObserver struct {
	mu           sync.Mutex
	observations []PayloadObservation
}

func (o *recordingPayloadObserver) ObservePayload(value PayloadObservation) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.observations = append(o.observations, value)
}

func TestPayloadObserverTrackedReceipts(t *testing.T) {
	for _, tc := range []struct {
		name   string
		laneID string
		legacy bool
	}{
		{name: "default lane"},
		{name: "isolated lane", laneID: "isolated"},
		{name: "legacy receiver", legacy: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metadata := cluster.NewMockMetadata(gomock.NewController(t))
			metadata.EXPECT().GetAllClusterInfo().Return(map[string]cluster.ClusterInformation{
				"source": {Enabled: true, InitialFailoverVersion: 1, ShardCount: 512},
			})
			observer := &recordingPayloadObserver{}
			scheduler := &mockScheduler{}
			box := ProcessToolBox{
				Config: tests.NewDynamicConfig(), ClusterMetadata: metadata,
				Logger: log.NewNoopLogger(), MetricsHandler: metrics.NoopMetricsHandler,
				HighPriorityTaskScheduler: scheduler, PayloadObserver: observer,
			}
			receiver := &laneStreamReceiver{
				ProcessToolBox: box, serverShardKey: ClusterShardKey{ClusterID: 1, ShardID: 7},
				clientShardKey:          ClusterShardKey{ClusterID: 2, ShardID: 3},
				highPriorityTaskTracker: NewExecutableTaskTracker(box.Logger, box.MetricsHandler),
				taskConverter:           NewExecutableTaskConverter(box), laneFeedback: NewNoopLaneFeedbackProvider(),
				logger: box.Logger, lanesEnabled: true, maxLanes: 1, memberLanes: make(map[string]*memberLane),
			}
			processMessages := receiver.processMessages
			if tc.legacy {
				box.NamespaceThrottler = NoopNamespaceThrottler{}
				legacy := &StreamReceiverImpl{
					ProcessToolBox: box, serverShardKey: receiver.serverShardKey, clientShardKey: receiver.clientShardKey,
					highPriorityTaskTracker: receiver.highPriorityTaskTracker, taskConverter: receiver.taskConverter,
					logger: box.Logger,
				}
				processMessages = legacy.processMessages
			}
			task := &replicationspb.ReplicationTask{
				SourceTaskId: 10, Priority: enumsspb.TASK_PRIORITY_HIGH,
				TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
				Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
					HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{NamespaceId: "namespace", WorkflowId: "workflow", RunId: "run"},
				},
			}
			messages := &replicationspb.WorkflowReplicationMessages{
				ReplicationTasks: []*replicationspb.ReplicationTask{task}, ExclusiveHighWatermark: 11,
				Priority: enumsspb.TASK_PRIORITY_HIGH,
			}
			if tc.laneID != "" {
				messages.LaneInfo = &replicationspb.ReplicationLaneInfo{LaneId: tc.laneID}
			}
			stream := &mockStream{respChan: make(chan StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse], 2)}
			for range 2 {
				stream.respChan <- StreamResp[*adminservice.StreamWorkflowReplicationMessagesResponse]{
					Resp: &adminservice.StreamWorkflowReplicationMessagesResponse{
						Attributes: &adminservice.StreamWorkflowReplicationMessagesResponse_Messages{Messages: messages},
					},
				}
			}
			close(stream.respChan)
			require.NoError(t, processMessages(stream))
			require.Len(t, scheduler.tasks, 1)
			require.Len(t, observer.observations, 1)
			received := observer.observations[0]
			require.Equal(t, PayloadReceived, received.Phase)
			require.Equal(t, "namespace", received.NamespaceID)
			require.Equal(t, "source", received.Origin.ClusterName)
			require.Equal(t, int32(7), received.Origin.ShardID)
			require.Equal(t, int64(10), received.Origin.TaskID)
			require.Equal(t, receiver.serverShardKey, received.Source)
			require.Equal(t, receiver.clientShardKey, received.Target)
			require.False(t, received.At.IsZero())
			scheduler.tasks[0].Ack()
			require.Len(t, observer.observations, 2)
			require.Equal(t, PayloadAcked, observer.observations[1].Phase)
			require.Equal(t, received.Origin, observer.observations[1].Origin)
			require.Equal(t, received.Source, observer.observations[1].Source)
			require.Equal(t, received.Target, observer.observations[1].Target)
		})
	}
}

func TestPayloadObserverConcurrentAck(t *testing.T) {
	t.Parallel()
	for _, enabled := range []bool{false, true} {
		name := "observer absent"
		if enabled {
			name = "observer enabled"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			handler := metricstest.NewCaptureHandler()
			capture := handler.StartCapture()
			defer handler.StopCapture(capture)
			observer := &recordingPayloadObserver{}
			box := ProcessToolBox{MetricsHandler: handler}
			if enabled {
				box.PayloadObserver = observer
			}
			task := NewExecutableTask(box,
				10, "test", time.Now(), time.Now(), "source", ClusterShardKey{ShardID: 7},
				&replicationspb.ReplicationTask{SourceTaskId: 10, Priority: enumsspb.TASK_PRIORITY_HIGH,
					RawTaskInfo: &persistencespb.ReplicationTaskInfo{NamespaceId: "namespace"}})
			task.MarkExecutionStart()
			start := make(chan struct{})
			var callers sync.WaitGroup
			for range 32 {
				callers.Go(func() { <-start; task.Ack() })
			}
			close(start)
			callers.Wait()
			task.Ack()
			require.Equal(t, ctasks.TaskStateAcked, task.State())
			if enabled {
				require.Len(t, observer.observations, 1)
				require.Equal(t, PayloadAcked, observer.observations[0].Phase)
			} else {
				require.Empty(t, observer.observations)
			}
			for _, name := range []string{
				metrics.ReplicationTaskQueueLatency.Name(), metrics.ReplicationTaskProcessingLatency.Name(),
				metrics.ReplicationLatency.Name(), metrics.ReplicationTaskTransmissionLatency.Name(), metrics.ReplicationTasksAttempt.Name(),
			} {
				require.Len(t, capture.Snapshot()[name], 1, "completion metric %s must be emitted once", name)
			}
		})
	}
}

func TestPayloadObserverUnsuccessfulTasks(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		finish func(*ExecutableTaskImpl)
		state  ctasks.State
	}{
		{name: "cancelled", finish: (*ExecutableTaskImpl).Cancel, state: ctasks.TaskStateCancelled},
		{name: "aborted", finish: (*ExecutableTaskImpl).Abort, state: ctasks.TaskStateAborted},
		{name: "failed", finish: func(task *ExecutableTaskImpl) { task.Nack(errors.New("apply failed")) }, state: ctasks.TaskStateNacked},
		{name: "retry pending", finish: (*ExecutableTaskImpl).Reschedule, state: ctasks.TaskStatePending},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			observer := &recordingPayloadObserver{}
			logger := log.NewNoopLogger()
			task := NewExecutableTask(ProcessToolBox{
				Config: tests.NewDynamicConfig(), Logger: logger, ThrottledLogger: logger,
				MetricsHandler: metrics.NoopMetricsHandler, PayloadObserver: observer,
			}, 10, "test", time.Now(), time.Now(), "source", ClusterShardKey{ShardID: 7},
				&replicationspb.ReplicationTask{SourceTaskId: 10, Priority: enumsspb.TASK_PRIORITY_HIGH,
					RawTaskInfo: &persistencespb.ReplicationTaskInfo{NamespaceId: "namespace"}})
			tc.finish(task)
			require.Equal(t, tc.state, task.State())
			require.Empty(t, observer.observations)
			task.Ack()
			if tc.state == ctasks.TaskStatePending {
				require.Len(t, observer.observations, 1)
				require.Equal(t, PayloadAcked, observer.observations[0].Phase)
				require.Equal(t, 2, task.Attempt())
			} else {
				require.Empty(t, observer.observations)
				require.Equal(t, tc.state, task.State())
			}
		})
	}
}

func TestPayloadObserverScope(t *testing.T) {
	for _, tc := range []struct {
		name     string
		priority enumsspb.TaskPriority
		id       string
	}{
		{name: "low", priority: enumsspb.TASK_PRIORITY_LOW, id: "namespace"},
		{name: "unknown namespace", priority: enumsspb.TASK_PRIORITY_HIGH},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observer := &recordingPayloadObserver{}
			task := &replicationspb.ReplicationTask{Priority: tc.priority,
				RawTaskInfo: &persistencespb.ReplicationTaskInfo{NamespaceId: tc.id}}
			observePayload(observer, task, "source", 7, PayloadReceived, time.Now())
			require.Empty(t, observer.observations)
		})
	}
	observePayload(nil, nil, "", 0, PayloadReceived, time.Now())
	require.Nil(t, payloadObserverForRoute(nil, ClusterShardKey{}, ClusterShardKey{}))
}

func TestPayloadObserverConverterKeepsConcurrentRoutesIndependent(t *testing.T) {
	t.Parallel()
	observer := &recordingPayloadObserver{}
	converter := NewExecutableTaskConverter(ProcessToolBox{Config: tests.NewDynamicConfig(),
		Logger: log.NewNoopLogger(), MetricsHandler: metrics.NoopMetricsHandler, PayloadObserver: observer})
	source := ClusterShardKey{ClusterID: 1, ShardID: 7}
	targets := []ClusterShardKey{{ClusterID: 2, ShardID: 3}, {ClusterID: 3, ShardID: 4}}
	var callers sync.WaitGroup
	for i, target := range targets {
		callers.Go(func() {
			task := &replicationspb.ReplicationTask{SourceTaskId: int64(i + 1), Priority: enumsspb.TASK_PRIORITY_HIGH,
				TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_V2_TASK,
				Attributes: &replicationspb.ReplicationTask_HistoryTaskAttributes{
					HistoryTaskAttributes: &replicationspb.HistoryTaskAttributes{NamespaceId: "namespace", WorkflowId: "workflow", RunId: "run"}}}
			converted := converter.Convert("source", target, source, task)
			converted[0].Ack()
		})
	}
	callers.Wait()
	require.Len(t, observer.observations, len(targets))
	for _, value := range observer.observations {
		require.Equal(t, source, value.Source)
		require.Equal(t, targets[value.Origin.TaskID-1], value.Target)
	}
}
