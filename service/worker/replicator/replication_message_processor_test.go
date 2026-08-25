package replicator

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/embedded"
	commonpb "go.temporal.io/api/common/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/adminservice/v1"
	"go.temporal.io/server/api/adminservicemock/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/wideevents"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type replicationEventCaptureLogger struct {
	embedded.Logger
	records []otellog.Record
}

type customNamespaceReplicationTaskEventDataProvider struct{}

func (customNamespaceReplicationTaskEventDataProvider) Extract(
	task *replicationspb.ReplicationTask,
) (wideevents.NamespaceReplicationTaskEventData, bool) {
	if int32(task.GetTaskType()) != 1002 {
		return wideevents.NamespaceReplicationTaskEventData{}, false
	}
	return wideevents.NamespaceReplicationTaskEventData{
		TaskType:            int32(task.GetTaskType()),
		TaskKind:            "custom_namespace_config",
		NamespaceID:         "custom-namespace-id",
		Operation:           "update_config",
		TaskPayload:         wrapperspb.Bytes(task.GetData().GetData()),
		TaskFingerprintData: task.GetData().GetData(),
	}, true
}

func (l *replicationEventCaptureLogger) Emit(_ context.Context, record otellog.Record) {
	l.records = append(l.records, record)
}

func (l *replicationEventCaptureLogger) Enabled(context.Context, otellog.EnabledParameters) bool {
	return true
}

// TestRetryPolicyForTask verifies that the processor's per-task retry-policy
// selector hands out a distinct policy for namespace tasks (which need the
// wider CAS-tolerant budget) and a shared default policy for all other task
// types.
func TestRetryPolicyForTask(t *testing.T) {
	p := newReplicationMessageProcessor(
		"currentCluster",
		"sourceCluster",
		nil, // logger
		nil, // eventLogger
		dynamicconfig.GetBoolPropertyFn(false),
		wideevents.NewDefaultNamespaceReplicationTaskEventDataProvider(),
		nil,                        // remotePeer
		metrics.NoopMetricsHandler, // metricsHandler — actually used by constructor
		nil,                        // namespaceTaskExecutor
		nil,                        // customTaskHandler
		nil,                        // hostInfo
		nil,                        // serviceResolver
		nil,                        // namespaceReplicationQueue
		nil,                        // matchingClient
		nil,                        // namespaceRegistry
	)

	nsTask := &replicationspb.ReplicationTask{TaskType: enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK}
	tqTask := &replicationspb.ReplicationTask{TaskType: enumsspb.REPLICATION_TASK_TYPE_TASK_QUEUE_USER_DATA}
	historyTask := &replicationspb.ReplicationTask{TaskType: enumsspb.REPLICATION_TASK_TYPE_HISTORY_TASK}
	unspecifiedTask := &replicationspb.ReplicationTask{TaskType: enumsspb.REPLICATION_TASK_TYPE_UNSPECIFIED}

	nsPolicy := p.retryPolicyForTask(nsTask)
	defaultPolicy := p.retryPolicyForTask(tqTask)

	require.NotSame(t, nsPolicy, defaultPolicy,
		"namespace task must use a different retry policy from other task types")

	// All non-namespace task types share the same default policy instance.
	require.Same(t, defaultPolicy, p.retryPolicyForTask(historyTask))
	require.Same(t, defaultPolicy, p.retryPolicyForTask(unspecifiedTask))

	// Selector is stable across calls.
	require.Same(t, nsPolicy, p.retryPolicyForTask(nsTask))
}

func TestHandleNamespaceReplicationTaskEmitsReceivedAndPassesProcessingContext(t *testing.T) {
	p, task, executor, _, eventLogger := newReplicationEventTestProcessor(t, true, 2)
	var processingContext wideevents.NamespaceReplicationTaskContext
	var processingContextSet bool
	executor.EXPECT().Execute(gomock.Any(), task.GetNamespaceTaskAttributes()).DoAndReturn(
		func(ctx context.Context, _ *replicationspb.NamespaceTaskAttributes) error {
			processingContext, processingContextSet = wideevents.NamespaceReplicationTaskContextFromContext(ctx)
			return nil
		},
	)

	p.handleReplicationTasks()
	require.Equal(t, []string{"received"}, replicationEventPhases(eventLogger.records))

	received := replicationEventDetails(t, eventLogger.records[0])
	require.InDelta(t, float64(42), received["source_task_id"], 0)
	require.Equal(t, "cluster-a", received["source_cluster"])
	require.Equal(t, "cluster-b", received["target_cluster"])
	require.True(t, processingContextSet)
	eventData, ok := wideevents.NewDefaultNamespaceReplicationTaskEventDataProvider().Extract(task)
	require.True(t, ok)
	require.Equal(t, wideevents.NamespaceReplicationTaskContext{
		SourceCluster: "cluster-a",
		TargetCluster: "cluster-b",
		SourceTaskID:  42,
		AttemptCount:  1,
		EventData:     eventData,
	}, processingContext)
}

func TestHandleNamespaceReplicationTaskCountsRetries(t *testing.T) {
	p, _, executor, _, eventLogger := newReplicationEventTestProcessor(t, true, 2)
	attempt := 0
	var processingContext wideevents.NamespaceReplicationTaskContext
	executor.EXPECT().Execute(gomock.Any(), gomock.Any()).Times(2).DoAndReturn(
		func(ctx context.Context, _ *replicationspb.NamespaceTaskAttributes) error {
			attempt++
			processingContext, _ = wideevents.NamespaceReplicationTaskContextFromContext(ctx)
			if attempt == 1 {
				return serviceerror.NewUnavailable("retry")
			}
			return nil
		},
	)

	p.handleReplicationTasks()
	require.Equal(t, []string{"received"}, replicationEventPhases(eventLogger.records))
	require.Equal(t, 2, processingContext.AttemptCount)
}

func TestHandleNamespaceReplicationTaskEmitsDLQed(t *testing.T) {
	p, task, executor, queue, eventLogger := newReplicationEventTestProcessor(t, true, 1)
	executor.EXPECT().Execute(gomock.Any(), task.GetNamespaceTaskAttributes()).Return(serviceerror.NewInvalidArgument("bad task"))
	queue.EXPECT().PublishToDLQ(gomock.Any(), task).Return(nil)

	p.handleReplicationTasks()
	require.Equal(t, []string{"received", "dlqed"}, replicationEventPhases(eventLogger.records))
	dlqed := replicationEventDetails(t, eventLogger.records[1])
	require.InDelta(t, float64(1), dlqed["attempt_count"], 0)
	require.Equal(t, "bad task", dlqed["error"])
	require.NotContains(t, dlqed, "persistence_request")
}

func TestHandleNamespaceReplicationTaskEventsDisabled(t *testing.T) {
	p, task, executor, _, eventLogger := newReplicationEventTestProcessor(t, false, 1)
	processingContextSet := false
	executor.EXPECT().Execute(gomock.Any(), task.GetNamespaceTaskAttributes()).DoAndReturn(
		func(ctx context.Context, _ *replicationspb.NamespaceTaskAttributes) error {
			_, processingContextSet = wideevents.NamespaceReplicationTaskContextFromContext(ctx)
			return nil
		},
	)

	p.handleReplicationTasks()
	require.Empty(t, eventLogger.records)
	require.False(t, processingContextSet)
}

func TestCustomNamespaceReplicationTaskUsesEventDataProvider(t *testing.T) {
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.ReplicationTaskType(1002),
		Data:     &commonpb.DataBlob{Data: []byte("custom-task")},
	}
	p := &replicationMessageProcessor{
		emitNamespaceLifecycleEvents: dynamicconfig.GetBoolPropertyFn(true),
		eventDataProvider:            customNamespaceReplicationTaskEventDataProvider{},
	}

	eventData, ok := p.namespaceReplicationEventData(task)
	require.True(t, ok)
	require.Equal(t, int32(1002), eventData.TaskType)
	require.Equal(t, "custom_namespace_config", eventData.TaskKind)
	require.Equal(t, "custom-namespace-id", eventData.NamespaceID)
	require.Equal(t, "update_config", eventData.Operation)
	require.Equal(t, []byte("custom-task"), eventData.TaskFingerprintData)
}

func newReplicationEventTestProcessor(
	t *testing.T,
	enabled bool,
	maximumAttempts int,
) (*replicationMessageProcessor, *replicationspb.ReplicationTask, *nsreplication.MockTaskExecutor, *persistence.MockNamespaceReplicationQueue, *replicationEventCaptureLogger) {
	t.Helper()
	controller := gomock.NewController(t)
	executor := nsreplication.NewMockTaskExecutor(controller)
	queue := persistence.NewMockNamespaceReplicationQueue(controller)
	remotePeer := adminservicemock.NewMockAdminServiceClient(controller)
	serviceResolver := membership.NewMockServiceResolver(controller)
	hostInfo := membership.NewHostInfoFromAddress("worker")
	eventLogger := &replicationEventCaptureLogger{}
	task := namespaceReplicationTaskForProcessorTest()
	policy := backoff.NewExponentialRetryPolicy(time.Millisecond).WithMaximumAttempts(maximumAttempts)
	serviceResolver.EXPECT().Lookup("cluster-a").Return(hostInfo, nil)
	remotePeer.EXPECT().GetNamespaceReplicationMessages(gomock.Any(), gomock.Any()).Return(
		&adminservice.GetNamespaceReplicationMessagesResponse{
			Messages: &replicationspb.ReplicationMessages{
				ReplicationTasks:       []*replicationspb.ReplicationTask{task},
				LastRetrievedMessageId: task.GetSourceTaskId(),
			},
		},
		nil,
	)
	p := &replicationMessageProcessor{
		hostInfo:                     hostInfo,
		serviceResolver:              serviceResolver,
		currentCluster:               "cluster-b",
		sourceCluster:                "cluster-a",
		logger:                       log.NewNoopLogger(),
		eventLogger:                  eventLogger,
		emitNamespaceLifecycleEvents: dynamicconfig.GetBoolPropertyFn(enabled),
		eventDataProvider:            wideevents.NewDefaultNamespaceReplicationTaskEventDataProvider(),
		remotePeer:                   remotePeer,
		namespaceTaskExecutor:        executor,
		metricsHandler:               metrics.NoopMetricsHandler,
		retryPolicyForTask:           func(*replicationspb.ReplicationTask) backoff.RetryPolicy { return policy },
		lastProcessedMessageID:       -1,
		lastRetrievedMessageID:       -1,
		namespaceReplicationQueue:    queue,
	}
	return p, task, executor, queue, eventLogger
}

func namespaceReplicationTaskForProcessorTest() *replicationspb.ReplicationTask {
	return &replicationspb.ReplicationTask{
		TaskType:     enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
		SourceTaskId: 42,
		Attributes: &replicationspb.ReplicationTask_NamespaceTaskAttributes{
			NamespaceTaskAttributes: &replicationspb.NamespaceTaskAttributes{
				NamespaceOperation: enumsspb.NAMESPACE_OPERATION_UPDATE,
				Id:                 "namespace-id",
				Info:               &namespacepb.NamespaceInfo{Name: "payments"},
				Config:             &namespacepb.NamespaceConfig{},
			},
		},
	}
}

func replicationEventPhases(records []otellog.Record) []string {
	phases := make([]string, 0, len(records))
	for _, record := range records {
		phases = append(phases, replicationEventValues(record)["phase"].AsString())
	}
	return phases
}

func replicationEventValues(record otellog.Record) map[string]otellog.Value {
	values := make(map[string]otellog.Value)
	record.WalkAttributes(func(kv otellog.KeyValue) bool {
		values[kv.Key] = kv.Value
		return true
	})
	return values
}

func replicationEventDetails(t *testing.T, record otellog.Record) map[string]any {
	t.Helper()
	var details map[string]any
	require.NoError(t, json.Unmarshal([]byte(replicationEventValues(record)["details"].AsString()), &details))
	return details
}
