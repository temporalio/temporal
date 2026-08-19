package replicator

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/embedded"
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
)

type replicationEventCaptureLogger struct {
	embedded.Logger
	records []otellog.Record
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
		nil,                        // logger
		nil,                        // eventLogger
		nil,                        // emitNamespaceLifecycleEvents
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

	received := replicationEventValues(eventLogger.records[0])
	require.Equal(t, int64(42), received["source_task_id"].AsInt64())
	require.Equal(t, "cluster-a", received["source_cluster"].AsString())
	require.Equal(t, "cluster-b", received["target_cluster"].AsString())
	require.True(t, processingContextSet)
	require.Equal(t, wideevents.NamespaceReplicationTaskContext{
		SourceCluster: "cluster-a",
		TargetCluster: "cluster-b",
		SourceTaskID:  42,
		AttemptCount:  1,
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
	dlqed := replicationEventValues(eventLogger.records[1])
	require.Equal(t, int64(1), dlqed["attempt_count"].AsInt64())
	require.Equal(t, "bad task", dlqed["error"].AsString())
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
