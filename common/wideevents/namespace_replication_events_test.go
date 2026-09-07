package wideevents

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/log"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/persistence"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestNamespaceReplicationLifecycleEventName(t *testing.T) {
	require.Equal(t, NamespaceLifecycleEventName, NamespaceReplicationLifecyclePayload{}.EventName())
}

func TestNamespaceReplicationTaskContext(t *testing.T) {
	task := namespaceReplicationTaskForTest()
	want := NamespaceReplicationTaskContext{
		SourceCluster: "cluster-a",
		TargetCluster: "cluster-b",
		SourceTaskID:  17,
		AttemptCount:  2,
		EventData:     namespaceReplicationEventDataForTest(t, task),
	}

	_, ok := NamespaceReplicationTaskContextFromContext(context.Background())
	require.False(t, ok)
	got, ok := NamespaceReplicationTaskContextFromContext(
		SetNamespaceReplicationTaskContext(context.Background(), want),
	)
	require.True(t, ok)
	require.Equal(t, want, got)
}

func TestEmitNamespaceReplicationLifecycle(t *testing.T) {
	logger := &captureLogger{}
	sourceTaskID := int64(0)
	task := namespaceReplicationTaskForTest()
	eventData := namespaceReplicationEventDataForTest(t, task)
	eventData.Details = map[string]any{"custom_detail": "custom-value"}

	EmitNamespaceReplicationLifecycle(logger, NamespaceReplicationLifecycleInput{
		Phase:         NamespaceReplicationDLQed,
		EventData:     eventData,
		SourceCluster: "cluster-a",
		TargetCluster: "cluster-b",
		SourceTaskID:  &sourceTaskID,
		AttemptCount:  3,
		Error:         errors.New("persistence unavailable"),
	})

	require.Len(t, logger.records, 1)
	require.Equal(t, NamespaceLifecycleEventName, logger.records[0].EventName())
	got := namespaceReplicationRecordValues(logger.records[0])
	require.Equal(t, map[string]any{
		"phase":        "dlqed",
		"namespace":    "payments",
		"namespace_id": "namespace-id",
		"details":      got["details"],
	}, got)
	details := namespaceReplicationRecordDetails(t, logger.records[0])
	require.InDelta(t, float64(enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK), details["task_type"], 0)
	require.Equal(t, "namespace", details["task_kind"])
	require.Equal(t, "Create", details["operation"])
	require.InDelta(t, float64(7), details["config_version"], 0)
	require.InDelta(t, float64(11), details["failover_version"], 0)
	require.Equal(t, "cluster-a", details["source_cluster"])
	require.Equal(t, "cluster-b", details["target_cluster"])
	require.InDelta(t, float64(0), details["source_task_id"], 0)
	require.InDelta(t, float64(3), details["attempt_count"], 0)
	require.Equal(t, "persistence unavailable", details["error"])
	require.Equal(t, "custom-value", details["custom_detail"])
	require.Len(t, details["task_fingerprint"], 64)

	taskJSON := details["task"].(map[string]any)
	require.Equal(t, "NAMESPACE_OPERATION_CREATE", taskJSON["namespace_operation"])
	require.Equal(t, "namespace-id", taskJSON["id"])
	require.Equal(t, "7", taskJSON["config_version"])
	require.Equal(t, "11", taskJSON["failover_version"])
	require.Equal(t, "payments", taskJSON["info"].(map[string]any)["name"])
	require.Equal(t, "cluster-a", taskJSON["replication_config"].(map[string]any)["active_cluster_name"])
}

func TestNamespaceReplicationTaskFingerprintLinksPhases(t *testing.T) {
	logger := &captureLogger{}
	task := namespaceReplicationTaskForTest()

	EmitNamespaceReplicationLifecycle(logger, NamespaceReplicationLifecycleInput{
		Phase:     NamespaceReplicationCreated,
		EventData: namespaceReplicationEventDataForTest(t, task),
	})
	EmitNamespaceReplicationLifecycle(logger, NamespaceReplicationLifecycleInput{
		Phase:     NamespaceReplicationProcessed,
		Outcome:   NamespaceReplicationOutcomeCreated,
		EventData: namespaceReplicationEventDataForTest(t, task),
	})
	task.ConfigVersion++
	EmitNamespaceReplicationLifecycle(logger, NamespaceReplicationLifecycleInput{
		Phase:     NamespaceReplicationCreated,
		EventData: namespaceReplicationEventDataForTest(t, task),
	})

	require.Len(t, logger.records, 3)
	first := namespaceReplicationRecordDetails(t, logger.records[0])["task_fingerprint"]
	require.Equal(t, first, namespaceReplicationRecordDetails(t, logger.records[1])["task_fingerprint"])
	require.NotEqual(t, first, namespaceReplicationRecordDetails(t, logger.records[2])["task_fingerprint"])
}

func TestNamespaceReplicationProcessedIncludesCreatePersistenceRequest(t *testing.T) {
	logger := &captureLogger{}
	request := &persistence.CreateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{Id: "namespace-id", Name: "payments"},
		},
		IsGlobalNamespace: true,
	}

	EmitNamespaceReplicationLifecycle(logger, NamespaceReplicationLifecycleInput{
		Phase:                  NamespaceReplicationProcessed,
		Outcome:                NamespaceReplicationOutcomeCreated,
		EventData:              namespaceReplicationEventDataForTest(t, namespaceReplicationTaskForTest()),
		CreateNamespaceRequest: request,
	})

	details := namespaceReplicationRecordDetails(t, logger.records[0])
	got := details["persistence_request"].(map[string]any)
	require.Equal(t, "CreateNamespaceRequest", got["request_type"])
	require.Equal(t, true, got["is_global_namespace"])
	require.NotContains(t, got, "notification_version")
	namespace := got["namespace"].(map[string]any)
	require.Equal(t, "namespace-id", namespace["info"].(map[string]any)["id"])
	require.Equal(t, "0", namespace["config_version"])
}

func TestNamespaceReplicationProcessedIncludesUpdatePersistenceRequest(t *testing.T) {
	logger := &captureLogger{}
	localNamespacePreMutation := &persistencespb.NamespaceDetail{
		Info:          &persistencespb.NamespaceInfo{Id: "namespace-id", Name: "payments"},
		ConfigVersion: 7,
	}
	request := &persistence.UpdateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info:          &persistencespb.NamespaceInfo{Id: "namespace-id", Name: "payments"},
			ConfigVersion: 8,
		},
		IsGlobalNamespace:   true,
		NotificationVersion: 19,
	}

	EmitNamespaceReplicationLifecycle(logger, NamespaceReplicationLifecycleInput{
		Phase:                     NamespaceReplicationProcessed,
		Outcome:                   NamespaceReplicationOutcomeUpdated,
		EventData:                 namespaceReplicationEventDataForTest(t, namespaceReplicationTaskForTest()),
		LocalNamespacePreMutation: localNamespacePreMutation,
		UpdateNamespaceRequest:    request,
	})

	details := namespaceReplicationRecordDetails(t, logger.records[0])
	got := details["persistence_request"].(map[string]any)
	require.Equal(t, "UpdateNamespaceRequest", got["request_type"])
	require.InDelta(t, float64(19), got["notification_version"].(float64), 0)
	require.Equal(t, "8", got["namespace"].(map[string]any)["config_version"])

	before := details["local_namespace_pre_mutation"].(map[string]any)
	require.Equal(t, "7", before["config_version"])
	require.Equal(t, "payments", before["info"].(map[string]any)["name"])
}

func TestDefaultNamespaceReplicationTaskEventDataProvider(t *testing.T) {
	provider := NewDefaultNamespaceReplicationTaskEventDataProvider()
	attributes := namespaceReplicationTaskForTest()
	task := &replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
		Attributes: &replicationspb.ReplicationTask_NamespaceTaskAttributes{
			NamespaceTaskAttributes: attributes,
		},
	}

	eventData, ok := provider.Extract(task)
	require.True(t, ok)
	require.Equal(t, int32(enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK), eventData.TaskType)
	require.Equal(t, "namespace", eventData.TaskKind)
	require.Equal(t, "payments", eventData.Namespace)
	require.Equal(t, "namespace-id", eventData.NamespaceID)
	require.Equal(t, "Create", eventData.Operation)
	require.Equal(t, int64(7), *eventData.ConfigVersion)
	require.Equal(t, int64(11), *eventData.FailoverVersion)
	require.Same(t, attributes, eventData.TaskPayload)

	_, ok = provider.Extract(&replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_TASK_QUEUE_USER_DATA,
	})
	require.False(t, ok)
}

func namespaceReplicationEventDataForTest(
	t *testing.T,
	attributes *replicationspb.NamespaceTaskAttributes,
) NamespaceReplicationTaskEventData {
	t.Helper()
	eventData, ok := NewDefaultNamespaceReplicationTaskEventDataProvider().Extract(&replicationspb.ReplicationTask{
		TaskType: enumsspb.REPLICATION_TASK_TYPE_NAMESPACE_TASK,
		Attributes: &replicationspb.ReplicationTask_NamespaceTaskAttributes{
			NamespaceTaskAttributes: attributes,
		},
	})
	require.True(t, ok)
	return eventData
}

func namespaceReplicationTaskForTest() *replicationspb.NamespaceTaskAttributes {
	return &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: enumsspb.NAMESPACE_OPERATION_CREATE,
		Id:                 "namespace-id",
		Info: &namespacepb.NamespaceInfo{
			Name:        "payments",
			State:       enumspb.NAMESPACE_STATE_REGISTERED,
			Description: "payment workflows",
			OwnerEmail:  "payments@example.com",
			Data:        map[string]string{"region": "west"},
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: durationpb.New(72 * time.Hour),
			HistoryArchivalState:          enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalState:       enumspb.ARCHIVAL_STATE_DISABLED,
			CustomSearchAttributeAliases:  map[string]string{"OrderId": "order_id"},
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: "cluster-a",
			Clusters: []*replicationpb.ClusterReplicationConfig{
				{ClusterName: "cluster-a"},
				{ClusterName: "cluster-b"},
			},
			State: enumspb.REPLICATION_STATE_NORMAL,
		},
		ConfigVersion:   7,
		FailoverVersion: 11,
		FailoverHistory: []*replicationpb.FailoverStatus{{FailoverVersion: 11}},
	}
}

func namespaceReplicationRecordValues(record log.Record) map[string]any {
	values := make(map[string]any)
	record.WalkAttributes(func(kv log.KeyValue) bool {
		switch kv.Value.Kind() {
		case log.KindString:
			values[kv.Key] = kv.Value.AsString()
		case log.KindInt64:
			values[kv.Key] = kv.Value.AsInt64()
		default:
		}
		return true
	})
	return values
}

func namespaceReplicationRecordDetails(t *testing.T, record log.Record) map[string]any {
	t.Helper()
	detailsJSON, ok := namespaceReplicationRecordValues(record)["details"].(string)
	require.True(t, ok)
	var details map[string]any
	require.NoError(t, json.Unmarshal([]byte(detailsJSON), &details))
	return details
}
