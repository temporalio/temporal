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
	require.Equal(t, "namespace_replication_lifecycle", NamespaceReplicationLifecyclePayload{}.EventName())
}

func TestNamespaceReplicationTaskContext(t *testing.T) {
	want := NamespaceReplicationTaskContext{
		SourceCluster: "cluster-a",
		TargetCluster: "cluster-b",
		SourceTaskID:  17,
		AttemptCount:  2,
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

	EmitNamespaceReplicationLifecycle(logger, NamespaceReplicationLifecycleInput{
		Phase:         NamespaceReplicationDLQed,
		Task:          task,
		SourceCluster: "cluster-a",
		TargetCluster: "cluster-b",
		SourceTaskID:  &sourceTaskID,
		AttemptCount:  3,
		Error:         errors.New("persistence unavailable"),
	})

	require.Len(t, logger.records, 1)
	require.Equal(t, NamespaceReplicationLifecycleEventName, logger.records[0].EventName())
	got := namespaceReplicationRecordValues(logger.records[0])
	require.Equal(t, map[string]any{
		"phase":            "dlqed",
		"namespace":        "payments",
		"namespace_id":     "namespace-id",
		"operation":        "Create",
		"config_version":   int64(7),
		"failover_version": int64(11),
		"source_cluster":   "cluster-a",
		"target_cluster":   "cluster-b",
		"source_task_id":   int64(0),
		"attempt_count":    int64(3),
		"error":            "persistence unavailable",
		"task_fingerprint": got["task_fingerprint"],
		"task":             got["task"],
	}, got)
	require.Len(t, got["task_fingerprint"], 64)

	var taskJSON map[string]any
	require.NoError(t, json.Unmarshal([]byte(got["task"].(string)), &taskJSON))
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
		Phase: NamespaceReplicationCreated,
		Task:  task,
	})
	EmitNamespaceReplicationLifecycle(logger, NamespaceReplicationLifecycleInput{
		Phase: NamespaceReplicationProcessed,
		Task:  task,
	})
	task.ConfigVersion++
	EmitNamespaceReplicationLifecycle(logger, NamespaceReplicationLifecycleInput{
		Phase: NamespaceReplicationCreated,
		Task:  task,
	})

	require.Len(t, logger.records, 3)
	first := namespaceReplicationRecordValues(logger.records[0])["task_fingerprint"]
	require.Equal(t, first, namespaceReplicationRecordValues(logger.records[1])["task_fingerprint"])
	require.NotEqual(t, first, namespaceReplicationRecordValues(logger.records[2])["task_fingerprint"])
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
		Task:                   namespaceReplicationTaskForTest(),
		CreateNamespaceRequest: request,
	})

	record := namespaceReplicationRecordValues(logger.records[0])
	var got map[string]any
	require.NoError(t, json.Unmarshal([]byte(record["persistence_request"].(string)), &got))
	require.Equal(t, "CreateNamespaceRequest", got["request_type"])
	require.Equal(t, true, got["is_global_namespace"])
	require.NotContains(t, got, "notification_version")
	namespace := got["namespace"].(map[string]any)
	require.Equal(t, "namespace-id", namespace["info"].(map[string]any)["id"])
	require.Equal(t, "0", namespace["config_version"])
}

func TestNamespaceReplicationProcessedIncludesUpdatePersistenceRequest(t *testing.T) {
	logger := &captureLogger{}
	request := &persistence.UpdateNamespaceRequest{
		Namespace: &persistencespb.NamespaceDetail{
			Info:          &persistencespb.NamespaceInfo{Id: "namespace-id", Name: "payments"},
			ConfigVersion: 8,
		},
		IsGlobalNamespace:   true,
		NotificationVersion: 19,
	}

	EmitNamespaceReplicationLifecycle(logger, NamespaceReplicationLifecycleInput{
		Phase:                  NamespaceReplicationProcessed,
		Task:                   namespaceReplicationTaskForTest(),
		UpdateNamespaceRequest: request,
	})

	record := namespaceReplicationRecordValues(logger.records[0])
	var got map[string]any
	require.NoError(t, json.Unmarshal([]byte(record["persistence_request"].(string)), &got))
	require.Equal(t, "UpdateNamespaceRequest", got["request_type"])
	require.Equal(t, float64(19), got["notification_version"])
	require.Equal(t, "8", got["namespace"].(map[string]any)["config_version"])
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
