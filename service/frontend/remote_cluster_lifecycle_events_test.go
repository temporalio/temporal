package frontend

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/log/embedded"
	"go.temporal.io/server/api/adminservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/wideevents"
	"go.uber.org/mock/gomock"
)

type captureRemoteClusterEventLogger struct {
	embedded.Logger
	records []otellog.Record
}

type panicRemoteClusterEventLogger struct {
	embedded.Logger
}

func (l *captureRemoteClusterEventLogger) Emit(_ context.Context, record otellog.Record) {
	l.records = append(l.records, record)
}

func (l *captureRemoteClusterEventLogger) Enabled(context.Context, otellog.EnabledParameters) bool {
	return true
}

func (*panicRemoteClusterEventLogger) Emit(context.Context, otellog.Record) {
	panic("event logger panic")
}

func (*panicRemoteClusterEventLogger) Enabled(context.Context, otellog.EnabledParameters) bool {
	return true
}

func TestRemoteClusterLifecycleUpsertCreated(t *testing.T) {
	ctrl := gomock.NewController(t)
	metadata := cluster.NewMockMetadata(ctrl)
	metadata.EXPECT().GetCurrentClusterName().Return("cluster-a")
	logger := &captureRemoteClusterEventLogger{}
	ctx := headers.SetCallerInfo(context.Background(), headers.NewCallerInfo("", "operator", "AddOrUpdateRemoteCluster"))
	ctx = context.WithValue(ctx, authorization.MappedClaims, &authorization.Claims{
		Subject:  "alice",
		AuthType: "jwt",
	})
	request := remoteClusterUpsertRequestFields{
		FrontendAddress:               "cluster-b:7233",
		FrontendHTTPAddress:           "cluster-b:7243",
		EnableRemoteClusterConnection: true,
		EnableReplication:             true,
	}
	event := newRemoteClusterUpsertLifecycleEvent(
		ctx,
		logger,
		metadata,
		remoteClusterLifecycleTestConfig(true),
		remoteClusterAPIOperator,
		request,
	)
	saveRequest := &persistence.SaveClusterMetadataRequest{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			ClusterName:          "cluster-b",
			ClusterId:            "cluster-b-id",
			ClusterAddress:       "cluster-b:7233",
			IsConnectionEnabled:  true,
			IsReplicationEnabled: true,
			Tags:                 map[string]string{"environment": "test"},
		},
		Version: 0,
	}
	event.emitUpsertSuccess(nil, saveRequest)

	attrs, details := remoteClusterEventValues(t, logger.records)
	require.Equal(t, wideevents.PhaseRemoteClusterUpsert, attrs["phase"])
	require.Equal(t, "N/A", attrs["namespace"])
	require.Equal(t, "N/A", attrs["namespace_id"])
	require.Equal(t, remoteClusterOutcomeSucceeded, details["outcome"])
	require.Equal(t, remoteClusterMutationCreated, details["mutation"])
	require.Equal(t, remoteClusterTransitionInitializedEnabled, details["requested_connection_transition"])
	require.Equal(t, remoteClusterTransitionInitializedEnabled, details["requested_replication_transition"])
	require.Equal(t, "cluster-b", details["remote_cluster"])
	require.Equal(t, "cluster-b-id", details["remote_cluster_id"])
	require.Nil(t, details["persisted_before"])
	require.Equal(t, "operator", details["caller_type"])
	require.Equal(t, "AddOrUpdateRemoteCluster", details["call_origin"])
	require.Equal(t, "alice", details["auth_subject"])
	require.Equal(t, "jwt", details["auth_type"])
	require.NotEmpty(t, details["request_fingerprint"])
	require.NotNil(t, details["persistence_request"])
}

func TestRemoteClusterLifecycleUpsertUpdatedFailure(t *testing.T) {
	ctrl := gomock.NewController(t)
	metadata := cluster.NewMockMetadata(ctrl)
	metadata.EXPECT().GetCurrentClusterName().Return("cluster-a")
	logger := &captureRemoteClusterEventLogger{}
	event := newRemoteClusterUpsertLifecycleEvent(
		context.Background(),
		logger,
		metadata,
		remoteClusterLifecycleTestConfig(true),
		remoteClusterAPIAdmin,
		remoteClusterUpsertRequestFields{
			EnableRemoteClusterConnection: false,
			EnableReplication:             true,
		},
	)
	persistedBefore := &persistence.GetClusterMetadataResponse{
		ClusterMetadata: &persistencespb.ClusterMetadata{
			IsConnectionEnabled:  true,
			IsReplicationEnabled: false,
		},
		Version: 7,
	}
	remoteResponse := &adminservice.DescribeClusterResponse{
		ClusterName: "cluster-b",
		ClusterId:   "cluster-b-id",
	}
	retError := errors.New("save failed")
	event.emitUpsertFailure(retError, remoteResponse, persistedBefore, nil)

	_, details := remoteClusterEventValues(t, logger.records)
	require.Equal(t, remoteClusterOutcomeFailed, details["outcome"])
	require.Equal(t, remoteClusterMutationUpdated, details["mutation"])
	require.Equal(t, remoteClusterTransitionDisabled, details["requested_connection_transition"])
	require.Equal(t, remoteClusterTransitionEnabled, details["requested_replication_transition"])
	require.Equal(t, "cluster-b", details["remote_cluster"])
	require.Equal(t, "cluster-b-id", details["remote_cluster_id"])
	require.NotContains(t, details, "failure_stage")
	require.Equal(t, "Unknown", details["error_code"])
	require.Equal(t, "save failed", details["error"])
	persistedBeforeDetails, ok := details["persisted_before"].(map[string]any)
	require.True(t, ok)
	require.InDelta(t, 7, persistedBeforeDetails["version"], 0)
}

func TestRemoteClusterLifecycleRemoveCachedBefore(t *testing.T) {
	ctrl := gomock.NewController(t)
	metadata := cluster.NewMockMetadata(ctrl)
	metadata.EXPECT().GetCurrentClusterName().Return("cluster-a")
	logger := &captureRemoteClusterEventLogger{}
	event := newRemoteClusterRemoveLifecycleEvent(
		context.Background(),
		logger,
		metadata,
		remoteClusterLifecycleTestConfig(true),
		remoteClusterAPIOperator,
		remoteClusterRemoveRequestFields{ClusterName: "cluster-b"},
	)
	cachedBefore := cluster.ClusterInformation{
		Enabled:            true,
		ReplicationEnabled: true,
		ClusterID:          "cluster-b-id",
		Tags:               map[string]string{"environment": "test"},
	}
	deleteRequest := &persistence.DeleteClusterMetadataRequest{ClusterName: "cluster-b"}
	event.emitRemoveSuccess(cachedRemoteClusterLookup{
		information:     cachedBefore,
		lookupPerformed: true,
		found:           true,
	}, deleteRequest)

	_, details := remoteClusterEventValues(t, logger.records)
	require.Equal(t, remoteClusterMutationRemoved, details["mutation"])
	require.Equal(t, "cluster-b", details["remote_cluster"])
	require.Equal(t, "cluster-b-id", details["remote_cluster_id"])
	require.NotNil(t, details["cached_before"])
	require.NotNil(t, details["persistence_request"])
	require.NotContains(t, details, "requested_connection_transition")
	require.NotContains(t, details, "requested_replication_transition")
}

func TestRemoteClusterLifecycleRemoveCacheMiss(t *testing.T) {
	ctrl := gomock.NewController(t)
	metadata := cluster.NewMockMetadata(ctrl)
	metadata.EXPECT().GetCurrentClusterName().Return("cluster-a")
	logger := &captureRemoteClusterEventLogger{}
	event := newRemoteClusterRemoveLifecycleEvent(
		context.Background(),
		logger,
		metadata,
		remoteClusterLifecycleTestConfig(true),
		remoteClusterAPIAdmin,
		remoteClusterRemoveRequestFields{ClusterName: "missing-cluster"},
	)
	retError := errors.New("blocked")
	event.emitRemoveFailure(retError, cachedRemoteClusterLookup{lookupPerformed: true}, nil)

	_, details := remoteClusterEventValues(t, logger.records)
	require.Nil(t, details["cached_before"])
	require.Equal(t, remoteClusterMutationUnknown, details["mutation"])
	require.NotContains(t, details, "persistence_request")
}

func TestRemoteClusterLifecycleRequestFingerprint(t *testing.T) {
	request := remoteClusterUpsertRequestFields{
		FrontendAddress:               "cluster-b:7233",
		EnableRemoteClusterConnection: true,
	}
	operatorFingerprint := remoteClusterRequestFingerprint(
		"cluster-a",
		remoteClusterAPIOperator,
		wideevents.PhaseRemoteClusterUpsert,
		request,
	)

	require.Equal(t, operatorFingerprint, remoteClusterRequestFingerprint(
		"cluster-a",
		remoteClusterAPIOperator,
		wideevents.PhaseRemoteClusterUpsert,
		request,
	))
	require.NotEqual(t, operatorFingerprint, remoteClusterRequestFingerprint(
		"cluster-a",
		remoteClusterAPIAdmin,
		wideevents.PhaseRemoteClusterUpsert,
		request,
	))
}

func TestRemoteClusterLifecycleEventsEnabled(t *testing.T) {
	require.False(t, remoteClusterLifecycleEventsEnabled(nil))
	require.False(t, remoteClusterLifecycleEventsEnabled(&Config{}))
	require.False(t, remoteClusterLifecycleEventsEnabled(&Config{
		EmitNamespaceLifecycleEvents: dynamicconfig.GetBoolPropertyFn(false),
	}))
	require.True(t, remoteClusterLifecycleEventsEnabled(&Config{
		EmitNamespaceLifecycleEvents: dynamicconfig.GetBoolPropertyFn(true),
	}))
}

func TestRemoteClusterLifecycleDisabledEmission(t *testing.T) {
	config := remoteClusterLifecycleTestConfig(false)
	upsertEvent := newRemoteClusterUpsertLifecycleEvent(
		context.Background(),
		nil,
		nil,
		config,
		remoteClusterAPIOperator,
		remoteClusterUpsertRequestFields{},
	)
	removeEvent := newRemoteClusterRemoveLifecycleEvent(
		context.Background(),
		nil,
		nil,
		config,
		remoteClusterAPIOperator,
		remoteClusterRemoveRequestFields{},
	)
	require.Nil(t, upsertEvent)
	require.Nil(t, removeEvent)
	require.NotPanics(t, func() {
		upsertEvent.emitUpsertSuccess(nil, nil)
		upsertEvent.emitUpsertFailure(errors.New("failed"), nil, nil, nil)
		removeEvent.emitRemoveSuccess(cachedRemoteClusterLookup{}, nil)
		removeEvent.emitRemoveFailure(errors.New("failed"), cachedRemoteClusterLookup{}, nil)
	})
}

func TestNewRemoteClusterLifecycleEventDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	metadata := cluster.NewMockMetadata(ctrl)

	event := newRemoteClusterUpsertLifecycleEvent(
		context.Background(),
		&captureRemoteClusterEventLogger{},
		metadata,
		remoteClusterLifecycleTestConfig(false),
		remoteClusterAPIOperator,
		remoteClusterUpsertRequestFields{},
	)

	require.Nil(t, event)
}

func remoteClusterLifecycleTestConfig(enabled bool) *Config {
	return &Config{EmitNamespaceLifecycleEvents: dynamicconfig.GetBoolPropertyFn(enabled)}
}

func remoteClusterEventValues(
	t *testing.T,
	records []otellog.Record,
) (map[string]string, map[string]any) {
	t.Helper()
	require.Len(t, records, 1)
	require.Equal(t, wideevents.NamespaceLifecycleEventName, records[0].EventName())
	attrs := make(map[string]string)
	records[0].WalkAttributes(func(kv otellog.KeyValue) bool {
		attrs[kv.Key] = kv.Value.AsString()
		return true
	})
	var details map[string]any
	require.NoError(t, json.Unmarshal([]byte(attrs["details"]), &details))
	return attrs, details
}
