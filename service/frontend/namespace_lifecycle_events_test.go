package frontend

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	rulespb "go.temporal.io/api/rules/v1"
	"go.temporal.io/api/workflowservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/wideevents"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testNamespaceDetail() *persistencespb.NamespaceDetail {
	return &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Name:        "ns",
			Id:          "ns-id",
			State:       enumspb.NAMESPACE_STATE_REGISTERED,
			Description: "desc",
			Owner:       "owner@example.com",
			Data:        map[string]string{"k": "v"},
		},
		Config: &persistencespb.NamespaceConfig{
			Retention:                    durationpb.New(168 * time.Hour),
			HistoryArchivalState:         enumspb.ARCHIVAL_STATE_DISABLED,
			HistoryArchivalUri:           "s3://history",
			VisibilityArchivalState:      enumspb.ARCHIVAL_STATE_ENABLED,
			VisibilityArchivalUri:        "s3://visibility",
			CustomSearchAttributeAliases: map[string]string{"alias": "Keyword01"},
			BadBinaries: &namespacepb.BadBinaries{
				Binaries: map[string]*namespacepb.BadBinaryInfo{
					"cksum1": {Reason: "bad deploy", Operator: "alice"},
				},
			},
			WorkflowRules: map[string]*rulespb.WorkflowRule{"rule-b": {}, "rule-a": {}},
		},
		ConfigVersion:               3,
		FailoverVersion:             10,
		FailoverNotificationVersion: 9,
		FailoverEndTime:             timestamppb.New(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)),
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: "clusterA",
			Clusters:          []string{"clusterA", "clusterB"},
			State:             enumspb.REPLICATION_STATE_NORMAL,
			FailoverHistory: []*persistencespb.FailoverStatus{
				{FailoverVersion: 10, FailoverTime: timestamppb.New(time.Unix(0, 0).UTC())},
			},
		},
	}
}

func TestNamespaceStateFields(t *testing.T) {
	f := namespaceStateFields(testNamespaceDetail(), true)

	require.Equal(t, "desc", f.Description)
	require.Equal(t, "owner@example.com", f.Owner)
	require.Equal(t, "Registered", f.State)
	require.True(t, f.IsGlobalNamespace)
	require.Equal(t, map[string]string{"k": "v"}, f.Data)
	require.EqualValues(t, 3, f.ConfigVersion)
	require.EqualValues(t, 10, f.FailoverVersion)
	require.EqualValues(t, 9, f.FailoverNotificationVersion)
	require.Equal(t, "2026-01-02T03:04:05Z", f.FailoverEndTime)
	require.Equal(t, (168 * time.Hour).String(), f.Retention)
	require.Equal(t, "Disabled", f.HistoryArchivalState)
	require.Equal(t, "s3://history", f.HistoryArchivalURI)
	require.Equal(t, "Enabled", f.VisibilityArchivalState)
	require.Equal(t, "s3://visibility", f.VisibilityArchivalURI)
	require.Equal(t, map[string]string{"alias": "Keyword01"}, f.CustomSearchAttributeAlias)
	require.Contains(t, f.BadBinaries["cksum1"], "bad deploy")
	require.Equal(t, []string{"rule-a", "rule-b"}, f.WorkflowRuleIDs)
	require.Equal(t, "clusterA", f.ActiveCluster)
	require.Equal(t, []string{"clusterA", "clusterB"}, f.Clusters)
	require.Equal(t, "Normal", f.ReplicationState)
	require.Len(t, f.FailoverHistory, 1)
	require.EqualValues(t, 10, f.FailoverHistory[0].FailoverVersion)
}

// The before snapshot is unaffected by a later in-place mutation of the record (maps/slices cloned).
func TestNamespaceStateFieldsClonesSlices(t *testing.T) {
	detail := testNamespaceDetail()
	before := namespaceStateFields(detail, true)
	detail.ReplicationConfig.Clusters[0] = "mutated"
	detail.Info.Data["k"] = "mutated"
	require.Equal(t, "clusterA", before.Clusters[0])
	require.Equal(t, "v", before.Data["k"])
}

func TestBuildNamespaceRegisteredInput(t *testing.T) {
	in := buildNamespaceRegisteredInput(&persistence.CreateNamespaceRequest{
		Namespace:         testNamespaceDetail(),
		IsGlobalNamespace: true,
	}, "ns-id", &workflowservice.RegisterNamespaceRequest{
		Namespace:         "ns",
		Description:       "requested-desc",
		OwnerEmail:        "owner@example.com",
		IsGlobalNamespace: true,
		ActiveClusterName: "clusterA",
	})

	require.Equal(t, "ns", in.Namespace)
	require.Equal(t, "ns-id", in.NamespaceID)
	require.True(t, in.Fields.IsGlobalNamespace)
	require.Equal(t, "clusterA", in.Fields.ActiveCluster)
	// Requested is the field snapshot of the raw request, distinct from the persisted Fields.
	require.Equal(t, "requested-desc", in.Requested.Description)
	require.Equal(t, "owner@example.com", in.Requested.Owner)
}

// A failover surfaces as before/after active-cluster + failover-version deltas plus the is_failover
// flag; the phase stays namespace_updated.
func TestBuildNamespaceUpdatedInputCapturesBeforeAndAfter(t *testing.T) {
	before := namespaceStateFields(testNamespaceDetail(), true)

	after := testNamespaceDetail()
	after.ReplicationConfig.ActiveClusterName = "clusterB"
	after.FailoverVersion = 21

	in := buildNamespaceUpdatedInput(before, after, true, true, false, &workflowservice.UpdateNamespaceRequest{
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{ActiveClusterName: "clusterB"},
		DeleteBadBinary:   "cksum-old",
		PromoteNamespace:  true,
	})

	require.Equal(t, "ns", in.Namespace)
	require.True(t, in.IsFailover)
	require.False(t, in.IsPromotion)
	require.Equal(t, "clusterA", in.Before.ActiveCluster)
	require.Equal(t, "clusterB", in.After.ActiveCluster)
	require.EqualValues(t, 10, in.Before.FailoverVersion)
	require.EqualValues(t, 21, in.After.FailoverVersion)
	require.Equal(t, "clusterB", in.Requested.ActiveCluster)
	require.Equal(t, "cksum-old", in.DeleteBadBinary)
	require.True(t, in.PromoteNamespaceRequested)
	require.Equal(t, []string{"active_cluster", "delete_bad_binary", "promote_namespace"}, in.RequestedFields)
}

func TestUpdateRequestFieldNames(t *testing.T) {
	req := &workflowservice.UpdateNamespaceRequest{
		UpdateInfo: &namespacepb.UpdateNamespaceInfo{
			Description: "description",
			State:       enumspb.NAMESPACE_STATE_DEPRECATED,
			Data:        map[string]string{},
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: durationpb.New(24 * time.Hour),
			HistoryArchivalState:          enumspb.ARCHIVAL_STATE_ENABLED,
			HistoryArchivalUri:            "s3://history",
			VisibilityArchivalState:       enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalUri:         "s3://visibility",
			BadBinaries:                   &namespacepb.BadBinaries{},
			CustomSearchAttributeAliases:  map[string]string{"alias": "Keyword01"},
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: "clusterB",
			Clusters:          []*replicationpb.ClusterReplicationConfig{{ClusterName: "clusterB"}},
			State:             enumspb.REPLICATION_STATE_NORMAL,
		},
		DeleteBadBinary:  "cksum-old",
		PromoteNamespace: true,
	}

	require.Equal(t, []string{
		"description",
		"state",
		"data",
		"retention",
		"history_archival_state",
		"history_archival_uri",
		"visibility_archival_state",
		"visibility_archival_uri",
		"bad_binaries",
		"custom_search_attribute_aliases",
		"active_cluster",
		"clusters",
		"replication_state",
		"delete_bad_binary",
		"promote_namespace",
	}, updateRequestFieldNames(req))
}

// DeprecateNamespace reuses namespace_updated with a nil request, so Requested is the empty snapshot.
func TestBuildNamespaceUpdatedInputNilRequest(t *testing.T) {
	before := namespaceStateFields(testNamespaceDetail(), true)
	in := buildNamespaceUpdatedInput(before, testNamespaceDetail(), true, false, false, nil)
	require.Equal(t, wideevents.NamespaceStateFields{}, in.Requested)
	require.Nil(t, in.RequestedFields)
}

// The request snapshot builders read only namespace fields, never the request's security_token.
func TestRequestFieldsOmitSecurityToken(t *testing.T) {
	reg := registerRequestFields(&workflowservice.RegisterNamespaceRequest{
		Namespace:     "ns",
		Description:   "d",
		SecurityToken: "super-secret-token",
	})
	regJSON, err := json.Marshal(reg)
	require.NoError(t, err)
	require.NotContains(t, string(regJSON), "super-secret-token")
	require.Equal(t, "d", reg.Description)

	upd := updateRequestFields(&workflowservice.UpdateNamespaceRequest{
		UpdateInfo:    &namespacepb.UpdateNamespaceInfo{Description: "d"},
		Config:        &namespacepb.NamespaceConfig{HistoryArchivalUri: "s3://h"},
		SecurityToken: "super-secret-token",
	})
	updJSON, err := json.Marshal(upd)
	require.NoError(t, err)
	require.NotContains(t, string(updJSON), "super-secret-token")
	require.Equal(t, "d", upd.Description)
	require.Equal(t, "s3://h", upd.HistoryArchivalURI)
}

func TestNamespaceEventEmittersRespectDynamicConfig(t *testing.T) {
	enabled := false
	lg := &captureNamespaceEventLogger{}
	handler := &namespaceHandler{
		eventLogger: lg,
		config: &Config{
			EmitNamespaceLifecycleEvents: func() bool { return enabled },
		},
	}

	handler.emitNamespaceRegistered(wideevents.NamespaceRegisteredInput{Namespace: "namespace"})
	handler.emitNamespaceUpdated(wideevents.NamespaceUpdatedInput{Namespace: "namespace"})
	require.Empty(t, lg.records)

	enabled = true
	handler.emitNamespaceRegistered(wideevents.NamespaceRegisteredInput{Namespace: "namespace"})
	handler.emitNamespaceUpdated(wideevents.NamespaceUpdatedInput{Namespace: "namespace"})
	require.Len(t, lg.records, 2)
}
