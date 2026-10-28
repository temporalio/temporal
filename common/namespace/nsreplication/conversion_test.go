package nsreplication

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	rulespb "go.temporal.io/api/rules/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestShouldReplicateNamespace pins the replicate/skip gate that an eventual
// CHASM-based transport will share with the legacy queue path. In particular it
// locks in that a DELETED namespace is never replicated regardless of
// globalness/cluster count or forceReplicate, so both transports make the
// identical decision. forceReplicate lives inside the gate (ahead of every other
// rule except DELETED) so no caller can bypass the DELETED short-circuit.
func TestShouldReplicateNamespace(t *testing.T) {
	testCases := []struct {
		name               string
		forceReplicate     bool
		isGlobal           bool
		clusters           []string
		clusterListChanged bool
		state              enumspb.NamespaceState
		want               bool
	}{
		{
			name:     "local namespace never replicates",
			isGlobal: false,
			clusters: []string{"a", "b"},
			state:    enumspb.NAMESPACE_STATE_REGISTERED,
			want:     false,
		},
		{
			name:           "forceReplicate replicates non-global single cluster",
			forceReplicate: true,
			isGlobal:       false,
			clusters:       []string{"a"},
			state:          enumspb.NAMESPACE_STATE_REGISTERED,
			want:           true,
		},
		{
			name:           "forceReplicate never replicates DELETED",
			forceReplicate: true,
			isGlobal:       true,
			clusters:       []string{"a", "b"},
			state:          enumspb.NAMESPACE_STATE_DELETED,
			want:           false,
		},
		{
			name:     "global single cluster, no list change",
			isGlobal: true,
			clusters: []string{"a"},
			state:    enumspb.NAMESPACE_STATE_REGISTERED,
			want:     false,
		},
		{
			name:               "global single cluster, list changed",
			isGlobal:           true,
			clusters:           []string{"a"},
			clusterListChanged: true,
			state:              enumspb.NAMESPACE_STATE_REGISTERED,
			want:               true,
		},
		{
			name:     "global multi cluster, registered",
			isGlobal: true,
			clusters: []string{"a", "b"},
			state:    enumspb.NAMESPACE_STATE_REGISTERED,
			want:     true,
		},
		{
			name:     "global multi cluster, deprecated",
			isGlobal: true,
			clusters: []string{"a", "b"},
			state:    enumspb.NAMESPACE_STATE_DEPRECATED,
			want:     true,
		},
		{
			name:     "global multi cluster, DELETED is never replicated",
			isGlobal: true,
			clusters: []string{"a", "b"},
			state:    enumspb.NAMESPACE_STATE_DELETED,
			want:     false,
		},
		{
			name:               "DELETED not replicated even when list changed",
			isGlobal:           true,
			clusters:           []string{"a", "b"},
			clusterListChanged: true,
			state:              enumspb.NAMESPACE_STATE_DELETED,
			want:               false,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			got := ShouldReplicateNamespace(tc.forceReplicate, tc.isGlobal, tc.clusters, tc.clusterListChanged, tc.state)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestNamespaceDetailToTaskAttributes pins the current detail-to-wire mapping
// that an eventual CHASM-based transport will also use. Comparing the complete
// result guards against either transport dropping a currently replicated field.
// Today only HandleTransmissionTask calls it.
func TestNamespaceDetailToTaskAttributes(t *testing.T) {
	detail, want := namespaceDetailConversionTestCase()

	got := NamespaceDetailToTaskAttributes(enumsspb.NAMESPACE_OPERATION_UPDATE, detail)
	protorequire.ProtoEqual(t, want, got)
}

func namespaceDetailConversionTestCase() (*persistencespb.NamespaceDetail, *replicationspb.NamespaceTaskAttributes) {
	failoverTime := timestamppb.New(time.Unix(12345, 0).UTC())
	// Populate replicated and source-local fields so the expected wire result
	// documents which current fields are intentionally omitted.
	detail := &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:          "ns-id",
			Name:        "ns-name",
			State:       enumspb.NAMESPACE_STATE_REGISTERED,
			Description: "desc",
			Owner:       "owner@example.com",
			Data:        map[string]string{"k": "v"},
		},
		Config: &persistencespb.NamespaceConfig{
			Retention:               durationpb.New(24 * time.Hour),
			ArchivalBucket:          "source-local-archive",
			HistoryArchivalState:    enumspb.ARCHIVAL_STATE_ENABLED,
			HistoryArchivalUri:      "s3://history",
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_ENABLED,
			VisibilityArchivalUri:   "s3://visibility",
			BadBinaries: &namespacepb.BadBinaries{
				Binaries: map[string]*namespacepb.BadBinaryInfo{"bad": {Reason: "nope"}},
			},
			CustomSearchAttributeAliases: map[string]string{"Bool01": "alias"},
			WorkflowRules:                map[string]*rulespb.WorkflowRule{"rule": {}},
		},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: "active",
			State:             enumspb.REPLICATION_STATE_NORMAL,
			Clusters:          []string{"active", "standby"},
			FailoverHistory: []*persistencespb.FailoverStatus{
				{FailoverTime: failoverTime, FailoverVersion: 7},
			},
			ClusterReplicationRamps: map[string]*persistencespb.NamespaceReplicationRamp{
				"standby": {
					StartTime: failoverTime,
					Duration:  durationpb.New(time.Hour),
				},
			},
		},
		ConfigVersion:               3,
		FailoverNotificationVersion: 5,
		FailoverVersion:             11,
		FailoverEndTime:             timestamppb.New(time.Unix(23456, 0).UTC()),
	}

	want := &replicationspb.NamespaceTaskAttributes{
		NamespaceOperation: enumsspb.NAMESPACE_OPERATION_UPDATE,
		Id:                 "ns-id",
		Info: &namespacepb.NamespaceInfo{
			Name:        "ns-name",
			State:       enumspb.NAMESPACE_STATE_REGISTERED,
			Description: "desc",
			OwnerEmail:  "owner@example.com",
			Data:        map[string]string{"k": "v"},
		},
		Config: &namespacepb.NamespaceConfig{
			WorkflowExecutionRetentionTtl: durationpb.New(24 * time.Hour),
			HistoryArchivalState:          enumspb.ARCHIVAL_STATE_ENABLED,
			HistoryArchivalUri:            "s3://history",
			VisibilityArchivalState:       enumspb.ARCHIVAL_STATE_ENABLED,
			VisibilityArchivalUri:         "s3://visibility",
			BadBinaries: &namespacepb.BadBinaries{
				Binaries: map[string]*namespacepb.BadBinaryInfo{"bad": {Reason: "nope"}},
			},
			CustomSearchAttributeAliases: map[string]string{"Bool01": "alias"},
		},
		ReplicationConfig: &replicationpb.NamespaceReplicationConfig{
			ActiveClusterName: "active",
			State:             enumspb.REPLICATION_STATE_NORMAL,
			Clusters: []*replicationpb.ClusterReplicationConfig{
				{ClusterName: "active"},
				{ClusterName: "standby"},
			},
		},
		ConfigVersion:   3,
		FailoverVersion: 11,
		FailoverHistory: []*replicationpb.FailoverStatus{
			{FailoverTime: failoverTime, FailoverVersion: 7},
		},
	}

	return detail, want
}

// TestNamespaceDetailToTaskAttributes_NonNormalStateDropped pins the special-case:
// the replication State is only carried on the wire when it is NORMAL. A HANDOVER
// (or any non-NORMAL) state must be dropped, matching the legacy queue behavior.
func TestNamespaceDetailToTaskAttributes_NonNormalStateDropped(t *testing.T) {
	detail := &persistencespb.NamespaceDetail{
		Info:   &persistencespb.NamespaceInfo{Id: "ns-id", Name: "ns-name", State: enumspb.NAMESPACE_STATE_REGISTERED},
		Config: &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: "active",
			State:             enumspb.REPLICATION_STATE_HANDOVER,
			Clusters:          []string{"active", "standby"},
		},
	}

	got := NamespaceDetailToTaskAttributes(enumsspb.NAMESPACE_OPERATION_UPDATE, detail)
	require.Equal(t, enumspb.REPLICATION_STATE_UNSPECIFIED, got.GetReplicationConfig().GetState())
}

func TestNamespaceDetailFromTransmissionTask(t *testing.T) {
	info := &persistencespb.NamespaceInfo{Id: "ns-id"}
	config := &persistencespb.NamespaceConfig{}
	replicationConfig := &persistencespb.NamespaceReplicationConfig{
		ActiveClusterName: "active",
		State:             enumspb.REPLICATION_STATE_HANDOVER,
		Clusters:          []string{"active", "standby"},
		FailoverHistory:   []*persistencespb.FailoverStatus{{FailoverVersion: 1}},
	}
	failoverHistory := []*persistencespb.FailoverStatus{{FailoverVersion: 2}}

	detail := NamespaceDetailFromTransmissionTask(
		info,
		config,
		replicationConfig,
		3,
		4,
		failoverHistory,
	)

	require.Same(t, info, detail.GetInfo())
	require.Same(t, config, detail.GetConfig())
	require.Equal(t, "active", detail.GetReplicationConfig().GetActiveClusterName())
	require.Equal(t, enumspb.REPLICATION_STATE_HANDOVER, detail.GetReplicationConfig().GetState())
	require.Equal(t, []string{"active", "standby"}, detail.GetReplicationConfig().GetClusters())
	require.Equal(t, failoverHistory, detail.GetReplicationConfig().GetFailoverHistory())
	require.Equal(t, int64(3), detail.GetConfigVersion())
	require.Equal(t, int64(4), detail.GetFailoverVersion())
}
