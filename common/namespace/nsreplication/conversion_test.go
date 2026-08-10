package nsreplication

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
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
// globalness/cluster count, so both transports make the identical decision.
func TestShouldReplicateNamespace(t *testing.T) {
	testCases := []struct {
		name               string
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
			got := ShouldReplicateNamespace(tc.isGlobal, tc.clusters, tc.clusterListChanged, tc.state)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestNamespaceDetailToTaskAttributes pins the detail->wire converter that an
// eventual CHASM-based transport will also build its requests through. Pinning
// the full field set here guards against the "field replicated by one transport
// but dropped by the other" failure mode: any replicated field added to
// NamespaceTaskAttributes must be threaded through this function or this test
// fails. Today only HandleTransmissionTask calls it.
func TestNamespaceDetailToTaskAttributes(t *testing.T) {
	failoverTime := timestamppb.New(time.Unix(12345, 0).UTC())
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
			HistoryArchivalState:    enumspb.ARCHIVAL_STATE_ENABLED,
			HistoryArchivalUri:      "s3://history",
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_ENABLED,
			VisibilityArchivalUri:   "s3://visibility",
			BadBinaries: &namespacepb.BadBinaries{
				Binaries: map[string]*namespacepb.BadBinaryInfo{"bad": {Reason: "nope"}},
			},
			CustomSearchAttributeAliases: map[string]string{"Bool01": "alias"},
		},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: "active",
			State:             enumspb.REPLICATION_STATE_NORMAL,
			Clusters:          []string{"active", "standby"},
			FailoverHistory: []*persistencespb.FailoverStatus{
				{FailoverTime: failoverTime, FailoverVersion: 7},
			},
		},
		ConfigVersion:   3,
		FailoverVersion: 11,
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

	got := NamespaceDetailToTaskAttributes(enumsspb.NAMESPACE_OPERATION_UPDATE, detail)
	protorequire.ProtoEqual(t, want, got)
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
