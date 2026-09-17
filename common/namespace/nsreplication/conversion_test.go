package nsreplication

import (
	"fmt"
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
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
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

func TestShouldReplicateNamespace_AllCombinations(t *testing.T) {
	states := enumspb.NamespaceState(0).Descriptor().Values()
	for _, forceReplicate := range []bool{false, true} {
		for _, isGlobal := range []bool{false, true} {
			for _, clusterCount := range []int{0, 1, 2} {
				for _, clusterListChanged := range []bool{false, true} {
					for i := 0; i < states.Len(); i++ {
						state := enumspb.NamespaceState(states.Get(i).Number())
						name := fmt.Sprintf(
							"force=%t/global=%t/clusters=%d/list-changed=%t/state=%s",
							forceReplicate,
							isGlobal,
							clusterCount,
							clusterListChanged,
							state,
						)
						t.Run(name, func(t *testing.T) {
							clusters := make([]string, clusterCount)
							want := state != enumspb.NAMESPACE_STATE_DELETED &&
								(forceReplicate || (isGlobal && (clusterCount > 1 || clusterListChanged)))
							got := ShouldReplicateNamespace(
								forceReplicate,
								isGlobal,
								clusters,
								clusterListChanged,
								state,
							)
							require.Equal(t, want, got)
						})
					}
				}
			}
		}
	}
}

// TestNamespaceDetailToTaskAttributes pins the detail->wire converter that an
// eventual CHASM-based transport will also build its requests through. Pinning
// the full field set here guards against the "field replicated by one transport
// but dropped by the other" failure mode: any replicated field added to
// NamespaceTaskAttributes must be threaded through this function or this test
// fails. Today only HandleTransmissionTask calls it.
func TestNamespaceDetailToTaskAttributes(t *testing.T) {
	detail, want := namespaceDetailConversionTestCase()

	got := NamespaceDetailToTaskAttributes(enumsspb.NAMESPACE_OPERATION_UPDATE, detail)
	protorequire.ProtoEqual(t, want, got)
}

func TestNamespaceDetailToTaskAttributes_FieldCoverage(t *testing.T) {
	detail, _ := namespaceDetailConversionTestCase()
	got := NamespaceDetailToTaskAttributes(enumsspb.NAMESPACE_OPERATION_UPDATE, detail)

	sourceCases := []struct {
		name    string
		message proto.Message
		mapped  []string
		ignored []string
	}{
		{
			name:    "persistence NamespaceDetail",
			message: detail,
			mapped:  []string{"info", "config", "replication_config", "config_version", "failover_version"},
			// Receiver-side namespace tasks do not carry source-local failover bookkeeping.
			ignored: []string{"failover_notification_version", "failover_end_time"},
		},
		{
			name:    "persistence NamespaceInfo",
			message: detail.Info,
			mapped:  []string{"id", "state", "name", "description", "owner", "data"},
		},
		{
			name:    "persistence NamespaceConfig",
			message: detail.Config,
			mapped: []string{
				"retention",
				"bad_binaries",
				"history_archival_state",
				"history_archival_uri",
				"visibility_archival_state",
				"visibility_archival_uri",
				"custom_search_attribute_aliases",
			},
			// These persistence-only fields have no NamespaceTaskAttributes wire counterpart.
			ignored: []string{"archival_bucket", "workflow_rules"},
		},
		{
			name:    "persistence NamespaceReplicationConfig",
			message: detail.ReplicationConfig,
			mapped:  []string{"active_cluster_name", "clusters", "state", "failover_history"},
			// Ramp schedules are source-local and must not be copied by namespace replication.
			ignored: []string{"cluster_replication_ramps"},
		},
		{
			name:    "persistence FailoverStatus",
			message: detail.ReplicationConfig.FailoverHistory[0],
			mapped:  []string{"failover_time", "failover_version"},
		},
	}

	for _, tc := range sourceCases {
		t.Run(tc.name, func(t *testing.T) {
			requireProtoFieldsClassified(t, tc.message, tc.mapped, tc.ignored)
			requireAllProtoFieldsSet(t, tc.message)
		})
	}

	destinationCases := []struct {
		name    string
		message proto.Message
		mapped  []string
		ignored []string
	}{
		{
			name:    "wire NamespaceTaskAttributes",
			message: got,
			mapped: []string{
				"namespace_operation",
				"id",
				"info",
				"config",
				"replication_config",
				"config_version",
				"failover_version",
				"failover_history",
			},
		},
		{
			name:    "wire NamespaceInfo",
			message: got.Info,
			mapped:  []string{"name", "state", "description", "owner_email", "data"},
			// ID is carried at the task's top level; the remaining fields are derived by the receiver.
			ignored: []string{"id", "capabilities", "limits", "supports_schedules"},
		},
		{
			name:    "wire NamespaceConfig",
			message: got.Config,
			mapped: []string{
				"workflow_execution_retention_ttl",
				"bad_binaries",
				"history_archival_state",
				"history_archival_uri",
				"visibility_archival_state",
				"visibility_archival_uri",
				"custom_search_attribute_aliases",
			},
		},
		{
			name:    "wire NamespaceReplicationConfig",
			message: got.ReplicationConfig,
			mapped:  []string{"active_cluster_name", "clusters", "state"},
		},
		{
			name:    "wire ClusterReplicationConfig",
			message: got.ReplicationConfig.Clusters[0],
			mapped:  []string{"cluster_name"},
			// Like persisted ramp schedules, requested ramp durations are source-local.
			ignored: []string{"replication_ramp_duration"},
		},
		{
			name:    "wire FailoverStatus",
			message: got.FailoverHistory[0],
			mapped:  []string{"failover_time", "failover_version"},
		},
	}

	for _, tc := range destinationCases {
		t.Run(tc.name, func(t *testing.T) {
			requireProtoFieldsClassified(t, tc.message, tc.mapped, tc.ignored)
			requireProtoFieldsSet(t, tc.message, tc.mapped)
			requireProtoFieldsUnset(t, tc.message, tc.ignored)
		})
	}
}

func namespaceDetailConversionTestCase() (*persistencespb.NamespaceDetail, *replicationspb.NamespaceTaskAttributes) {
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

func requireProtoFieldsClassified(t *testing.T, message proto.Message, mapped, ignored []string) {
	t.Helper()

	fields := message.ProtoReflect().Descriptor().Fields()
	actual := make([]string, 0, fields.Len())
	for i := 0; i < fields.Len(); i++ {
		actual = append(actual, string(fields.Get(i).Name()))
	}
	classified := append(append([]string(nil), mapped...), ignored...)
	require.ElementsMatch(t, actual, classified)
}

func requireAllProtoFieldsSet(t *testing.T, message proto.Message) {
	t.Helper()

	reflection := message.ProtoReflect()
	fields := reflection.Descriptor().Fields()
	for i := 0; i < fields.Len(); i++ {
		field := fields.Get(i)
		require.True(t, reflection.Has(field), "test fixture must set field %q", field.FullName())
	}
}

func requireProtoFieldsSet(t *testing.T, message proto.Message, names []string) {
	t.Helper()

	reflection := message.ProtoReflect()
	for _, name := range names {
		field := reflection.Descriptor().Fields().ByName(protoreflect.Name(name))
		require.NotNil(t, field, "unknown field %q", name)
		require.True(t, reflection.Has(field), "mapped field %q must be set", field.FullName())
	}
}

func requireProtoFieldsUnset(t *testing.T, message proto.Message, names []string) {
	t.Helper()

	reflection := message.ProtoReflect()
	for _, name := range names {
		field := reflection.Descriptor().Fields().ByName(protoreflect.Name(name))
		require.NotNil(t, field, "unknown field %q", name)
		require.False(t, reflection.Has(field), "ignored field %q must be unset", field.FullName())
	}
}
