package tdbg

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	namespacepb "go.temporal.io/api/namespace/v1"
	rulespb "go.temporal.io/api/rules/v1"
	"go.temporal.io/server/api/adminservice/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"go.temporal.io/server/common/namespace/nsreplication"
	"go.temporal.io/server/common/testing/protorequire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// TestNamespaceReplicationProjectionMatchesTransmission covers the sender's
// actual field selection, including fields added to existing wire messages.
// The persistence containers and failover history entry must populate every
// field, so schema additions require extending the fixture, including fields
// that are currently source-local.
func TestNamespaceReplicationProjectionMatchesTransmission(t *testing.T) {
	t.Parallel()
	at := timestamppb.New(time.Unix(123, 0))
	detail := &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id: "namespace-id", Name: "namespace", State: enumspb.NAMESPACE_STATE_REGISTERED,
			Description: "description", Owner: "owner@example.com", Data: map[string]string{"key": "value"},
			RestoreInfo: &persistencespb.NamespaceRestoreInfo{RestoreId: "source-local"},
		},
		Config: &persistencespb.NamespaceConfig{
			Retention: durationpb.New(time.Hour), ArchivalBucket: "source-local",
			BadBinaries: &namespacepb.BadBinaries{Binaries: map[string]*namespacepb.BadBinaryInfo{
				"checksum": {Reason: "bad", Operator: "operator", CreateTime: at},
			}},
			HistoryArchivalState: enumspb.ARCHIVAL_STATE_ENABLED, HistoryArchivalUri: "s3://history",
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_ENABLED, VisibilityArchivalUri: "s3://visibility",
			CustomSearchAttributeAliases: map[string]string{"Alias": "Keyword01"},
			WorkflowRules:                map[string]*rulespb.WorkflowRule{"source-local": {}},
		},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: "cluster-a", State: enumspb.REPLICATION_STATE_NORMAL,
			Clusters:        []string{"cluster-a", "cluster-b"},
			FailoverHistory: []*persistencespb.FailoverStatus{{FailoverTime: at, FailoverVersion: 20}},
			ClusterReplicationRamps: map[string]*persistencespb.NamespaceReplicationRamp{
				"cluster-b": {StartTime: at, Duration: durationpb.New(time.Hour)},
			},
		},
		ConfigVersion: 10, FailoverVersion: 20, FailoverNotificationVersion: 30, FailoverEndTime: at,
	}
	for _, message := range []proto.Message{detail, detail.Info, detail.Config, detail.ReplicationConfig, detail.ReplicationConfig.FailoverHistory[0]} {
		reflected := message.ProtoReflect()
		fields := reflected.Descriptor().Fields()
		for i := range fields.Len() {
			field := fields.Get(i)
			require.True(t, reflected.Has(field), "populate %s in the replication contract fixture", field.FullName())
		}
	}

	transmitted := nsreplication.NamespaceDetailToTaskAttributes(enumsspb.NAMESPACE_OPERATION_UPDATE, detail)
	response := &adminservice.GetNamespaceResponse{
		Info: proto.CloneOf(transmitted.Info), Config: proto.CloneOf(transmitted.Config),
		ReplicationConfig: proto.CloneOf(transmitted.ReplicationConfig), FailoverHistory: transmitted.FailoverHistory,
		ConfigVersion: transmitted.ConfigVersion, FailoverVersion: transmitted.FailoverVersion, IsGlobalNamespace: true,
	}
	response.Info.Id = transmitted.Id
	config, failover, err := namespaceReplicationProjections(response, nil)
	require.NoError(t, err)
	combined := proto.CloneOf(config)
	proto.Merge(combined, failover)
	// Versions are compared separately; the operation is not namespace state.
	transmitted.NamespaceOperation = enumsspb.NAMESPACE_OPERATION_UNSPECIFIED
	transmitted.ConfigVersion, transmitted.FailoverVersion = 0, 0
	protorequire.ProtoEqual(t, transmitted, combined)

	// Every populated projected field must also produce exactly one difference,
	// under a name no other field uses, so a field is never reported under
	// another field's name. The top-level namespace ID is checked by replica
	// identity validation.
	fieldsByDifference := make(map[string]string)
	requireDifference := func(projection, changed *replicationspb.NamespaceTaskAttributes, field string) {
		difference := namespaceReplicationDifferenceFor(t, config, failover, projection, changed, field)
		require.NotContains(t, fieldsByDifference, difference,
			"%s and %s are both reported as %s", fieldsByDifference[difference], field, difference)
		fieldsByDifference[difference] = field
	}
	for _, projection := range []*replicationspb.NamespaceTaskAttributes{config, failover} {
		projection.ProtoReflect().Range(func(field protoreflect.FieldDescriptor, value protoreflect.Value) bool {
			if field.Name() == "id" {
				return true
			}
			if field.Kind() == protoreflect.MessageKind && !field.IsList() && !field.IsMap() {
				value.Message().Range(func(child protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
					changed := proto.CloneOf(projection)
					changed.ProtoReflect().Mutable(field).Message().Clear(child)
					requireDifference(projection, changed, string(child.FullName()))
					return true
				})
			} else {
				changed := proto.CloneOf(projection)
				changed.ProtoReflect().Clear(field)
				requireDifference(projection, changed, string(field.FullName()))
			}
			return true
		})
	}
}

// namespaceReplicationDifferenceFor returns the one difference reported when
// changed replaces original, which is either the config or the failover
// projection.
func namespaceReplicationDifferenceFor(
	t *testing.T,
	config, failover, original, changed *replicationspb.NamespaceTaskAttributes,
	field string,
) string {
	t.Helper()
	actualConfig, actualFailover := config, failover
	if original == config {
		actualConfig = changed
	} else {
		actualFailover = changed
	}
	differences := namespaceReplicationDifferences(config, actualConfig, failover, actualFailover)
	require.Len(t, differences, 1, "report exactly the differing replicated field %s", field)
	return differences[0]
}
