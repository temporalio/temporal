package nsreplication

import (
	"testing"

	"github.com/stretchr/testify/require"
	namespacepb "go.temporal.io/api/namespace/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"google.golang.org/protobuf/proto"
)

func TestNamespaceTaskFingerprint_Deterministic(t *testing.T) {
	first := NamespaceDetailToTaskAttributes(enumsspb.NAMESPACE_OPERATION_UPDATE, &persistencespb.NamespaceDetail{
		Info:              &persistencespb.NamespaceInfo{Id: "ns-id", Data: map[string]string{"a": "1", "b": "2"}},
		Config:            &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
	})
	second := proto.Clone(first).(*replicationspb.NamespaceTaskAttributes)
	second.Info.Data = map[string]string{"b": "2", "a": "1"}

	firstFingerprint, err := NamespaceTaskFingerprint(first)
	require.NoError(t, err)
	secondFingerprint, err := NamespaceTaskFingerprint(second)
	require.NoError(t, err)
	require.Equal(t, firstFingerprint, secondFingerprint)
}

func TestDifferingNamespaceTaskFields(t *testing.T) {
	first := &replicationspb.NamespaceTaskAttributes{
		Id:            "ns-id",
		Info:          &namespacepb.NamespaceInfo{Name: "before"},
		ConfigVersion: 1,
	}
	second := proto.Clone(first).(*replicationspb.NamespaceTaskAttributes)
	second.Info.Name = "after"
	second.ConfigVersion = 2

	require.Equal(t, []string{"info", "config_version"}, DifferingNamespaceTaskFields(first, second))
}
