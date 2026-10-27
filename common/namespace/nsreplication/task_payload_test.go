package nsreplication

import (
	"testing"

	"github.com/stretchr/testify/require"
	namespacepb "go.temporal.io/api/namespace/v1"
	replicationpb "go.temporal.io/api/replication/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	replicationspb "go.temporal.io/server/api/replication/v1"
	"google.golang.org/protobuf/proto"
)

func TestMarshalNamespaceTaskDeterministic(t *testing.T) {
	t.Parallel()
	first := NamespaceDetailToTaskAttributes(enumsspb.NAMESPACE_OPERATION_UPDATE, &persistencespb.NamespaceDetail{
		Info:              &persistencespb.NamespaceInfo{Id: "ns-id", Data: map[string]string{"a": "1", "b": "2"}},
		Config:            &persistencespb.NamespaceConfig{},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{},
	})
	second := proto.Clone(first).(*replicationspb.NamespaceTaskAttributes)
	second.Info.Data = map[string]string{"b": "2", "a": "1"}

	firstPayload, err := MarshalNamespaceTask(first)
	require.NoError(t, err)
	secondPayload, err := MarshalNamespaceTask(second)
	require.NoError(t, err)
	require.Equal(t, firstPayload, secondPayload)
}

func TestDifferingNamespaceTaskFields(t *testing.T) {
	t.Parallel()
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

func TestDifferingNamespaceTaskFieldsCoversSchema(t *testing.T) {
	t.Parallel()
	mutations := map[string]func(*replicationspb.NamespaceTaskAttributes){
		"namespace_operation": func(task *replicationspb.NamespaceTaskAttributes) {
			task.NamespaceOperation = enumsspb.NAMESPACE_OPERATION_UPDATE
		},
		"id":     func(task *replicationspb.NamespaceTaskAttributes) { task.Id = "namespace-id" },
		"info":   func(task *replicationspb.NamespaceTaskAttributes) { task.Info = &namespacepb.NamespaceInfo{} },
		"config": func(task *replicationspb.NamespaceTaskAttributes) { task.Config = &namespacepb.NamespaceConfig{} },
		"replication_config": func(task *replicationspb.NamespaceTaskAttributes) {
			task.ReplicationConfig = &replicationpb.NamespaceReplicationConfig{}
		},
		"config_version":   func(task *replicationspb.NamespaceTaskAttributes) { task.ConfigVersion = 1 },
		"failover_version": func(task *replicationspb.NamespaceTaskAttributes) { task.FailoverVersion = 1 },
		"failover_history": func(task *replicationspb.NamespaceTaskAttributes) {
			task.FailoverHistory = []*replicationpb.FailoverStatus{{}}
		},
	}
	fields := (&replicationspb.NamespaceTaskAttributes{}).ProtoReflect().Descriptor().Fields()
	require.Len(t, mutations, fields.Len(), "update field diagnostics and their tests when NamespaceTaskAttributes changes")
	for i := range fields.Len() {
		name := string(fields.Get(i).Name())
		mutate, ok := mutations[name]
		require.True(t, ok, "missing diagnostic coverage for %s", name)
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			before := &replicationspb.NamespaceTaskAttributes{}
			after := &replicationspb.NamespaceTaskAttributes{}
			mutate(after)
			require.Equal(t, []string{name}, DifferingNamespaceTaskFields(before, after))
		})
	}
}
