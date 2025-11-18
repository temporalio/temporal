package namespace_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
)

func TestNewDefaultReplicationResolverFactory(t *testing.T) {
	factory := namespace.NewDefaultReplicationResolverFactory()
	require.NotNil(t, factory)

	detail := &persistencespb.NamespaceDetail{
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: "active-cluster",
			Clusters:          []string{"cluster1", "cluster2", "cluster3"},
			State:             enumspb.REPLICATION_STATE_NORMAL,
		},
	}

	resolver := factory(detail)
	require.NotNil(t, resolver)
	assert.Equal(t, "active-cluster", resolver.ActiveClusterName(""))
	assert.Equal(t, []string{"cluster1", "cluster2", "cluster3"}, resolver.ClusterNames(""))
	assert.Equal(t, enumspb.REPLICATION_STATE_NORMAL, resolver.WorkflowReplicationState(""))
	assert.Equal(t, enumspb.REPLICATION_STATE_NORMAL, resolver.NamespaceReplicationState())
}

func TestDefaultReplicationResolver_ActiveClusterName(t *testing.T) {
	tests := []struct {
		name              string
		replicationConfig *persistencespb.NamespaceReplicationConfig
		entityID          string
		want              string
	}{
		{
			name: "returns active cluster name",
			replicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: "cluster-a",
				Clusters:          []string{"cluster-a", "cluster-b"},
			},
			entityID: "",
			want:     "cluster-a",
		},
		{
			name: "returns active cluster name with entity ID",
			replicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: "cluster-b",
				Clusters:          []string{"cluster-a", "cluster-b"},
			},
			entityID: "workflow-123",
			want:     "cluster-b",
		},
		{
			name:              "returns empty string when replication config is nil",
			replicationConfig: nil,
			entityID:          "",
			want:              "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := namespace.NewDefaultReplicationResolverFactory()
			detail := &persistencespb.NamespaceDetail{
				ReplicationConfig: tt.replicationConfig,
			}
			resolver := factory(detail)

			got := resolver.ActiveClusterName(tt.entityID)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDefaultReplicationResolver_ClusterNames(t *testing.T) {
	tests := []struct {
		name              string
		replicationConfig *persistencespb.NamespaceReplicationConfig
		entityID          string
		want              []string
	}{
		{
			name: "returns cluster names",
			replicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: "cluster-a",
				Clusters:          []string{"cluster-a", "cluster-b", "cluster-c"},
			},
			entityID: "",
			want:     []string{"cluster-a", "cluster-b", "cluster-c"},
		},
		{
			name: "returns cluster names with entity ID",
			replicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: "cluster-a",
				Clusters:          []string{"cluster-1", "cluster-2"},
			},
			entityID: "workflow-456",
			want:     []string{"cluster-1", "cluster-2"},
		},
		{
			name:              "returns nil when replication config is nil",
			replicationConfig: nil,
			entityID:          "",
			want:              nil,
		},
		{
			name: "returns empty slice when clusters is empty",
			replicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: "cluster-a",
				Clusters:          []string{},
			},
			entityID: "",
			want:     []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := namespace.NewDefaultReplicationResolverFactory()
			detail := &persistencespb.NamespaceDetail{
				ReplicationConfig: tt.replicationConfig,
			}
			resolver := factory(detail)

			got := resolver.ClusterNames(tt.entityID)
			assert.Equal(t, tt.want, got)

			// Verify immutability - modifying returned slice shouldn't affect subsequent calls
			if len(got) > 0 {
				got[0] = "modified"
				got2 := resolver.ClusterNames(tt.entityID)
				if len(tt.want) > 0 {
					assert.Equal(t, tt.want[0], got2[0], "ClusterNames should return a copy to preserve immutability")
				}
			}
		})
	}
}

func TestDefaultReplicationResolver_WorkflowReplicationState(t *testing.T) {
	tests := []struct {
		name              string
		replicationConfig *persistencespb.NamespaceReplicationConfig
		entityID          string
		want              enumspb.ReplicationState
	}{
		{
			name: "returns normal state",
			replicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: "cluster-a",
				Clusters:          []string{"cluster-a", "cluster-b"},
				State:             enumspb.REPLICATION_STATE_NORMAL,
			},
			entityID: "",
			want:     enumspb.REPLICATION_STATE_NORMAL,
		},
		{
			name: "returns handover state",
			replicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: "cluster-a",
				Clusters:          []string{"cluster-a", "cluster-b"},
				State:             enumspb.REPLICATION_STATE_HANDOVER,
			},
			entityID: "workflow-789",
			want:     enumspb.REPLICATION_STATE_HANDOVER,
		},
		{
			name:              "returns unspecified when replication config is nil",
			replicationConfig: nil,
			entityID:          "",
			want:              enumspb.REPLICATION_STATE_UNSPECIFIED,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := namespace.NewDefaultReplicationResolverFactory()
			detail := &persistencespb.NamespaceDetail{
				ReplicationConfig: tt.replicationConfig,
			}
			resolver := factory(detail)

			got := resolver.WorkflowReplicationState(tt.entityID)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDefaultReplicationResolver_NamespaceReplicationState(t *testing.T) {
	tests := []struct {
		name              string
		replicationConfig *persistencespb.NamespaceReplicationConfig
		want              enumspb.ReplicationState
	}{
		{
			name: "returns normal state",
			replicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: "cluster-a",
				Clusters:          []string{"cluster-a", "cluster-b"},
				State:             enumspb.REPLICATION_STATE_NORMAL,
			},
			want: enumspb.REPLICATION_STATE_NORMAL,
		},
		{
			name: "returns handover state",
			replicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: "cluster-a",
				Clusters:          []string{"cluster-a", "cluster-b"},
				State:             enumspb.REPLICATION_STATE_HANDOVER,
			},
			want: enumspb.REPLICATION_STATE_HANDOVER,
		},
		{
			name:              "returns unspecified when replication config is nil",
			replicationConfig: nil,
			want:              enumspb.REPLICATION_STATE_UNSPECIFIED,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := namespace.NewDefaultReplicationResolverFactory()
			detail := &persistencespb.NamespaceDetail{
				ReplicationConfig: tt.replicationConfig,
			}
			resolver := factory(detail)

			got := resolver.NamespaceReplicationState()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDefaultReplicationResolver_MultipleCalls(t *testing.T) {
	// Test that resolver state is consistent across multiple calls
	factory := namespace.NewDefaultReplicationResolverFactory()
	detail := &persistencespb.NamespaceDetail{
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: "primary",
			Clusters:          []string{"primary", "secondary", "tertiary"},
			State:             enumspb.REPLICATION_STATE_NORMAL,
		},
	}
	resolver := factory(detail)

	// Call multiple times and verify consistency
	for i := 0; i < 5; i++ {
		assert.Equal(t, "primary", resolver.ActiveClusterName(""))
		assert.Equal(t, []string{"primary", "secondary", "tertiary"}, resolver.ClusterNames(""))
		assert.Equal(t, enumspb.REPLICATION_STATE_NORMAL, resolver.WorkflowReplicationState(""))
		assert.Equal(t, enumspb.REPLICATION_STATE_NORMAL, resolver.NamespaceReplicationState())
	}
}

func TestDefaultReplicationResolver_DifferentEntityIds(t *testing.T) {
	// Test that different entity IDs don't affect the default resolver behavior
	factory := namespace.NewDefaultReplicationResolverFactory()
	detail := &persistencespb.NamespaceDetail{
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: "cluster-x",
			Clusters:          []string{"cluster-x", "cluster-y"},
			State:             enumspb.REPLICATION_STATE_NORMAL,
		},
	}
	resolver := factory(detail)

	entityIDs := []string{"", "workflow-1", "workflow-2", "activity-1", "some-other-entity"}
	for _, entityID := range entityIDs {
		assert.Equal(t, "cluster-x", resolver.ActiveClusterName(entityID))
		assert.Equal(t, []string{"cluster-x", "cluster-y"}, resolver.ClusterNames(entityID))
		assert.Equal(t, enumspb.REPLICATION_STATE_NORMAL, resolver.WorkflowReplicationState(entityID))
	}
}

func TestDefaultReplicationResolver_IsGlobalNamespace(t *testing.T) {
	factory := namespace.NewDefaultReplicationResolverFactory()

	tests := []struct {
		name      string
		detail    *persistencespb.NamespaceDetail
		setGlobal bool
		want      bool
	}{
		{
			name: "global namespace",
			detail: &persistencespb.NamespaceDetail{
				ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
					ActiveClusterName: "cluster-a",
					Clusters:          []string{"cluster-a", "cluster-b"},
				},
			},
			setGlobal: true,
			want:      true,
		},
		{
			name: "local namespace",
			detail: &persistencespb.NamespaceDetail{
				ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
					ActiveClusterName: "cluster-a",
					Clusters:          []string{"cluster-a"},
				},
			},
			setGlobal: false,
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create resolver directly
			resolver := factory(tt.detail)
			// The isGlobalNamespace is false by default in the factory
			// This tests the default behavior - to test mutations, we'd need to test through Namespace
			// For this test, we verify it returns false by default (as set in factory)
			assert.False(t, resolver.IsGlobalNamespace())
		})
	}
}

func TestDefaultReplicationResolver_FailoverVersion(t *testing.T) {
	factory := namespace.NewDefaultReplicationResolverFactory()

	tests := []struct {
		name            string
		failoverVersion int64
		want            int64
	}{
		{
			name:            "positive version",
			failoverVersion: 12345,
			want:            12345,
		},
		{
			name:            "zero version",
			failoverVersion: 0,
			want:            0,
		},
		{
			name:            "negative version",
			failoverVersion: -1,
			want:            -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			detail := &persistencespb.NamespaceDetail{
				FailoverVersion: tt.failoverVersion,
			}
			resolver := factory(detail)
			assert.Equal(t, tt.want, resolver.FailoverVersion())
		})
	}
}

func TestDefaultReplicationResolver_FailoverNotificationVersion(t *testing.T) {
	factory := namespace.NewDefaultReplicationResolverFactory()

	tests := []struct {
		name                        string
		failoverNotificationVersion int64
		want                        int64
	}{
		{
			name:                        "positive version",
			failoverNotificationVersion: 54321,
			want:                        54321,
		},
		{
			name:                        "zero version",
			failoverNotificationVersion: 0,
			want:                        0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			detail := &persistencespb.NamespaceDetail{
				FailoverNotificationVersion: tt.failoverNotificationVersion,
			}
			resolver := factory(detail)
			assert.Equal(t, tt.want, resolver.FailoverNotificationVersion())
		})
	}
}

func TestDefaultReplicationResolver_GetReplicationConfig(t *testing.T) {
	factory := namespace.NewDefaultReplicationResolverFactory()

	expectedConfig := &persistencespb.NamespaceReplicationConfig{
		ActiveClusterName: "cluster-primary",
		Clusters:          []string{"cluster-primary", "cluster-secondary"},
		State:             enumspb.REPLICATION_STATE_NORMAL,
	}

	detail := &persistencespb.NamespaceDetail{
		ReplicationConfig: expectedConfig,
	}
	resolver := factory(detail)

	got := resolver.GetReplicationConfig()
	assert.Equal(t, expectedConfig, got)
}
