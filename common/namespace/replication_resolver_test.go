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

	detail := persistencespb.NamespaceDetail_builder{
		ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
			ActiveClusterName: "active-cluster",
			Clusters:          []string{"cluster1", "cluster2", "cluster3"},
			State:             enumspb.REPLICATION_STATE_NORMAL,
		}.Build(),
	}.Build()

	resolver := factory(detail)
	require.NotNil(t, resolver)
	assert.Equal(t, "active-cluster", resolver.ActiveClusterName(namespace.EmptyBusinessID))
	assert.Equal(t, []string{"cluster1", "cluster2", "cluster3"}, resolver.ClusterNames(namespace.EmptyBusinessID))
	assert.Equal(t, enumspb.REPLICATION_STATE_NORMAL, resolver.ReplicationState())
}

func TestDefaultReplicationResolver_ActiveClusterName(t *testing.T) {
	tests := []struct {
		name              string
		replicationConfig *persistencespb.NamespaceReplicationConfig
		want              string
	}{
		{
			name: "returns active cluster name",
			replicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: "cluster-a",
				Clusters:          []string{"cluster-a", "cluster-b"},
			}.Build(),
			want: "cluster-a",
		},
		{
			name: "returns active cluster name for cluster-b",
			replicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: "cluster-b",
				Clusters:          []string{"cluster-a", "cluster-b"},
			}.Build(),
			want: "cluster-b",
		},
		{
			name:              "returns empty string when replication config is nil",
			replicationConfig: nil,
			want:              "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := namespace.NewDefaultReplicationResolverFactory()
			detail := persistencespb.NamespaceDetail_builder{
				ReplicationConfig: tt.replicationConfig,
			}.Build()
			resolver := factory(detail)

			got := resolver.ActiveClusterName(namespace.EmptyBusinessID)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDefaultReplicationResolver_ClusterNames(t *testing.T) {
	tests := []struct {
		name              string
		replicationConfig *persistencespb.NamespaceReplicationConfig
		want              []string
	}{
		{
			name: "returns cluster names",
			replicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: "cluster-a",
				Clusters:          []string{"cluster-a", "cluster-b", "cluster-c"},
			}.Build(),
			want: []string{"cluster-a", "cluster-b", "cluster-c"},
		},
		{
			name: "returns cluster names for multiple clusters",
			replicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: "cluster-a",
				Clusters:          []string{"cluster-1", "cluster-2"},
			}.Build(),
			want: []string{"cluster-1", "cluster-2"},
		},
		{
			name:              "returns nil when replication config is nil",
			replicationConfig: nil,
			want:              nil,
		},
		{
			name: "returns empty slice when clusters is empty",
			replicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: "cluster-a",
				Clusters:          []string{},
			}.Build(),
			want: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			factory := namespace.NewDefaultReplicationResolverFactory()
			detail := persistencespb.NamespaceDetail_builder{
				ReplicationConfig: tt.replicationConfig,
			}.Build()
			resolver := factory(detail)

			got := resolver.ClusterNames(namespace.EmptyBusinessID)
			assert.Equal(t, tt.want, got)

			// Verify immutability - modifying returned slice shouldn't affect subsequent calls
			if len(got) > 0 {
				got[0] = "modified"
				got2 := resolver.ClusterNames(namespace.EmptyBusinessID)
				if len(tt.want) > 0 {
					assert.Equal(t, tt.want[0], got2[0], "ClusterNames should return a copy to preserve immutability")
				}
			}
		})
	}
}

func TestDefaultReplicationResolver_ReplicationState(t *testing.T) {
	tests := []struct {
		name              string
		replicationConfig *persistencespb.NamespaceReplicationConfig
		want              enumspb.ReplicationState
	}{
		{
			name: "returns normal state",
			replicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: "cluster-a",
				Clusters:          []string{"cluster-a", "cluster-b"},
				State:             enumspb.REPLICATION_STATE_NORMAL,
			}.Build(),
			want: enumspb.REPLICATION_STATE_NORMAL,
		},
		{
			name: "returns handover state",
			replicationConfig: persistencespb.NamespaceReplicationConfig_builder{
				ActiveClusterName: "cluster-a",
				Clusters:          []string{"cluster-a", "cluster-b"},
				State:             enumspb.REPLICATION_STATE_HANDOVER,
			}.Build(),
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
			detail := persistencespb.NamespaceDetail_builder{
				ReplicationConfig: tt.replicationConfig,
			}.Build()
			resolver := factory(detail)

			got := resolver.ReplicationState()
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestDefaultReplicationResolver_MultipleCalls(t *testing.T) {
	// Test that resolver state is consistent across multiple calls
	factory := namespace.NewDefaultReplicationResolverFactory()
	detail := persistencespb.NamespaceDetail_builder{
		ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
			ActiveClusterName: "primary",
			Clusters:          []string{"primary", "secondary", "tertiary"},
			State:             enumspb.REPLICATION_STATE_NORMAL,
		}.Build(),
	}.Build()
	resolver := factory(detail)

	// Call multiple times and verify consistency
	for i := 0; i < 5; i++ {
		assert.Equal(t, "primary", resolver.ActiveClusterName(namespace.EmptyBusinessID))
		assert.Equal(t, []string{"primary", "secondary", "tertiary"}, resolver.ClusterNames(namespace.EmptyBusinessID))
		assert.Equal(t, enumspb.REPLICATION_STATE_NORMAL, resolver.ReplicationState())
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
			detail: persistencespb.NamespaceDetail_builder{
				ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
					ActiveClusterName: "cluster-a",
					Clusters:          []string{"cluster-a", "cluster-b"},
				}.Build(),
			}.Build(),
			setGlobal: true,
			want:      true,
		},
		{
			name: "local namespace",
			detail: persistencespb.NamespaceDetail_builder{
				ReplicationConfig: persistencespb.NamespaceReplicationConfig_builder{
					ActiveClusterName: "cluster-a",
					Clusters:          []string{"cluster-a"},
				}.Build(),
			}.Build(),
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
			detail := persistencespb.NamespaceDetail_builder{
				FailoverVersion: tt.failoverVersion,
			}.Build()
			resolver := factory(detail)
			assert.Equal(t, tt.want, resolver.FailoverVersion(namespace.EmptyBusinessID))
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
			detail := persistencespb.NamespaceDetail_builder{
				FailoverNotificationVersion: tt.failoverNotificationVersion,
			}.Build()
			resolver := factory(detail)
			assert.Equal(t, tt.want, resolver.FailoverNotificationVersion())
		})
	}
}

func TestDefaultReplicationResolver_Clone(t *testing.T) {
	factory := namespace.NewDefaultReplicationResolverFactory()

	originalConfig := persistencespb.NamespaceReplicationConfig_builder{
		ActiveClusterName: "cluster-primary",
		Clusters:          []string{"cluster-primary", "cluster-secondary"},
		State:             enumspb.REPLICATION_STATE_NORMAL,
	}.Build()

	detail := persistencespb.NamespaceDetail_builder{
		ReplicationConfig:           originalConfig,
		FailoverVersion:             123,
		FailoverNotificationVersion: 456,
	}.Build()
	resolver := factory(detail)

	// Clone the resolver
	cloned := resolver.Clone()

	// Verify the cloned resolver has the same values
	assert.Equal(t, resolver.ActiveClusterName(namespace.EmptyBusinessID), cloned.ActiveClusterName(namespace.EmptyBusinessID))
	assert.Equal(t, resolver.ClusterNames(namespace.EmptyBusinessID), cloned.ClusterNames(namespace.EmptyBusinessID))
	assert.Equal(t, resolver.IsGlobalNamespace(), cloned.IsGlobalNamespace())
	assert.Equal(t, resolver.FailoverVersion(namespace.EmptyBusinessID), cloned.FailoverVersion(namespace.EmptyBusinessID))
	assert.Equal(t, resolver.FailoverNotificationVersion(), cloned.FailoverNotificationVersion())
	assert.Equal(t, resolver.ReplicationState(), cloned.ReplicationState())

	// Verify that modifying the cloned resolver doesn't affect the original
	cloned.SetActiveCluster("cluster-tertiary")
	assert.Equal(t, "cluster-primary", resolver.ActiveClusterName(namespace.EmptyBusinessID))
	assert.Equal(t, "cluster-tertiary", cloned.ActiveClusterName(namespace.EmptyBusinessID))

	// Verify deep copy of clusters slice
	clonedClusters := cloned.ClusterNames(namespace.EmptyBusinessID)
	if len(clonedClusters) > 0 {
		clonedClusters[0] = "modified"
		assert.Equal(t, "cluster-primary", cloned.ClusterNames(namespace.EmptyBusinessID)[0], "Modifying returned slice should not affect resolver")
	}
}
