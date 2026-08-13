package frontend

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/persistence"
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
		},
		Config: &persistencespb.NamespaceConfig{
			Retention:               durationpb.New(168 * time.Hour),
			HistoryArchivalState:    enumspb.ARCHIVAL_STATE_DISABLED,
			VisibilityArchivalState: enumspb.ARCHIVAL_STATE_ENABLED,
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
	require.Equal(t, "Registered", f.State)
	require.True(t, f.IsGlobalNamespace)
	require.EqualValues(t, 3, f.ConfigVersion)
	require.EqualValues(t, 10, f.FailoverVersion)
	require.EqualValues(t, 9, f.FailoverNotificationVersion)
	require.Equal(t, "2026-01-02T03:04:05Z", f.FailoverEndTime)
	require.Equal(t, (168 * time.Hour).String(), f.Retention)
	require.Equal(t, "Disabled", f.HistoryArchivalState)
	require.Equal(t, "Enabled", f.VisibilityArchivalState)
	require.Equal(t, "clusterA", f.ActiveCluster)
	require.Equal(t, []string{"clusterA", "clusterB"}, f.Clusters)
	require.Equal(t, "Normal", f.ReplicationState)
	require.Len(t, f.FailoverHistory, 1)
	require.EqualValues(t, 10, f.FailoverHistory[0].FailoverVersion)
}

// The before snapshot is unaffected by a later in-place mutation of the record (clusters are cloned).
func TestNamespaceStateFieldsClonesSlices(t *testing.T) {
	detail := testNamespaceDetail()
	before := namespaceStateFields(detail, true)
	detail.ReplicationConfig.Clusters[0] = "mutated"
	require.Equal(t, "clusterA", before.Clusters[0])
}

func TestBuildNamespaceRegisteredInput(t *testing.T) {
	in := buildNamespaceRegisteredInput(&persistence.CreateNamespaceRequest{
		Namespace:         testNamespaceDetail(),
		IsGlobalNamespace: true,
	}, "ns-id")

	require.Equal(t, "ns", in.Namespace)
	require.Equal(t, "ns-id", in.NamespaceID)
	require.True(t, in.Fields.IsGlobalNamespace)
	require.Equal(t, "clusterA", in.Fields.ActiveCluster)
}

// A failover surfaces as before/after active-cluster + failover-version deltas plus the is_failover
// flag; the phase stays namespace_updated.
func TestBuildNamespaceUpdatedInputCapturesBeforeAndAfter(t *testing.T) {
	before := namespaceStateFields(testNamespaceDetail(), true)

	after := testNamespaceDetail()
	after.ReplicationConfig.ActiveClusterName = "clusterB"
	after.FailoverVersion = 21

	in := buildNamespaceUpdatedInput(before, after, true, true, false)

	require.Equal(t, "ns", in.Namespace)
	require.True(t, in.IsFailover)
	require.False(t, in.IsPromotion)
	require.Equal(t, "clusterA", in.Before.ActiveCluster)
	require.Equal(t, "clusterB", in.After.ActiveCluster)
	require.EqualValues(t, 10, in.Before.FailoverVersion)
	require.EqualValues(t, 21, in.After.FailoverVersion)
}
