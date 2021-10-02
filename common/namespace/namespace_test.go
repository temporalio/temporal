package namespace_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
	persistence "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/primitives/timestamp"
)

func base(t *testing.T) *namespace.Namespace {
	return namespace.FromPersistentState(&persistence.GetNamespaceResponse{
		Namespace: &persistencespb.NamespaceDetail{
			Info: &persistencespb.NamespaceInfo{
				Id:   uuid.NewString(),
				Name: t.Name(),
				Data: make(map[string]string),
			},
			Config: &persistencespb.NamespaceConfig{},
			ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
				ActiveClusterName: "foo",
				Clusters:          []string{"foo", "bar"},
			},
		},
	})
}

func TestActiveInCluster(t *testing.T) {
	base := base(t)

	for _, tt := range [...]struct {
		name        string
		testCluster string
		entry       *namespace.Namespace
		want        bool
	}{
		{
			name:        "global and cluster match",
			testCluster: "foo",
			entry: base.Clone(namespace.WithActiveCluster("foo"),
				namespace.WithGlobalFlag(true)),
			want: true,
		},
		{
			name:        "global and cluster mismatch",
			testCluster: "bar",
			entry: base.Clone(namespace.WithActiveCluster("foo"),
				namespace.WithGlobalFlag(true)),
			want: false,
		},
		{
			name:        "non-global and cluster mismatch",
			testCluster: "bar",
			entry: base.Clone(namespace.WithActiveCluster("foo"),
				namespace.WithGlobalFlag(false)),
			want: true,
		},
		{
			name:        "non-global and cluster match",
			testCluster: "foo",
			entry: base.Clone(namespace.WithActiveCluster("foo"),
				namespace.WithGlobalFlag(false)),
			want: true,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, tt.entry.ActiveInCluster(tt.testCluster))
		})
	}
}

func Test_GetRetentionDays(t *testing.T) {
	const defaultRetention = 7 * 24 * time.Hour
	base := base(t).Clone(namespace.WithRetention(timestamp.DurationFromDays(7)))
	for _, tt := range [...]struct {
		name       string
		retention  string
		rate       string
		workflowID string
		want       time.Duration
	}{
		{
			name:       "30x0",
			retention:  "30",
			rate:       "0",
			workflowID: uuid.NewString(),
			want:       defaultRetention,
		},
		{
			name:       "30x1",
			retention:  "30",
			rate:       "1",
			workflowID: uuid.NewString(),
			want:       30 * 24 * time.Hour,
		},
		{
			name:       "invalid retention",
			retention:  "invalid-value",
			workflowID: uuid.NewString(),
			want:       defaultRetention,
		},
		{
			name:       "invalid rate",
			retention:  "30",
			rate:       "invalid-value",
			workflowID: uuid.NewString(),
			want:       defaultRetention,
		},
		{
			name:       "hash outside of sample rate",
			retention:  "30",
			rate:       "0.8",
			workflowID: "3aef42a8-db0a-4a3b-b8b7-9829d74b4ebf",
			want:       defaultRetention,
		},
		{
			name:       "hash inside sample rate",
			retention:  "30",
			rate:       "0.9",
			workflowID: "3aef42a8-db0a-4a3b-b8b7-9829d74b4ebf",
			want:       30 * 24 * time.Hour,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ns := base.Clone(
				namespace.WithData(namespace.SampleRetentionKey, tt.retention),
				namespace.WithData(namespace.SampleRateKey, tt.rate))
			require.Equal(t, tt.want, ns.Retention(tt.workflowID))
		})
	}
}

func TestIsSampledForLongerRetentionEnabled(t *testing.T) {
	ns := base(t)
	wid := uuid.NewString()
	require.False(t, ns.IsSampledForLongerRetentionEnabled(wid))
	ns = ns.Clone(
		namespace.WithData(namespace.SampleRetentionKey, "30"),
		namespace.WithData(namespace.SampleRateKey, "0"))
	require.True(t, ns.IsSampledForLongerRetentionEnabled(wid))
}
