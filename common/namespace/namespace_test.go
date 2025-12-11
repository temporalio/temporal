package namespace_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	namespacepb "go.temporal.io/api/namespace/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
)

func base(t *testing.T) *namespace.Namespace {
	detail := &persistencespb.NamespaceDetail{
		Info: &persistencespb.NamespaceInfo{
			Id:   namespace.NewID().String(),
			Name: t.Name(),
			Data: make(map[string]string),
		},
		Config: &persistencespb.NamespaceConfig{
			BadBinaries: &namespacepb.BadBinaries{
				Binaries: make(map[string]*namespacepb.BadBinaryInfo),
			},
		},
		ReplicationConfig: &persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: "foo",
			Clusters:          []string{"foo", "bar"},
		},
	}
	factory := namespace.NewDefaultReplicationResolverFactory()
	resolver := factory(detail)
	ns, err := namespace.FromPersistentState(detail, resolver)
	require.NoError(t, err)
	return ns
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
			entry: base.Clone(
				namespace.WithActiveCluster("foo"),
				namespace.WithGlobalFlag(true)),
			want: true,
		},
		{
			name:        "global and cluster mismatch",
			testCluster: "bar",
			entry: base.Clone(
				namespace.WithActiveCluster("foo"),
				namespace.WithGlobalFlag(true)),
			want: false,
		},
		{
			name:        "non-global and cluster mismatch",
			testCluster: "bar",
			entry: base.Clone(
				namespace.WithActiveCluster("foo"),
				namespace.WithGlobalFlag(false)),
			want: true,
		},
		{
			name:        "non-global and cluster match",
			testCluster: "foo",
			entry: base.Clone(
				namespace.WithActiveCluster("foo"),
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
		workflowID string
		want       time.Duration
	}{
		{
			name:       "30x0",
			retention:  "30",
			workflowID: uuid.NewString(),
			want:       defaultRetention,
		},
		{
			name:       "invalid retention",
			retention:  "invalid-value",
			workflowID: uuid.NewString(),
			want:       defaultRetention,
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ns := base.Clone()
			require.Equal(t, tt.want, ns.Retention())
		})
	}
}

func TestNamespace_GetCustomData(t *testing.T) {
	base := base(t)
	ns := base.Clone(namespace.WithData("foo", "bar"))
	data := ns.GetCustomData("foo")
	assert.Equal(t, "bar", data)
	data2 := ns.GetCustomData("fake")
	assert.Equal(t, "", data2)
}
