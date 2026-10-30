package namespace_test

import (
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	test "go.temporal.io/server/common/testing"
)

func TestActiveInCluster(t *testing.T) {
	base := test.NewNamespace(t)

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
	base := test.NewNamespace(t).Clone(namespace.WithRetention(timestamp.DurationFromDays(7)))
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
	base := test.NewNamespace(t)
	ns := base.Clone(namespace.WithData("foo", "bar"))
	data := ns.GetCustomData("foo")
	assert.Equal(t, "bar", data)
	data2 := ns.GetCustomData("fake")
	assert.Equal(t, "", data2)
}
