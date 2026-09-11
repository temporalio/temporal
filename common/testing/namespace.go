package testing

import (
	"testing"

	"github.com/stretchr/testify/require"
	namespacepb "go.temporal.io/api/namespace/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
)

// NewNamespace returns a Namespace named after the running test, with a generated
// ID and a two-cluster replication config active in "foo".
func NewNamespace(t *testing.T) *namespace.Namespace {
	t.Helper()
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
