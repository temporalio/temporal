//go:build test_dep

package testcore

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/namespace"
)

func TestRPCFaultOptionsNamespaceScopes(t *testing.T) {
	t.Parallel()

	testCases := []struct {
		name          string
		options       rpcFaultOptions
		namespaceID   namespace.ID
		namespaceName namespace.Name
	}{
		{
			name: "no namespace scope",
		},
		{
			name:        "namespace ID",
			options:     rpcFaultOptions{namespaceID: "namespace-id"},
			namespaceID: "namespace-id",
		},
		{
			name:          "namespace name",
			options:       rpcFaultOptions{namespaceName: "namespace-name"},
			namespaceName: "namespace-name",
		},
		{
			name: "namespace ID and name",
			options: rpcFaultOptions{
				namespaceID:   "namespace-id",
				namespaceName: "namespace-name",
			},
			namespaceID:   "namespace-id",
			namespaceName: "namespace-name",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			namespaceID, namespaceName, ok := tc.options.namespaceScopes()
			require.Equal(t, tc.namespaceID != "" || tc.namespaceName != "", ok)
			require.Equal(t, tc.namespaceID, namespaceID)
			require.Equal(t, tc.namespaceName, namespaceName)
		})
	}
}
