package namespace_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
	"go.uber.org/mock/gomock"
)

func TestCachedID(t *testing.T) {
	t.Parallel()

	registry := namespace.NewMockRegistry(gomock.NewController(t))
	known := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Id: "known-id", Name: "known"}, nil, "")
	// Both lookups must not read through to persistence.
	registry.EXPECT().
		GetNamespaceWithOptions(namespace.Name("known"), namespace.GetNamespaceOptions{DisableReadthrough: true}).
		Return(known, nil)
	registry.EXPECT().
		GetNamespaceWithOptions(namespace.Name("unknown"), namespace.GetNamespaceOptions{DisableReadthrough: true}).
		Return(nil, errors.New("not found"))

	id, ok := namespace.CachedID(registry, "known")
	require.True(t, ok)
	require.Equal(t, namespace.ID("known-id"), id)

	id, ok = namespace.CachedID(registry, "unknown")
	require.False(t, ok)
	require.Empty(t, id)
}

func TestCachedName(t *testing.T) {
	t.Parallel()

	registry := namespace.NewMockRegistry(gomock.NewController(t))
	known := namespace.NewLocalNamespaceForTest(&persistencespb.NamespaceInfo{Id: "known-id", Name: "known"}, nil, "")
	// Both lookups must not read through to persistence.
	registry.EXPECT().
		GetNamespaceByIDWithOptions(namespace.ID("known-id"), namespace.GetNamespaceOptions{DisableReadthrough: true}).
		Return(known, nil)
	registry.EXPECT().
		GetNamespaceByIDWithOptions(namespace.ID("unknown-id"), namespace.GetNamespaceOptions{DisableReadthrough: true}).
		Return(nil, errors.New("not found"))

	name, ok := namespace.CachedName(registry, "known-id")
	require.True(t, ok)
	require.Equal(t, namespace.Name("known"), name)

	name, ok = namespace.CachedName(registry, "unknown-id")
	require.False(t, ok)
	require.Empty(t, name)
}
