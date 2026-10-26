package activity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/chasm"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/protobuf/types/known/durationpb"
)

func TestComponentRetention(t *testing.T) {
	registry := chasm.NewRegistry(log.NewNoopLogger())
	require.NoError(t, registry.Register(newComponentOnlyLibrary(nil)))
	rc, ok := registry.ComponentByID(ArchetypeID)
	require.True(t, ok)

	// The namespace retention is longer than the archetype retention to show that the archetype
	// retention replaces it rather than only acting as a fallback.
	ns := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "test-namespace"},
		&persistencespb.NamespaceConfig{Retention: durationpb.New(30 * 24 * time.Hour)},
		"active",
	)
	retention, ok := rc.Retention(ns)
	require.True(t, ok)
	require.Equal(t, 24*time.Hour, retention)
}
