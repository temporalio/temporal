package wideevents

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRemoteClusterLifecycleEventName(t *testing.T) {
	require.Equal(t, NamespaceLifecycleEventName, RemoteClusterLifecyclePayload{}.EventName())
}

func TestRemoteClusterLifecycleFieldSetMatchesNamespaceLifecycle(t *testing.T) {
	payload := RemoteClusterLifecyclePayload{
		Phase:       "remote_cluster_upsert",
		Namespace:   "N/A",
		NamespaceID: "N/A",
		Details:     map[string]any{"outcome": "succeeded"},
	}

	require.Equal(t, NamespaceLifecyclePayload(payload).Attributes(), payload.Attributes())
}
