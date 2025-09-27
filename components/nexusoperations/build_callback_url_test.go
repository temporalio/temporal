package nexusoperations

import (
	"testing"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
)

func TestBuildCallbackURL(t *testing.T) {
	ns := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "ns-name", Id: "ns-id"},
		&persistencespb.NamespaceConfig{},
		"active-cluster",
	)

	workerEndpoint := &persistencespb.NexusEndpointEntry{
		Endpoint: &persistencespb.NexusEndpoint{
			Spec: &persistencespb.NexusEndpointSpec{
				Name: "endpoint",
				Target: &persistencespb.NexusEndpointTarget{
					Variant: &persistencespb.NexusEndpointTarget_Worker_{
						Worker: &persistencespb.NexusEndpointTarget_Worker{
							NamespaceId: "ns-id",
							TaskQueue:   "nexus-tq",
						},
					},
				},
			},
		},
	}

	externalEndpoint := &persistencespb.NexusEndpointEntry{
		Endpoint: &persistencespb.NexusEndpoint{
			Spec: &persistencespb.NexusEndpointSpec{
				Name: "endpoint",
				Target: &persistencespb.NexusEndpointTarget{
					Variant: &persistencespb.NexusEndpointTarget_External_{
						External: &persistencespb.NexusEndpointTarget_External{Url: "https://api.example.com"},
					},
				},
			},
		},
	}

	// When UseSystemCallbackURL is true and target is worker, return the system URL
	got, err := buildCallbackURL(true, "http://example/callback/{{.NamespaceName}}", ns, workerEndpoint)
	require.NoError(t, err)
	require.Equal(t, "temporal://system", got)

	// When UseSystemCallbackURL is true but target is external, use the template
	got, err = buildCallbackURL(true, "http://example/callback/{{.NamespaceName}}-{{.NamespaceID}}", ns, externalEndpoint)
	require.NoError(t, err)
	require.Equal(t, "http://example/callback/ns-name-ns-id", got)

	// When UseSystemCallbackURL is false, always use the template
	got, err = buildCallbackURL(false, "https://cb/{{.NamespaceID}}/{{.NamespaceName}}", ns, workerEndpoint)
	require.NoError(t, err)
	require.Equal(t, "https://cb/ns-id/ns-name", got)
}
