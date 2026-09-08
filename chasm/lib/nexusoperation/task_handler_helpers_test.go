package nexusoperation

import (
	"testing"
	"text/template"

	"github.com/stretchr/testify/require"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
)

func TestBuildCallbackURL(t *testing.T) {
	t.Parallel()

	ns := namespace.NewLocalNamespaceForTest(
		&persistencespb.NamespaceInfo{Name: "ns-name", Id: "ns-id"},
		&persistencespb.NamespaceConfig{},
		"active-cluster",
	)
	callbackTemplate, err := template.New("callback").Parse("http://example/callback/{{.NamespaceName}}-{{.NamespaceID}}")
	require.NoError(t, err)

	workerEndpoint := &persistencespb.NexusEndpointEntry{
		Endpoint: &persistencespb.NexusEndpoint{
			Spec: &persistencespb.NexusEndpointSpec{
				Target: &persistencespb.NexusEndpointTarget{
					Variant: &persistencespb.NexusEndpointTarget_Worker_{},
				},
			},
		},
	}
	externalEndpoint := &persistencespb.NexusEndpointEntry{
		Endpoint: &persistencespb.NexusEndpoint{
			Spec: &persistencespb.NexusEndpointSpec{
				Target: &persistencespb.NexusEndpointTarget{
					Variant: &persistencespb.NexusEndpointTarget_External_{},
				},
			},
		},
	}

	got, err := buildCallbackURL(callbackTemplate, ns, workerEndpoint)
	require.NoError(t, err)
	require.Equal(t, commonnexus.SystemCallbackURL, got)

	got, err = buildCallbackURL(callbackTemplate, ns, externalEndpoint)
	require.NoError(t, err)
	require.Equal(t, "http://example/callback/ns-name-ns-id", got)
}
