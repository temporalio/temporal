package nexus

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/api/serviceerror"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
)

func TestCallbackTokenGenerator_DecodeCompletion(t *testing.T) {
	g := NewCallbackTokenGenerator()

	for _, tc := range []struct {
		name       string
		completion *tokenspb.NexusOperationCompletion
		wantErr    string
	}{
		{
			name: "valid HSM",
			completion: &tokenspb.NexusOperationCompletion{
				NamespaceId: "ns-id",
				WorkflowId:  "wf-id",
				RunId:       "run-id",
				Ref:         &persistencespb.StateMachineRef{},
			},
		},
		{
			name: "valid CHASM",
			completion: &tokenspb.NexusOperationCompletion{
				ComponentRef: []byte("component-ref"),
			},
		},
		{
			// CHASM tokens may carry HSM-shaped routing hints alongside ComponentRef
			// so legacy HSM-only callback routers can still route the completion.
			name: "CHASM with HSM routing hints",
			completion: &tokenspb.NexusOperationCompletion{
				NamespaceId:  "ns-id",
				WorkflowId:   "wf-id",
				RunId:        "run-id",
				ComponentRef: []byte("component-ref"),
			},
		},
		{
			name: "CHASM with partial HSM routing hints",
			completion: &tokenspb.NexusOperationCompletion{
				NamespaceId:  "ns-id",
				ComponentRef: []byte("component-ref"),
			},
		},
		{
			name:       "empty",
			completion: &tokenspb.NexusOperationCompletion{},
			wantErr:    "either all HSM fields or a component ref",
		},
	} {
		tokenString, err := g.Tokenize(tc.completion)
		require.NoError(t, err, tc.name)

		token, err := DecodeCallbackToken(tokenString)
		require.NoError(t, err, tc.name)

		completion, err := g.DecodeCompletion(token)
		if tc.wantErr == "" {
			require.NoError(t, err, tc.name)
			require.NotNil(t, completion, tc.name)
			continue
		}

		require.ErrorContains(t, err, tc.wantErr, tc.name)
		var invalidArgumentErr *serviceerror.InvalidArgument
		require.ErrorAs(t, err, &invalidArgumentErr, tc.name)
		require.Nil(t, completion, tc.name)
	}
}
