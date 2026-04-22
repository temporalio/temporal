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
			name: "mixed with namespace id",
			completion: &tokenspb.NexusOperationCompletion{
				NamespaceId:  "ns-id",
				ComponentRef: []byte("component-ref"),
			},
			wantErr: "both HSM and CHASM",
		},
		{
			name: "mixed with workflow id",
			completion: &tokenspb.NexusOperationCompletion{
				WorkflowId:   "wf-id",
				ComponentRef: []byte("component-ref"),
			},
			wantErr: "both HSM and CHASM",
		},
		{
			name: "mixed with run id",
			completion: &tokenspb.NexusOperationCompletion{
				RunId:        "run-id",
				ComponentRef: []byte("component-ref"),
			},
			wantErr: "both HSM and CHASM",
		},
		{
			name: "mixed with ref",
			completion: &tokenspb.NexusOperationCompletion{
				Ref:          &persistencespb.StateMachineRef{},
				ComponentRef: []byte("component-ref"),
			},
			wantErr: "both HSM and CHASM",
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
