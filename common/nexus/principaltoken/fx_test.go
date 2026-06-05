package principaltoken

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

// TestModule_FxGraph validates that the module's provider chain
// (Config -> Bundle -> Signer/Verifier/KeyProvider) resolves, so it wires
// cleanly into both the frontend (verify) and history (sign) service graphs.
func TestModule_FxGraph(t *testing.T) {
	require.NoError(t, fx.ValidateApp(
		Module,
		fx.Invoke(func(Signer, Verifier, KeyProvider, PrincipalResolver) {}),
	))
}

// TestModule_DefaultIsFeatureOff confirms the default config yields nil signer
// and verifier (feature off, safe default) and an always-present key provider.
func TestModule_DefaultIsFeatureOff(t *testing.T) {
	b, err := New(defaultConfig())
	require.NoError(t, err)
	require.Nil(t, signerFromBundle(b))
	require.NotNil(t, keyProviderFromBundle(b))
	require.Nil(t, NewVerifier(defaultConfig(), keyProviderFromBundle(b), defaultPeerTrust()))
}
