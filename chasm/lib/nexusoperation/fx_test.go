package nexusoperation

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/nexus/principaltoken"
	"go.uber.org/fx"
)

// TestPrincipalTokenConfigDecorator_FxGraph validates that the official-config
// decorator composes with principaltoken.Module: given a *config.Config in
// scope, the module's default Config is overridden and Signer/Verifier still
// resolve.
func TestPrincipalTokenConfigDecorator_FxGraph(t *testing.T) {
	require.NoError(t, fx.ValidateApp(
		fx.Supply(&config.Config{}),
		principaltoken.Module,
		fx.Decorate(principalTokenConfigFromServerConfig),
		fx.Invoke(func(principaltoken.Signer, principaltoken.Verifier, principaltoken.KeyProvider) {}),
	))
}

func TestPrincipalTokenConfigFromServerConfig(t *testing.T) {
	// Empty server config => feature off (no key material).
	out, err := principalTokenConfigFromServerConfig(principaltoken.Config{}, &config.Config{})
	require.NoError(t, err)
	require.Empty(t, out.SigningKeyPEM)
	require.Nil(t, out.TrustedIssuers)

	// Populated inline => mapped through verbatim.
	cfg := &config.Config{}
	cfg.Global.NexusPrincipalPropagation = config.NexusPrincipalPropagation{
		Issuer:         "iss",
		SigningKeyID:   "k1",
		SigningKeyData: "PRIV-PEM",
		TrustedIssuers: []config.NexusTrustedIssuer{
			{Issuer: "iss", KeyID: "k1", PublicKeyData: "PUB-PEM"},
		},
	}
	out, err = principalTokenConfigFromServerConfig(principaltoken.Config{}, cfg)
	require.NoError(t, err)
	require.Equal(t, "iss", out.Issuer)
	require.Equal(t, "k1", out.SigningKeyID)
	require.Equal(t, []byte("PRIV-PEM"), out.SigningKeyPEM)
	require.Equal(t, []byte("PUB-PEM"), out.TrustedIssuers["iss"]["k1"])
}
