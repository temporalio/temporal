package principaltoken

import (
	"context"

	"go.uber.org/fx"
)

// Module materializes the principal-token components from a Config and exposes
// them all at once: the Signer (outbound dispatch), the Verifier (inbound Nexus
// HTTP handler), and the KeyProvider (JWKS endpoint). Include it once in any
// service graph that needs to sign and/or verify propagated identity.
//
// The default Config is empty, so the feature is OFF: Signer and Verifier
// resolve to nil and callers fall back to their no-propagation behavior.
// Operators / Cloud activate it by overriding the Config — supply real key
// material (a PEM signing key and/or trusted-issuer public keys) via
// fx.Decorate:
//
//	fx.Decorate(func(principaltoken.Config) principaltoken.Config {
//	    return principaltoken.Config{Issuer: ..., SigningKeyPEM: ..., ...}
//	})
//
// No call-site change is needed to turn it on.
var Module = fx.Module(
	"common.nexus.principaltoken",
	fx.Provide(defaultConfig),
	fx.Provide(New), // Config -> *Bundle
	fx.Provide(signerFromBundle),
	fx.Provide(keyProviderFromBundle),
	fx.Provide(defaultResolver),
	fx.Provide(defaultPeerTrust),
	fx.Provide(NewVerifier), // (Config, KeyProvider, PeerTrustFunc) -> Verifier
)

// defaultConfig is the feature-off default. Override via fx.Decorate.
func defaultConfig() Config {
	return Config{}
}

// defaultResolver is the OSS default: names are already human-readable, so
// nothing is resolved. Cloud overrides this provider with an ID->name resolver.
func defaultResolver() PrincipalResolver {
	return NoopResolver{}
}

// defaultPeerTrust is the OSS default for transport mode: trust nothing. A host
// that uses TrustModeTransport (e.g. Cloud cell-to-cell mTLS) MUST override this
// with a function that recognizes trusted server peers from the request context.
func defaultPeerTrust() PeerTrustFunc {
	return func(context.Context) bool { return false }
}

// signerFromBundle exposes the bundle's signer (nil when no signing key is
// configured) for the outbound dispatch task handler.
func signerFromBundle(b *Bundle) Signer {
	return b.Signer
}

// keyProviderFromBundle exposes the key provider (always non-nil) for serving
// this cluster's public JWKS.
func keyProviderFromBundle(b *Bundle) KeyProvider {
	return b.KeyProvider
}
