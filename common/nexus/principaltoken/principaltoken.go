// Package principaltoken carries caller identity across the Nexus hop as a
// compact JWS (ES256): trust comes from the signature, not the transport, so it
// works regardless of network topology. The durable artifact is the Principal;
// the token is minted fresh per hop and never persisted. Sign/verify live behind
// interfaces so Cloud can swap implementations (KMS signer, JWKS, transport
// trust). It carries two principals (RFC 8693 style): the end-user (root) to
// preserve end-to-end and the immediate service caller, as structured claims
// (a Principal Name may contain "/", so it is not round-trippable as a "sub").
package principaltoken

import (
	"context"
	"errors"

	"github.com/golang-jwt/jwt/v4"
	commonpb "go.temporal.io/api/common/v1"
)

// Header carries the signed principal token across the Nexus hop. Stripped from
// inbound external requests (headers.StripPrincipalHTTP), so only the
// server-to-server caller-side write can set it.
const Header = "Temporal-Nexus-Principal-Token"

// signingAlg is the only accepted JWS algorithm. Asymmetric so verifiers hold
// only public keys and cannot mint tokens.
const signingAlg = "ES256"

var (
	// ErrNoToken: carrier header absent; treated as "no propagated identity".
	ErrNoToken = errors.New("principaltoken: no token present")
	// ErrVerification: any verification failure. Opaque on purpose — does not
	// leak which check failed.
	ErrVerification = errors.New("principaltoken: token verification failed")
	// ErrNoSigningKey: no signing key configured (feature off).
	ErrNoSigningKey = errors.New("principaltoken: no signing key configured")
)

// Content is the identity payload to mint into a token.
type Content struct {
	// ServiceCaller is the immediate actor (the worker / SDK client that issued
	// the outbound Nexus call). May be nil.
	ServiceCaller *commonpb.Principal
	// EndUser is the chain-originating identity to preserve end-to-end. May be
	// nil. For a single-hop standalone operation it equals ServiceCaller.
	EndUser *commonpb.Principal
	// Display-name snapshots for the respective principals (empty in OSS). They
	// travel in the token because the handler often cannot resolve the caller's
	// identity itself (e.g. cross-account).
	ServiceCallerResolvedName string
	EndUserResolvedName       string
	// NamespaceCaller is the caller's own namespace, self-asserted by the minting
	// cluster; only meaningful scoped to the issuer. May be nil.
	NamespaceCaller *commonpb.Principal
}

// Verified holds the trusted principals extracted from a token that passed the
// verifier's trust check.
type Verified struct {
	ServiceCaller             *commonpb.Principal
	EndUser                   *commonpb.Principal
	NamespaceCaller           *commonpb.Principal
	ServiceCallerResolvedName string
	EndUserResolvedName       string
	Issuer                    string
}

// Signer mints a signed carrier token for the given content. The OSS default
// is ECDSASigner; Cloud may provide a KMS-backed implementation.
type Signer interface {
	Sign(ctx context.Context, content Content) (string, error)
}

// Verifier validates a carrier token and returns the trusted principals.
// Implementations must return ErrVerification (not a descriptive error) on any
// failure, and must never return principals from an untrusted token.
type Verifier interface {
	Verify(ctx context.Context, token string) (*Verified, error)
}

// principalClaim is the structured, round-trippable representation of a
// Principal inside the token (avoids the lossy display string), plus the
// resolved-name snapshot.
type principalClaim struct {
	Type         string `json:"type,omitempty"`
	Name         string `json:"name,omitempty"`
	ResolvedName string `json:"resolved_name,omitempty"`
}

func toPrincipalClaim(p *commonpb.Principal, resolvedName string) *principalClaim {
	empty := p.GetType() == "" && p.GetName() == ""
	if empty && resolvedName == "" {
		return nil
	}
	return &principalClaim{
		Type:         p.GetType(),
		Name:         p.GetName(),
		ResolvedName: resolvedName,
	}
}

func (c *principalClaim) toPrincipal() *commonpb.Principal {
	if c == nil || (c.Type == "" && c.Name == "") {
		return nil
	}
	return &commonpb.Principal{Type: c.Type, Name: c.Name}
}

func (c *principalClaim) resolvedName() string {
	if c == nil {
		return ""
	}
	return c.ResolvedName
}

// displayString renders a principal as type/name. Informational only — set as
// "sub" for debugging/auditing, never parsed back.
func displayString(p *commonpb.Principal) string {
	if p == nil {
		return ""
	}
	return p.GetType() + "/" + p.GetName()
}

// tokenClaims is the JWS payload: end-user (subject), service caller, and
// namespace caller as structured claims.
type tokenClaims struct {
	jwt.RegisteredClaims
	EndUser         *principalClaim `json:"https://temporal.io/end_user,omitempty"`
	ServiceCaller   *principalClaim `json:"https://temporal.io/service_caller,omitempty"`
	NamespaceCaller *principalClaim `json:"https://temporal.io/namespace_caller,omitempty"`
}

// toVerified projects the claims into a Verified (shared by both verifiers).
func (c *tokenClaims) toVerified() *Verified {
	return &Verified{
		ServiceCaller:             c.ServiceCaller.toPrincipal(),
		EndUser:                   c.EndUser.toPrincipal(),
		NamespaceCaller:           c.NamespaceCaller.toPrincipal(),
		ServiceCallerResolvedName: c.ServiceCaller.resolvedName(),
		EndUserResolvedName:       c.EndUser.resolvedName(),
		Issuer:                    c.Issuer,
	}
}
