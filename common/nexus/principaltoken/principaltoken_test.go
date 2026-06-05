package principaltoken

import (
	"context"
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
)

const (
	testIssuer = "cluster-a"
	testKID    = "key-1"
)

func newKey(t *testing.T) *ecdsa.PrivateKey {
	t.Helper()
	k, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	return k
}

// harness wires a signer + JWS verifier sharing a trusted key, with a
// controllable clock shared between them.
type harness struct {
	signer   *ECDSASigner
	verifier *JWSVerifier
	now      time.Time
}

func newHarness(t *testing.T, key *ecdsa.PrivateKey, ttl, leeway time.Duration) *harness {
	t.Helper()
	h := &harness{now: time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC)}
	nowFn := func() time.Time { return h.now }

	signer, err := NewECDSASigner(ECDSASignerOptions{Key: key, KID: testKID, Issuer: testIssuer, TTL: ttl, NowFn: nowFn})
	require.NoError(t, err)
	h.signer = signer

	kp := NewStaticKeyProvider(
		map[string]map[string]crypto.PublicKey{testIssuer: {testKID: key.Public()}},
		map[string]crypto.PublicKey{testKID: key.Public()},
	)
	h.verifier = NewJWSVerifier(JWSVerifierOptions{Keys: kp, Leeway: leeway, NowFn: nowFn})
	return h
}

func TestSignVerify_RoundTrip(t *testing.T) {
	h := newHarness(t, newKey(t), time.Minute, time.Second)
	svc := &commonpb.Principal{Type: "service-accounts", Name: "worker"}
	eu := &commonpb.Principal{Type: "users", Name: "alice@example.com"}

	tok, err := h.signer.Sign(context.Background(), Content{ServiceCaller: svc, EndUser: eu})
	require.NoError(t, err)
	got, err := h.verifier.Verify(context.Background(), tok)
	require.NoError(t, err)
	require.True(t, eq(svc, got.ServiceCaller))
	require.True(t, eq(eu, got.EndUser))
	require.Equal(t, testIssuer, got.Issuer)
}

// Guards the reason we use structured claims instead of a slash-joined "sub":
// a Name may contain "/".
func TestSignVerify_PreservesSlashInName(t *testing.T) {
	h := newHarness(t, newKey(t), time.Minute, time.Second)
	eu := &commonpb.Principal{Type: "service-accounts", Name: "my-project/sa-prod"}
	tok, err := h.signer.Sign(context.Background(), Content{EndUser: eu})
	require.NoError(t, err)
	got, err := h.verifier.Verify(context.Background(), tok)
	require.NoError(t, err)
	require.True(t, eq(eu, got.EndUser))
}

// Cloud case: principal.Name is an opaque ID and the human-readable snapshot
// rides in the token so the handler (which can't resolve the ID) can surface it.
func TestSignVerify_CarriesResolvedName(t *testing.T) {
	h := newHarness(t, newKey(t), time.Minute, time.Second)
	eu := &commonpb.Principal{Type: "cloud-identity", Name: "id-uuid-123"}
	tok, err := h.signer.Sign(context.Background(), Content{
		EndUser: eu, EndUserResolvedName: "alice@example.com",
	})
	require.NoError(t, err)
	got, err := h.verifier.Verify(context.Background(), tok)
	require.NoError(t, err)
	require.Equal(t, "alice@example.com", got.EndUserResolvedName)
}

func TestVerify_RejectsTamperedToken(t *testing.T) {
	h := newHarness(t, newKey(t), time.Minute, time.Second)
	tok, err := h.signer.Sign(context.Background(), Content{EndUser: &commonpb.Principal{Type: "users", Name: "alice"}})
	require.NoError(t, err)
	b := []byte(tok)
	b[len(b)/2] ^= 0x01
	_, err = h.verifier.Verify(context.Background(), string(b))
	require.ErrorIs(t, err, ErrVerification)
}

func TestVerify_RejectsWrongKey(t *testing.T) {
	h := newHarness(t, newKey(t), time.Minute, time.Second)
	other, err := NewECDSASigner(ECDSASignerOptions{Key: newKey(t), KID: testKID, Issuer: testIssuer, NowFn: func() time.Time { return h.now }})
	require.NoError(t, err)
	tok, err := other.Sign(context.Background(), Content{EndUser: &commonpb.Principal{Type: "users", Name: "mallory"}})
	require.NoError(t, err)
	_, err = h.verifier.Verify(context.Background(), tok)
	require.ErrorIs(t, err, ErrVerification)
}

func TestVerify_RejectsUnknownIssuerOrKID(t *testing.T) {
	key := newKey(t)
	nowFn := func() time.Time { return time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC) }
	tok, err := mint(t, key, "cluster-unknown", "key-x", nowFn)
	require.NoError(t, err)
	kp := NewStaticKeyProvider(map[string]map[string]crypto.PublicKey{testIssuer: {testKID: key.Public()}}, nil)
	v := NewJWSVerifier(JWSVerifierOptions{Keys: kp, NowFn: nowFn})
	_, err = v.Verify(context.Background(), tok)
	require.ErrorIs(t, err, ErrVerification)
}

func TestVerify_RejectsExpired(t *testing.T) {
	h := newHarness(t, newKey(t), time.Minute, 5*time.Second)
	tok, err := h.signer.Sign(context.Background(), Content{EndUser: &commonpb.Principal{Type: "users", Name: "alice"}})
	require.NoError(t, err)
	h.now = h.now.Add(time.Minute + 6*time.Second)
	_, err = h.verifier.Verify(context.Background(), tok)
	require.ErrorIs(t, err, ErrVerification)
}

func TestVerify_ToleratesClockSkewWithinLeeway(t *testing.T) {
	h := newHarness(t, newKey(t), 10*time.Second, 30*time.Second)
	tok, err := h.signer.Sign(context.Background(), Content{EndUser: &commonpb.Principal{Type: "users", Name: "alice"}})
	require.NoError(t, err)
	h.now = h.now.Add(20 * time.Second) // 10s past exp, within 30s leeway
	_, err = h.verifier.Verify(context.Background(), tok)
	require.NoError(t, err)
}

func TestVerify_EmptyTokenIsErrNoToken(t *testing.T) {
	h := newHarness(t, newKey(t), time.Minute, time.Second)
	_, err := h.verifier.Verify(context.Background(), "")
	require.ErrorIs(t, err, ErrNoToken)
}

func TestSign_NoKeyIsErr(t *testing.T) {
	_, err := NewECDSASigner(ECDSASignerOptions{Issuer: testIssuer})
	require.ErrorIs(t, err, ErrNoSigningKey)
}

func TestStaticKeyProvider_PublicJWKS(t *testing.T) {
	key := newKey(t)
	kp := NewStaticKeyProvider(nil, map[string]crypto.PublicKey{testKID: key.Public()})
	jwks, err := kp.PublicJWKS(context.Background())
	require.NoError(t, err)
	require.Contains(t, string(jwks), testKID)
	require.Contains(t, string(jwks), "\"kty\":\"EC\"")
}

// TransportVerifier trusts the claims because the peer is trusted, without a
// signature check — the Cloud cell-to-cell mTLS path.
func TestTransportVerifier(t *testing.T) {
	key := newKey(t)
	nowFn := func() time.Time { return time.Date(2026, 6, 5, 12, 0, 0, 0, time.UTC) }
	svc := &commonpb.Principal{Type: "service-accounts", Name: "worker"}
	eu := &commonpb.Principal{Type: "users", Name: "alice"}
	signed, err := mintWith(t, key, Content{ServiceCaller: svc, EndUser: eu}, nowFn)
	require.NoError(t, err)

	t.Run("trusted peer accepted without signature check", func(t *testing.T) {
		v := NewTransportVerifier(func(context.Context) bool { return true })
		got, err := v.Verify(context.Background(), signed)
		require.NoError(t, err)
		require.True(t, eq(eu, got.EndUser))
		require.True(t, eq(svc, got.ServiceCaller))
	})
	t.Run("untrusted peer refused", func(t *testing.T) {
		v := NewTransportVerifier(func(context.Context) bool { return false })
		_, err := v.Verify(context.Background(), signed)
		require.ErrorIs(t, err, ErrVerification)
	})
	t.Run("nil trust func refused", func(t *testing.T) {
		v := NewTransportVerifier(nil)
		_, err := v.Verify(context.Background(), signed)
		require.ErrorIs(t, err, ErrVerification)
	})
}

func TestNewVerifier_SelectsByTrustMode(t *testing.T) {
	key := newKey(t)
	kp := NewStaticKeyProvider(map[string]map[string]crypto.PublicKey{testIssuer: {testKID: key.Public()}}, nil)
	trustAll := func(context.Context) bool { return true }

	require.IsType(t, &TransportVerifier{}, NewVerifier(Config{TrustMode: TrustModeTransport}, kp, trustAll))
	require.IsType(t, &JWSVerifier{},
		NewVerifier(Config{TrustedIssuers: map[string]map[string][]byte{testIssuer: {testKID: nil}}}, kp, trustAll))
	require.Nil(t, NewVerifier(Config{}, kp, trustAll), "signature mode with no trusted issuers => off")
}

// --- helpers ---

func mint(t *testing.T, key *ecdsa.PrivateKey, issuer, kid string, nowFn func() time.Time) (string, error) {
	t.Helper()
	s, err := NewECDSASigner(ECDSASignerOptions{Key: key, KID: kid, Issuer: issuer, NowFn: nowFn})
	require.NoError(t, err)
	return s.Sign(context.Background(), Content{EndUser: &commonpb.Principal{Type: "users", Name: "alice"}})
}

func mintWith(t *testing.T, key *ecdsa.PrivateKey, c Content, nowFn func() time.Time) (string, error) {
	t.Helper()
	s, err := NewECDSASigner(ECDSASignerOptions{Key: key, KID: testKID, Issuer: testIssuer, NowFn: nowFn})
	require.NoError(t, err)
	return s.Sign(context.Background(), c)
}

func eq(a, b *commonpb.Principal) bool {
	return a.GetType() == b.GetType() && a.GetName() == b.GetName()
}
