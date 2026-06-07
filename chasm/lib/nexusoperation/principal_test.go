package nexusoperation

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	commonspb "go.temporal.io/server/api/common/v1"
	"go.temporal.io/server/common/nexus/principaltoken"
)

func testSigner(t *testing.T) principaltoken.Signer {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	s, err := principaltoken.NewECDSASigner(principaltoken.ECDSASignerOptions{
		Key: key, KID: "k1", Issuer: "test",
	})
	require.NoError(t, err)
	return s
}

func stored(typ, name string) *commonspb.AttributedPrincipal {
	return &commonspb.AttributedPrincipal{Principal: &commonpb.Principal{Type: typ, Name: name}}
}

func TestAttachPrincipalIdentity_MintsToken(t *testing.T) {
	t.Parallel()

	h := nexus.Header{}
	err := attachPrincipalIdentity(context.Background(), h, testSigner(t), principaltoken.NoopResolver{},
		stored("service-accounts", "sa-worker"), stored("users", "alice@example.com"),
		&commonpb.Principal{Type: namespacePrincipalType, Name: "caller-ns"})
	require.NoError(t, err)
	require.NotEmpty(t, h.Get(principaltoken.Header), "a signed token should be attached")
}

func TestAttachPrincipalIdentity_NoSignerNoPropagation(t *testing.T) {
	t.Parallel()

	// Without a signer the token is the only carrier, so nothing is attached.
	h := nexus.Header{}
	err := attachPrincipalIdentity(context.Background(), h, nil, principaltoken.NoopResolver{},
		stored("users", "alice"), stored("users", "alice"), nil)
	require.NoError(t, err)
	require.Empty(t, h.Get(principaltoken.Header))
}

func TestAttachPrincipalIdentity_NoPrincipalsNoToken(t *testing.T) {
	t.Parallel()

	h := nexus.Header{}
	err := attachPrincipalIdentity(context.Background(), h, testSigner(t), principaltoken.NoopResolver{}, nil, nil, nil)
	require.NoError(t, err)
	require.Empty(t, h.Get(principaltoken.Header))
}
