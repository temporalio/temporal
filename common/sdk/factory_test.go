package sdk

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/rpc/auth"
	"go.temporal.io/server/common/rpc/auth/authtest"
)

func newTestFactory(tp auth.TokenProvider) *clientFactory {
	return NewClientFactory(
		"localhost:7233",
		nil,
		metrics.NoopMetricsHandler,
		log.NewNoopLogger(),
		dynamicconfig.GetIntPropertyFn(0),
		tp,
	)
}

func TestNewClientFactory_withTokenProvider_setsHeadersProvider(t *testing.T) {
	t.Parallel()
	f := newTestFactory(authtest.StaticTokenProvider("tok"))
	require.NotNil(t, f.headersProvider)
}

func TestNewClientFactory_nilTokenProvider_leavesHeadersProviderNil(t *testing.T) {
	t.Parallel()
	f := newTestFactory(nil)
	require.Nil(t, f.headersProvider)
}

func TestOptions_withHeadersProvider_setsOnOptions(t *testing.T) {
	t.Parallel()
	f := newTestFactory(authtest.StaticTokenProvider("tok"))
	opts := f.options(sdkclient.Options{Namespace: "test"})
	require.NotNil(t, opts.HeadersProvider)
}

func TestOptions_withoutHeadersProvider_leavesHeadersProviderNil(t *testing.T) {
	t.Parallel()
	f := newTestFactory(nil)
	opts := f.options(sdkclient.Options{Namespace: "test"})
	require.Nil(t, opts.HeadersProvider)
}

func TestTokenProviderHeadersAdapter_returnsBearer(t *testing.T) {
	t.Parallel()
	a := &tokenProviderHeadersAdapter{tp: authtest.StaticTokenProvider("mytoken"), hostPort: "h:1"}
	hdrs, err := a.GetHeaders(context.Background())
	require.NoError(t, err)
	require.Equal(t, "Bearer mytoken", hdrs["authorization"])
}

func TestTokenProviderHeadersAdapter_emptyToken_returnsNilMap(t *testing.T) {
	t.Parallel()
	a := &tokenProviderHeadersAdapter{tp: authtest.StaticTokenProvider(""), hostPort: "h:1"}
	hdrs, err := a.GetHeaders(context.Background())
	require.NoError(t, err)
	require.Nil(t, hdrs)
}

func TestTokenProviderHeadersAdapter_providerError_wrapsErr(t *testing.T) {
	t.Parallel()
	a := &tokenProviderHeadersAdapter{tp: errorTokenProvider{}, hostPort: "h:1"}
	_, err := a.GetHeaders(context.Background())
	require.ErrorContains(t, err, "sdk auth:")
}

type errorTokenProvider struct{}

func (errorTokenProvider) GetToken(context.Context, string) (string, error) {
	return "", errors.New("idp unavailable")
}
