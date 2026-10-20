package frontend

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
)

type claimMapperFunc func(*authorization.AuthInfo) (*authorization.Claims, error)

func (f claimMapperFunc) GetClaims(authInfo *authorization.AuthInfo) (*authorization.Claims, error) {
	return f(authInfo)
}

func TestSentByNonServerPrincipal(t *testing.T) {
	t.Parallel()

	newInterceptor := func(claimMapper authorization.ClaimMapper) *authorization.Interceptor {
		return authorization.NewInterceptor(
			claimMapper,
			authorization.NewNoopAuthorizer(),
			metrics.NoopMetricsHandler,
			log.NewNoopLogger(),
			nil,
			nil,
			"",
			"",
			dynamicconfig.GetBoolPropertyFn(false),
			dynamicconfig.GetBoolPropertyFn(false),
			dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
			dynamicconfig.GetBoolPropertyFn(false),
		)
	}
	claims := func(c *authorization.Claims, err error) authorization.ClaimMapper {
		return claimMapperFunc(func(*authorization.AuthInfo) (*authorization.Claims, error) { return c, err })
	}
	withToken := func() *http.Request {
		r, err := http.NewRequest(http.MethodPost, "/", nil)
		require.NoError(t, err)
		r.Header.Set("Authorization", "Bearer token")
		return r
	}

	for _, tc := range []struct {
		name    string
		auth    *authorization.Interceptor
		request *http.Request
		want    bool
	}{
		{name: "authorization not configured", request: withToken()},
		{name: "default claim mapper", auth: newInterceptor(authorization.NewNoopClaimMapper()), request: withToken()},
		{
			name:    "no credentials",
			auth:    newInterceptor(claims(&authorization.Claims{Namespaces: map[string]authorization.Role{"ns": authorization.RoleWriter}}, nil)),
			request: &http.Request{Header: http.Header{}},
		},
		{
			name:    "claims error",
			auth:    newInterceptor(claims(nil, errors.New("bad token"))),
			request: withToken(),
		},
		{
			name:    "server principal",
			auth:    newInterceptor(claims(&authorization.Claims{System: authorization.RoleAdmin}, nil)),
			request: withToken(),
		},
		{
			name:    "namespace principal",
			auth:    newInterceptor(claims(&authorization.Claims{Namespaces: map[string]authorization.Role{"ns": authorization.RoleWriter}}, nil)),
			request: withToken(),
			want:    true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, sentByNonServerPrincipal(tc.auth, tc.request))
		})
	}
}
