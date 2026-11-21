package frontend

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.uber.org/mock/gomock"
)

const testExpectedAudience = "expected-audience"

var errTestClaims = errors.New("claims error")

// newAudienceTestInterceptor returns an Interceptor whose audience mapper returns testExpectedAudience, and whose
// claim mapper records the AuthInfo.Audience it is given.
func newAudienceTestInterceptor(t *testing.T, gotAudience *string) *authorization.Interceptor {
	t.Helper()
	ctrl := gomock.NewController(t)

	audienceMapper := authorization.NewMockJWTAudienceMapper(ctrl)
	audienceMapper.EXPECT().Audience(gomock.Any(), nil, nil).Return(testExpectedAudience).AnyTimes()

	claimMapper := authorization.NewMockClaimMapper(ctrl)
	claimMapper.EXPECT().GetClaims(gomock.Any()).DoAndReturn(func(authInfo *authorization.AuthInfo) (*authorization.Claims, error) {
		*gotAudience = authInfo.Audience
		// Short-circuit before authorization; only the AuthInfo passed to the claim mapper matters here.
		return nil, errTestClaims
	})

	return authorization.NewInterceptor(
		claimMapper,
		authorization.NewMockAuthorizer(ctrl),
		metrics.NoopMetricsHandler,
		log.NewNoopLogger(),
		nil,
		audienceMapper,
		"",
		"",
		dynamicconfig.GetBoolPropertyFn(false),
		dynamicconfig.GetBoolPropertyFn(false),
		dynamicconfig.GetBoolPropertyFnFilteredByNamespace(false),
		dynamicconfig.GetBoolPropertyFn(false),
	)
}

func newAudienceTestRequest() *http.Request {
	r := httptest.NewRequest(http.MethodPost, "/", nil)
	r.Header.Set("Authorization", "Bearer some-token")
	return r
}

func TestNexusOperationHTTPHandler_JWTAudience(t *testing.T) {
	for _, tc := range []struct {
		name     string
		enforce  bool
		expected string
	}{
		{name: "enforcement disabled", enforce: false, expected: ""},
		{name: "enforcement enabled", enforce: true, expected: testExpectedAudience},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gotAudience string
			h := &NexusOperationHTTPHandler{
				auth:               newAudienceTestInterceptor(t, &gotAudience),
				enforceJWTAudience: dynamicconfig.GetBoolPropertyFn(tc.enforce),
			}

			_, err := h.parseTLSAndAuthInfo(newAudienceTestRequest(), &nexusContext{})
			require.ErrorIs(t, err, errTestClaims)
			require.Equal(t, tc.expected, gotAudience)
		})
	}
}

func TestNexusCompletionHTTPHandler_JWTAudience(t *testing.T) {
	for _, tc := range []struct {
		name     string
		enforce  bool
		expected string
	}{
		{name: "enforcement disabled", enforce: false, expected: ""},
		{name: "enforcement enabled", enforce: true, expected: testExpectedAudience},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var gotAudience string
			rc := &requestContext{
				nexusCompletionHandler: &nexusCompletionHandler{
					Config:          &Config{NexusEnforceJWTAudience: dynamicconfig.GetBoolPropertyFn(tc.enforce)},
					AuthInterceptor: newAudienceTestInterceptor(t, &gotAudience),
				},
			}

			err := rc.interceptRequest(context.Background(), &nexusrpc.CompletionRequest{HTTPRequest: newAudienceTestRequest()})
			require.ErrorIs(t, err, errTestClaims)
			require.Equal(t, tc.expected, gotAudience)
		})
	}
}
