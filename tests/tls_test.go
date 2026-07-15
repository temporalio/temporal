package tests

import (
	"context"
	"net/http"
	"sync"
	"testing"

	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/testing/parallelsuite"
	"go.temporal.io/server/common/testing/testvars"
	"go.temporal.io/server/tests/testcore"
)

type TLSFunctionalSuite struct {
	parallelsuite.Suite[*TLSFunctionalSuite]
}

func TestTLSFunctionalSuite(t *testing.T) {
	parallelsuite.Run(t, &TLSFunctionalSuite{})
}

func (s *TLSFunctionalSuite) newTestEnv(opts ...testcore.TestOption) (*testcore.TestEnv, *testvars.TestVars) {
	baseOpts := []testcore.TestOption{
		testcore.WithMTLS(),
	}
	return testcore.NewEnv(s.T(), append(baseOpts, opts...)...)
}

func (s *TLSFunctionalSuite) TestGRPCMTLS() {
	env, _ := s.newTestEnv()

	// Track auth info
	calls := s.trackAuthInfoByCall(env)

	// Make a list-open call
	_, _ = env.SdkClient().ListOpenWorkflow(s.Context(), &workflowservice.ListOpenWorkflowExecutionsRequest{})

	// Confirm auth info as expected
	authInfo, ok := calls.Load("/temporal.api.workflowservice.v1.WorkflowService/ListOpenWorkflowExecutions")
	s.True(ok)
	s.Equal(testcore.TlsCertCommonName, authInfo.(*authorization.AuthInfo).TLSSubject.CommonName)
}

func (s *TLSFunctionalSuite) TestHTTPMTLS() {
	env, _ := s.newTestEnv()
	if env.HttpAPIAddress() == "" {
		s.T().Skip("HTTP API server not enabled")
	}
	// Track auth info
	calls := s.trackAuthInfoByCall(env)

	// Confirm non-HTTPS call is rejected with 400
	resp, err := http.Get("http://" + env.HttpAPIAddress() + "/namespaces/" + env.Namespace().String() + "/workflows")
	s.NoError(err)
	s.Equal(http.StatusBadRequest, resp.StatusCode)

	// Create HTTP client with TLS config
	httpClient := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: env.GetTestCluster().Host().TLSConfigProvider().FrontendClientConfig,
		},
	}

	// Make a list call
	req, err := http.NewRequest("GET", "https://"+env.HttpAPIAddress()+"/namespaces/"+env.Namespace().String()+"/workflows", nil)
	s.NoError(err)
	resp, err = httpClient.Do(req)
	s.NoError(err)
	s.Equal(http.StatusOK, resp.StatusCode)

	// Confirm auth info as expected
	authInfo, ok := calls.Load("/temporal.api.workflowservice.v1.WorkflowService/ListWorkflowExecutions")
	s.True(ok)
	s.Equal(testcore.TlsCertCommonName, authInfo.(*authorization.AuthInfo).TLSSubject.CommonName)
}

func (s *TLSFunctionalSuite) trackAuthInfoByCall(env *testcore.TestEnv) *sync.Map {
	var calls sync.Map
	// Put auth info on claim, then use authorizer to set on the map by call
	env.SetOnGetClaims(func(authInfo *authorization.AuthInfo) (*authorization.Claims, error) {
		return &authorization.Claims{
			System:     authorization.RoleAdmin,
			Extensions: authInfo,
		}, nil
	})
	env.SetOnAuthorize(func(
		ctx context.Context,
		caller *authorization.Claims,
		target *authorization.CallTarget,
	) (authorization.Result, error) {
		//nolint:revive
		if authInfo, _ := caller.Extensions.(*authorization.AuthInfo); authInfo != nil {
			calls.Store(target.APIName, authInfo)
		}
		return authorization.Result{Decision: authorization.DecisionAllow}, nil
	})
	return &calls
}
