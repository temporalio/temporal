package tests

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservice/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/tests/testcore"
)

type TLSFunctionalSuite struct {
	testcore.FunctionalTestBase
}

func TestTLSFunctionalSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(TLSFunctionalSuite))
}

func (s *TLSFunctionalSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuiteWithCluster(testcore.WithMTLS())
}

func (s *TLSFunctionalSuite) TearDownSuite() {
	s.FunctionalTestBase.TearDownCluster()
}

func (s *TLSFunctionalSuite) TestGRPCMTLS() {
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(time.Minute)
	defer cancel()

	// Track auth info
	calls := s.trackAuthInfoByCall()

	// Make a list-open call
	_, _ = s.SdkClient().ListOpenWorkflow(ctx, &workflowservice.ListOpenWorkflowExecutionsRequest{})

	// Confirm auth info as expected
	authInfo, ok := calls.Load("/temporal.api.workflowservice.v1.WorkflowService/ListOpenWorkflowExecutions")
	s.Require().True(ok)
	s.Require().Equal(testcore.TlsCertCommonName, authInfo.(*authorization.AuthInfo).TLSSubject.CommonName)
}

func (s *TLSFunctionalSuite) TestHTTPMTLS() {
	if s.HttpAPIAddress() == "" {
		s.T().Skip("HTTP API server not enabled")
	}
	// Track auth info
	calls := s.trackAuthInfoByCall()

	// Confirm non-HTTPS call is rejected with 400
	resp, err := http.Get("http://" + s.HttpAPIAddress() + "/namespaces/" + s.Namespace().String() + "/workflows")
	s.Require().NoError(err)
	s.Require().Equal(http.StatusBadRequest, resp.StatusCode)

	// Create HTTP client with TLS config
	httpClient := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: s.GetTestCluster().Host().TlsConfigProvider().FrontendClientConfig,
		},
	}

	// Make a list call
	req, err := http.NewRequest("GET", "https://"+s.HttpAPIAddress()+"/namespaces/"+s.Namespace().String()+"/workflows", nil)
	s.Require().NoError(err)
	resp, err = httpClient.Do(req)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)

	// Confirm auth info as expected
	authInfo, ok := calls.Load("/temporal.api.workflowservice.v1.WorkflowService/ListWorkflowExecutions")
	s.Require().True(ok)
	s.Require().Equal(testcore.TlsCertCommonName, authInfo.(*authorization.AuthInfo).TLSSubject.CommonName)
}

func (s *TLSFunctionalSuite) trackAuthInfoByCall() *sync.Map {
	var calls sync.Map
	// Put auth info on claim, then use authorizer to set on the map by call
	s.GetTestCluster().Host().SetOnGetClaims(func(authInfo *authorization.AuthInfo) (*authorization.Claims, error) {
		return &authorization.Claims{
			System:     authorization.RoleAdmin,
			Extensions: authInfo,
		}, nil
	})
	s.GetTestCluster().Host().SetOnAuthorize(func(
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
