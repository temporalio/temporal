// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package tests

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/suite"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/tests/testcore"
)

type TLSFunctionalSuite struct {
	testcore.FunctionalTestBase
	sdkClient sdkclient.Client
}

func TestTLSFunctionalSuite(t *testing.T) {
	t.Parallel()
	suite.Run(t, new(TLSFunctionalSuite))
}

func (s *TLSFunctionalSuite) SetupSuite() {
	s.FunctionalTestBase.SetupSuite("testdata/tls_cluster.yaml")
}

func (s *TLSFunctionalSuite) TearDownSuite() {
	s.FunctionalTestBase.TearDownSuite()
}

func (s *TLSFunctionalSuite) SetupTest() {
	s.FunctionalTestBase.SetupTest()

	var err error
	s.sdkClient, err = sdkclient.Dial(sdkclient.Options{
		HostPort:  s.FrontendGRPCAddress(),
		Namespace: s.Namespace(),
		ConnectionOptions: sdkclient.ConnectionOptions{
			TLS: s.GetTestCluster().Host().TlsConfigProvider().FrontendClientConfig,
		},
	})
	if err != nil {
		s.Logger.Fatal("Error when creating SDK client", tag.Error(err))
	}
}

func (s *TLSFunctionalSuite) TearDownTest() {
	if s.sdkClient != nil {
		s.sdkClient.Close()
	}
}

func (s *TLSFunctionalSuite) TestGRPCMTLS() {
	ctx, cancel := rpc.NewContextWithTimeoutAndVersionHeaders(time.Minute)
	defer cancel()

	// Track auth info
	calls := s.trackAuthInfoByCall()

	// Make a list-open call
	_, _ = s.sdkClient.ListOpenWorkflow(ctx, &workflowservice.ListOpenWorkflowExecutionsRequest{})

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
	resp, err := http.Get("http://" + s.HttpAPIAddress() + "/namespaces/" + s.Namespace() + "/workflows")
	s.Require().NoError(err)
	s.Require().Equal(http.StatusBadRequest, resp.StatusCode)

	// Create HTTP client with TLS config
	httpClient := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: s.GetTestCluster().Host().TlsConfigProvider().FrontendClientConfig,
		},
	}

	// Make a list call
	req, err := http.NewRequest("GET", "https://"+s.HttpAPIAddress()+"/namespaces/"+s.Namespace()+"/workflows", nil)
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
