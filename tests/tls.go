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
	"time"

	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"

	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/rpc"
)

type TLSFunctionalSuite struct {
	FunctionalTestBase
	sdkClient sdkclient.Client
}

func (s *TLSFunctionalSuite) SetupSuite() {
	s.setupSuite("testdata/tls_cluster.yaml")

}

func (s *TLSFunctionalSuite) TearDownSuite() {
	s.tearDownSuite()
}

func (s *TLSFunctionalSuite) SetupTest() {
	var err error
	s.sdkClient, err = sdkclient.Dial(sdkclient.Options{
		HostPort:  s.hostPort,
		Namespace: s.namespace,
		ConnectionOptions: sdkclient.ConnectionOptions{
			TLS: s.testCluster.host.tlsConfigProvider.FrontendClientConfig,
		},
	})
	if err != nil {
		s.Logger.Fatal("Error when creating SDK client", tag.Error(err))
	}
}

func (s *TLSFunctionalSuite) TearDownTest() {
	s.sdkClient.Close()
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
	s.Require().Equal(tlsCertCommonName, authInfo.(*authorization.AuthInfo).TLSSubject.CommonName)
}

func (s *TLSFunctionalSuite) TestHTTPMTLS() {
	if s.httpAPIAddress == "" {
		s.T().Skip("HTTP API server not enabled")
	}
	// Track auth info
	calls := s.trackAuthInfoByCall()

	// Confirm non-HTTPS call is rejected with 400
	resp, err := http.Get("http://" + s.httpAPIAddress + "/api/v1/namespaces/" + s.namespace + "/workflows")
	s.Require().NoError(err)
	s.Require().Equal(http.StatusBadRequest, resp.StatusCode)

	// Create HTTP client with TLS config
	httpClient := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: s.testCluster.host.tlsConfigProvider.FrontendClientConfig,
		},
	}

	// Make a list call
	req, err := http.NewRequest("GET", "https://"+s.httpAPIAddress+"/api/v1/namespaces/"+s.namespace+"/workflows", nil)
	s.Require().NoError(err)
	resp, err = httpClient.Do(req)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)

	// Confirm auth info as expected
	authInfo, ok := calls.Load("/temporal.api.workflowservice.v1.WorkflowService/ListWorkflowExecutions")
	s.Require().True(ok)
	s.Require().Equal(tlsCertCommonName, authInfo.(*authorization.AuthInfo).TLSSubject.CommonName)
}

func (s *TLSFunctionalSuite) trackAuthInfoByCall() *sync.Map {
	var calls sync.Map
	// Put auth info on claim, then use authorizer to set on the map by call
	s.testCluster.host.SetOnGetClaims(func(authInfo *authorization.AuthInfo) (*authorization.Claims, error) {
		return &authorization.Claims{
			System:     authorization.RoleAdmin,
			Extensions: authInfo,
		}, nil
	})
	s.testCluster.host.SetOnAuthorize(func(
		ctx context.Context,
		caller *authorization.Claims,
		target *authorization.CallTarget,
	) (authorization.Result, error) {
		if authInfo, _ := caller.Extensions.(*authorization.AuthInfo); authInfo != nil {
			calls.Store(target.APIName, authInfo)
		}
		return authorization.Result{Decision: authorization.DecisionAllow}, nil
	})
	return &calls
}
