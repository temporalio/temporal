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
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	taskqueuepb "go.temporal.io/api/taskqueue/v1"
	workflowpb "go.temporal.io/api/workflow/v1"
	"go.temporal.io/api/workflowservice/v1"
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"
	"google.golang.org/protobuf/types/known/durationpb"

	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/testing/protoassert"
	"go.temporal.io/server/internal/temporalite"
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
	resp, err := http.Get("http://" + s.httpAPIAddress + "/namespaces/" + s.namespace + "/workflows")
	s.Require().NoError(err)
	s.Require().Equal(http.StatusBadRequest, resp.StatusCode)

	// Create HTTP client with TLS config
	httpClient := http.Client{
		Transport: &http.Transport{
			TLSClientConfig: s.testCluster.host.tlsConfigProvider.FrontendClientConfig,
		},
	}

	// Make a list call
	req, err := http.NewRequest("GET", "https://"+s.httpAPIAddress+"/namespaces/"+s.namespace+"/workflows", nil)
	s.Require().NoError(err)
	resp, err = httpClient.Do(req)
	s.Require().NoError(err)
	s.Require().Equal(http.StatusOK, resp.StatusCode)

	// Confirm auth info as expected
	authInfo, ok := calls.Load("/temporal.api.workflowservice.v1.WorkflowService/ListWorkflowExecutions")
	s.Require().True(ok)
	s.Require().Equal(tlsCertCommonName, authInfo.(*authorization.AuthInfo).TLSSubject.CommonName)
}

func (s *TLSFunctionalSuite) TestSameClusterCallbackMTLS() {
	if s.httpAPIAddress == "" {
		s.T().Skip("HTTP API server not enabled")
	}
	// Track auth info
	calls := s.trackAuthInfoByCall()

	ctx := NewContext()
	pp := temporalite.NewPortProvider()

	taskQueue := s.randomizeStr(s.T().Name())
	workflowType := "test"
	wf := func(ctx workflow.Context) (int, error) {
		return 666, nil
	}

	ch := &nexusCompletionHandler{
		requestCh:         make(chan *nexus.CompletionRequest, 1),
		requestCompleteCh: make(chan error, 1),
	}
	callbackAddress := fmt.Sprintf("localhost:%d", pp.MustGetFreePort())
	s.NoError(pp.Close())
	shutdownServer := s.runNexusCompletionHTTPServer(ch, callbackAddress)
	s.T().Cleanup(func() {
		require.NoError(s.T(), shutdownServer())
	})

	w := worker.New(s.sdkClient, taskQueue, worker.Options{})
	w.RegisterWorkflowWithOptions(wf, workflow.RegisterOptions{Name: workflowType})
	s.NoError(w.Start())
	defer w.Stop()

	request := &workflowservice.StartWorkflowExecutionRequest{
		RequestId:          uuid.NewString(),
		Namespace:          s.namespace,
		WorkflowId:         s.randomizeStr(s.T().Name()),
		WorkflowType:       &commonpb.WorkflowType{Name: workflowType},
		TaskQueue:          &taskqueuepb.TaskQueue{Name: taskQueue, Kind: enumspb.TASK_QUEUE_KIND_NORMAL},
		Input:              nil,
		WorkflowRunTimeout: durationpb.New(100 * time.Second),
		Identity:           s.T().Name(),
		RetryPolicy: &commonpb.RetryPolicy{
			InitialInterval:    durationpb.New(1 * time.Second),
			MaximumInterval:    durationpb.New(1 * time.Second),
			BackoffCoefficient: 1,
		},
		CompletionCallbacks: []*commonpb.Callback{
			{
				Variant: &commonpb.Callback_Nexus_{
					Nexus: &commonpb.Callback_Nexus{
						Url: "http://" + callbackAddress,
					},
				},
			},
		},
	}

	_, err := s.engine.StartWorkflowExecution(ctx, request)
	s.NoError(err)

	run := s.sdkClient.GetWorkflow(ctx, request.WorkflowId, "")
	s.NoError(run.Get(ctx, nil))

	completion := <-ch.requestCh
	s.Equal(nexus.OperationStateSucceeded, completion.State)
	var result int
	s.NoError(completion.Result.Consume(&result))
	s.Equal(666, result)
	ch.requestCompleteCh <- nil
	description, err := s.sdkClient.DescribeWorkflowExecution(ctx, request.WorkflowId, "")
	s.NoError(err)
	s.Equal(1, len(description.Callbacks))
	callbackInfo := description.Callbacks[0]
	protoassert.ProtoEqual(s.T(), request.CompletionCallbacks[0], callbackInfo.Callback)
	protoassert.ProtoEqual(s.T(), &workflowpb.CallbackInfo_Trigger{Variant: &workflowpb.CallbackInfo_Trigger_WorkflowClosed{WorkflowClosed: &workflowpb.CallbackInfo_WorkflowClosed{}}}, callbackInfo.Trigger)
	s.Equal(enumspb.CALLBACK_STATE_SUCCEEDED, callbackInfo.State)
	s.Nil(callbackInfo.LastAttemptFailure)

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

type nexusCompletionHandler struct {
	requestCh         chan *nexus.CompletionRequest
	requestCompleteCh chan error
}

func (h *nexusCompletionHandler) CompleteOperation(ctx context.Context, request *nexus.CompletionRequest) error {
	h.requestCh <- request
	return <-h.requestCompleteCh
}

func (s *TLSFunctionalSuite) runNexusCompletionHTTPServer(h *nexusCompletionHandler, listenAddr string) func() error {
	hh := nexus.NewCompletionHTTPHandler(nexus.CompletionHandlerOptions{Handler: h})
	srv := &http.Server{Addr: listenAddr, Handler: hh}
	listener, err := net.Listen("tcp", listenAddr)
	s.NoError(err)

	errCh := make(chan error, 1)
	go func() {
		errCh <- srv.Serve(listener)
	}()

	return func() error {
		// Graceful shutdown
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(ctx); err != nil {
			return err
		}
		if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	}
}
