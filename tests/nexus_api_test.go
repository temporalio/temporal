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
	"net/http"
	"testing"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/metrics"
)

var op = nexus.NewOperationReference[string, string]("my-operation")

// This test will be modified once nexus task dispatching is implemented in matching.
func (s *clientFunctionalSuite) TestNexusStartOperation_WithNamespaceAndTaskQueue_NotImplemented() {
	taskQueue := s.randomizeStr("task-queue")
	url := fmt.Sprintf("http://%s/api/v1/namespaces/%s/task-queues/%s/dispatch-nexus-task", s.httpAPIAddress, s.namespace, taskQueue)
	client, err := nexus.NewClient(nexus.ClientOptions{ServiceBaseURL: url})
	s.Require().NoError(err)
	ctx := NewContext()
	capture := s.testCluster.host.captureMetricsHandler.StartCapture()
	defer s.testCluster.host.captureMetricsHandler.StopCapture(capture)
	_, err = nexus.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
	var unexpectedResponse *nexus.UnexpectedResponseError
	s.Require().ErrorAs(err, &unexpectedResponse)
	s.Require().Equal("internal server error", unexpectedResponse.Failure.Message)

	snap := capture.Snapshot()

	s.Equal(1, len(snap["nexus_requests"]))
	s.Equal(map[string]string{"namespace": s.namespace, "method": "StartOperation", "outcome": "internal_error"}, snap["nexus_requests"][0].Tags)
	s.Equal(int64(1), snap["nexus_requests"][0].Value)
	s.Equal(metrics.MetricUnit(""), snap["nexus_requests"][0].Unit)

	s.Equal(1, len(snap["nexus_latency"]))
	s.Equal(map[string]string{"namespace": s.namespace, "method": "StartOperation", "outcome": "internal_error"}, snap["nexus_latency"][0].Tags)
	s.Equal(metrics.MetricUnit(metrics.Milliseconds), snap["nexus_latency"][0].Unit)
}

func (s *clientFunctionalSuite) TestNexusStartOperation_WithNamespaceAndTaskQueue_NamespaceNotFound() {
	taskQueue := s.randomizeStr("task-queue")
	url := fmt.Sprintf("http://%s/api/v1/namespaces/%s/task-queues/%s/dispatch-nexus-task", s.httpAPIAddress, "namespace-not-found", taskQueue)
	client, err := nexus.NewClient(nexus.ClientOptions{ServiceBaseURL: url})
	s.Require().NoError(err)
	ctx := NewContext()
	capture := s.testCluster.host.captureMetricsHandler.StartCapture()
	defer s.testCluster.host.captureMetricsHandler.StopCapture(capture)
	_, err = nexus.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
	var unexpectedResponse *nexus.UnexpectedResponseError
	s.Require().ErrorAs(err, &unexpectedResponse)
	s.Require().Equal(http.StatusNotFound, unexpectedResponse.Response.StatusCode)
	s.Require().Equal(`namespace not found: "namespace-not-found"`, unexpectedResponse.Failure.Message)

	snap := capture.Snapshot()

	s.Equal(1, len(snap["nexus_requests"]))
	s.Equal(map[string]string{"namespace": "namespace-not-found", "method": "StartOperation", "outcome": "namespace_not_found"}, snap["nexus_requests"][0].Tags)
	s.Equal(int64(1), snap["nexus_requests"][0].Value)
}

func (s *clientFunctionalSuite) TestNexusStartOperation_WithNamespaceAndTaskQueue_Forbidden() {
	type testcase struct {
		name           string
		onAuthorize    func(context.Context, *authorization.Claims, *authorization.CallTarget) (authorization.Result, error)
		failureMessage string
	}
	testCases := []testcase{
		{
			name: "deny with reason",
			onAuthorize: func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
				if ct.APIName == "/temporal.api.nexusservice.v1/DispatchNexusTask" {
					return authorization.Result{Decision: authorization.DecisionDeny, Reason: "unauthorized in test"}, nil
				}
				return authorization.Result{Decision: authorization.DecisionAllow}, nil
			},
			failureMessage: `permission denied: unauthorized in test`,
		},
		{
			name: "deny without reason",
			onAuthorize: func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
				if ct.APIName == "/temporal.api.nexusservice.v1/DispatchNexusTask" {
					return authorization.Result{Decision: authorization.DecisionDeny}, nil
				}
				return authorization.Result{Decision: authorization.DecisionAllow}, nil
			},
			failureMessage: "permission denied",
		},
		{
			name: "deny with generic error",
			onAuthorize: func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
				if ct.APIName == "/temporal.api.nexusservice.v1/DispatchNexusTask" {
					return authorization.Result{}, errors.New("some generic error")
				}
				return authorization.Result{Decision: authorization.DecisionAllow}, nil
			},
			failureMessage: "permission denied",
		},
	}

	taskQueue := s.randomizeStr("task-queue")
	url := fmt.Sprintf("http://%s/api/v1/namespaces/%s/task-queues/%s/dispatch-nexus-task", s.httpAPIAddress, s.namespace, taskQueue)
	client, err := nexus.NewClient(nexus.ClientOptions{ServiceBaseURL: url})
	s.Require().NoError(err)
	ctx := NewContext()

	for _, tc := range testCases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			s.testCluster.host.SetOnAuthorize(tc.onAuthorize)
			defer s.testCluster.host.SetOnAuthorize(nil)

			capture := s.testCluster.host.captureMetricsHandler.StartCapture()
			defer s.testCluster.host.captureMetricsHandler.StopCapture(capture)
			_, err = nexus.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{})
			var unexpectedResponse *nexus.UnexpectedResponseError
			s.Require().ErrorAs(err, &unexpectedResponse)
			s.Require().Equal(http.StatusForbidden, unexpectedResponse.Response.StatusCode)
			s.Require().Equal(tc.failureMessage, unexpectedResponse.Failure.Message)

			snap := capture.Snapshot()

			s.Equal(1, len(snap["nexus_requests"]))
			s.Equal(map[string]string{"namespace": s.namespace, "method": "StartOperation", "outcome": "unauthorized"}, snap["nexus_requests"][0].Tags)
			s.Equal(int64(1), snap["nexus_requests"][0].Value)
		})
	}
}

func (s *clientFunctionalSuite) TestNexusStartOperation_WithNamespaceAndTaskQueue_Claims() {
	type testcase struct {
		name           string
		header         nexus.Header
		statusCode     int
		failureMessage string
	}
	testCases := []testcase{
		{
			name:           "no header",
			statusCode:     http.StatusForbidden,
			failureMessage: "permission denied",
		},
		{
			name: "invalid bearer",
			header: nexus.Header{
				"authorization": "Bearer invalid",
			},
			statusCode:     http.StatusUnauthorized,
			failureMessage: "unauthorized",
		},
		{
			name: "valid bearer",
			header: nexus.Header{
				"authorization": "Bearer test",
			},
			// TODO: change once matching dispatch is implemented
			statusCode:     http.StatusInternalServerError,
			failureMessage: "internal server error",
		},
	}

	s.testCluster.host.SetOnAuthorize(func(ctx context.Context, c *authorization.Claims, ct *authorization.CallTarget) (authorization.Result, error) {
		if ct.APIName == "/temporal.api.nexusservice.v1/DispatchNexusTask" && (c == nil || c.Subject != "test") {
			return authorization.Result{Decision: authorization.DecisionDeny}, nil
		}
		return authorization.Result{Decision: authorization.DecisionAllow}, nil
	})
	defer s.testCluster.host.SetOnAuthorize(nil)
	s.testCluster.host.SetOnGetClaims(func(ai *authorization.AuthInfo) (*authorization.Claims, error) {
		if ai.AuthToken != "Bearer test" {
			return nil, errors.New("invalid auth token")
		}
		return &authorization.Claims{Subject: "test"}, nil
	})
	defer s.testCluster.host.SetOnGetClaims(nil)

	taskQueue := s.randomizeStr("task-queue")
	url := fmt.Sprintf("http://%s/api/v1/namespaces/%s/task-queues/%s/dispatch-nexus-task", s.httpAPIAddress, s.namespace, taskQueue)
	client, err := nexus.NewClient(nexus.ClientOptions{ServiceBaseURL: url})
	s.Require().NoError(err)
	ctx := NewContext()

	for _, tc := range testCases {
		tc := tc
		s.T().Run(tc.name, func(t *testing.T) {
			_, err = nexus.StartOperation(ctx, client, op, "input", nexus.StartOperationOptions{
				Header: tc.header,
			})
			var unexpectedResponse *nexus.UnexpectedResponseError
			s.Require().ErrorAs(err, &unexpectedResponse)
			s.Require().Equal(tc.statusCode, unexpectedResponse.Response.StatusCode)
			s.Require().Equal(tc.failureMessage, unexpectedResponse.Failure.Message)
		})
	}
}
