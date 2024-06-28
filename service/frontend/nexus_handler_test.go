// The MIT License
//
// Copyright (c) 2023 Temporal Technologies Inc.  All rights reserved.
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

package frontend

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/api/matchingservice/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/cluster/clustertest"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics/metricstest"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/primitives/timestamp"
	"go.temporal.io/server/common/quotas"
	"go.temporal.io/server/common/rpc/interceptor"
)

type mockAuthorizer struct{}

// Authorize implements authorization.Authorizer.
func (mockAuthorizer) Authorize(ctx context.Context, caller *authorization.Claims, target *authorization.CallTarget) (authorization.Result, error) {
	return authorization.Result{Decision: authorization.DecisionAllow}, nil
}

var _ authorization.Authorizer = mockAuthorizer{}

type mockRateLimiter struct {
	allow bool
}

// Allow implements quotas.RequestRateLimiter.
func (r mockRateLimiter) Allow(now time.Time, request quotas.Request) bool {
	return r.allow
}

// Reserve implements quotas.RequestRateLimiter.
func (mockRateLimiter) Reserve(now time.Time, request quotas.Request) quotas.Reservation {
	panic("unimplemented for test")
}

// Wait implements quotas.RequestRateLimiter.
func (mockRateLimiter) Wait(ctx context.Context, request quotas.Request) error {
	panic("unimplemented for test")
}

var _ quotas.RequestRateLimiter = mockRateLimiter{}

type mockNamespaceChecker namespace.Name

func (n mockNamespaceChecker) Exists(name namespace.Name) error {
	if name == namespace.Name(n) {
		return nil
	}
	return errors.New("doesn't exist")
}

type contextOptions struct {
	namespaceState          enumspb.NamespaceState
	namespacePassive        bool
	quota                   int
	namespaceRateLimitAllow bool
	rateLimitAllow          bool
	redirectAllow           bool
}

func newOperationContext(options contextOptions) *operationContext {
	oc := &operationContext{
		nexusContext: &nexusContext{},
	}
	oc.logger = log.NewTestLogger()
	mh := metricstest.NewCaptureHandler()
	oc.metricsHandlerForInterceptors = mh
	oc.metricsHandler = mh
	oc.clientVersionChecker = headers.NewDefaultVersionChecker()
	oc.apiName = "/temporal.api.nexusservice.v1.NexusService/DispatchNexusTask"
	oc.responseHeaders = make(map[string]string)

	oc.namespaceName = "test-namespace"
	activeClusterName := cluster.TestCurrentClusterName
	if options.namespacePassive {
		activeClusterName = cluster.TestAlternativeClusterName
	}
	oc.namespace = namespace.NewGlobalNamespaceForTest(
		&persistencespb.NamespaceInfo{
			Id:    uuid.NewString(),
			Name:  oc.namespaceName,
			State: options.namespaceState,
		},
		&persistencespb.NamespaceConfig{
			Retention:                    timestamp.DurationFromDays(1),
			CustomSearchAttributeAliases: make(map[string]string),
		},
		&persistencespb.NamespaceReplicationConfig{
			ActiveClusterName: activeClusterName,
			Clusters: []string{
				cluster.TestCurrentClusterName,
				cluster.TestAlternativeClusterName,
			},
		},
		1,
	)

	checker := mockNamespaceChecker(oc.namespace.Name())
	oc.auth = authorization.NewInterceptor(nil, mockAuthorizer{}, oc.metricsHandler, oc.logger, checker, nil, "", "")
	oc.namespaceConcurrencyLimitInterceptor = interceptor.NewConcurrentRequestLimitInterceptor(
		nil,
		nil,
		oc.logger,
		func(ns string) int { return options.quota },
		func(ns string) int { return options.quota },
		map[string]int{
			oc.apiName: 1,
		},
	)
	oc.namespaceRateLimitInterceptor = interceptor.NewNamespaceRateLimitInterceptor(nil, mockRateLimiter{options.namespaceRateLimitAllow}, make(map[string]int))
	oc.rateLimitInterceptor = interceptor.NewRateLimitInterceptor(mockRateLimiter{options.rateLimitAllow}, make(map[string]int))

	oc.clusterMetadata = clustertest.NewMetadataForTest(cluster.NewTestClusterMetadataConfig(true, !options.namespacePassive))
	oc.forwardingEnabledForNamespace = dynamicconfig.GetBoolPropertyFnFilteredByNamespace(options.redirectAllow)
	oc.redirectionInterceptor = interceptor.NewRedirection(nil, nil, config.DCRedirectionPolicy{Policy: interceptor.DCRedirectionPolicyAllAPIsForwarding}, oc.logger, nil, oc.metricsHandlerForInterceptors, clock.NewRealTimeSource(), oc.clusterMetadata)

	return oc
}

func TestNexusInterceptRequest_InvalidNamespaceState_ResultsInBadRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var err error
	oc := newOperationContext(contextOptions{
		namespaceState:          enumspb.NAMESPACE_STATE_DELETED,
		quota:                   1,
		namespaceRateLimitAllow: true,
		rateLimitAllow:          true,
	})
	err = oc.interceptRequest(ctx, &matchingservice.DispatchNexusTaskRequest{}, nexus.Header{})
	var handlerError *nexus.HandlerError
	require.ErrorAs(t, err, &handlerError)
	require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerError.Type)
	require.Equal(t, "bad request", handlerError.Failure.Message)
	mh := oc.metricsHandler.(*metricstest.CaptureHandler) //nolint:revive
	capture := mh.StartCapture()
	oc.metricsHandler.Counter("test").Record(1)
	mh.StopCapture(capture)
	snap := capture.Snapshot()
	require.Equal(t, 1, len(snap["test"]))
	require.Equal(t, map[string]string{"outcome": "invalid_namespace_state"}, snap["test"][0].Tags)
}

func TestNexusInterceptRequest_NamespaceConcurrencyLimited_ResultsInResourceExhausted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var err error
	oc := newOperationContext(contextOptions{
		namespaceState:          enumspb.NAMESPACE_STATE_REGISTERED,
		quota:                   0,
		namespaceRateLimitAllow: true,
		rateLimitAllow:          true,
	})
	err = oc.interceptRequest(ctx, &matchingservice.DispatchNexusTaskRequest{}, nexus.Header{})
	var handlerError *nexus.HandlerError
	require.ErrorAs(t, err, &handlerError)
	require.Equal(t, nexus.HandlerErrorTypeResourceExhausted, handlerError.Type)
	require.Equal(t, "resource exhausted", handlerError.Failure.Message)
	mh := oc.metricsHandler.(*metricstest.CaptureHandler) //nolint:revive
	capture := mh.StartCapture()
	oc.metricsHandler.Counter("test").Record(1)
	mh.StopCapture(capture)
	snap := capture.Snapshot()
	require.Equal(t, 1, len(snap["test"]))
	require.Equal(t, map[string]string{"outcome": "namespace_concurrency_limited"}, snap["test"][0].Tags)
}

func TestNexusInterceptRequest_NamespaceRateLimited_ResultsInResourceExhausted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var err error
	oc := newOperationContext(contextOptions{
		namespaceState:          enumspb.NAMESPACE_STATE_REGISTERED,
		quota:                   1,
		namespaceRateLimitAllow: false,
		rateLimitAllow:          true,
	})
	err = oc.interceptRequest(ctx, &matchingservice.DispatchNexusTaskRequest{}, nexus.Header{})
	var handlerError *nexus.HandlerError
	require.ErrorAs(t, err, &handlerError)
	require.Equal(t, nexus.HandlerErrorTypeResourceExhausted, handlerError.Type)
	require.Equal(t, "namespace rate limit exceeded", handlerError.Failure.Message)
	mh := oc.metricsHandler.(*metricstest.CaptureHandler) //nolint:revive
	capture := mh.StartCapture()
	oc.metricsHandler.Counter("test").Record(1)
	mh.StopCapture(capture)
	snap := capture.Snapshot()
	require.Equal(t, 1, len(snap["test"]))
	require.Equal(t, map[string]string{"outcome": "namespace_rate_limited"}, snap["test"][0].Tags)
}

func TestNexusInterceptRequest_GlobalRateLimited_ResultsInResourceExhausted(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var err error
	oc := newOperationContext(contextOptions{
		namespaceState:          enumspb.NAMESPACE_STATE_REGISTERED,
		quota:                   1,
		namespaceRateLimitAllow: true,
		rateLimitAllow:          false,
	})
	err = oc.interceptRequest(ctx, &matchingservice.DispatchNexusTaskRequest{}, nexus.Header{})
	var handlerError *nexus.HandlerError
	require.ErrorAs(t, err, &handlerError)
	require.Equal(t, nexus.HandlerErrorTypeResourceExhausted, handlerError.Type)
	require.Equal(t, "service rate limit exceeded", handlerError.Failure.Message)
	mh := oc.metricsHandler.(*metricstest.CaptureHandler) //nolint:revive
	capture := mh.StartCapture()
	oc.metricsHandler.Counter("test").Record(1)
	mh.StopCapture(capture)
	snap := capture.Snapshot()
	require.Equal(t, 1, len(snap["test"]))
	require.Equal(t, map[string]string{"outcome": "global_rate_limited"}, snap["test"][0].Tags)
}

func TestNexusInterceptRequest_ForwardingDisabled_ResultsInUnavailable(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var err error
	oc := newOperationContext(contextOptions{
		namespaceState:          enumspb.NAMESPACE_STATE_REGISTERED,
		namespacePassive:        true,
		quota:                   1,
		namespaceRateLimitAllow: true,
		rateLimitAllow:          true,
		redirectAllow:           false,
	})
	err = oc.interceptRequest(ctx, &matchingservice.DispatchNexusTaskRequest{}, nexus.Header{})
	var handlerError *nexus.HandlerError
	require.ErrorAs(t, err, &handlerError)
	require.Equal(t, nexus.HandlerErrorTypeUnavailable, handlerError.Type)
	mh := oc.metricsHandler.(*metricstest.CaptureHandler) //nolint:revive
	capture := mh.StartCapture()
	oc.metricsHandler.Counter("test").Record(1)
	mh.StopCapture(capture)
	snap := capture.Snapshot()
	require.Equal(t, 1, len(snap["test"]))
	require.Equal(t, map[string]string{"outcome": "namespace_inactive_forwarding_disabled"}, snap["test"][0].Tags)
}

func TestNexusInterceptRequest_ForwardingEnabled_ResultsInNotActiveError(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var err error
	oc := newOperationContext(contextOptions{
		namespaceState:          enumspb.NAMESPACE_STATE_REGISTERED,
		namespacePassive:        true,
		quota:                   1,
		namespaceRateLimitAllow: true,
		rateLimitAllow:          true,
		redirectAllow:           true,
	})
	err = oc.interceptRequest(ctx, &matchingservice.DispatchNexusTaskRequest{}, nexus.Header{})
	var notActiveErr *serviceerror.NamespaceNotActive
	require.ErrorAs(t, err, &notActiveErr)
	mh := oc.metricsHandler.(*metricstest.CaptureHandler) //nolint:revive
	capture := mh.StartCapture()
	oc.metricsHandler.Counter("test").Record(1)
	mh.StopCapture(capture)
	snap := capture.Snapshot()
	require.Equal(t, 1, len(snap["test"]))
	require.Equal(t, map[string]string{"outcome": "request_forwarded"}, snap["test"][0].Tags)
}

func TestNexusInterceptRequest_InvalidSDKVersion_ResultsInBadRequest(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var err error
	oc := newOperationContext(contextOptions{
		namespaceState:          enumspb.NAMESPACE_STATE_REGISTERED,
		namespacePassive:        false,
		quota:                   1,
		namespaceRateLimitAllow: true,
		rateLimitAllow:          true,
		redirectAllow:           true,
	})
	header := nexus.Header{headerUserAgent: "Nexus-go-sdk/v99.0.0"}
	ctx = oc.augmentContext(ctx, header)
	err = oc.interceptRequest(ctx, &matchingservice.DispatchNexusTaskRequest{}, header)
	var handlerError *nexus.HandlerError
	require.ErrorAs(t, err, &handlerError)
	require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerError.Type)
	mh := oc.metricsHandler.(*metricstest.CaptureHandler) //nolint:revive
	capture := mh.StartCapture()
	oc.metricsHandler.Counter("test").Record(1)
	mh.StopCapture(capture)
	snap := capture.Snapshot()
	require.Equal(t, 1, len(snap["test"]))
	require.Equal(t, map[string]string{"outcome": "unsupported_client"}, snap["test"][0].Tags)
}
