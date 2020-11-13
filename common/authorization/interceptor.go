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

package authorization

import (
	"context"
	"crypto/x509/pkix"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/metrics"
)

var (
	errUnauthorized = serviceerror.NewPermissionDenied("Request unauthorized.")
)

func (a *interceptor) Interceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {

	var caller *Claims

	if a.claimMapper != nil {
		var tlsSubject *pkix.Name
		var authHeaders []string

		if md, ok := metadata.FromIncomingContext(ctx); ok {
			authHeaders = md["authorization"]
		}
		if p, ok := peer.FromContext(ctx); ok {
			if tlsAuth, ok := p.AuthInfo.(credentials.TLSInfo); ok {
				if len(tlsAuth.State.VerifiedChains) > 0 && len(tlsAuth.State.VerifiedChains[0]) > 0 {
					tlsSubject = &tlsAuth.State.VerifiedChains[0][0].Subject
				}
			}
		}
		// Add auth into to ctx only if there's some auth info
		if tlsSubject != nil || len(authHeaders) > 0 {
			authInfo := AuthInfo{authHeaders[0], tlsSubject}
			if a.authorizer != nil {
				claims, err := a.claimMapper.GetClaims(authInfo)
				if err != nil {
					return nil, err
				}
				caller = claims
				ctx = context.WithValue(ctx, "auth-claims", claims)
			}
		}
	}

	if a.authorizer != nil {
		var namespace string
		requestWithNamespace, ok := req.(requestWithNamespace)
		if ok {
			namespace = requestWithNamespace.GetNamespace()
		}

		apiFullName := info.FullMethod
		apiName := apiFullName
		index := strings.LastIndex(apiFullName, "/")
		if index > -1 {
			apiName = apiFullName[index+1:]
		}

		scope := a.getMetricsScope(metrics.NumAuthorizationScopes, namespace)
		sw := scope.StartTimer(metrics.ServiceAuthorizationLatency)
		defer sw.Stop()

		result, err := a.authorizer.Authorize(ctx, claims, &CallTarget{Namespace: namespace, APIFullName: apiFullName, APIName: apiName})
		if err != nil {
			scope.IncCounter(metrics.ServiceErrAuthorizeFailedCounter)
			return nil, err
		}
		if result.Decision != DecisionAllow {
			scope.IncCounter(metrics.ServiceErrUnauthorizedCounter)
			return nil, errUnauthorized
		}
	}
	return handler(ctx, req)
}

type interceptor struct {
	authorizer    Authorizer
	claimMapper   ClaimMapper
	metricsClient metrics.Client
}

// GetAuthorizationInterceptor creates an authorization interceptor and return a func that points to its Interceptor method
func NewAuthorizationInterceptor(
	claimMapper ClaimMapper,
	authorizer Authorizer,
	metrics metrics.Client,
) grpc.UnaryServerInterceptor {
	return (&interceptor{claimMapper: claimMapper, authorizer: authorizer, metricsClient: metrics}).Interceptor
}

// getMetricsScopeWithNamespace return metrics scope with namespace tag
func (a *interceptor) getMetricsScope(
	scope int,
	namespace string,
) metrics.Scope {
	var metricsScope metrics.Scope
	if namespace != "" {
		metricsScope = a.metricsClient.Scope(scope).Tagged(metrics.NamespaceTag(namespace))
	} else {
		metricsScope = a.metricsClient.Scope(scope).Tagged(metrics.NamespaceUnknownTag())
	}
	return metricsScope
}
