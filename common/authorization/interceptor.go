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

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

var (
	errUnauthorized = serviceerror.NewPermissionDenied("Request unauthorized.")
)

const (
	ContextKeyMappedClaims = "auth-mappedClaims"
)

func (a *interceptor) Interceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {

	var claims *Claims

	if a.claimMapper != nil && a.authorizer != nil {
		var tlsSubject *pkix.Name
		var authHeaders []string
		var authExtraHeaders []string
		var tlsConnection *credentials.TLSInfo

		if md, ok := metadata.FromIncomingContext(ctx); ok {
			authHeaders = md["authorization"]
			authExtraHeaders = md["authorization-extras"]
		}
		if p, ok := peer.FromContext(ctx); ok {
			if tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo); ok {
				tlsConnection = &tlsInfo
				if len(tlsInfo.State.VerifiedChains) > 0 && len(tlsInfo.State.VerifiedChains[0]) > 0 {
					// The assumption here is that we only expect a single verified chain of certs (first[0]).
					// It's unclear how we should handle a situation when more than one chain is presented,
					// which subject to use. It's okay for us to limit ourselves to one chain.
					// We can always extend this logic later.
					// We tale the first element in the chain ([0]) because that's the client cert
					// (at the beginning of the chain), not intermediary CAs or the root CA (at the end of the chain).
					tlsSubject = &tlsInfo.State.VerifiedChains[0][0].Subject
				}
			}
		}
		// Add auth info to context only if there's some auth info
		if tlsSubject != nil || len(authHeaders) > 0 {
			var authHeader string
			var authExtraHeader string
			if len(authHeaders) > 0 {
				authHeader = authHeaders[0]
			}
			if len(authExtraHeaders) > 0 {
				authExtraHeader = authExtraHeaders[0]
			}
			authInfo := AuthInfo{
				AuthToken:     authHeader,
				TLSSubject:    tlsSubject,
				TLSConnection: tlsConnection,
				ExtraData:     authExtraHeader,
			}
			mappedClaims, err := a.claimMapper.GetClaims(&authInfo)
			if err != nil {
				return nil, a.logAuthError(err)
			}
			claims = mappedClaims
			ctx = context.WithValue(ctx, ContextKeyMappedClaims, mappedClaims)
		}
	}

	if a.authorizer != nil {
		var namespace string
		requestWithNamespace, ok := req.(requestWithNamespace)
		if ok {
			namespace = requestWithNamespace.GetNamespace()
		}

		apiName := info.FullMethod

		scope := a.getMetricsScope(metrics.AuthorizerScope, namespace)
		sw := scope.StartTimer(metrics.ServiceAuthorizationLatency)
		defer sw.Stop()

		result, err := a.authorizer.Authorize(ctx, claims, &CallTarget{Namespace: namespace, APIName: apiName})
		if err != nil {
			scope.IncCounter(metrics.ServiceErrAuthorizeFailedCounter)
			return nil, a.logAuthError(err)
		}
		if result.Decision != DecisionAllow {
			scope.IncCounter(metrics.ServiceErrUnauthorizedCounter)
			return nil, errUnauthorized
		}
	}
	return handler(ctx, req)
}

func (a *interceptor) logAuthError(err error) error {
	a.logger.Error("authorization error", tag.Error(err))
	return errUnauthorized // return a generic error to the caller without disclosing details
}

type interceptor struct {
	authorizer    Authorizer
	claimMapper   ClaimMapper
	metricsClient metrics.Client
	logger        log.Logger
}

// GetAuthorizationInterceptor creates an authorization interceptor and return a func that points to its Interceptor method
func NewAuthorizationInterceptor(
	claimMapper ClaimMapper,
	authorizer Authorizer,
	metrics metrics.Client,
	logger log.Logger,
) grpc.UnaryServerInterceptor {
	return (&interceptor{
		claimMapper:   claimMapper,
		authorizer:    authorizer,
		metricsClient: metrics,
		logger:        logger,
	}).Interceptor
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
