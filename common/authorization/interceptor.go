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
	"crypto/x509"
	"crypto/x509/pkix"

	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
)

type (
	contextKeyMappedClaims struct{}
	contextKeyAuthHeader   struct{}
)

type (
	// JWTAudienceMapper returns JWT audience for a given request
	JWTAudienceMapper interface {
		Audience(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo) string
	}
)

const (
	RequestUnauthorized = "Request unauthorized."
)

var (
	errUnauthorized = serviceerror.NewPermissionDenied(RequestUnauthorized, "")

	MappedClaims contextKeyMappedClaims
	AuthHeader   contextKeyAuthHeader
)

// GetClaims returns mapped claims and a new context with MappedClaims and AuthHeader values set if applicable
func GetClaims(
	ctx context.Context,
	claimMapper ClaimMapper,
	audienceGetter JWTAudienceMapper,
	req interface{},
	info *grpc.UnaryServerInfo,
	logger log.Logger,
) (context.Context, *Claims, error) {
	var claims *Claims
	var tlsSubject *pkix.Name
	var authHeaders []string
	var authExtraHeaders []string
	var tlsConnection *credentials.TLSInfo

	if md, ok := metadata.FromIncomingContext(ctx); ok {
		authHeaders = md["authorization"]
		authExtraHeaders = md["authorization-extras"]
	}
	tlsConnection = TLSInfoFormContext(ctx)
	clientCert := PeerCert(tlsConnection)
	if clientCert != nil {
		tlsSubject = &clientCert.Subject
	}

	// Add auth info to context only if there's some auth info
	if tlsSubject != nil || len(authHeaders) > 0 {
		var authHeader string
		var authExtraHeader string
		var audience string
		if len(authHeaders) > 0 {
			authHeader = authHeaders[0]
		}
		if len(authExtraHeaders) > 0 {
			authExtraHeader = authExtraHeaders[0]
		}
		if audienceGetter != nil {
			audience = audienceGetter.Audience(ctx, req, info)
		}
		authInfo := AuthInfo{
			AuthToken:     authHeader,
			TLSSubject:    tlsSubject,
			TLSConnection: tlsConnection,
			ExtraData:     authExtraHeader,
			Audience:      audience,
		}
		mappedClaims, err := claimMapper.GetClaims(&authInfo)
		if err != nil {
			logAuthError(logger, err)
			return nil, nil, errUnauthorized // return a generic error to the caller without disclosing details
		}
		claims = mappedClaims
		ctx = context.WithValue(ctx, MappedClaims, mappedClaims)
		if authHeader != "" {
			ctx = context.WithValue(ctx, AuthHeader, authHeader)
		}
	}

	return ctx, claims, nil
}

// AuthorizeRequest authorizes the request and its claims
func AuthorizeRequest(
	ctx context.Context,
	authorizer Authorizer,
	req interface{},
	info *grpc.UnaryServerInfo,
	claims *Claims,
	logger log.Logger,
	metricsClient metrics.Client,
) error {
	var namespace string
	requestWithNamespace, ok := req.(hasNamespace)
	if ok {
		namespace = requestWithNamespace.GetNamespace()
	}

	scope := getMetricsScope(metricsClient, metrics.AuthorizationScope, namespace)
	result, err := authorize(ctx, authorizer, claims, &CallTarget{
		Namespace: namespace,
		APIName:   info.FullMethod,
		Request:   req,
	}, scope)
	if err != nil {
		scope.IncCounter(metrics.ServiceErrAuthorizeFailedCounter)
		logAuthError(logger, err)
		return errUnauthorized // return a generic error to the caller without disclosing details
	}
	if result.Decision != DecisionAllow {
		scope.IncCounter(metrics.ServiceErrUnauthorizedCounter)
		// if a reason is included in the result, include it in the error message
		if result.Reason != "" {
			return serviceerror.NewPermissionDenied(RequestUnauthorized, result.Reason)
		}
		return errUnauthorized // return a generic error to the caller without disclosing details
	}

	return nil
}

func (a *interceptor) Interceptor(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	var claims *Claims
	if a.claimMapper != nil && a.authorizer != nil {
		var err error
		ctx, claims, err = GetClaims(ctx, a.claimMapper, a.audienceGetter, req, info, a.logger)
		if err != nil {
			return nil, err
		}
	}

	if a.authorizer != nil {
		if err := AuthorizeRequest(ctx, a.authorizer, req, info, claims, a.logger, a.metricsClient); err != nil {
			return nil, err
		}
	}

	return handler(ctx, req)
}

func authorize(
	ctx context.Context,
	authorizer Authorizer,
	claims *Claims,
	callTarget *CallTarget,
	scope metrics.Scope,
) (Result, error) {
	sw := scope.StartTimer(metrics.ServiceAuthorizationLatency)
	defer sw.Stop()
	return authorizer.Authorize(ctx, claims, callTarget)
}

func logAuthError(logger log.Logger, err error) {
	logger.Error("Authorization error", tag.Error(err))
}

type interceptor struct {
	authorizer     Authorizer
	claimMapper    ClaimMapper
	metricsClient  metrics.Client
	logger         log.Logger
	audienceGetter JWTAudienceMapper
}

// NewAuthorizationInterceptor creates an authorization interceptor and return a func that points to its Interceptor method
func NewAuthorizationInterceptor(
	claimMapper ClaimMapper,
	authorizer Authorizer,
	metrics metrics.Client,
	logger log.Logger,
	audienceGetter JWTAudienceMapper,
) grpc.UnaryServerInterceptor {
	return (&interceptor{
		claimMapper:    claimMapper,
		authorizer:     authorizer,
		metricsClient:  metrics,
		logger:         logger,
		audienceGetter: audienceGetter,
	}).Interceptor
}

// getMetricsScope return metrics scope with namespace tag
func getMetricsScope(
	metricsClient metrics.Client,
	scope int,
	namespace string,
) metrics.Scope {
	var metricsScope metrics.Scope
	if namespace != "" {
		metricsScope = metricsClient.Scope(scope).Tagged(metrics.NamespaceTag(namespace))
	} else {
		metricsScope = metricsClient.Scope(scope).Tagged(metrics.NamespaceUnknownTag())
	}
	return metricsScope
}

func TLSInfoFormContext(ctx context.Context) *credentials.TLSInfo {

	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil
	}
	if tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo); ok {
		return &tlsInfo
	}
	return nil
}

func PeerCert(tlsInfo *credentials.TLSInfo) *x509.Certificate {

	if tlsInfo == nil || len(tlsInfo.State.VerifiedChains) == 0 || len(tlsInfo.State.VerifiedChains[0]) == 0 {
		return nil
	}
	// The assumption here is that we only expect a single verified chain of certs (first[0]).
	// It's unclear how we should handle a situation when more than one chain is presented,
	// which subject to use. It's okay for us to limit ourselves to one chain.
	// We can always extend this logic later.
	// We take the first element in the chain ([0]) because that's the client cert
	// (at the beginning of the chain), not intermediary CAs or the root CA (at the end of the chain).
	return tlsInfo.State.VerifiedChains[0][0]
}
