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
	"time"

	"go.temporal.io/api/serviceerror"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"

	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/util"
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

	defaultAuthHeaderName      = "authorization"
	defaultAuthExtraHeaderName = "authorization-extras"
)

var (
	errUnauthorized = serviceerror.NewPermissionDenied(RequestUnauthorized, "")

	MappedClaims contextKeyMappedClaims
	AuthHeader   contextKeyAuthHeader
)

// TLSInfoFromContext extracts TLS information from the context's peer value.
func TLSInfoFromContext(ctx context.Context) *credentials.TLSInfo {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil
	}
	if tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo); ok {
		return &tlsInfo
	}
	return nil
}

// PeerCert extracts an x509 certificate from given tlsInfo.
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

// HeaderGetter is an interface for getting a single header value from a case insensitive key.
type HeaderGetter interface {
	Get(string) string
}

// Wrapper for gRPC metadata that exposes a helper to extract a single metadata value.
type grpcHeaderGetter struct {
	metadata metadata.MD
}

// Get a single value from the underlying gRPC metadata.
// Returns an empty string if the metadata key is unset.
func (h grpcHeaderGetter) Get(key string) string {
	values := h.metadata.Get(key)
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

type Interceptor struct {
	claimMapper         ClaimMapper
	authorizer          Authorizer
	metricsHandler      metrics.Handler
	logger              log.Logger
	audienceGetter      JWTAudienceMapper
	authHeaderName      string
	authExtraHeaderName string
}

// NewInterceptor creates an authorization interceptor.
func NewInterceptor(
	claimMapper ClaimMapper,
	authorizer Authorizer,
	metricsHandler metrics.Handler,
	logger log.Logger,
	audienceGetter JWTAudienceMapper,
	authHeaderName string,
	authExtraHeaderName string,
) *Interceptor {
	return &Interceptor{
		claimMapper:         claimMapper,
		authorizer:          authorizer,
		logger:              logger,
		metricsHandler:      metricsHandler,
		authHeaderName:      util.Coalesce(authHeaderName, defaultAuthHeaderName),
		authExtraHeaderName: util.Coalesce(authExtraHeaderName, defaultAuthExtraHeaderName),
		audienceGetter:      audienceGetter,
	}
}

func (a *Interceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	tlsConnection := TLSInfoFromContext(ctx)
	md, _ := metadata.FromIncomingContext(ctx)

	authInfo := a.GetAuthInfo(tlsConnection, grpcHeaderGetter{md}, func() string {
		if a.audienceGetter != nil {
			return a.audienceGetter.Audience(ctx, req, info)
		}
		return ""
	})

	var claims *Claims
	if authInfo != nil {
		var err error
		claims, err = a.GetClaims(authInfo)
		if err != nil {
			a.logger.Error("Authorization error", tag.Error(err))
			// return a generic error to the caller without disclosing details
			return nil, errUnauthorized
		}
		ctx = a.EnhanceContext(ctx, authInfo, claims)
	}

	if a.authorizer != nil {
		var namespace string
		requestWithNamespace, ok := req.(hasNamespace)
		if ok {
			namespace = requestWithNamespace.GetNamespace()
		}
		ct := &CallTarget{
			Namespace: namespace,
			APIName:   info.FullMethod,
			Request:   req,
		}
		if err := a.Authorize(ctx, claims, ct); err != nil {
			return nil, err
		}
	}
	return handler(ctx, req)
}

// GetAuthInfo extracts auth info from TLS info and headers.
// Returns nil if either the policy's claimMapper or authorizer are nil or when there is no auth information in the
// provided TLS info or headers.
func (a *Interceptor) GetAuthInfo(tlsConnection *credentials.TLSInfo, header HeaderGetter, audienceGetter func() string) *AuthInfo {
	if a.claimMapper == nil || a.authorizer == nil {
		return nil
	}
	var tlsSubject *pkix.Name
	var authHeader string
	var authExtraHeader string

	if header != nil {
		authHeader = header.Get(a.authHeaderName)
		authExtraHeader = header.Get(a.authExtraHeaderName)
	}
	clientCert := PeerCert(tlsConnection)
	if clientCert != nil {
		tlsSubject = &clientCert.Subject
	}

	authInfoRequired := true
	if cm, ok := a.claimMapper.(ClaimMapperWithAuthInfoRequired); ok {
		authInfoRequired = cm.AuthInfoRequired()
	}

	// Add auth info to context only if there's some auth info
	if tlsSubject == nil && authHeader == "" && authInfoRequired {
		return nil
	}

	return &AuthInfo{
		AuthToken:     authHeader,
		TLSSubject:    tlsSubject,
		TLSConnection: tlsConnection,
		ExtraData:     authExtraHeader,
		Audience:      audienceGetter(),
	}
}

// GetClaims uses the policy's claimMapper to map the provided authInfo to claims.
func (a *Interceptor) GetClaims(authInfo *AuthInfo) (*Claims, error) {
	return a.claimMapper.GetClaims(authInfo)
}

// EnhanceContext returns a new context with [MappedClaims] and [AuthHeader] values.
func (a *Interceptor) EnhanceContext(ctx context.Context, authInfo *AuthInfo, claims *Claims) context.Context {
	ctx = context.WithValue(ctx, MappedClaims, claims)
	if authInfo.AuthToken != "" {
		ctx = context.WithValue(ctx, AuthHeader, authInfo.AuthToken)
	}
	return ctx
}

// Authorize uses the policy's authorizer to authorize a request based on provided claims and call target.
// Logs and emits metrics when unauthorized.
func (a *Interceptor) Authorize(ctx context.Context, claims *Claims, ct *CallTarget) error {
	if a.authorizer == nil {
		return nil
	}

	mh := a.getMetricsHandler(ct.Namespace)

	startTime := time.Now().UTC()
	result, err := a.authorizer.Authorize(ctx, claims, ct)
	mh.Timer(metrics.ServiceAuthorizationLatency.GetMetricName()).Record(time.Since(startTime))
	if err != nil {
		mh.Counter(metrics.ServiceErrAuthorizeFailedCounter.GetMetricName()).Record(1)
		a.logger.Error("Authorization error", tag.Error(err))
		return errUnauthorized // return a generic error to the caller without disclosing details
	}
	if result.Decision != DecisionAllow {
		mh.Counter(metrics.ServiceErrUnauthorizedCounter.GetMetricName()).Record(1)
		// if a reason is included in the result, include it in the error message
		if result.Reason != "" {
			return serviceerror.NewPermissionDenied(RequestUnauthorized, result.Reason)
		}
		return errUnauthorized // return a generic error to the caller without disclosing details
	}
	return nil
}

// getMetricsHandler returns a metrics handler with a namespace tag
func (a *Interceptor) getMetricsHandler(namespace string) metrics.Handler {
	var metricsHandler metrics.Handler
	if namespace != "" {
		metricsHandler = a.metricsHandler.WithTags(metrics.OperationTag(metrics.AuthorizationScope), metrics.NamespaceTag(namespace))
	} else {
		metricsHandler = a.metricsHandler.WithTags(metrics.OperationTag(metrics.AuthorizationScope), metrics.NamespaceUnknownTag())
	}
	return metricsHandler
}
