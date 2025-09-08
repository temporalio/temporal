package authorization

import (
	"cmp"
	"context"
	"crypto/x509"
	"crypto/x509/pkix"
	"time"

	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
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

	NamespaceChecker interface {
		// Exists returns nil if the namespace exists, otherwise an error.
		Exists(name namespace.Name) error
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

type Interceptor struct {
	claimMapper            ClaimMapper
	authorizer             Authorizer
	metricsHandler         metrics.Handler
	logger                 log.Logger
	namespaceChecker       NamespaceChecker
	audienceGetter         JWTAudienceMapper
	authHeaderName         string
	authExtraHeaderName    string
	exposeAuthorizerErrors dynamicconfig.BoolPropertyFn
}

// NewInterceptor creates an authorization interceptor.
func NewInterceptor(
	claimMapper ClaimMapper,
	authorizer Authorizer,
	metricsHandler metrics.Handler,
	logger log.Logger,
	namespaceChecker NamespaceChecker,
	audienceGetter JWTAudienceMapper,
	authHeaderName string,
	authExtraHeaderName string,
	exposeAuthorizerErrors dynamicconfig.BoolPropertyFn,
) *Interceptor {
	return &Interceptor{
		claimMapper:            claimMapper,
		authorizer:             authorizer,
		logger:                 logger,
		namespaceChecker:       namespaceChecker,
		metricsHandler:         metricsHandler,
		authHeaderName:         cmp.Or(authHeaderName, defaultAuthHeaderName),
		authExtraHeaderName:    cmp.Or(authExtraHeaderName, defaultAuthExtraHeaderName),
		audienceGetter:         audienceGetter,
		exposeAuthorizerErrors: exposeAuthorizerErrors,
	}
}

func (a *Interceptor) Intercept(
	ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler,
) (interface{}, error) {
	tlsConnection := TLSInfoFromContext(ctx)

	authInfo := a.GetAuthInfo(tlsConnection, headers.NewGRPCHeaderGetter(ctx), func() string {
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
func (a *Interceptor) GetAuthInfo(tlsConnection *credentials.TLSInfo, header headers.HeaderGetter, audienceGetter func() string) *AuthInfo {
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
	metrics.ServiceAuthorizationLatency.With(mh).Record(time.Since(startTime))
	if err != nil {
		metrics.ServiceErrAuthorizeFailedCounter.With(mh).Record(1)
		a.logger.Error("Authorization error", tag.Error(err))
		if a.exposeAuthorizerErrors() {
			return err
		}
		return errUnauthorized // return a generic error to the caller without disclosing details
	}
	if result.Decision != DecisionAllow {
		metrics.ServiceErrUnauthorizedCounter.With(mh).Record(1)
		// if a reason is included in the result, include it in the error message
		if result.Reason != "" {
			return serviceerror.NewPermissionDenied(RequestUnauthorized, result.Reason)
		}
		return errUnauthorized // return a generic error to the caller without disclosing details
	}
	return nil
}

// getMetricsHandler returns a metrics handler with a namespace tag
func (a *Interceptor) getMetricsHandler(nsName string) metrics.Handler {
	nsTag := metrics.NamespaceUnknownTag()
	if nsName != "" {
		// Note that this is before the namespace state validation interceptor, so this
		// namespace name is not validated. We should only use it as a metric tag if it's a
		// real namespace, to avoid unbounded cardinality issues.
		if a.namespaceChecker.Exists(namespace.Name(nsName)) == nil {
			nsTag = metrics.NamespaceTag(nsName)
		}
	}
	return a.metricsHandler.WithTags(metrics.OperationTag(metrics.AuthorizationScope), nsTag)
}
