package interceptor

import (
	"context"
	"errors"
	"net/http"
	"slices"

	"github.com/nexus-rpc/sdk-go/nexus"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

type NexusHandlerFunc func(ctx context.Context, in NexusInterceptorInput) (any, error)

type NexusInterceptor func(ctx context.Context, in NexusInterceptorInput, next NexusHandlerFunc) (any, error)

type NexusInterceptorInput interface {
	ServiceName() string
	OperationName() string
	NamespaceName() string
	ForwardingInfo() NexusForwardingInfo
	sealNexusOp()
}

var (
	_ NexusInterceptorInput = StartNexusOpInput{}
	_ NexusInterceptorInput = CancelNexusOpInput{}
	_ NexusInterceptorInput = CompleteNexusOpInput{}
)

// NexusForwardingInfo contains the request data needed to forward a Nexus operation.
type NexusForwardingInfo struct {
	OriginalRequestHeaders http.Header
	TaskQueue              string
	EndpointID             string
	EndpointName           string
	BusinessID             string
}

type InterceptorError struct {
	// wrapped error
	Err error
	// Outcome tag for metrics reporting, (draft-review: should Outcomes be enum or at least constants instead)
	Outcome string
}

func (t *InterceptorError) Error() string {
	return t.Err.Error()
}

func (t *InterceptorError) Unwrap() error {
	return t.Err
}

// container for ServiceName(), OperationName(), NamespaceName(), ForwardingInfo()
type nexusOpBase struct {
	serviceName, operation, namespaceName string
	forwardingInfo                        NexusForwardingInfo
}

func (b *nexusOpBase) WithForwardingInfo(info NexusForwardingInfo) {
	b.forwardingInfo = info
}

func (b nexusOpBase) ServiceName() string {
	return b.serviceName
}

func (b nexusOpBase) OperationName() string {
	return b.operation
}

func (b nexusOpBase) NamespaceName() string {
	return b.namespaceName
}

func (b nexusOpBase) ForwardingInfo() NexusForwardingInfo {
	return b.forwardingInfo
}

func (nexusOpBase) sealNexusOp() {}

func NexusHeaderFromInterceptorInput(in NexusInterceptorInput) (headers.HeaderGetter, error) {
	switch opts := in.(type) {
	case StartNexusOpInput:
		return opts.StartOperationOptions.Header, nil
	case CancelNexusOpInput:
		return opts.CancelOperationOptions.Header, nil
	case CompleteNexusOpInput:
		if opts.CompletionRequest == nil || opts.CompletionRequest.HTTPRequest == nil {
			return nil, errors.New("nexus completion request not found")
		}
		return opts.CompletionRequest.HTTPRequest.Header, nil
	default:
		return nil, errors.New("unknown Nexus interceptor input")
	}
}

// draft-review: verify that these are the "right" methods/names
//
//nolint:staticcheck
func NexusMethodName(in NexusInterceptorInput) string {
	switch in.(type) {
	case StartNexusOpInput:
		return "StartNexusOperation"
	case CancelNexusOpInput:
		return "CancelNexusOperation"
	case CompleteNexusOpInput:
		return "CompleteNexusOperation"
	default:
		return ""
	}
}

type StartNexusOpInput struct {
	nexusOpBase
	StartOperationOptions nexus.StartOperationOptions
	StartOperationInput   *nexus.LazyValue
}

func NewStartNexusOpInput(
	serviceName string,
	operation string,
	namespaceName string,
	options nexus.StartOperationOptions,
	input *nexus.LazyValue,
) StartNexusOpInput {
	return StartNexusOpInput{
		nexusOpBase: nexusOpBase{
			serviceName:   serviceName,
			operation:     operation,
			namespaceName: namespaceName,
		},
		StartOperationOptions: options,
		StartOperationInput:   input,
	}
}

type CancelNexusOpInput struct {
	nexusOpBase
	CancelOperationOptions nexus.CancelOperationOptions
	CancellationToken      string
}

func NewCancelNexusOpInput(
	serviceName string,
	operation string,
	namespaceName string,
	options nexus.CancelOperationOptions,
	cancellationToken string,
) CancelNexusOpInput {
	return CancelNexusOpInput{
		nexusOpBase: nexusOpBase{
			serviceName:   serviceName,
			operation:     operation,
			namespaceName: namespaceName,
		},
		CancelOperationOptions: options,
		CancellationToken:      cancellationToken,
	}
}

type CompleteNexusOpInput struct {
	nexusOpBase
	CompletionRequest *nexusrpc.CompletionRequest
}

// draft-review: Complete doesnt need servicename/op - verify
//
//nolint:staticcheck
func NewCompleteNexusOpInput(
	namespaceName string,
	request *nexusrpc.CompletionRequest,
) CompleteNexusOpInput {
	return CompleteNexusOpInput{
		nexusOpBase: nexusOpBase{
			namespaceName: namespaceName,
		},
		CompletionRequest: request,
	}
}

func ChainNexusInterceptors(final NexusHandlerFunc, chain []NexusInterceptor) NexusHandlerFunc {
	for _, curr := range slices.Backward(chain) {
		next := final
		final = func(ctx context.Context, opts NexusInterceptorInput) (any, error) {
			return curr(ctx, opts, next)
		}
	}
	return final
}

type nexusAPINameContextKey struct{}
type nexusEndpointNameContextKey struct{}

// draft-review: only endpoint and apiName are unknowable - the namespace we should be able to get via lookup
type nexusNamespaceContextKey struct{}

// WithNexusAPIName adds the internal Nexus API name to a request context.
func WithNexusAPIName(ctx context.Context, apiName string) context.Context {
	return context.WithValue(ctx, nexusAPINameContextKey{}, apiName)
}

func NexusAPINameFromContext(ctx context.Context) (string, error) {
	apiName, ok := ctx.Value(nexusAPINameContextKey{}).(string)
	if !ok {
		return "", errors.New("nexus API name not found in context")
	}
	return apiName, nil
}

// WithNexusEndpointName adds the resolved Nexus endpoint name to a request context.
func WithNexusEndpointName(ctx context.Context, endpointName string) context.Context {
	return context.WithValue(ctx, nexusEndpointNameContextKey{}, endpointName)
}

func NexusEndpointNameFromContext(ctx context.Context) (string, error) {
	endpointName, ok := ctx.Value(nexusEndpointNameContextKey{}).(string)
	if !ok {
		return "", errors.New("nexus endpoint name not found in context")
	}
	return endpointName, nil
}

// WithNexusNamespace adds the resolved namespace to a request context.
func WithNexusNamespace(ctx context.Context, namespaceEntry *namespace.Namespace) context.Context {
	return context.WithValue(ctx, nexusNamespaceContextKey{}, namespaceEntry)
}

// draft-review: ideally, there is some utility to lookup by name -> Namespace
//
//nolint:staticcheck
func NexusNamespaceFromContext(ctx context.Context) (*namespace.Namespace, error) {
	namespaceEntry, ok := ctx.Value(nexusNamespaceContextKey{}).(*namespace.Namespace)
	if !ok {
		return nil, errors.New("nexus namespace not found in context")
	}
	return namespaceEntry, nil
}
