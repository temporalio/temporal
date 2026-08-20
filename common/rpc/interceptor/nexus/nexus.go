package nexus

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

type HandlerFunc func(ctx context.Context, in InterceptorInput) (any, error)

type Interceptor func(ctx context.Context, in InterceptorInput, next HandlerFunc) (any, error)

type InterceptorInput interface {
	ServiceName() string
	OperationName() string
	NamespaceName() string // TODO: this should just use NamespaceEntry() instead
	ForwardingInfo() ForwardingInfo
	APIName() string // analogous to the gRPC FullMethod
	NamespaceEntry() (*namespace.Namespace, error)
	EndpointName() string
	sealNexusOp()
}

var (
	_ InterceptorInput = StartOpInput{}
	_ InterceptorInput = CancelOpInput{}
	_ InterceptorInput = CompleteOpInput{}
)

// ForwardingInfo contains the request data needed to forward a Nexus operation.
type ForwardingInfo struct {
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

// RequestMetadata carries request metadata that is only known once the handler
// has resolved it (e.g. after a namespace registry lookup), and so cannot be supplied
// at InterceptorInput construction time. Set via nexusOpBase.WithRequestMetadata.
type RequestMetadata struct {
	APIName        string
	NamespaceEntry *namespace.Namespace
	EndpointName   string
}

// container for ServiceName(), OperationName(), NamespaceName(), ForwardingInfo(), and
// the fields in RequestMetadata.
type nexusOpBase struct {
	serviceName, operation, namespaceName string
	forwardingInfo                        ForwardingInfo
	requestMetadata                       RequestMetadata
}

func (b *nexusOpBase) WithForwardingInfo(info ForwardingInfo) {
	b.forwardingInfo = info
}

func (b *nexusOpBase) WithRequestMetadata(metadata RequestMetadata) {
	b.requestMetadata = metadata
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

func (b nexusOpBase) ForwardingInfo() ForwardingInfo {
	return b.forwardingInfo
}

func (b nexusOpBase) APIName() string {
	return b.requestMetadata.APIName
}

func (b nexusOpBase) NamespaceEntry() (*namespace.Namespace, error) {
	if b.requestMetadata.NamespaceEntry == nil {
		return nil, errors.New("namespace not found in request metadata")
	}
	return b.requestMetadata.NamespaceEntry, nil
}

func (b nexusOpBase) EndpointName() string {
	return b.requestMetadata.EndpointName
}

func (nexusOpBase) sealNexusOp() {}

func HeaderFromInterceptorInput(in InterceptorInput) (headers.HeaderGetter, error) {
	switch opts := in.(type) {
	case StartOpInput:
		return opts.StartOperationOptions.Header, nil
	case CancelOpInput:
		return opts.CancelOperationOptions.Header, nil
	case CompleteOpInput:
		if opts.CompletionRequest == nil || opts.CompletionRequest.HTTPRequest == nil {
			return nil, errors.New("nexus completion request not found")
		}
		return opts.CompletionRequest.HTTPRequest.Header, nil
	default:
		return nil, errors.New("unknown Nexus interceptor input")
	}
}

// draft-review: verify that these are the "right" methods/names
// TBD: is this different from api.MethodName(in.APIName())?
//
//nolint:staticcheck
func MethodName(in InterceptorInput) string {
	switch in.(type) {
	case StartOpInput:
		return "StartNexusOperation"
	case CancelOpInput:
		return "CancelNexusOperation"
	case CompleteOpInput:
		return "CompleteNexusOperation"
	default:
		return ""
	}
}

type StartOpInput struct {
	nexusOpBase
	StartOperationOptions nexus.StartOperationOptions
	StartOperationInput   *nexus.LazyValue
}

func NewStartOpInput(
	serviceName string,
	operation string,
	namespaceName string,
	options nexus.StartOperationOptions,
	input *nexus.LazyValue,
) StartOpInput {
	return StartOpInput{
		nexusOpBase: nexusOpBase{
			serviceName:   serviceName,
			operation:     operation,
			namespaceName: namespaceName,
		},
		StartOperationOptions: options,
		StartOperationInput:   input,
	}
}

type CancelOpInput struct {
	nexusOpBase
	CancelOperationOptions nexus.CancelOperationOptions
	CancellationToken      string
}

func NewCancelOpInput(
	serviceName string,
	operation string,
	namespaceName string,
	options nexus.CancelOperationOptions,
	cancellationToken string,
) CancelOpInput {
	return CancelOpInput{
		nexusOpBase: nexusOpBase{
			serviceName:   serviceName,
			operation:     operation,
			namespaceName: namespaceName,
		},
		CancelOperationOptions: options,
		CancellationToken:      cancellationToken,
	}
}

type CompleteOpInput struct {
	nexusOpBase
	CompletionRequest *nexusrpc.CompletionRequest
}

// draft-review: Complete doesnt need servicename/op - verify
//
//nolint:staticcheck
func NewCompleteOpInput(
	namespaceName string,
	request *nexusrpc.CompletionRequest,
) CompleteOpInput {
	return CompleteOpInput{
		nexusOpBase: nexusOpBase{
			namespaceName: namespaceName,
		},
		CompletionRequest: request,
	}
}

func ChainInterceptors(final HandlerFunc, chain []Interceptor) HandlerFunc {
	for _, curr := range slices.Backward(chain) {
		next := final
		final = func(ctx context.Context, opts InterceptorInput) (any, error) {
			return curr(ctx, opts, next)
		}
	}
	return final
}
