package nexus

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

// HandlerFunc handles a Nexus request after interception.
type HandlerFunc func(ctx context.Context, in InterceptorInput) (any, error)

// Interceptor wraps a Nexus [HandlerFunc] to build an interceptor chain.
type Interceptor func(ctx context.Context, in InterceptorInput, next HandlerFunc) (any, error)

// InterceptorInput provides metadata for a Nexus request.
type InterceptorInput interface {
	ServiceName() string
	OperationName() string
	ForwardingInfo() ForwardingInfo
	APIName() string // analogous to the gRPC FullMethod
	NamespaceEntry() *namespace.Namespace
	EndpointName() string
	Header() headers.HeaderGetter
	MethodName() string
	Request() any
	Outcome(out any, err error) string
	StartTime() time.Time
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

// InterceptorError carries the outcome and reporting policy for a rejected Nexus request.
type InterceptorError struct {
	// wrapped error
	Err error
	// Outcome tag for metrics reporting
	Outcome string
	// ExposeDetails preserves the original message when converting errors.
	ExposeDetails bool
	// SkipServiceErrorReporting prevents reporting the error as a frontend service failure.
	SkipServiceErrorReporting bool
}

// Error includes the metric outcome alongside the underlying error.
func (t *InterceptorError) Error() string {
	return fmt.Sprintf("interceptor error (%s): %v", t.Outcome, t.Err)
}

// Unwrap exposes the underlying error for error classification.
func (t *InterceptorError) Unwrap() error {
	return t.Err
}

// InterceptorResult carries an outcome for a successful request that bypasses the handler.
type InterceptorResult struct {
	Value   any
	Outcome string
}

// RequestMetadata carries request metadata resolved by the handler (e.g. after a
// namespace registry lookup) that is supplied alongside the rest of the params at
// InterceptorInput construction time.
type RequestMetadata struct {
	APIName        string
	NamespaceEntry *namespace.Namespace
	EndpointName   string
	Request        any // preserves the request shape passed to custom authorizers.
}

// container for ServiceName(), OperationName(), ForwardingInfo(), and
// the fields in RequestMetadata.
type nexusOpBase struct {
	serviceName, operation, methodName string

	header          headers.HeaderGetter
	forwardingInfo  ForwardingInfo
	requestMetadata RequestMetadata
	startTime       time.Time
}

// StartTime returns the time the request entered the frontend.
func (b nexusOpBase) StartTime() time.Time {
	return b.startTime
}

// ServiceName returns the requested Nexus service, or an empty string for completion callbacks.
func (b nexusOpBase) ServiceName() string {
	return b.serviceName
}

// OperationName returns the requested Nexus operation, or an empty string for completion callbacks.
func (b nexusOpBase) OperationName() string {
	return b.operation
}

// ForwardingInfo returns the routing data and original HTTP headers needed for forwarding.
func (b nexusOpBase) ForwardingInfo() ForwardingInfo {
	return b.forwardingInfo
}

// APIName returns the full API method used for authorization and rate limiting.
func (b nexusOpBase) APIName() string {
	return b.requestMetadata.APIName
}

// NamespaceEntry returns the namespace resolved before entering the interceptor chain.
func (b nexusOpBase) NamespaceEntry() *namespace.Namespace {
	return b.requestMetadata.NamespaceEntry
}

// EndpointName returns the resolved endpoint name when the request targets an endpoint.
func (b nexusOpBase) EndpointName() string {
	return b.requestMetadata.EndpointName
}

// Header returns the HTTP request headers getter for forwarding requests.
func (b nexusOpBase) Header() headers.HeaderGetter {
	return b.header
}

// MethodName returns the operation label used for service metrics.
func (b nexusOpBase) MethodName() string {
	return b.methodName
}

// Request returns the request shape passed to custom authorizers.
func (b nexusOpBase) Request() any {
	return b.requestMetadata.Request
}

func (nexusOpBase) sealNexusOp() {}

// StartOpInput carries a Nexus start-operation request.
type StartOpInput struct {
	nexusOpBase
	StartOperationOptions nexus.StartOperationOptions
	StartOperationInput   *nexus.LazyValue
}

// NewStartOpInput constructs a request with its resolved namespace metadata.
func NewStartOpInput(
	serviceName string,
	operation string,
	startTime time.Time,
	options nexus.StartOperationOptions,
	input *nexus.LazyValue,
	forwardingInfo ForwardingInfo,
	requestMetadata RequestMetadata,
) StartOpInput {
	return StartOpInput{
		nexusOpBase: nexusOpBase{
			serviceName:     serviceName,
			operation:       operation,
			header:          options.Header,
			methodName:      "StartNexusOperation",
			forwardingInfo:  forwardingInfo,
			requestMetadata: requestMetadata,
			startTime:       startTime,
		},
		StartOperationOptions: options,
		StartOperationInput:   input,
	}
}

// CancelOpInput carries a Nexus cancel-operation request.
type CancelOpInput struct {
	nexusOpBase
	CancelOperationOptions nexus.CancelOperationOptions
	CancellationToken      string
}

// NewCancelOpInput constructs a request with its resolved namespace metadata.
func NewCancelOpInput(
	serviceName string,
	operation string,
	startTime time.Time,
	options nexus.CancelOperationOptions,
	cancellationToken string,
	forwardingInfo ForwardingInfo,
	requestMetadata RequestMetadata,
) CancelOpInput {
	return CancelOpInput{
		nexusOpBase: nexusOpBase{
			serviceName:     serviceName,
			operation:       operation,
			header:          options.Header,
			methodName:      "CancelNexusOperation",
			forwardingInfo:  forwardingInfo,
			requestMetadata: requestMetadata,
			startTime:       startTime,
		},
		CancelOperationOptions: options,
		CancellationToken:      cancellationToken,
	}
}

// CompleteOpInput carries a Nexus operation completion request.
type CompleteOpInput struct {
	nexusOpBase
	CompletionRequest *nexusrpc.CompletionRequest
	Completion        *tokenspb.NexusOperationCompletion
}

// NewCompleteOpInput constructs a request with its resolved namespace metadata.
func NewCompleteOpInput(
	startTime time.Time,
	request *nexusrpc.CompletionRequest,
	completion *tokenspb.NexusOperationCompletion,
	forwardingInfo ForwardingInfo,
	requestMetadata RequestMetadata,
) (CompleteOpInput, error) {
	if request == nil || request.HTTPRequest == nil {
		return CompleteOpInput{}, errors.New("nexus completion request not found")
	}
	requestMetadata.Request = request
	return CompleteOpInput{
		nexusOpBase: nexusOpBase{
			header:          request.HTTPRequest.Header,
			methodName:      "CompleteNexusOperation",
			forwardingInfo:  forwardingInfo,
			requestMetadata: requestMetadata,
			startTime:       startTime,
		},
		CompletionRequest: request,
		Completion:        completion,
	}, nil
}

// Outcome classifies completion results using the existing completion metric labels.
func (c CompleteOpInput) Outcome(out any, err error) string {
	if err == nil {
		return "success"
	}
	if ie, ok := errors.AsType[*InterceptorError](err); ok {
		if ie.Outcome != "" {
			return ie.Outcome
		}
		err = ie.Err
	}
	// retaining behavior
	if handlerErr, ok := errors.AsType[*nexus.HandlerError](err); ok {
		return "error_" + strings.ToLower(string(handlerErr.Type))
	}
	return "error_internal"
}

// Outcome distinguishes synchronous and asynchronous success and interceptor failures.
func (s StartOpInput) Outcome(out any, err error) string {
	if outcome, ok := errorOutcome(err); ok {
		return outcome
	}
	switch out.(type) {
	case *nexus.HandlerStartOperationResultSync[any]:
		return "sync_success"
	case *nexus.HandlerStartOperationResultAsync:
		return "async_success"
	}
	return "internal_error"
}

// Outcome classifies cancellation results for Nexus request metrics.
func (c CancelOpInput) Outcome(out any, err error) string {
	if outcome, ok := errorOutcome(err); ok {
		return outcome
	}
	return "success"
}

func errorOutcome(err error) (string, bool) {
	if err != nil {
		if ie, ok := errors.AsType[*InterceptorError](err); ok && ie.Outcome != "" {
			return ie.Outcome, true
		}
		return "internal_error", true
	}
	return "", false
}

// ChainInterceptors wraps the handler with interceptors in order, outermost first.
func ChainInterceptors(final HandlerFunc, chain []Interceptor) HandlerFunc {
	for _, curr := range slices.Backward(chain) {
		next := final
		final = func(ctx context.Context, opts InterceptorInput) (any, error) {
			return curr(ctx, opts, next)
		}
	}
	return final
}
