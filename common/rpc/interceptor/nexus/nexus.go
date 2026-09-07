package nexus

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/metrics"
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
	MetricTags() []metrics.Tag
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

type InterceptorError struct {
	// wrapped error
	Err error
	// Outcome tag for metrics reporting
	Outcome string
	// Flag for propagating the error to the caller as-is without conversion
	ExposeDetails bool
	// SkipServiceErrorReporting prevents reporting the error as a frontend service failure.
	SkipServiceErrorReporting bool
}

func (t *InterceptorError) Error() string {
	return fmt.Sprintf("interceptor error (%s): %v", t.Outcome, t.Err.Error())
}

func (t *InterceptorError) Unwrap() error {
	return t.Err
}

type outcomeOverrideCtxKey struct{}

// OutcomeOverride lets an inner interceptor that short-circuits the chain(eg. request forwarder)
// replace the success outcome that would otherwise be derived from the response type
type OutcomeOverride struct {
	mu    sync.Mutex
	value string
}

func (o *OutcomeOverride) Set(v string) {
	if o == nil {
		return
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	o.value = v
}

func (o *OutcomeOverride) Get() string {
	if o == nil {
		return ""
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.value
}

func NewOutcomeOverrideContext(ctx context.Context) (context.Context, *OutcomeOverride) {
	override := &OutcomeOverride{}
	return context.WithValue(ctx, outcomeOverrideCtxKey{}, override), override
}

func SetOutcomeOverride(ctx context.Context, v string) {
	override, ok := ctx.Value(outcomeOverrideCtxKey{}).(*OutcomeOverride)
	if !ok {
		return
	}
	override.Set(v)
}

// RequestMetadata carries request metadata resolved by the handler (e.g. after a
// namespace registry lookup) that is supplied alongside the rest of the params at
// InterceptorInput construction time.
type RequestMetadata struct {
	APIName        string
	NamespaceEntry *namespace.Namespace
	EndpointName   string
	MetricTags     []metrics.Tag // handler-resolved frontend dynamic config for the tags to record
	Request        any           // preserves the request shape passed to custom authorizers
}

// container for ServiceName(), OperationName(), NamespaceName(), ForwardingInfo(), and
// the fields in RequestMetadata.
type nexusOpBase struct {
	serviceName, operation, namespaceName, methodName string

	header          headers.HeaderGetter
	forwardingInfo  ForwardingInfo
	requestMetadata RequestMetadata
	startTime       time.Time
}

func (b nexusOpBase) StartTime() time.Time {
	return b.startTime
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

func (b nexusOpBase) MetricTags() []metrics.Tag {
	return b.requestMetadata.MetricTags
}

func (b nexusOpBase) Header() headers.HeaderGetter {
	return b.header
}

func (b nexusOpBase) MethodName() string {
	return b.methodName
}

func (b nexusOpBase) Request() any {
	return b.requestMetadata.Request
}

func (nexusOpBase) sealNexusOp() {}

type StartOpInput struct {
	nexusOpBase
	StartOperationOptions nexus.StartOperationOptions
	StartOperationInput   *nexus.LazyValue
}

func NewStartOpInput(
	serviceName string,
	operation string,
	namespaceName string,
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
			namespaceName:   namespaceName,
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

type CancelOpInput struct {
	nexusOpBase
	CancelOperationOptions nexus.CancelOperationOptions
	CancellationToken      string
}

func NewCancelOpInput(
	serviceName string,
	operation string,
	namespaceName string,
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
			namespaceName:   namespaceName,
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

type CompleteOpInput struct {
	nexusOpBase
	CompletionRequest *nexusrpc.CompletionRequest
	Completion        *tokenspb.NexusOperationCompletion
}

func NewCompleteOpInput(
	namespaceName string,
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
			namespaceName:   namespaceName,
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

func ChainInterceptors(final HandlerFunc, chain []Interceptor) HandlerFunc {
	for _, curr := range slices.Backward(chain) {
		next := final
		final = func(ctx context.Context, opts InterceptorInput) (any, error) {
			return curr(ctx, opts, next)
		}
	}
	return final
}
