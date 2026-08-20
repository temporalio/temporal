package frontend

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	tokenspb "go.temporal.io/server/api/token/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc"
	"go.temporal.io/server/common/rpc/interceptor"
	interceptornexus "go.temporal.io/server/common/rpc/interceptor/nexus"
	"go.temporal.io/server/nexusworkflowref"
	"go.temporal.io/server/service/frontend/configs"
	"go.temporal.io/server/service/history/consts"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const nexusCompletionAPIName = configs.CompleteNexusOperation
const nexusCompletionMethodName = "CompleteNexusOperation"

type nexusCompletionHandler struct {
	ClusterMetadata         cluster.Metadata
	NamespaceRegistry       namespace.Registry
	Logger                  log.Logger
	MetricsHandler          metrics.Handler
	Config                  *Config
	CallbackTokenGenerator  *commonnexus.CallbackTokenGenerator
	HistoryClient           resource.HistoryClient
	RequestErrorHandler     *interceptor.RequestErrorHandler
	AuthInterceptor         *authorization.Interceptor // required for parsing auth info, not used as an interceptor
	HTTPTraceProvider       commonnexus.HTTPClientTraceProvider
	nexusInterceptors       []interceptornexus.Interceptor
	clientVersionChecker    headers.VersionChecker
	preProcessErrorsCounter metrics.CounterIface
}

type nexusCompletionHTTPHandler struct {
	httpHandler http.Handler
}

func newNexusCompletionHandler(
	clusterMetadata cluster.Metadata,
	namespaceRegistry namespace.Registry,
	logger log.Logger,
	metricsHandler metrics.Handler,
	serviceConfig *Config,
	callbackTokenGenerator *commonnexus.CallbackTokenGenerator,
	historyClient resource.HistoryClient,
	requestErrorHandler *interceptor.RequestErrorHandler,
	authInterceptor *authorization.Interceptor,
	httpTraceProvider commonnexus.HTTPClientTraceProvider,
	interceptorsProvider *InterceptorsProvider,
	customNexusInterceptors []interceptornexus.Interceptor,
) *nexusCompletionHandler {

	return &nexusCompletionHandler{
		ClusterMetadata:         clusterMetadata,
		NamespaceRegistry:       namespaceRegistry,
		Logger:                  logger,
		MetricsHandler:          metricsHandler,
		Config:                  serviceConfig,
		CallbackTokenGenerator:  callbackTokenGenerator,
		HistoryClient:           historyClient,
		RequestErrorHandler:     requestErrorHandler,
		AuthInterceptor:         authInterceptor,
		HTTPTraceProvider:       httpTraceProvider,
		nexusInterceptors:       interceptorsProvider.GetNexusInterceptors(),
		clientVersionChecker:    headers.NewDefaultVersionChecker(),
		preProcessErrorsCounter: metricsHandler.Counter(metrics.NexusCompletionRequestPreProcessErrors.Name()),
	}
}

func newNexusCompletionHTTPHandler(handler *nexusCompletionHandler, logger log.Logger) *nexusCompletionHTTPHandler {
	return &nexusCompletionHTTPHandler{
		httpHandler: nexusrpc.NewCompletionHTTPHandler(nexusrpc.CompletionHandlerOptions{
			Handler:    handler,
			Logger:     log.NewSlogLogger(logger),
			Serializer: commonnexus.PayloadSerializer,
		}),
	}
}

// CompleteOperation implements nexus.CompletionHandler.
// nolint:revive // (cyclomatic complexity) This function is long but the complexity is justified.
func (h *nexusCompletionHandler) CompleteOperation(ctx context.Context, r *nexusrpc.CompletionRequest) (retErr error) {
	startTime := time.Now()
	token, err := commonnexus.DecodeCallbackToken(r.HTTPRequest.Header.Get(commonnexus.CallbackTokenHeader))
	if err != nil {
		h.Logger.Error("failed to decode callback token", tag.Error(err))
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid callback token")
	}

	completion, err := h.CallbackTokenGenerator.DecodeCompletion(token)
	if err != nil {
		h.Logger.Error("failed to decode completion from token", tag.Error(err))
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid callback token")
	}

	// Determine the target namespace, workflow, and run ID from the completion token. The CHASM
	// ComponentRef is canonical when present; otherwise the top-level HSM fields are used. Shared
	// with the system-callback router via commonnexus.CompletionTarget so the two can't drift.
	targetNamespaceID, targetBusinessID, targetRunID, err := commonnexus.CompletionTarget(completion)
	if err != nil {
		h.Logger.Error("failed to unmarshal CHASM component ref", tag.Error(err))
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid callback token")
	}

	ns, err := h.NamespaceRegistry.GetNamespaceByID(namespace.ID(targetNamespaceID))
	if err != nil {
		h.Logger.Error("failed to get namespace for nexus completion request", tag.WorkflowNamespaceID(targetNamespaceID), tag.Error(err))
		h.preProcessErrorsCounter.Record(1)
		if _, ok := errors.AsType[*serviceerror.NamespaceNotFound](err); ok {
			return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeNotFound, "namespace %q not found", targetNamespaceID)
		}
		return commonnexus.ConvertGRPCError(err, false)
	}
	logger := log.With(
		h.Logger,
		tag.Operation(nexusCompletionMethodName),
		tag.WorkflowNamespace(ns.Name().String()),
		tag.WorkflowNamespaceID(targetNamespaceID),
		tag.WorkflowID(targetBusinessID),
		tag.WorkflowRunID(targetRunID),
		tag.RequestID(completion.GetRequestId()),
	)
	rCtx := &requestContext{
		nexusCompletionHandler: h,
		namespace:              ns,
		businessID:             targetBusinessID,
		logger:                 logger,
		metricsHandler:         h.MetricsHandler.WithTags(metrics.NamespaceTag(ns.Name().String())),
		metricsHandlerForInterceptors: h.MetricsHandler.WithTags(
			metrics.OperationTag(nexusCompletionMethodName),
			metrics.NamespaceTag(ns.Name().String()),
		),
		requestStartTime: startTime,
	}
	if r.HTTPRequest.Header != nil {
		rCtx.originalHeaders = r.HTTPRequest.Header.Clone()
	}
	ctx = rCtx.augmentContext(ctx, r.HTTPRequest.Header)
	defer captureOperationPanic(rCtx.logger, &retErr)

	if r.HTTPRequest.URL.Path != commonnexus.PathCompletionCallbackNoIdentifier {
		nsNameEscaped := commonnexus.RouteCompletionCallback.Deserialize(mux.Vars(r.HTTPRequest))
		nsName, err := url.PathUnescape(nsNameEscaped)
		if err != nil {
			logger.Error("failed to extract namespace from request", tag.Error(err))
			h.preProcessErrorsCounter.Record(1)
			return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid URL")
		}
		if nsName != ns.Name().String() {
			logger.Error(
				"namespace in callback URL doesn't match the completion token",
				tag.String("url-namespace", nsName),
			)
			return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid callback token")
		}
	}
	ctx, err = rCtx.parseTLSAndAuthInfo(ctx, r)
	if err != nil {
		return err
	}

	interceptorInput := interceptornexus.NewCompleteOpInput(ns.Name().String(), r)
	interceptorInput.WithForwardingInfo(interceptornexus.ForwardingInfo{
		OriginalRequestHeaders: rCtx.originalHeaders,
		BusinessID:             rCtx.businessID,
	})
	interceptorInput.WithRequestMetadata(interceptornexus.RequestMetadata{
		APIName:        nexusCompletionAPIName,
		NamespaceEntry: ns,
	})
	finalHandler := func(ctx context.Context, _ interceptornexus.InterceptorInput) (any, error) {
		return nil, h.completeOperationRequest(ctx, logger, completion, r, rCtx)
	}
	_, err = interceptornexus.ChainInterceptors(finalHandler, h.nexusInterceptors)(ctx, interceptorInput)
	if err != nil {
		if taggedErr, ok := errors.AsType[*interceptornexus.InterceptorError](err); ok {
			return taggedErr.Err
		}
		return err
	}
	return nil
}

func (h *nexusCompletionHandler) completeOperationRequest(
	ctx context.Context,
	logger log.Logger,
	completion *tokenspb.NexusOperationCompletion,
	r *nexusrpc.CompletionRequest,
	rCtx *requestContext,
) error {
	ns := rCtx.namespace
	tokenLimit := h.Config.MaxNexusOperationTokenLength(ns.Name().String())
	if len(r.OperationToken) > tokenLimit {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "operation token length exceeds allowed limit (%d/%d)", len(r.OperationToken), tokenLimit)
	}

	links := commonnexus.ConvertNexusLinksToProtoLinks(r.Links, logger)

	var successPayload *commonpb.Payload
	switch r.State { // nolint:exhaustive
	case nexus.OperationStateFailed, nexus.OperationStateCanceled:
		// no validation needed
	case nexus.OperationStateSucceeded:
		var result *commonpb.Payload
		if err := r.Result.Consume(&result); err != nil {
			logger.Error("cannot deserialize payload from completion result", tag.Error(err))
			return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid result content")
		}
		if result.Size() > h.Config.BlobSizeLimitError(ns.Name().String()) {
			logger.Error("payload size exceeds error limit for Nexus CompleteOperation request")
			return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "result exceeds size limit")
		}
		successPayload = result
	default:
		// The Nexus SDK ensures this never happens but just in case...
		logger.Error("invalid operation state in completion request", tag.String("state", string(r.State)))
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid completion state")
	}

	err := h.completeOperation(ctx, logger, completion, successPayload, r, links, h.Config.EnableChasm(ns.Name().String()))
	if err == nil {
		return nil
	}
	logger.Error("failed to process nexus completion request", tag.Error(err))
	if _, ok := errors.AsType[*serviceerror.NamespaceNotActive](err); ok {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeUnavailable, "cluster inactive")
	}
	if _, ok := errors.AsType[*serviceerror.NotFound](err); ok {
		return commonnexus.ConvertGRPCError(err, true)
	}
	return commonnexus.ConvertGRPCError(err, false)
}

// completeOperation dispatches the completion to the framework named by its
// token. If that framework no longer has the operation, the token is converted
// to the other framework and retried once.
func (h *nexusCompletionHandler) completeOperation(
	ctx context.Context,
	logger log.Logger,
	completion *tokenspb.NexusOperationCompletion,
	successPayload *commonpb.Payload,
	req *nexusrpc.CompletionRequest,
	links []*commonpb.Link,
	chasmEnabled bool,
) error {
	isChasm := len(completion.GetComponentRef()) > 0
	var err error
	if isChasm {
		err = h.completeChasmOperation(ctx, logger, completion, successPayload, req, links)
	} else {
		err = h.completeHSMOperation(ctx, completion, successPayload, req, links)
	}
	if _, notFound := errors.AsType[*serviceerror.NotFound](err); !notFound || completion.GetRequestId() == "" {
		return err
	}
	// If the workflow itself is gone, the operation cannot exist in either framework.
	if isTerminalCompletionError(err) {
		return err
	}
	// Only try HSM -> CHASM when this namespace can have CHASM workflow state.
	if !isChasm && !chasmEnabled {
		return err
	}
	converted, convErr := convertCompletionToOtherFramework(completion)
	if convErr != nil {
		logger.Warn("failed to convert nexus completion token to the other framework", tag.Error(convErr))
		return err
	}
	var fallbackErr error
	if isChasm {
		fallbackErr = h.completeHSMOperation(ctx, converted, successPayload, req, links)
	} else {
		fallbackErr = h.completeChasmOperation(ctx, logger, converted, successPayload, req, links)
	}
	// If the fallback also reports NotFound, the operation is gone in both frameworks.
	// Return the error from the initial attempt.
	if _, fbNotFound := errors.AsType[*serviceerror.NotFound](fallbackErr); fbNotFound {
		return err
	}
	return fallbackErr
}

// isTerminalCompletionError reports whether err means the workflow is already
// completed or does not exist. These arrive as NotFound errors from history.
func isTerminalCompletionError(err error) bool {
	var nfe *serviceerror.NotFound
	if !errors.As(err, &nfe) {
		return false
	}
	msg := nfe.Error()
	return msg == consts.ErrWorkflowCompleted.Error() || msg == consts.ErrWorkflowExecutionNotFound.Error()
}

// convertCompletionToOtherFramework converts a workflow Nexus completion token
// between HSM and CHASM forms.
func convertCompletionToOtherFramework(completion *tokenspb.NexusOperationCompletion) (*tokenspb.NexusOperationCompletion, error) {
	if len(completion.GetComponentRef()) > 0 {
		return nexusworkflowref.CHASMRefToHSMRef(completion)
	}
	return nexusworkflowref.HSMRefToCHASMRef(completion)
}

func (h *nexusCompletionHandler) completeHSMOperation(
	ctx context.Context,
	completion *tokenspb.NexusOperationCompletion,
	successPayload *commonpb.Payload,
	req *nexusrpc.CompletionRequest,
	links []*commonpb.Link,
) error {
	hr := &historyservice.CompleteNexusOperationRequest{
		Completion:     completion,
		State:          string(req.State),
		OperationToken: req.OperationToken,
		StartTime:      timestamppb.New(req.StartTime),
		Links:          links,
	}

	switch req.State { // nolint:exhaustive
	case nexus.OperationStateFailed, nexus.OperationStateCanceled:
		hr.Outcome = &historyservice.CompleteNexusOperationRequest_Failure{
			Failure: commonnexus.NexusFailureToProtoFailure(*req.Error.OriginalFailure),
		}
	case nexus.OperationStateSucceeded:
		hr.Outcome = &historyservice.CompleteNexusOperationRequest_Success{
			Success: successPayload,
		}
	default:
		// Should be unreachable as validated earlier.
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid completion state")
	}

	_, err := h.HistoryClient.CompleteNexusOperation(ctx, hr)
	return err
}

func (h *nexusCompletionHandler) completeChasmOperation(
	ctx context.Context,
	logger log.Logger,
	completion *tokenspb.NexusOperationCompletion,
	successPayload *commonpb.Payload,
	req *nexusrpc.CompletionRequest,
	links []*commonpb.Link,
) error {
	hr := &historyservice.CompleteNexusOperationChasmRequest{
		Completion: &tokenspb.NexusOperationCompletion{
			RequestId:    completion.GetRequestId(),
			ComponentRef: completion.GetComponentRef(),
		},
		Links:          links,
		OperationToken: req.OperationToken,
	}
	if !req.StartTime.IsZero() {
		hr.StartTime = timestamppb.New(req.StartTime)
	}
	if !req.CloseTime.IsZero() {
		hr.CloseTime = timestamppb.New(req.CloseTime)
	}

	switch req.State { // nolint:exhaustive
	case nexus.OperationStateFailed, nexus.OperationStateCanceled:
		// Temporal->Temporal calls transmit the real failure as the wrapper OperationError's cause.
		// Unwrap it so the caller sees the handler's original error (message, type, details, and
		// canceled/terminated info) rather than the generic wrapper.
		nexusFailure := nexusrpc.UnwrapFailure(req.Error.OriginalFailure)
		failure, err := commonnexus.NexusFailureToTemporalFailure(*nexusFailure)
		if err != nil {
			logger.Error("cannot convert nexus failure from completion request", tag.Error(err))
			return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid failure content")
		}
		hr.Outcome = &historyservice.CompleteNexusOperationChasmRequest_Failure{
			Failure: failure,
		}
	case nexus.OperationStateSucceeded:
		hr.Outcome = &historyservice.CompleteNexusOperationChasmRequest_Success{
			Success: successPayload,
		}
	default:
		// Should be unreachable as validated earlier.
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid completion state")
	}

	_, err := h.HistoryClient.CompleteNexusOperationChasm(ctx, hr)
	return err
}

func (h *nexusCompletionHTTPHandler) RegisterRoutes(r *mux.Router) {
	r.Path("/" + commonnexus.RouteCompletionCallback.Representation()).HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, rpc.MaxNexusAPIRequestBodyBytes)
		h.httpHandler.ServeHTTP(w, r)
	})
	r.Path(commonnexus.PathCompletionCallbackNoIdentifier).HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, rpc.MaxNexusAPIRequestBodyBytes)
		h.httpHandler.ServeHTTP(w, r)
	})
}

type requestContext struct {
	*nexusCompletionHandler
	logger                        log.Logger
	metricsHandler                metrics.Handler
	metricsHandlerForInterceptors metrics.Handler
	namespace                     *namespace.Namespace
	businessID                    string
	requestStartTime              time.Time
	outcomeTag                    metrics.Tag
	originalHeaders               http.Header
}

func (c *requestContext) augmentContext(ctx context.Context, header http.Header) context.Context {
	ctx = interceptor.WithTelemetryContext(ctx, c)
	if userAgent := header.Get(headerUserAgent); userAgent != "" {
		// Preserve original strict behavior: only process if exactly one delimiter present.
		if strings.Count(userAgent, clientNameVersionDelim) == 1 {
			parts := strings.SplitN(userAgent, clientNameVersionDelim, 2)
			if len(parts) == 2 { // defensive
				mdIncoming, ok := metadata.FromIncomingContext(ctx)
				if !ok {
					mdIncoming = metadata.MD{}
				}
				mdIncoming.Set(headers.ClientNameHeaderName, parts[0])
				mdIncoming.Set(headers.ClientVersionHeaderName, parts[1])
				ctx = metadata.NewIncomingContext(ctx, mdIncoming)
			}
		}
	}
	return ctx
}

func (c *requestContext) MetricsHandler(err error) metrics.Handler {
	if c.outcomeTag.Key != "" {
		return c.metricsHandler.WithTags(c.outcomeTag)
	}
	if err == nil {
		return c.metricsHandler.WithTags(metrics.OutcomeTag("success"))
	}
	if handlerErr, ok := errors.AsType[*nexus.HandlerError](err); ok {
		return c.metricsHandler.WithTags(metrics.OutcomeTag("error_" + strings.ToLower(string(handlerErr.Type))))
	}
	return c.metricsHandler.WithTags(metrics.OutcomeTag("error_internal"))
}

func (c *requestContext) MetricsHandlerForInterceptors() metrics.Handler {
	return c.metricsHandlerForInterceptors
}

func (c *requestContext) MetricsLogger() log.Logger {
	return c.logger
}

func (c *requestContext) SetMetricsOutcome(outcome string) {
	c.outcomeTag = metrics.OutcomeTag(outcome)
}

// no-op for completion as it doesnt report back via headers
func (c *requestContext) SetFailureSource(string) {}

func (c *requestContext) HandleRequestError(err error) {
	if err == nil {
		return
	}
	c.RequestErrorHandler.HandleError(
		// The request is only read to extract workflow log tags, which is keyed off the
		// gRPC full method. Nexus has none, so it is never used.
		nil,
		"",
		c.metricsHandlerForInterceptors,
		[]tag.Tag{tag.Operation(nexusCompletionMethodNameForMetrics), tag.WorkflowNamespace(c.namespace.Name().String())},
		err,
		c.namespace.Name(),
	)
}

// enrich context with authInfo
func (c *requestContext) parseTLSAndAuthInfo(ctx context.Context, request *nexusrpc.CompletionRequest) (context.Context, error) {
	var tlsInfo *credentials.TLSInfo
	if request.HTTPRequest.TLS != nil {
		tlsInfo = &credentials.TLSInfo{
			State:          *request.HTTPRequest.TLS,
			CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.PrivacyAndIntegrity},
		}
	}

	authInfo := c.AuthInterceptor.GetAuthInfo(tlsInfo, request.HTTPRequest.Header, func() string {
		return "" // TODO: support audience getter
	})
	if authInfo == nil {
		return ctx, nil
	}
	claims, err := c.AuthInterceptor.GetClaims(authInfo)
	if err != nil {
		return nil, err
	}
	return c.AuthInterceptor.EnhanceContext(ctx, authInfo, claims), nil
}
