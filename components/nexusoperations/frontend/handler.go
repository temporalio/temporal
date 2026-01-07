package frontend

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
	"github.com/nexus-rpc/sdk-go/nexus"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/authorization"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/headers"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	commonnexus "go.temporal.io/server/common/nexus"
	"go.temporal.io/server/common/nexus/nexusrpc"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/components/nexusoperations"
	"go.temporal.io/server/service/frontend/configs"
	"go.uber.org/fx"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var apiName = configs.CompleteNexusOperation

const (
	methodNameForMetrics = "CompleteNexusOperation"
	// user-agent header contains Nexus SDK client info in the form <sdk-name>/v<sdk-version>
	headerUserAgent        = "user-agent"
	clientNameVersionDelim = "/v"
)

type Config struct {
	Enabled                       dynamicconfig.BoolPropertyFn
	MaxOperationTokenLength       dynamicconfig.IntPropertyFnWithNamespaceFilter
	PayloadSizeLimit              dynamicconfig.IntPropertyFnWithNamespaceFilter
	ForwardingEnabledForNamespace dynamicconfig.BoolPropertyFnWithNamespaceFilter
}

type HandlerOptions struct {
	fx.In

	ClusterMetadata                      cluster.Metadata
	NamespaceRegistry                    namespace.Registry
	Logger                               log.Logger
	MetricsHandler                       metrics.Handler
	Config                               *Config
	CallbackTokenGenerator               *commonnexus.CallbackTokenGenerator
	HistoryClient                        resource.HistoryClient
	TelemetryInterceptor                 *interceptor.TelemetryInterceptor
	RequestErrorHandler                  *interceptor.RequestErrorHandler
	NamespaceValidationInterceptor       *interceptor.NamespaceValidatorInterceptor
	NamespaceRateLimitInterceptor        interceptor.NamespaceRateLimitInterceptor
	NamespaceConcurrencyLimitInterceptor *interceptor.ConcurrentRequestLimitInterceptor
	RateLimitInterceptor                 *interceptor.RateLimitInterceptor
	AuthInterceptor                      *authorization.Interceptor
	RedirectionInterceptor               *interceptor.Redirection
	ForwardingClients                    *cluster.FrontendHTTPClientCache
	HTTPTraceProvider                    commonnexus.HTTPClientTraceProvider
}

type completionHandler struct {
	HandlerOptions
	clientVersionChecker    headers.VersionChecker
	preProcessErrorsCounter metrics.CounterIface
}

// CompleteOperation implements nexus.CompletionHandler.
// nolint:revive // (cyclomatic complexity) This function is long but the complexity is justified.
func (h *completionHandler) CompleteOperation(ctx context.Context, r *nexusrpc.CompletionRequest) (retErr error) {
	startTime := time.Now()
	if !h.Config.Enabled() {
		h.preProcessErrorsCounter.Record(1)
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeNotFound, "Nexus APIs are disabled")
	}
	token, err := commonnexus.DecodeCallbackToken(r.HTTPRequest.Header.Get(commonnexus.CallbackTokenHeader))
	if err != nil {
		h.Logger.Error("failed to decode callback token", tag.Error(err))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid callback token")
	}

	completion, err := h.CallbackTokenGenerator.DecodeCompletion(token)
	if err != nil {
		h.Logger.Error("failed to decode completion from token", tag.Error(err))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid callback token")
	}
	ns, err := h.NamespaceRegistry.GetNamespaceByID(namespace.ID(completion.NamespaceId))
	if err != nil {
		h.Logger.Error("failed to get namespace for nexus completion request", tag.WorkflowNamespaceID(completion.NamespaceId), tag.Error(err))
		h.preProcessErrorsCounter.Record(1)
		var nfe *serviceerror.NamespaceNotFound
		if errors.As(err, &nfe) {
			return nexus.HandlerErrorf(nexus.HandlerErrorTypeNotFound, "namespace %q not found", completion.NamespaceId)
		}
		return commonnexus.ConvertGRPCError(err, false)
	}
	logger := log.With(
		h.Logger,
		tag.WorkflowNamespace(ns.Name().String()),
		tag.WorkflowID(completion.GetWorkflowId()),
		tag.WorkflowRunID(completion.GetRunId()),
	)
	rCtx := &requestContext{
		completionHandler: h,
		namespace:         ns,
		workflowID:        completion.GetWorkflowId(),
		logger:            log.With(h.Logger, tag.WorkflowNamespace(ns.Name().String())),
		metricsHandler:    h.MetricsHandler.WithTags(metrics.NamespaceTag(ns.Name().String())),
		metricsHandlerForInterceptors: h.MetricsHandler.WithTags(
			metrics.OperationTag(methodNameForMetrics),
			metrics.NamespaceTag(ns.Name().String()),
		),
		requestStartTime: startTime,
	}
	ctx = rCtx.augmentContext(ctx, r.HTTPRequest.Header)
	defer rCtx.capturePanicAndRecordMetrics(&ctx, &retErr)
	if r.HTTPRequest.URL.Path != commonnexus.PathCompletionCallbackNoIdentifier {
		nsNameEscaped := commonnexus.RouteCompletionCallback.Deserialize(mux.Vars(r.HTTPRequest))
		nsName, err := url.PathUnescape(nsNameEscaped)
		if err != nil {
			h.Logger.Error("failed to extract namespace from request", tag.Error(err))
			h.preProcessErrorsCounter.Record(1)
			return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid URL")
		}
		if nsName != ns.Name().String() {
			logger.Error(
				"namespace ID in token doesn't match the token",
				tag.WorkflowNamespaceID(ns.ID().String()),
				tag.Error(err),
				tag.NewStringTag("completion-namespace-id", completion.GetNamespaceId()),
			)
			return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid callback token")
		}
	}

	if err := rCtx.interceptRequest(ctx, r); err != nil {
		var notActiveErr *serviceerror.NamespaceNotActive
		if errors.As(err, &notActiveErr) {
			return h.forwardCompleteOperation(ctx, r, rCtx)
		}
		return err
	}
	tokenLimit := h.Config.MaxOperationTokenLength(ns.Name().String())
	if len(r.OperationToken) > tokenLimit {
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "operation token length exceeds allowed limit (%d/%d)", len(r.OperationToken), tokenLimit)
	}

	var links []*commonpb.Link
	for _, nexusLink := range r.Links {
		switch nexusLink.Type {
		case string((&commonpb.Link_WorkflowEvent{}).ProtoReflect().Descriptor().FullName()):
			link, err := nexusoperations.ConvertNexusLinkToLinkWorkflowEvent(nexusLink)
			if err != nil {
				// TODO(rodrigozhou): links are non-essential for the execution of the workflow,
				// so ignoring the error for now; we will revisit how to handle these errors later.
				h.Logger.Warn(
					fmt.Sprintf("failed to parse link to %q: %s", nexusLink.Type, nexusLink.URL),
					tag.Error(err),
				)
				continue
			}
			links = append(links, &commonpb.Link{
				Variant: &commonpb.Link_WorkflowEvent_{
					WorkflowEvent: link,
				},
			})
		default:
			// If the link data type is unsupported, just ignore it for now.
			h.Logger.Warn(fmt.Sprintf("invalid link data type: %q", nexusLink.Type))
		}
	}
	hr := &historyservice.CompleteNexusOperationRequest{
		Completion:     completion,
		State:          string(r.State),
		OperationToken: r.OperationToken,
		StartTime:      timestamppb.New(r.StartTime),
		Links:          links,
	}
	switch r.State { // nolint:exhaustive
	case nexus.OperationStateFailed, nexus.OperationStateCanceled:
		failureErr, ok := r.Error.(*nexus.FailureError)
		if !ok {
			// This shouldn't happen as the Nexus SDK is always expected to convert Failures from the wire to
			// FailureErrors.
			logger.Error("result error is not a FailureError", tag.Error(err))
			return nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "internal server error")
		}
		hr.Outcome = &historyservice.CompleteNexusOperationRequest_Failure{
			Failure: commonnexus.NexusFailureToProtoFailure(failureErr.Failure),
		}
	case nexus.OperationStateSucceeded:
		var result *commonpb.Payload
		if err := r.Result.Consume(&result); err != nil {
			logger.Error("cannot deserialize payload from completion result", tag.Error(err))
			return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid result content")
		}
		if result.Size() > h.Config.PayloadSizeLimit(ns.Name().String()) {
			logger.Error("payload size exceeds error limit for Nexus CompleteOperation request", tag.WorkflowNamespace(ns.Name().String()))
			return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "result exceeds size limit")
		}
		hr.Outcome = &historyservice.CompleteNexusOperationRequest_Success{
			Success: result,
		}
	default:
		// The Nexus SDK ensures this never happens but just in case...
		logger.Error("invalid operation state in completion request", tag.NewStringTag("state", string(r.State)), tag.Error(err))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid completion state")
	}
	_, err = h.HistoryClient.CompleteNexusOperation(ctx, hr)
	if err != nil {
		logger.Error("failed to process nexus completion request", tag.Error(err))
		var namespaceInactiveErr *serviceerror.NamespaceNotActive
		if errors.As(err, &namespaceInactiveErr) {
			return nexus.HandlerErrorf(nexus.HandlerErrorTypeUnavailable, "cluster inactive")
		}
		var notFoundErr *serviceerror.NotFound
		if errors.As(err, &notFoundErr) {
			return commonnexus.ConvertGRPCError(err, true)
		}
		return commonnexus.ConvertGRPCError(err, false)
	}
	return nil
}

func (h *completionHandler) forwardCompleteOperation(ctx context.Context, r *nexusrpc.CompletionRequest, rCtx *requestContext) error {
	client, err := h.ForwardingClients.Get(rCtx.namespace.ActiveClusterName(rCtx.workflowID))
	if err != nil {
		h.Logger.Error("unable to get HTTP client for forward request", tag.Operation(apiName), tag.WorkflowNamespace(rCtx.namespace.Name().String()), tag.Error(err), tag.SourceCluster(h.ClusterMetadata.GetCurrentClusterName()), tag.TargetCluster(rCtx.namespace.ActiveClusterName(rCtx.workflowID)))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
	}

	forwardURL, err := url.JoinPath(client.BaseURL(), commonnexus.RouteCompletionCallback.Path(rCtx.namespace.Name().String()))
	if err != nil {
		h.Logger.Error("failed to construct forwarding request URL", tag.Operation(apiName), tag.WorkflowNamespace(rCtx.namespace.Name().String()), tag.Error(err), tag.TargetCluster(rCtx.namespace.ActiveClusterName(rCtx.workflowID)))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
	}

	if h.HTTPTraceProvider != nil {
		traceLogger := log.With(h.Logger,
			tag.Operation(apiName),
			tag.WorkflowNamespace(rCtx.namespace.Name().String()),
			tag.AttemptStart(time.Now().UTC()),
			tag.SourceCluster(h.ClusterMetadata.GetCurrentClusterName()),
			tag.TargetCluster(rCtx.namespace.ActiveClusterName(rCtx.workflowID)),
		)
		if trace := h.HTTPTraceProvider.NewForwardingTrace(traceLogger); trace != nil {
			ctx = httptrace.WithClientTrace(ctx, trace)
		}
	}

	var forwardReq *http.Request
	switch r.State {
	case nexus.OperationStateSucceeded:
		// For successful operations, the Nexus framework streams the result as a LazyValue, so we can reuse the
		// incoming request body.
		forwardReq, err = http.NewRequestWithContext(ctx, r.HTTPRequest.Method, forwardURL, r.HTTPRequest.Body)
		if err != nil {
			h.Logger.Error("failed to construct forwarding HTTP request", tag.Operation(apiName), tag.WorkflowNamespace(rCtx.namespace.Name().String()), tag.Error(err))
			return nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
		}
	case nexus.OperationStateFailed, nexus.OperationStateCanceled:
		// For unsuccessful operations, the Nexus framework reads and closes the original request body to deserialize
		// the failure, so we must construct a new completion to forward.
		var failureErr *nexus.FailureError
		if !errors.As(r.Error, &failureErr) {
			// This shouldn't happen as the Nexus SDK is always expected to convert Failures from the wire to
			// FailureErrors.
			h.Logger.Error("received unexpected error type when trying to forward Nexus operation completion", tag.WorkflowNamespace(rCtx.namespace.Name().String()), tag.Error(err))
			return nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
		}
		c := &nexusrpc.OperationCompletionUnsuccessful{
			Header:         httpHeaderToNexusHeader(r.HTTPRequest.Header),
			State:          r.State,
			OperationToken: r.OperationToken,
			StartTime:      r.StartTime,
			Links:          r.Links,
			Failure:        failureErr.Failure,
		}
		forwardReq, err = nexusrpc.NewCompletionHTTPRequest(ctx, forwardURL, c)
		if err != nil {
			h.Logger.Error("failed to construct forwarding HTTP request", tag.Operation(apiName), tag.WorkflowNamespace(rCtx.namespace.Name().String()), tag.Error(err))
			return nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
		}
	default:
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid operation state: %q", r.State)
	}

	if r.HTTPRequest.Header != nil {
		forwardReq.Header = r.HTTPRequest.Header.Clone()
	}
	forwardReq.Header.Set(interceptor.DCRedirectionApiHeaderName, "true")

	resp, err := client.Do(forwardReq)
	if err != nil {
		h.Logger.Error("received error from HTTP client when forwarding request", tag.Operation(apiName), tag.WorkflowNamespace(rCtx.namespace.Name().String()), tag.Error(err))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
	}

	// TODO: The following response handling logic is duplicated in the nexus_invocation executor. Eventually it should live in the Nexus SDK.
	body, err := readAndReplaceBody(resp)
	if err != nil {
		h.Logger.Error("unable to read HTTP response for forwarded request", tag.Operation(apiName), tag.WorkflowNamespace(rCtx.namespace.Name().String()), tag.Error(err))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
	}

	if resp.StatusCode == http.StatusOK {
		return nil
	}

	if !isMediaTypeJSON(resp.Header.Get("Content-Type")) {
		h.Logger.Error("received invalid content-type header for non-OK HTTP response to forwarded request", tag.Operation(apiName), tag.WorkflowNamespace(rCtx.namespace.Name().String()), tag.Value(resp.Header.Get("Content-Type")))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
	}

	var failure nexus.Failure
	err = json.Unmarshal(body, &failure)
	if err != nil {
		h.Logger.Error("failed to deserialize Nexus Failure from HTTP response to forwarded request", tag.Operation(apiName), tag.WorkflowNamespace(rCtx.namespace.Name().String()), tag.Error(err))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeInternal, "internal error")
	}

	// TODO: Upgrade Nexus SDK in order to reduce HTTP exposure
	handlerErr := &nexus.HandlerError{
		Type:  commonnexus.HandlerErrorTypeFromHTTPStatus(resp.StatusCode),
		Cause: &nexus.FailureError{Failure: failure},
	}

	if handlerErr.Type == nexus.HandlerErrorTypeInternal && resp.StatusCode != http.StatusInternalServerError {
		h.Logger.Warn("received unknown status code on Nexus client unexpected response error", tag.Value(resp.StatusCode))
		handlerErr.Cause = errors.New("internal error")
	}

	return handlerErr
}

// readAndReplaceBody reads the response body in its entirety and closes it, and then replaces the original response
// body with an in-memory buffer.
// The body is replaced even when there was an error reading the entire body.
func readAndReplaceBody(response *http.Response) ([]byte, error) {
	responseBody := response.Body
	body, err := io.ReadAll(responseBody)
	_ = responseBody.Close()
	response.Body = io.NopCloser(bytes.NewReader(body))
	return body, err
}

func isMediaTypeJSON(contentType string) bool {
	if contentType == "" {
		return false
	}
	mediaType, _, err := mime.ParseMediaType(contentType)
	return err == nil && mediaType == "application/json"
}

// Copies HTTP request headers to Nexus headers except those starting with content- since those will be added by the client.
func httpHeaderToNexusHeader(httpHeader http.Header) nexus.Header {
	header := nexus.Header{}
	for k, v := range httpHeader {
		lowerK := strings.ToLower(k)
		if !strings.HasPrefix(lowerK, "content-") {
			// Nexus headers can only have single values, ignore multiple values.
			header[lowerK] = v[0]
		}
	}
	return header
}

type requestContext struct {
	*completionHandler
	logger                        log.Logger
	metricsHandler                metrics.Handler
	metricsHandlerForInterceptors metrics.Handler
	namespace                     *namespace.Namespace
	workflowID                    string
	cleanupFunctions              []func(error)
	requestStartTime              time.Time
	outcomeTag                    metrics.Tag
	forwarded                     bool
}

func (c *requestContext) augmentContext(ctx context.Context, header http.Header) context.Context {
	ctx = metrics.AddMetricsContext(ctx)
	ctx = interceptor.AddTelemetryContext(ctx, c.metricsHandlerForInterceptors)
	ctx = interceptor.PopulateCallerInfo(
		ctx,
		func() string { return c.namespace.Name().String() },
		func() string { return methodNameForMetrics },
	)
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
	return headers.Propagate(ctx)
}

func (c *requestContext) capturePanicAndRecordMetrics(ctxPtr *context.Context, errPtr *error) {
	recovered := recover() //nolint:revive
	if recovered != nil {
		err, ok := recovered.(error)
		if !ok {
			err = fmt.Errorf("panic: %v", recovered)
		}

		st := string(debug.Stack())

		c.logger.Error("Panic captured", tag.SysStackTrace(st), tag.Error(err))
		*errPtr = err
	}
	if *errPtr == nil {
		if c.forwarded {
			c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("request_forwarded"))
		} else {
			c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("success"))
		}
	} else if c.outcomeTag.Key != "" {
		c.metricsHandler = c.metricsHandler.WithTags(c.outcomeTag)
	} else {
		var he *nexus.HandlerError
		if errors.As(*errPtr, &he) {
			c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("error_" + strings.ToLower(string(he.Type))))
		} else {
			c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("error_internal"))
		}
	}

	// Record Nexus-specific metrics
	c.metricsHandler.Counter(metrics.NexusCompletionRequests.Name()).Record(1)
	c.metricsHandler.Histogram(metrics.NexusCompletionLatencyHistogram.Name(), metrics.Milliseconds).Record(time.Since(c.requestStartTime).Milliseconds())

	// Record general telemetry metrics
	metrics.ServiceRequests.With(c.metricsHandlerForInterceptors).Record(1)
	c.TelemetryInterceptor.RecordLatencyMetrics(*ctxPtr, c.requestStartTime, c.metricsHandlerForInterceptors)

	for _, fn := range c.cleanupFunctions {
		fn(*errPtr)
	}
}

// TODO(bergundy): Merge this with the interceptRequest method in nexus_handler.go.
func (c *requestContext) interceptRequest(ctx context.Context, request *nexusrpc.CompletionRequest) error {
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

	var claims *authorization.Claims
	var err error
	if authInfo != nil {
		claims, err = c.AuthInterceptor.GetClaims(authInfo)
		if err != nil {
			return err
		}
		// Make the auth info and claims available on the context.
		ctx = c.AuthInterceptor.EnhanceContext(ctx, authInfo, claims)
	}

	err = c.AuthInterceptor.Authorize(ctx, claims, &authorization.CallTarget{
		APIName:   apiName,
		Namespace: c.namespace.Name().String(),
		Request:   request,
	})
	if err != nil {
		// If frontend.exposeAuthorizerErrors is false, Authorize err is either an explicitly set reason, or a generic
		// "Request unauthorized." message.
		// Otherwise, expose the underlying error.
		var permissionDeniedError *serviceerror.PermissionDenied
		if errors.As(err, &permissionDeniedError) {
			c.outcomeTag = metrics.OutcomeTag("unauthorized")
			return commonnexus.AdaptAuthorizeError(permissionDeniedError)
		}
		c.outcomeTag = metrics.OutcomeTag("internal_auth_error")
		c.logger.Error("Authorization internal error with processing nexus callback", tag.Error(err))
		return commonnexus.ConvertGRPCError(err, false)
	}

	if err := c.NamespaceValidationInterceptor.ValidateState(c.namespace, apiName); err != nil {
		c.outcomeTag = metrics.OutcomeTag("invalid_namespace_state")
		return commonnexus.ConvertGRPCError(err, false)
	}

	// Redirect if current cluster is passive for this namespace.
	if c.namespace.ActiveClusterName(c.workflowID) != c.ClusterMetadata.GetCurrentClusterName() {
		if c.shouldForwardRequest(ctx, request.HTTPRequest.Header, c.workflowID) {
			c.forwarded = true
			handler, forwardStartTime := c.RedirectionInterceptor.BeforeCall(methodNameForMetrics)
			c.cleanupFunctions = append(c.cleanupFunctions, func(retErr error) {
				c.RedirectionInterceptor.AfterCall(handler, forwardStartTime, c.namespace.ActiveClusterName(c.workflowID), c.namespace.Name().String(), retErr)
			})
			// Handler methods should have special logic to forward requests if this method returns a serviceerror.NamespaceNotActive error.
			return serviceerror.NewNamespaceNotActive(c.namespace.Name().String(), c.ClusterMetadata.GetCurrentClusterName(), c.namespace.ActiveClusterName(c.workflowID))
		}
		c.metricsHandler = c.metricsHandler.WithTags(metrics.OutcomeTag("namespace_inactive_forwarding_disabled"))
		return nexus.HandlerErrorf(nexus.HandlerErrorTypeUnavailable, "cluster inactive")
	}

	c.cleanupFunctions = append(c.cleanupFunctions, func(retErr error) {
		if retErr != nil {
			c.RequestErrorHandler.HandleError(
				request,
				"",
				c.metricsHandlerForInterceptors,
				[]tag.Tag{tag.Operation(methodNameForMetrics), tag.WorkflowNamespace(c.namespace.Name().String())},
				retErr,
				c.namespace.Name(),
			)
		}
	})

	cleanup, err := c.NamespaceConcurrencyLimitInterceptor.Allow(c.namespace.Name(), apiName, c.metricsHandlerForInterceptors, request)
	c.cleanupFunctions = append(c.cleanupFunctions, func(error) { cleanup() })
	if err != nil {
		c.outcomeTag = metrics.OutcomeTag("namespace_concurrency_limited")
		return commonnexus.ConvertGRPCError(err, false)
	}

	if err := c.NamespaceRateLimitInterceptor.Allow(c.namespace.Name(), apiName, request.HTTPRequest.Header); err != nil {
		c.outcomeTag = metrics.OutcomeTag("namespace_rate_limited")
		return commonnexus.ConvertGRPCError(err, true)
	}

	if err := c.RateLimitInterceptor.Allow(apiName, request.HTTPRequest.Header); err != nil {
		c.outcomeTag = metrics.OutcomeTag("global_rate_limited")
		return commonnexus.ConvertGRPCError(err, true)
	}

	if err := c.clientVersionChecker.ClientSupported(ctx); err != nil {
		c.outcomeTag = metrics.OutcomeTag("unsupported_client")
		return commonnexus.ConvertGRPCError(err, true)
	}

	return nil
}

// TODO: copied from nexus_handler.go; should be combined with other intercept logic.
// Combines logic from RedirectionInterceptor.redirectionAllowed and some from
// SelectedAPIsForwardingRedirectionPolicy.getTargetClusterAndIsNamespaceNotActiveAutoForwarding so all
// redirection conditions can be checked at once. If either of those methods are updated, this should
// be kept in sync.
func (c *requestContext) shouldForwardRequest(ctx context.Context, header http.Header, businessID string) bool {
	redirectHeader := header.Get(interceptor.DCRedirectionContextHeaderName)
	redirectAllowed, err := strconv.ParseBool(redirectHeader)
	if err != nil {
		redirectAllowed = true
	}
	return redirectAllowed &&
		c.RedirectionInterceptor.RedirectionAllowed(ctx) &&
		c.namespace.IsGlobalNamespace() &&
		len(c.namespace.ClusterNames(businessID)) > 1 &&
		c.Config.ForwardingEnabledForNamespace(c.namespace.Name().String())
}
