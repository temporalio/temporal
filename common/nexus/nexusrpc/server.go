package nexusrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
)

func applyResultToHTTPResponse(r nexus.HandlerStartOperationResult[any], writer http.ResponseWriter, handler *httpHandler) {
	switch r := r.(type) {
	case interface{ ValueAsAny() any }:
		handler.writeResult(writer, r.ValueAsAny())
	case *nexus.HandlerStartOperationResultAsync:
		info := nexus.OperationInfo{
			Token: r.OperationToken,
			State: nexus.OperationStateRunning,
		}
		b, err := json.Marshal(info)
		if err != nil {
			handler.logger.Error("failed to serialize operation info", "error", err)
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}

		writer.Header().Set("Content-Type", contentTypeJSON)
		writer.WriteHeader(http.StatusCreated)
		if _, err := writer.Write(b); err != nil {
			handler.logger.Error("failed to write response body", "error", err)
		}
	}
}

type baseHTTPHandler struct {
	logger           *slog.Logger
	failureConverter nexus.FailureConverter
}

type httpHandler struct {
	baseHTTPHandler
	options HandlerOptions
}

func (h *httpHandler) writeResult(writer http.ResponseWriter, result any) {
	var reader *nexus.Reader
	if r, ok := result.(*nexus.Reader); ok {
		// Close the request body in case we error before sending the HTTP request (which may double close but
		// that's fine since we ignore the error).
		// nolint:errcheck // ignore error on close
		defer r.Close()
		reader = r
	} else {
		content, ok := result.(*nexus.Content)
		if !ok {
			var err error
			content, err = h.options.Serializer.Serialize(result)
			if err != nil {
				h.writeFailure(writer, fmt.Errorf("failed to serialize handler result: %w", err))
				return
			}
		}
		header := maps.Clone(content.Header)
		header["length"] = strconv.Itoa(len(content.Data))

		reader = &nexus.Reader{
			ReadCloser: io.NopCloser(bytes.NewReader(content.Data)),
			Header:     header,
		}
	}

	header := writer.Header()
	addContentHeaderToHTTPHeader(reader.Header, header)
	if reader.ReadCloser == nil {
		return
	}
	if _, err := io.Copy(writer, reader); err != nil {
		h.logger.Error("failed to write response body", "error", err)
	}
}

func (h *baseHTTPHandler) writeFailure(writer http.ResponseWriter, err error) {
	var failure nexus.Failure
	var opError *nexus.OperationError
	var handlerError *nexus.HandlerError
	var operationState nexus.OperationState
	statusCode := http.StatusInternalServerError

	if errors.As(err, &opError) {
		operationState = opError.State
		failure = h.failureConverter.ErrorToFailure(opError.Cause)
		statusCode = statusOperationFailed

		if operationState != nexus.OperationStateFailed && operationState != nexus.OperationStateCanceled {
			h.logger.Error("unexpected operation state", "state", operationState)
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		writer.Header().Set(headerOperationState, string(operationState))
	} else if errors.As(err, &handlerError) {
		failure = h.failureConverter.ErrorToFailure(handlerError.Cause)
		switch handlerError.Type {
		case nexus.HandlerErrorTypeBadRequest:
			statusCode = http.StatusBadRequest
		case nexus.HandlerErrorTypeRequestTimeout:
			statusCode = http.StatusRequestTimeout
		case nexus.HandlerErrorTypeConflict:
			statusCode = http.StatusConflict
		case nexus.HandlerErrorTypeUnauthenticated:
			statusCode = http.StatusUnauthorized
		case nexus.HandlerErrorTypeUnauthorized:
			statusCode = http.StatusForbidden
		case nexus.HandlerErrorTypeNotFound:
			statusCode = http.StatusNotFound
		case nexus.HandlerErrorTypeResourceExhausted:
			statusCode = http.StatusTooManyRequests
		case nexus.HandlerErrorTypeInternal:
			statusCode = http.StatusInternalServerError
		case nexus.HandlerErrorTypeNotImplemented:
			statusCode = http.StatusNotImplemented
		case nexus.HandlerErrorTypeUnavailable:
			statusCode = http.StatusServiceUnavailable
		case nexus.HandlerErrorTypeUpstreamTimeout:
			statusCode = nexus.StatusUpstreamTimeout
		default:
			h.logger.Error("unexpected handler error type", "type", handlerError.Type)
		}
	} else {
		failure = nexus.Failure{
			Message: "internal server error",
		}
		h.logger.Error("handler failed", "error", err)
	}

	b, err := json.Marshal(failure)
	if err != nil {
		h.logger.Error("failed to marshal failure", "error", err)
		writer.WriteHeader(http.StatusInternalServerError)
		return
	}
	writer.Header().Set("Content-Type", contentTypeJSON)

	// Set the retry header here after ensuring that we don't fail with internal error due to failed marshaling to
	// preserve the user's intent.
	if handlerError != nil {
		switch handlerError.RetryBehavior {
		case nexus.HandlerErrorRetryBehaviorNonRetryable:
			writer.Header().Set(headerRetryable, "false")
		case nexus.HandlerErrorRetryBehaviorRetryable:
			writer.Header().Set(headerRetryable, "true")
		default:
			// don't set the header
		}
	}

	writer.WriteHeader(statusCode)

	if _, err := writer.Write(b); err != nil {
		h.logger.Error("failed to write response body", "error", err)
	}
}

func (h *httpHandler) startOperation(service, operation string, writer http.ResponseWriter, request *http.Request) {
	links, err := getLinksFromHeader(request.Header)
	if err != nil {
		h.writeFailure(writer, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid %q header", headerLink))
		return
	}
	options := nexus.StartOperationOptions{
		RequestID:      request.Header.Get(headerRequestID),
		CallbackURL:    request.URL.Query().Get(queryCallbackURL),
		CallbackHeader: prefixStrippedHTTPHeaderToNexusHeader(request.Header, "nexus-callback-"),
		Header:         httpHeaderToNexusHeader(request.Header, "content-", "nexus-callback-"),
		Links:          links,
	}
	value := nexus.NewLazyValue(
		h.options.Serializer,
		&nexus.Reader{
			ReadCloser: request.Body,
			Header:     prefixStrippedHTTPHeaderToNexusHeader(request.Header, "content-"),
		},
	)

	ctx, cancel, ok := h.contextWithTimeoutFromHTTPRequest(writer, request)
	if !ok {
		return
	}
	defer cancel()

	ctx = nexus.WithHandlerContext(ctx, nexus.HandlerInfo{
		Service:   service,
		Operation: operation,
		Header:    options.Header,
	})
	response, err := h.options.Handler.StartOperation(ctx, service, operation, value, options)
	if err != nil {
		h.writeFailure(writer, err)
	} else {
		if err := addLinksToHTTPHeader(nexus.HandlerLinks(ctx), writer.Header()); err != nil {
			h.logger.Error("failed to serialize links into header", "error", err)
			// clear any previous links already written to the header
			writer.Header().Del(headerLink)
			writer.WriteHeader(http.StatusInternalServerError)
			return
		}
		applyResultToHTTPResponse(response, writer, h)
	}
}

func (h *httpHandler) cancelOperation(service, operation, token string, writer http.ResponseWriter, request *http.Request) {
	options := nexus.CancelOperationOptions{Header: httpHeaderToNexusHeader(request.Header)}

	ctx, cancel, ok := h.contextWithTimeoutFromHTTPRequest(writer, request)
	if !ok {
		return
	}
	defer cancel()

	ctx = nexus.WithHandlerContext(ctx, nexus.HandlerInfo{
		Service:   service,
		Operation: operation,
		Header:    options.Header,
	})
	if err := h.options.Handler.CancelOperation(ctx, service, operation, token, options); err != nil {
		h.writeFailure(writer, err)
		return
	}

	writer.WriteHeader(http.StatusAccepted)
}

// parseRequestTimeoutHeader checks if the Request-Timeout HTTP header is set and returns the parsed duration if so.
// Returns (0, true) if unset. Returns ({parsedDuration}, true) if set. If set and there is an error parsing the
// duration, it writes a failure response and returns (0, false).
func (h *httpHandler) parseRequestTimeoutHeader(writer http.ResponseWriter, request *http.Request) (time.Duration, bool) {
	timeoutStr := request.Header.Get(nexus.HeaderRequestTimeout)
	if timeoutStr != "" {
		timeoutDuration, err := ParseDuration(timeoutStr)
		if err != nil {
			h.logger.Warn("invalid request timeout header", "timeout", timeoutStr)
			h.writeFailure(writer, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid request timeout header"))
			return 0, false
		}
		return timeoutDuration, true
	}
	return 0, true
}

// contextWithTimeoutFromHTTPRequest extracts the context from the HTTP request and applies the timeout indicated by
// the Request-Timeout header, if set.
func (h *httpHandler) contextWithTimeoutFromHTTPRequest(writer http.ResponseWriter, request *http.Request) (context.Context, context.CancelFunc, bool) {
	requestTimeout, ok := h.parseRequestTimeoutHeader(writer, request)
	if !ok {
		return nil, nil, false
	}
	if requestTimeout > 0 {
		ctx, cancel := context.WithTimeout(request.Context(), requestTimeout)
		return ctx, cancel, true
	}
	return request.Context(), func() {}, true
}

// HandlerOptions are options for [NewHTTPHandler].
type HandlerOptions struct {
	// Handler for handling service requests.
	Handler nexus.Handler
	// A stuctured logger.
	// Defaults to slog.Default().
	Logger *slog.Logger
	// Max duration to allow waiting for a single get result request.
	// Enforced if provided for requests with the wait query parameter set.
	//
	// Defaults to one minute.
	GetResultTimeout time.Duration
	// A [Serializer] to customize handler serialization behavior.
	// By default the handler handles JSONables, byte slices, and nil.
	Serializer nexus.Serializer
	// A [FailureConverter] to convert a [Failure] instance to and from an [error].
	// Defaults to [DefaultFailureConverter].
	FailureConverter nexus.FailureConverter
}

func (h *httpHandler) handleRequest(writer http.ResponseWriter, request *http.Request) {
	if request.Method != "POST" {
		h.writeFailure(writer, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid request method: expected POST, got %q", request.Method))
		return
	}
	parts := strings.Split(request.URL.EscapedPath(), "/")
	// First part is empty (due to leading /)
	if len(parts) < 3 {
		h.writeFailure(writer, nexus.HandlerErrorf(nexus.HandlerErrorTypeNotFound, "not found"))
		return
	}
	service, err := url.PathUnescape(parts[1])
	if err != nil {
		h.writeFailure(writer, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "failed to parse URL path"))
		return
	}
	operation, err := url.PathUnescape(parts[2])
	if err != nil {
		h.writeFailure(writer, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "failed to parse URL path"))
		return
	}

	// First handle StartOperation at /{service}/{operation}
	if len(parts) == 3 {
		h.startOperation(service, operation, writer, request)
		return
	}

	// Handle deprecated /{service}/{operation}/{operation_token}/cancel
	// TODO(bergundy): remove in server release v1.31.0
	if len(parts) == 5 && parts[4] == "cancel" {
		token, err := url.PathUnescape(parts[3])
		if err != nil {
			h.writeFailure(writer, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "failed to parse URL path"))
			return
		}

		h.cancelOperation(service, operation, token, writer, request)
		return
	}

	if len(parts) != 4 || parts[3] != "cancel" {
		h.writeFailure(writer, nexus.HandlerErrorf(nexus.HandlerErrorTypeNotFound, "not found"))
		return
	}

	token := request.Header.Get(nexus.HeaderOperationToken)
	if token == "" {
		token = request.URL.Query().Get("token")
		if token == "" {
			h.writeFailure(writer, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "missing operation token"))
			return
		}
	} else {
		// Sanitize this header as it is explicitly passed in as an argument.
		request.Header.Del(nexus.HeaderOperationToken)
	}

	h.cancelOperation(service, operation, token, writer, request)
}

// NewHTTPHandler constructs an [http.Handler] from given options for handling Nexus service requests.
func NewHTTPHandler(options HandlerOptions) http.Handler {
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.GetResultTimeout == 0 {
		options.GetResultTimeout = time.Minute
	}
	if options.Serializer == nil {
		options.Serializer = nexus.DefaultSerializer()
	}
	if options.FailureConverter == nil {
		options.FailureConverter = nexus.DefaultFailureConverter()
	}
	handler := &httpHandler{
		baseHTTPHandler: baseHTTPHandler{
			logger:           options.Logger,
			failureConverter: options.FailureConverter,
		},
		options: options,
	}

	return http.HandlerFunc(handler.handleRequest)
}
