package nexusrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
)

// CompletionHTTPClient is a client for sending Nexus operation completion callbacks via HTTP.
type CompletionHTTPClient struct {
	baseHTTPClient
}

// CompletionHTTPClientOptions are options for [NewCompletionHTTPClient].
type CompletionHTTPClientOptions struct {
	// A function for making HTTP requests.
	// Defaults to [http.DefaultClient.Do].
	HTTPCaller func(*http.Request) (*http.Response, error)
	// A [serializer] to customize client serialization behavior.
	// By default the client handles JSONables, byte slices, and nil.
	Serializer nexus.Serializer
	// A [failureConverter] to convert a [Failure] instance to and from an [error]. Defaults to
	// [DefaultFailureConverter].
	FailureConverter FailureConverter
}

// NewCompletionHTTPClient constructs a [CompletionHTTPClient] from given options for sending Nexus operation completion
// callbacks via HTTP.
func NewCompletionHTTPClient(options CompletionHTTPClientOptions) *CompletionHTTPClient {
	if options.HTTPCaller == nil {
		options.HTTPCaller = http.DefaultClient.Do
	}
	if options.Serializer == nil {
		options.Serializer = nexus.DefaultSerializer()
	}
	if options.FailureConverter == nil {
		options.FailureConverter = DefaultFailureConverter()
	}
	return &CompletionHTTPClient{
		baseHTTPClient: baseHTTPClient{
			httpCaller:       options.HTTPCaller,
			serializer:       options.Serializer,
			failureConverter: options.FailureConverter,
		},
	}
}

// CompleteOperation sends a completion callback for a Nexus operation to the given URL with the given completion details.
func (c *CompletionHTTPClient) CompleteOperation(ctx context.Context, url string, completion CompleteOperationOptions) error {
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, nil)
	if err != nil {
		return err
	}
	if err := completion.applyToHTTPRequest(c, httpReq); err != nil {
		return err
	}

	response, err := c.httpCaller(httpReq)
	if err != nil {
		return err
	}

	if response.StatusCode >= 200 && response.StatusCode < 300 {
		// Body is not read but should be discarded to keep the underlying TCP connection alive.
		// Just in case something unexpected happens while discarding or closing the body,
		// propagate errors to the machine.
		if _, err = io.Copy(io.Discard, response.Body); err == nil {
			if err = response.Body.Close(); err != nil {
				return err
			}
		}
		return nil
	}

	body, err := readAndReplaceBody(response)
	if err != nil {
		return err
	}

	return c.bestEffortHandlerErrorFromResponse(response, body)
}

// CompleteOperationOptions is input for CompleteOperation.
// If Error is set, the completion is unsuccessful. Otherwise, it is successful.
type CompleteOperationOptions struct {
	// Header to send in the completion request.
	// Note that this is a Nexus header, not an HTTP header.
	Header nexus.Header
	// OperationToken is the unique token for this operation. Used when a completion callback is received before a
	// started response.
	OperationToken string
	// StartTime is the time the operation started. Used when a completion callback is received before a started response.
	StartTime time.Time
	// CloseTime is the time the operation completed. This may be different from the time the completion callback is delivered.
	CloseTime time.Time
	// Links are used to link back to the operation when a completion callback is received before a started response.
	Links []nexus.Link
	// Error to send with the completion. If set, the completion is unsuccessful.
	Error *nexus.OperationError
	// Result to deliver with the completion. Uses the client's serializer to serialize the result into the request body.
	// Only used for successful completions.
	Result any
}

// nolint:revive // This method is long but it's more readable to keep the logic in one place since it's all related to
// constructing the completion request.
func (c CompleteOperationOptions) applyToHTTPRequest(cc *CompletionHTTPClient, request *http.Request) error {
	if request.Header == nil {
		request.Header = make(http.Header)
	}

	// Set the body and operation state based on whether the completion is successful or not.
	if c.Error != nil {
		failure, err := cc.failureConverter.ErrorToFailure(c.Error)
		if err != nil {
			return err
		}
		// Backwards compatibility: if the failure has a cause, unwrap it to maintain the behavior as older servers.
		if failure.Cause != nil {
			failure = *failure.Cause
		}
		b, err := json.Marshal(failure)
		if err != nil {
			return err
		}
		request.Body = io.NopCloser(bytes.NewReader(b))
		// Set the operation state header for backwards compatibility.
		request.Header.Set(headerOperationState, string(c.Error.State))
		request.Header.Set("Content-Type", contentTypeJSON)
	} else {
		reader, ok := c.Result.(*nexus.Reader)
		if !ok {
			content, ok := c.Result.(*nexus.Content)
			if !ok {
				var err error
				content, err = cc.serializer.Serialize(c.Result)
				if err != nil {
					return err
				}
			}
			request.ContentLength = int64(len(content.Data))
			reader = &nexus.Reader{
				Header:     content.Header,
				ReadCloser: io.NopCloser(bytes.NewReader(content.Data)),
			}
		}
		if reader.Header != nil {
			addContentHeaderToHTTPHeader(reader.Header, request.Header)
		}
		request.Body = reader.ReadCloser
		request.Header.Set(headerOperationState, string(nexus.OperationStateSucceeded))
	}

	if c.Header != nil {
		addNexusHeaderToHTTPHeader(c.Header, request.Header)
	}
	if c.Header.Get(headerUserAgent) == "" {
		request.Header.Set(headerUserAgent, userAgent)
	}
	if c.Header.Get(nexus.HeaderOperationToken) == "" && c.OperationToken != "" {
		request.Header.Set(nexus.HeaderOperationToken, c.OperationToken)
	}
	if c.Header.Get(headerOperationStartTime) == "" && !c.StartTime.IsZero() {
		request.Header.Set(headerOperationStartTime, c.StartTime.Format(http.TimeFormat))
	}
	if c.Header.Get(headerOperationCloseTime) == "" && !c.CloseTime.IsZero() {
		request.Header.Set(headerOperationCloseTime, marshalTimestamp(c.CloseTime))
	}
	if c.Header.Get(headerLink) == "" {
		if err := addLinksToHTTPHeader(c.Links, request.Header); err != nil {
			return err
		}
	}
	return nil
}

// CompletionRequest is input for CompletionHandler.CompleteOperation.
type CompletionRequest struct {
	// The original HTTP request.
	HTTPRequest *http.Request
	// State of the operation.
	State nexus.OperationState
	// OperationToken is the unique token for this operation. Used when a completion callback is received before a
	// started response.
	OperationToken string
	// StartTime is the time the operation started. Used when a completion callback is received before a started response.
	StartTime time.Time
	// CloseTime is the time the operation completed. This may be different from the time the completion callback is delivered.
	CloseTime time.Time
	// Links are used to link back to the operation when a completion callback is received before a started response.
	Links []nexus.Link
	// Parsed from request and set if State is failed or canceled.
	Error *nexus.OperationError
	// Extracted from request and set if State is succeeded.
	Result *nexus.LazyValue
}

// A CompletionHandler can receive operation completion requests as delivered via the callback URL provided in
// start-operation requests.
type CompletionHandler interface {
	CompleteOperation(context.Context, *CompletionRequest) error
}

// CompletionHandlerOptions are options for [NewCompletionHTTPHandler].
type CompletionHandlerOptions struct {
	// Handler for completion requests.
	Handler CompletionHandler
	// A stuctured logging handler.
	// Defaults to slog.Default().
	Logger *slog.Logger
	// A [Serializer] to customize handler serialization behavior.
	// By default the handler handles, JSONables, byte slices, and nil.
	Serializer nexus.Serializer
	// A [FailureConverter] to convert a [Failure] instance to and from an [error]. Defaults to
	// [DefaultFailureConverter].
	FailureConverter FailureConverter
}

type completionHTTPHandler struct {
	BaseHTTPHandler
	options CompletionHandlerOptions
}

func (h *completionHTTPHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	ctx := request.Context()
	completion := CompletionRequest{
		State:          nexus.OperationState(request.Header.Get(headerOperationState)),
		OperationToken: request.Header.Get(nexus.HeaderOperationToken),
		HTTPRequest:    request,
	}
	if startTimeHeader := request.Header.Get(headerOperationStartTime); startTimeHeader != "" {
		var parseTimeErr error
		if completion.StartTime, parseTimeErr = http.ParseTime(startTimeHeader); parseTimeErr != nil {
			h.WriteFailure(writer, request, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "failed to parse operation start time header"))
			return
		}
	}
	if closeTimeHeader := request.Header.Get(headerOperationCloseTime); closeTimeHeader != "" {
		var parseTimeErr error
		if completion.CloseTime, parseTimeErr = unmarshalTimestamp(closeTimeHeader); parseTimeErr != nil {
			h.WriteFailure(writer, request, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "failed to parse operation close time header"))
			return
		}
	}
	var decodeErr error
	if completion.Links, decodeErr = getLinksFromHeader(request.Header); decodeErr != nil {
		h.WriteFailure(writer, request, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "failed to decode links from request headers"))
		return
	}
	switch completion.State {
	case nexus.OperationStateFailed, nexus.OperationStateCanceled:
		if !isMediaTypeJSON(request.Header.Get("Content-Type")) {
			h.WriteFailure(writer, request, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid request content type: %q", request.Header.Get("Content-Type")))
			return
		}
		var failure nexus.Failure
		b, err := io.ReadAll(request.Body)
		if err != nil {
			h.WriteFailure(writer, request, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "failed to read Failure from request body"))
			return
		}
		if err := json.Unmarshal(b, &failure); err != nil {
			h.WriteFailure(writer, request, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "failed to decode Failure from request body"))
			return
		}
		completionErr, err := h.FailureConverter.FailureToError(failure)
		if err != nil {
			h.WriteFailure(writer, request, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "failed to decode Failure from request body"))
			return
		}
		opErr, ok := completionErr.(*nexus.OperationError)
		if !ok {
			// Backwards compatibility: wrap non-OperationError errors in an OperationError with the appropriate state.
			completion.Error = &nexus.OperationError{
				Message: "nexus operation completed unsuccessfully",
				State:   completion.State,
				Cause:   completionErr,
			}
			if err := MarkAsWrapperError(h.FailureConverter, completion.Error); err != nil {
				h.WriteFailure(writer, request, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "failed to decode Failure from request body"))
				return
			}
		} else {
			completion.Error = opErr
		}
	case nexus.OperationStateSucceeded:
		completion.Result = nexus.NewLazyValue(
			h.options.Serializer,
			&nexus.Reader{
				ReadCloser: request.Body,
				Header:     prefixStrippedHTTPHeaderToNexusHeader(request.Header, "content-"),
			},
		)
	default:
		h.WriteFailure(writer, request, nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid request operation state: %q", completion.State))
		return
	}
	if err := h.options.Handler.CompleteOperation(ctx, &completion); err != nil {
		h.WriteFailure(writer, request, err)
	}
}

// NewCompletionHTTPHandler constructs an [http.Handler] from given options for handling operation completion requests.
func NewCompletionHTTPHandler(options CompletionHandlerOptions) http.Handler {
	if options.Logger == nil {
		options.Logger = slog.Default()
	}
	if options.Serializer == nil {
		options.Serializer = nexus.DefaultSerializer()
	}
	if options.FailureConverter == nil {
		options.FailureConverter = DefaultFailureConverter()
	}
	return &completionHTTPHandler{
		options: options,
		BaseHTTPHandler: BaseHTTPHandler{
			Logger:           options.Logger,
			FailureConverter: options.FailureConverter,
		},
	}
}
