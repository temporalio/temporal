package nexusrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strings"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
)

// HTTPClientOptions are options for creating an [HTTPClient].
type HTTPClientOptions struct {
	// Base URL for all requests. Required.
	BaseURL string
	// Service name. Required.
	Service string
	// A function for making HTTP requests.
	// Defaults to [http.DefaultClient.Do].
	HTTPCaller func(*http.Request) (*http.Response, error)
	// A [Serializer] to customize client serialization behavior.
	// By default the client handles JSONables, byte slices, and nil.
	Serializer nexus.Serializer
	// A [FailureConverter] to convert a [Failure] instance to and from an [error]. Defaults to
	// [DefaultFailureConverter].
	FailureConverter nexus.FailureConverter
}

// User-Agent header set on HTTP requests.
const userAgent = "temporalio/server"

const headerUserAgent = "User-Agent"

var errEmptyOperationName = errors.New("empty operation name")

var errEmptyOperationToken = errors.New("empty operation token")

// UnexpectedResponseError indicates a client encountered something unexpected in the server's response.
type UnexpectedResponseError struct {
	// Error message.
	Message string
	// Optional failure that may have been emedded in the response.
	Failure *nexus.Failure
	// Additional transport specific details.
	// For HTTP, this would include the HTTP response. The response body will have already been read into memory and
	// does not need to be closed.
	Details any
}

// Error implements the error interface.
func (e *UnexpectedResponseError) Error() string {
	return e.Message
}

func newUnexpectedResponseError(message string, response *http.Response, body []byte) error {
	var failure *nexus.Failure
	if isMediaTypeJSON(response.Header.Get("Content-Type")) {
		if err := json.Unmarshal(body, &failure); err == nil && failure.Message != "" {
			message += ": " + failure.Message
		}
	}

	return &UnexpectedResponseError{
		Message: message,
		Details: response,
		Failure: failure,
	}
}

// An HTTPClient makes Nexus service requests as defined in the [Nexus HTTP API].
//
// It can start a new operation and get an [OperationHandle] to an existing, asynchronous operation.
//
// Use an [OperationHandle] to cancel, get the result of, and get information about asynchronous operations.
//
// OperationHandles can be obtained either by starting new operations or by calling [HTTPClient.NewOperationHandle] for
// existing operations.
//
// [Nexus HTTP API]: https://github.com/nexus-rpc/api
type HTTPClient struct {
	// The options this client was created with after applying defaults.
	options        HTTPClientOptions
	serviceBaseURL *url.URL
}

// NewHTTPClient creates a new [HTTPClient] from provided [HTTPClientOptions].
// BaseURL and Service are required.
func NewHTTPClient(options HTTPClientOptions) (*HTTPClient, error) {
	if options.HTTPCaller == nil {
		options.HTTPCaller = http.DefaultClient.Do
	}
	if options.BaseURL == "" {
		return nil, errors.New("empty BaseURL")
	}
	if options.Service == "" {
		return nil, errors.New("empty Service")
	}
	var baseURL *url.URL
	var err error
	baseURL, err = url.Parse(options.BaseURL)
	if err != nil {
		return nil, err
	}
	if baseURL.Scheme != "http" && baseURL.Scheme != "https" {
		return nil, fmt.Errorf("invalid URL scheme: %s", baseURL.Scheme)
	}
	if options.Serializer == nil {
		options.Serializer = nexus.DefaultSerializer()
	}
	if options.FailureConverter == nil {
		options.FailureConverter = nexus.DefaultFailureConverter()
	}

	return &HTTPClient{
		options:        options,
		serviceBaseURL: baseURL,
	}, nil
}

// ClientStartOperationResponse is the return type of [HTTPClient.StartOperation].
// One and only one of Successful or Pending will be non-nil.
type ClientStartOperationResponse[T any] struct {
	// Set when start completes synchronously and successfully.
	//
	// If T is a [LazyValue], ensure that your consume it or read the underlying content in its entirety and close it to
	// free up the underlying connection.
	Successful T
	// Set when the handler indicates that it started an asynchronous operation.
	// The attached handle can be used to perform actions such as cancel the operation or get its result.
	Pending *OperationHandle[T]
	// Links contain information about the operations done by the handler.
	Links []nexus.Link
}

// StartOperation calls the configured Nexus endpoint to start an operation.
//
// This method has the following possible outcomes:
//
//  1. The operation completes successfully. The result of this call will be set as a [LazyValue] in
//     ClientStartOperationResult.Successful and must be consumed to free up the underlying connection.
//
//  2. The operation was started and the handler has indicated that it will complete asynchronously. An
//     [OperationHandle] will be returned as ClientStartOperationResult.Pending, which can be used to perform actions
//     such as getting its result.
//
//  3. The operation was unsuccessful. The returned result will be nil and error will be an
//     [OperationError].
//
//  4. Any other error.
//
// nolint:revive // (cyclomatic complexity) Containing the entire implementation inline is clearer here.
func (c *HTTPClient) StartOperation(
	ctx context.Context,
	operation string,
	input any,
	options nexus.StartOperationOptions,
) (*ClientStartOperationResponse[*nexus.LazyValue], error) {
	var reader *nexus.Reader
	var contentLength *int64
	if r, ok := input.(*nexus.Reader); ok {
		// Close the input reader in case we error before sending the HTTP request (which may double close but
		// that's fine since we ignore the error).
		// nolint:errcheck // double close is fine
		defer r.Close()
		reader = r
	} else {
		content, ok := input.(*nexus.Content)
		if !ok {
			var err error
			content, err = c.options.Serializer.Serialize(input)
			if err != nil {
				return nil, err
			}
		}
		header := maps.Clone(content.Header)
		if header == nil {
			header = make(nexus.Header, 1)
		}
		contentLength = new(int64)
		*contentLength = int64(len(content.Data))

		reader = &nexus.Reader{
			ReadCloser: io.NopCloser(bytes.NewReader(content.Data)),
			Header:     header,
		}
	}

	url := c.serviceBaseURL.JoinPath(url.PathEscape(c.options.Service), url.PathEscape(operation))

	if options.CallbackURL != "" {
		q := url.Query()
		q.Set(queryCallbackURL, options.CallbackURL)
		url.RawQuery = q.Encode()
	}
	request, err := http.NewRequestWithContext(ctx, "POST", url.String(), reader)
	if contentLength != nil {
		request.ContentLength = *contentLength
	}
	if err != nil {
		return nil, err
	}

	if options.RequestID == "" {
		options.RequestID = uuid.NewString()
	}
	request.Header.Set(headerRequestID, options.RequestID)
	request.Header.Set(headerUserAgent, userAgent)
	addContentHeaderToHTTPHeader(reader.Header, request.Header)
	addCallbackHeaderToHTTPHeader(options.CallbackHeader, request.Header)
	if err := addLinksToHTTPHeader(options.Links, request.Header); err != nil {
		return nil, fmt.Errorf("failed to serialize links into header: %w", err)
	}
	addContextTimeoutToHTTPHeader(ctx, request.Header)
	addNexusHeaderToHTTPHeader(options.Header, request.Header)

	response, err := c.options.HTTPCaller(request)
	if err != nil {
		return nil, err
	}

	links, err := getLinksFromHeader(response.Header)
	if err != nil {
		// Have to read body here to check if it is a Failure.
		body, err := readAndReplaceBody(response)
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf(
			"%w: %w",
			newUnexpectedResponseError(
				fmt.Sprintf("invalid links header: %q", response.Header.Values(headerLink)),
				response,
				body,
			),
			err,
		)
	}

	// Do not close response body here to allow successful result to read it.
	if response.StatusCode == http.StatusOK {
		return &ClientStartOperationResponse[*nexus.LazyValue]{
			Successful: nexus.NewLazyValue(
				c.options.Serializer,
				&nexus.Reader{
					ReadCloser: response.Body,
					Header:     prefixStrippedHTTPHeaderToNexusHeader(response.Header, "content-"),
				},
			),
			Links: links,
		}, nil
	}

	// Do this once here and make sure it doesn't leak.
	body, err := readAndReplaceBody(response)
	if err != nil {
		return nil, err
	}

	switch response.StatusCode {
	case http.StatusCreated:
		info, err := operationInfoFromResponse(response, body)
		if err != nil {
			return nil, err
		}
		if info.State != nexus.OperationStateRunning {
			return nil, newUnexpectedResponseError(fmt.Sprintf("invalid operation state in response info: %q", info.State), response, body)
		}
		handle, err := c.NewOperationHandle(operation, info.Token)
		if err != nil {
			return nil, newUnexpectedResponseError("empty operation token in response", response, body)
		}
		return &ClientStartOperationResponse[*nexus.LazyValue]{
			Pending: handle,
			Links:   links,
		}, nil
	case statusOperationFailed:
		state, err := getUnsuccessfulStateFromHeader(response, body)
		if err != nil {
			return nil, err
		}

		failure, err := c.failureFromResponse(response, body)
		if err != nil {
			return nil, err
		}

		failureErr := c.options.FailureConverter.FailureToError(failure)
		return nil, &nexus.OperationError{
			State: state,
			Cause: failureErr,
		}
	default:
		return nil, c.bestEffortHandlerErrorFromResponse(response, body)
	}
}

// NewOperationHandle gets a handle to an asynchronous operation by name and token.
// Does not incur a trip to the server.
// Fails if provided an empty operation or token.
func (c *HTTPClient) NewOperationHandle(operation string, token string) (*OperationHandle[*nexus.LazyValue], error) {
	var es []error
	if operation == "" {
		es = append(es, errEmptyOperationName)
	}
	if token == "" {
		es = append(es, errEmptyOperationToken)
	}
	if len(es) > 0 {
		return nil, errors.Join(es...)
	}
	return &OperationHandle[*nexus.LazyValue]{
		client:    c,
		Operation: operation,
		Token:     token,
	}, nil
}

// readAndReplaceBody reads the response body in its entirety and closes it, and then replaces the original response
// body with an in-memory buffer.
// The body is replaced even when there was an error reading the entire body.
func readAndReplaceBody(response *http.Response) ([]byte, error) {
	responseBody := response.Body
	body, err := io.ReadAll(responseBody)
	if err := responseBody.Close(); err != nil {
		return nil, err
	}
	response.Body = io.NopCloser(bytes.NewReader(body))
	return body, err
}

func operationInfoFromResponse(response *http.Response, body []byte) (*nexus.OperationInfo, error) {
	if !isMediaTypeJSON(response.Header.Get("Content-Type")) {
		return nil, newUnexpectedResponseError(fmt.Sprintf("invalid response content type: %q", response.Header.Get("Content-Type")), response, body)
	}
	var info nexus.OperationInfo
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, err
	}
	return &info, nil
}

func (c *HTTPClient) failureFromResponse(response *http.Response, body []byte) (nexus.Failure, error) {
	if !isMediaTypeJSON(response.Header.Get("Content-Type")) {
		return nexus.Failure{}, newUnexpectedResponseError(fmt.Sprintf("invalid response content type: %q", response.Header.Get("Content-Type")), response, body)
	}
	var failure nexus.Failure
	err := json.Unmarshal(body, &failure)
	return failure, err
}

func (c *HTTPClient) failureFromResponseOrDefault(response *http.Response, body []byte, defaultMessage string) nexus.Failure {
	failure, err := c.failureFromResponse(response, body)
	if err != nil {
		failure.Message = defaultMessage
	}
	return failure
}

func (c *HTTPClient) failureErrorFromResponseOrDefault(response *http.Response, body []byte, defaultMessage string) error {
	failure := c.failureFromResponseOrDefault(response, body, defaultMessage)
	failureErr := c.options.FailureConverter.FailureToError(failure)
	return failureErr
}

func (c *HTTPClient) bestEffortHandlerErrorFromResponse(response *http.Response, body []byte) error {
	switch response.StatusCode {
	case http.StatusBadRequest:
		return &nexus.HandlerError{
			Type:          nexus.HandlerErrorTypeBadRequest,
			Cause:         c.failureErrorFromResponseOrDefault(response, body, "bad request"),
			RetryBehavior: retryBehaviorFromHeader(response.Header),
		}
	case http.StatusUnauthorized:
		return &nexus.HandlerError{
			Type:          nexus.HandlerErrorTypeUnauthenticated,
			Cause:         c.failureErrorFromResponseOrDefault(response, body, "unauthenticated"),
			RetryBehavior: retryBehaviorFromHeader(response.Header),
		}
	case http.StatusRequestTimeout:
		return &nexus.HandlerError{
			Type:          nexus.HandlerErrorTypeRequestTimeout,
			Cause:         c.failureErrorFromResponseOrDefault(response, body, "request timeout"),
			RetryBehavior: retryBehaviorFromHeader(response.Header),
		}
	case http.StatusConflict:
		return &nexus.HandlerError{
			Type:          nexus.HandlerErrorTypeConflict,
			Cause:         c.failureErrorFromResponseOrDefault(response, body, "conflict"),
			RetryBehavior: retryBehaviorFromHeader(response.Header),
		}
	case http.StatusForbidden:
		return &nexus.HandlerError{
			Type:          nexus.HandlerErrorTypeUnauthorized,
			Cause:         c.failureErrorFromResponseOrDefault(response, body, "unauthorized"),
			RetryBehavior: retryBehaviorFromHeader(response.Header),
		}
	case http.StatusNotFound:
		return &nexus.HandlerError{
			Type:          nexus.HandlerErrorTypeNotFound,
			Cause:         c.failureErrorFromResponseOrDefault(response, body, "not found"),
			RetryBehavior: retryBehaviorFromHeader(response.Header),
		}
	case http.StatusTooManyRequests:
		return &nexus.HandlerError{
			Type:          nexus.HandlerErrorTypeResourceExhausted,
			Cause:         c.failureErrorFromResponseOrDefault(response, body, "resource exhausted"),
			RetryBehavior: retryBehaviorFromHeader(response.Header),
		}
	case http.StatusInternalServerError:
		return &nexus.HandlerError{
			Type:          nexus.HandlerErrorTypeInternal,
			Cause:         c.failureErrorFromResponseOrDefault(response, body, "internal error"),
			RetryBehavior: retryBehaviorFromHeader(response.Header),
		}
	case http.StatusNotImplemented:
		return &nexus.HandlerError{
			Type:          nexus.HandlerErrorTypeNotImplemented,
			Cause:         c.failureErrorFromResponseOrDefault(response, body, "not implemented"),
			RetryBehavior: retryBehaviorFromHeader(response.Header),
		}
	case http.StatusServiceUnavailable:
		return &nexus.HandlerError{
			Type:          nexus.HandlerErrorTypeUnavailable,
			Cause:         c.failureErrorFromResponseOrDefault(response, body, "unavailable"),
			RetryBehavior: retryBehaviorFromHeader(response.Header),
		}
	case nexus.StatusUpstreamTimeout:
		return &nexus.HandlerError{
			Type:          nexus.HandlerErrorTypeUpstreamTimeout,
			Cause:         c.failureErrorFromResponseOrDefault(response, body, "upstream timeout"),
			RetryBehavior: retryBehaviorFromHeader(response.Header),
		}
	default:
		return newUnexpectedResponseError(fmt.Sprintf("unexpected response status: %q", response.Status), response, body)
	}
}

func retryBehaviorFromHeader(header http.Header) nexus.HandlerErrorRetryBehavior {
	switch strings.ToLower(header.Get(headerRetryable)) {
	case "true":
		return nexus.HandlerErrorRetryBehaviorRetryable
	case "false":
		return nexus.HandlerErrorRetryBehaviorNonRetryable
	default:
		return nexus.HandlerErrorRetryBehaviorUnspecified
	}
}

func getUnsuccessfulStateFromHeader(response *http.Response, body []byte) (nexus.OperationState, error) {
	state := nexus.OperationState(response.Header.Get(headerOperationState))
	switch state {
	case nexus.OperationStateCanceled, nexus.OperationStateFailed:
		return state, nil
	default:
		return state, newUnexpectedResponseError(fmt.Sprintf("invalid operation state header: %q", state), response, body)
	}
}

// StartOperation is the type safe version of [HTTPClient.StartOperation].
// It accepts input of type I and returns a [ClientStartOperationResponse] of type O, removing the need to consume the
// [LazyValue] returned by the client method.
func StartOperation[I, O any](ctx context.Context, client *HTTPClient, operation nexus.OperationReference[I, O], input I, request nexus.StartOperationOptions) (*ClientStartOperationResponse[O], error) {
	result, err := client.StartOperation(ctx, operation.Name(), input, request)
	if err != nil {
		return nil, err
	}
	if result.Successful != nil {
		var o O
		if err := result.Successful.Consume(&o); err != nil {
			return nil, err
		}
		return &ClientStartOperationResponse[O]{
			Successful: o,
			Links:      result.Links,
		}, nil
	}
	handle := OperationHandle[O]{
		client:    client,
		Operation: operation.Name(),
		Token:     result.Pending.Token,
	}
	return &ClientStartOperationResponse[O]{
		Pending: &handle,
		Links:   result.Links,
	}, nil
}
