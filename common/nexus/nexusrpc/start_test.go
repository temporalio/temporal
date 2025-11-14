package nexusrpc_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/url"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

type successHandler struct {
	nexus.UnimplementedHandler
}

func (h *successHandler) StartOperation(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
	var body []byte
	if err := input.Consume(&body); err != nil {
		return nil, err
	}
	if service != testService {
		return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "unexpected service: %s", service)
	}
	if operation != "i need to/be escaped" {
		return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "unexpected operation: %s", operation)
	}
	if options.CallbackURL != "http://test/callback" {
		return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "unexpected callback URL: %s", options.CallbackURL)
	}
	if options.CallbackHeader.Get("callback-test") != "ok" {
		return nil, nexus.HandlerErrorf(
			nexus.HandlerErrorTypeBadRequest,
			"invalid 'callback-test' callback header: %q",
			options.CallbackHeader.Get("callback-test"),
		)
	}
	if options.Header.Get("test") != "ok" {
		return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid 'test' header: %q", options.Header.Get("test"))
	}
	if options.Header.Get("nexus-callback-callback-test") != "" {
		return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "callback header not omitted from options Header")
	}
	if options.Header.Get("User-Agent") != "temporalio/server" {
		return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid 'User-Agent' header: %q", options.Header.Get("User-Agent"))
	}

	return &nexus.HandlerStartOperationResultSync[any]{Value: body}, nil
}

func TestSuccess(t *testing.T) {
	ctx, client, teardown := setup(t, &successHandler{})
	defer teardown()

	requestBody := []byte{0x00, 0x01}

	response, err := client.StartOperation(ctx, "i need to/be escaped", requestBody, nexus.StartOperationOptions{
		CallbackURL:    "http://test/callback",
		CallbackHeader: nexus.Header{"callback-test": "ok"},
		Header:         nexus.Header{"test": "ok"},
	})
	require.NoError(t, err)
	require.NotNil(t, response.Successful)
	var responseBody []byte
	err = response.Successful.Consume(&responseBody)
	require.NoError(t, err)
	require.Equal(t, requestBody, responseBody)
}

type errorHandler struct {
	nexus.UnimplementedHandler
}

func (h *errorHandler) StartOperation(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
	return nil, &nexus.HandlerError{
		Type:          nexus.HandlerErrorTypeInternal,
		Cause:         errors.New("Some error"),
		RetryBehavior: nexus.HandlerErrorRetryBehaviorNonRetryable,
	}
}

func TestHandlerErrorRetryBehavior(t *testing.T) {
	ctx, client, teardown := setup(t, &errorHandler{})
	defer teardown()

	_, err := client.StartOperation(ctx, "op", nil, nexus.StartOperationOptions{})
	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorRetryBehaviorNonRetryable, handlerErr.RetryBehavior)
}

type requestIDEchoHandler struct {
	nexus.UnimplementedHandler
}

func (h *requestIDEchoHandler) StartOperation(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
	return &nexus.HandlerStartOperationResultSync[any]{
		Value: []byte(options.RequestID),
	}, nil
}

func TestClientRequestID(t *testing.T) {
	ctx, client, teardown := setup(t, &requestIDEchoHandler{})
	defer teardown()

	type testcase struct {
		name      string
		request   nexus.StartOperationOptions
		validator func(*testing.T, []byte)
	}

	cases := []testcase{
		{
			name:    "unspecified",
			request: nexus.StartOperationOptions{},
			validator: func(t *testing.T, body []byte) {
				_, err := uuid.ParseBytes(body)
				require.NoError(t, err)
			},
		},
		{
			name:    "provided directly",
			request: nexus.StartOperationOptions{RequestID: "direct"},
			validator: func(t *testing.T, body []byte) {
				require.Equal(t, []byte("direct"), body)
			},
		},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			result, err := client.StartOperation(ctx, "foo", nil, c.request)
			require.NoError(t, err)
			response := result.Successful
			require.NotNil(t, response)
			var responseBody []byte
			err = response.Consume(&responseBody)
			require.NoError(t, err)
			c.validator(t, responseBody)
		})
	}
}

type jsonHandler struct {
	nexus.UnimplementedHandler
}

func (h *jsonHandler) StartOperation(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
	var s string
	if err := input.Consume(&s); err != nil {
		return nil, err
	}
	return &nexus.HandlerStartOperationResultSync[any]{Value: s}, nil
}

func TestJSON(t *testing.T) {
	ctx, client, teardown := setup(t, &jsonHandler{})
	defer teardown()

	result, err := client.StartOperation(ctx, "foo", "success", nexus.StartOperationOptions{})
	require.NoError(t, err)
	response := result.Successful
	require.NotNil(t, response)
	var operationResult string
	err = response.Consume(&operationResult)
	require.NoError(t, err)
	require.Equal(t, "success", operationResult)
}

type echoHandler struct {
	nexus.UnimplementedHandler
}

func (h *echoHandler) StartOperation(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
	var output any
	switch options.Header.Get("input-type") {
	case "reader":
		output = input.Reader
	case "content":
		data, err := io.ReadAll(input.Reader)
		if err != nil {
			return nil, err
		}
		output = &nexus.Content{
			Header: input.Reader.Header,
			Data:   data,
		}
	default:
		return nil, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "unknown input-type header")
	}
	nexus.AddHandlerLinks(ctx, options.Links...)
	return &nexus.HandlerStartOperationResultSync[any]{Value: output}, nil
}

func TestReaderIO(t *testing.T) {
	ctx, client, teardown := setup(t, &echoHandler{})
	defer teardown()

	content, err := nexus.DefaultSerializer().Serialize("success")
	require.NoError(t, err)
	reader := &nexus.Reader{
		ReadCloser: io.NopCloser(bytes.NewReader(content.Data)),
		Header:     content.Header,
	}
	testCases := []struct {
		name   string
		input  any
		header nexus.Header
		links  []nexus.Link
	}{
		{
			name:   "content",
			input:  content,
			header: nexus.Header{"input-type": "content"},
		},
		{
			name:   "reader",
			input:  reader,
			header: nexus.Header{"input-type": "reader"},
			links: []nexus.Link{{
				URL: &url.URL{
					Scheme:   "https",
					Host:     "example.com",
					Path:     "/path/to/something",
					RawQuery: "param=value",
				},
				Type: "url",
			}},
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			result, err := client.StartOperation(ctx, "foo", tc.input, nexus.StartOperationOptions{Header: tc.header, Links: tc.links})
			require.NoError(t, err)
			require.Equal(t, tc.links, result.Links)
			response := result.Successful
			require.NotNil(t, response)
			var operationResult string
			err = response.Consume(&operationResult)
			require.NoError(t, err)
			require.Equal(t, "success", operationResult)
		})
	}
}

type asyncHandler struct {
	nexus.UnimplementedHandler
}

func (h *asyncHandler) StartOperation(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
	return &nexus.HandlerStartOperationResultAsync{
		OperationToken: "async",
	}, nil
}

func TestAsync(t *testing.T) {
	ctx, client, teardown := setup(t, &asyncHandler{})
	defer teardown()

	result, err := client.StartOperation(ctx, "foo", nil, nexus.StartOperationOptions{})
	require.NoError(t, err)
	require.NotNil(t, result.Pending)
}

type unsuccessfulHandler struct {
	nexus.UnimplementedHandler
}

func (h *unsuccessfulHandler) StartOperation(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
	return nil, &nexus.OperationError{
		// We're passing the desired state via request ID in this test.
		State: nexus.OperationState(options.RequestID),
		Cause: errors.New("intentional"),
	}
}

func TestUnsuccessful(t *testing.T) {
	ctx, client, teardown := setup(t, &unsuccessfulHandler{})
	defer teardown()

	cases := []string{"canceled", "failed"}
	for _, c := range cases {
		_, err := client.StartOperation(ctx, "foo", nil, nexus.StartOperationOptions{RequestID: c})
		var unsuccessfulError *nexus.OperationError
		require.ErrorAs(t, err, &unsuccessfulError)
		require.Equal(t, nexus.OperationState(c), unsuccessfulError.State)
	}
}

type timeoutEchoHandler struct {
	nexus.UnimplementedHandler
}

func (h *timeoutEchoHandler) StartOperation(ctx context.Context, service, operation string, input *nexus.LazyValue, options nexus.StartOperationOptions) (nexus.HandlerStartOperationResult[any], error) {
	deadline, set := ctx.Deadline()
	if !set {
		return &nexus.HandlerStartOperationResultSync[any]{
			Value: []byte("not set"),
		}, nil
	}
	return &nexus.HandlerStartOperationResultSync[any]{
		Value: []byte(nexusrpc.FormatDuration(time.Until(deadline))),
	}, nil
}

func TestStart_ContextDeadlinePropagated(t *testing.T) {
	ctx, client, teardown := setup(t, &timeoutEchoHandler{})
	defer teardown()

	deadline, _ := ctx.Deadline()
	initialTimeout := time.Until(deadline)
	result, err := client.StartOperation(ctx, "foo", nil, nexus.StartOperationOptions{})

	require.NoError(t, err)
	requireTimeoutPropagated(t, result, initialTimeout)
}

func TestStart_RequestTimeoutHeaderOverridesContextDeadline(t *testing.T) {
	// relies on ctx returned here having default testTimeout set greater than expected timeout
	ctx, client, teardown := setup(t, &timeoutEchoHandler{})
	defer teardown()

	timeout := 100 * time.Millisecond
	result, err := client.StartOperation(ctx, "foo", nil, nexus.StartOperationOptions{Header: nexus.Header{nexus.HeaderRequestTimeout: nexusrpc.FormatDuration(timeout)}})

	require.NoError(t, err)
	requireTimeoutPropagated(t, result, timeout)
}

func requireTimeoutPropagated(t *testing.T, result *nexusrpc.ClientStartOperationResponse[*nexus.LazyValue], expected time.Duration) {
	response := result.Successful
	require.NotNil(t, response)
	var responseBody []byte
	err := response.Consume(&responseBody)
	require.NoError(t, err)
	parsedTimeout, err := nexusrpc.ParseDuration(string(responseBody))
	require.NoError(t, err)
	require.NotZero(t, parsedTimeout)
	require.LessOrEqual(t, parsedTimeout, expected)
}

func TestStart_TimeoutNotPropagated(t *testing.T) {
	_, client, teardown := setup(t, &timeoutEchoHandler{})
	defer teardown()

	result, err := client.StartOperation(context.Background(), "foo", nil, nexus.StartOperationOptions{})

	require.NoError(t, err)
	response := result.Successful
	require.NotNil(t, response)
	var responseBody []byte
	err = response.Consume(&responseBody)
	require.NoError(t, err)
	require.Equal(t, []byte("not set"), responseBody)
}

func TestStart_NilContentHeaderDoesNotPanic(t *testing.T) {
	_, client, teardown := setup(t, &requestIDEchoHandler{})
	defer teardown()

	result, err := client.StartOperation(context.Background(), "op", &nexus.Content{Data: []byte("abc")}, nexus.StartOperationOptions{})

	require.NoError(t, err)
	response := result.Successful
	require.NotNil(t, response)
	var responseBody []byte
	err = response.Consume(&responseBody)
	require.NoError(t, err)
}

var numberValidatorOperation = nexus.NewSyncOperation("number-validator", func(ctx context.Context, input int, _ nexus.StartOperationOptions) (int, error) {
	if input != 3 {
		return input, nexus.HandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid number: %d", input)
	}
	return input, nil
})

var service = nexus.NewService(testService)
var registry = nexus.NewServiceRegistry()

func init() {
	service.MustRegister(numberValidatorOperation)
	registry.MustRegister(service)
}

func TestCustomSerializer(t *testing.T) {
	handler, err := registry.NewHandler()
	require.NoError(t, err)

	c := &customSerializer{}
	ctx, client, teardown := setupCustom(t, handler, c, nil)
	defer teardown()

	result, err := nexusrpc.StartOperation(ctx, client, numberValidatorOperation, 3, nexus.StartOperationOptions{})
	require.NoError(t, err)
	require.Equal(t, 3, result.Successful)
	require.Equal(t, 2, c.decoded)
	require.Equal(t, 2, c.encoded)
}

func TestCustomFailureConverter(t *testing.T) {
	handler, err := registry.NewHandler()
	require.NoError(t, err)

	c := customFailureConverter{}
	ctx, client, teardown := setupCustom(t, handler, nil, c)
	defer teardown()

	_, err = nexusrpc.StartOperation(ctx, client, numberValidatorOperation, 0, nexus.StartOperationOptions{})
	require.ErrorIs(t, err, errCustom)
}
