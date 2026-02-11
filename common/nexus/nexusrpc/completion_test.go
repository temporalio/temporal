package nexusrpc_test

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

type successfulCompletionHandler struct {
	expectedStartTime time.Time
	expectedCloseTime time.Time
}

// validateExpectedTime returns false if the times are set but aren't equal.
func validateExpectedTime(expected, actual time.Time, resolution time.Duration) bool {
	if expected.IsZero() {
		return true
	}

	expected = expected.Truncate(resolution)
	actual = actual.Truncate(resolution)

	return expected.Equal(actual)
}

func (h *successfulCompletionHandler) CompleteOperation(ctx context.Context, completion *nexusrpc.CompletionRequest) error {
	if completion.HTTPRequest.URL.Path != "/callback" {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid URL path: %q", completion.HTTPRequest.URL.Path)
	}
	if completion.HTTPRequest.URL.Query().Get("a") != "b" {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid 'a' query param: %q", completion.HTTPRequest.URL.Query().Get("a"))
	}
	if completion.HTTPRequest.Header.Get("foo") != "bar" {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid 'foo' header: %q", completion.HTTPRequest.Header.Get("foo"))
	}
	if completion.HTTPRequest.Header.Get("User-Agent") != "temporalio/server" {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid 'User-Agent' header: %q", completion.HTTPRequest.Header.Get("User-Agent"))
	}
	if completion.OperationToken != "test-operation-token" {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid operation token: %q", completion.OperationToken)
	}
	if len(completion.Links) == 0 {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "expected Links to be set on CompletionRequest")
	}
	if !validateExpectedTime(h.expectedStartTime, completion.StartTime, time.Second) {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "expected StartTime to be equal")
	}
	if !validateExpectedTime(h.expectedCloseTime, completion.CloseTime, time.Millisecond) {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "expected CloseTime to be equal")
	}
	var result int
	err := completion.Result.Consume(&result)
	if err != nil {
		return err
	}
	if result != 666 {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid result: %q", result)
	}
	return nil
}

func TestSuccessfulCompletion(t *testing.T) {
	startTime := time.Now().Add(-time.Hour).UTC()
	closeTime := time.Now().UTC()

	ctx, callbackURL, teardown := setupForCompletion(t, &successfulCompletionHandler{
		expectedStartTime: startTime,
		expectedCloseTime: closeTime,
	}, nil, nil)
	defer teardown()

	completion := &nexusrpc.OperationCompletionSuccessful{
		Result:         666,
		OperationToken: "test-operation-token",
		StartTime:      startTime,
		CloseTime:      closeTime,
		Links: []nexus.Link{{
			URL: &url.URL{
				Scheme:   "https",
				Host:     "example.com",
				Path:     "/path/to/something",
				RawQuery: "param=value",
			},
			Type: "url",
		}},
		Header: nexus.Header{"foo": "bar"},
	}

	err := nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{}).CompleteOperation(ctx, callbackURL, completion)
	require.NoError(t, err)
}

func TestSuccessfulCompletion_CustomSerializer(t *testing.T) {
	serializer := &customSerializer{}
	ctx, callbackURL, teardown := setupForCompletion(t, &successfulCompletionHandler{}, serializer, nil)
	defer teardown()

	completion := &nexusrpc.OperationCompletionSuccessful{
		Result: 666,
		Links: []nexus.Link{{
			URL: &url.URL{
				Scheme:   "https",
				Host:     "example.com",
				Path:     "/path/to/something",
				RawQuery: "param=value",
			},
			Type: "url",
		}},
		Header: nexus.Header{"foo": "bar"},
	}

	completion.Header.Set("foo", "bar")
	completion.Header.Set(nexus.HeaderOperationToken, "test-operation-token")

	err := nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{
		Serializer: serializer,
	}).CompleteOperation(ctx, callbackURL, completion)
	require.NoError(t, err)

	require.Equal(t, 1, serializer.decoded)
	require.Equal(t, 1, serializer.encoded)
}

type failureExpectingCompletionHandler struct {
	errorChecker      func(error) error
	expectedStartTime time.Time
	expectedCloseTime time.Time
}

func (h *failureExpectingCompletionHandler) CompleteOperation(ctx context.Context, completion *nexusrpc.CompletionRequest) error {
	if completion.State != nexus.OperationStateCanceled {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "unexpected completion state: %q", completion.State)
	}
	if err := h.errorChecker(completion.Error); err != nil {
		return err
	}
	if completion.HTTPRequest.Header.Get("foo") != "bar" {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid 'foo' header: %q", completion.HTTPRequest.Header.Get("foo"))
	}
	if completion.OperationToken != "test-operation-token" {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid operation token: %q", completion.OperationToken)
	}
	if len(completion.Links) == 0 {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "expected Links to be set on CompletionRequest")
	}
	if !validateExpectedTime(h.expectedStartTime, completion.StartTime, time.Second) {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "expected StartTime to be equal")
	}
	if !validateExpectedTime(h.expectedCloseTime, completion.CloseTime, time.Millisecond) {
		return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "expected CloseTime to be equal")
	}

	return nil
}

func TestFailureCompletion(t *testing.T) {
	startTime := time.Now().Add(-time.Hour).UTC()
	closeTime := time.Now().UTC()

	ctx, callbackURL, teardown := setupForCompletion(t, &failureExpectingCompletionHandler{
		errorChecker: func(err error) error {
			if opErr, ok := err.(*nexus.OperationError); ok && opErr.Message == "expected message" {
				return nil
			}
			return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid failure: %v", err)
		},
		expectedStartTime: startTime,
		expectedCloseTime: closeTime,
	}, nil, nil)
	defer teardown()

	completion := &nexusrpc.OperationCompletionUnsuccessful{
		Error:          nexus.NewOperationCanceledErrorf("expected message"),
		OperationToken: "test-operation-token",
		StartTime:      startTime,
		CloseTime:      closeTime,
		Links: []nexus.Link{{
			URL: &url.URL{
				Scheme:   "https",
				Host:     "example.com",
				Path:     "/path/to/something",
				RawQuery: "param=value",
			},
			Type: "url",
		}},
		Header: nexus.Header{"foo": "bar"},
	}
	err := nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{}).CompleteOperation(ctx, callbackURL, completion)
	require.NoError(t, err)
}

func TestFailureCompletion_CustomFailureConverter(t *testing.T) {
	fc := customFailureConverter{}
	startTime := time.Now().Add(-time.Hour).UTC()
	closeTime := time.Now().UTC()

	ctx, callbackURL, teardown := setupForCompletion(t, &failureExpectingCompletionHandler{
		errorChecker: func(err error) error {
			if !errors.Is(err, errCustom) {
				return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "invalid failure, expected a custom error: %v", err)
			}
			return nil
		},
		expectedStartTime: startTime,
		expectedCloseTime: closeTime,
	}, nil, fc)
	defer teardown()

	completion := &nexusrpc.OperationCompletionUnsuccessful{
		Error:          nexus.NewOperationCanceledErrorf("expected message"),
		OperationToken: "test-operation-token",
		StartTime:      startTime,
		CloseTime:      closeTime,
		Links: []nexus.Link{{
			URL: &url.URL{
				Scheme:   "https",
				Host:     "example.com",
				Path:     "/path/to/something",
				RawQuery: "param=value",
			},
			Type: "url",
		}},
		Header: nexus.Header{"foo": "bar"},
	}
	err := nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{
		FailureConverter: fc,
	}).CompleteOperation(ctx, callbackURL, completion)
	require.NoError(t, err)
}

type failingCompletionHandler struct {
}

func (h *failingCompletionHandler) CompleteOperation(ctx context.Context, completion *nexusrpc.CompletionRequest) error {
	return nexus.NewHandlerErrorf(nexus.HandlerErrorTypeBadRequest, "I can't get no satisfaction")
}

func TestBadRequestCompletion(t *testing.T) {
	ctx, callbackURL, teardown := setupForCompletion(t, &failingCompletionHandler{}, nil, nil)
	defer teardown()

	completion := &nexusrpc.OperationCompletionSuccessful{
		Result: []byte("success"),
	}
	err := nexusrpc.NewCompletionHTTPClient(nexusrpc.CompletionHTTPClientOptions{}).CompleteOperation(ctx, callbackURL, completion)
	var handlerErr *nexus.HandlerError
	require.ErrorAs(t, err, &handlerErr)
	require.Equal(t, nexus.HandlerErrorTypeBadRequest, handlerErr.Type)
}
