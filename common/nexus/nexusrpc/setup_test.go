package nexusrpc_test

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/nexus-rpc/sdk-go/nexus"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/nexus/nexusrpc"
)

const testTimeout = time.Second * 5
const testService = "Ser/vic e"
const getResultMaxTimeout = time.Millisecond * 300

func setupCustom(t *testing.T, handler nexus.Handler, serializer nexus.Serializer, failureConverter nexus.FailureConverter) (ctx context.Context, client *nexusrpc.HTTPClient, teardown func()) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)

	httpHandler := nexusrpc.NewHTTPHandler(nexusrpc.HandlerOptions{
		GetResultTimeout: getResultMaxTimeout,
		Handler:          handler,
		Serializer:       serializer,
		FailureConverter: failureConverter,
	})

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	client, err = nexusrpc.NewHTTPClient(nexusrpc.HTTPClientOptions{
		BaseURL:          fmt.Sprintf("http://%s/", listener.Addr().String()),
		Service:          testService,
		Serializer:       serializer,
		FailureConverter: failureConverter,
	})
	require.NoError(t, err)

	go func() {
		// Ignore for test purposes
		_ = http.Serve(listener, httpHandler)
	}()

	return ctx, client, func() {
		cancel()
		// nolint // ignore error on close in test
		listener.Close()
	}
}

func setup(t *testing.T, handler nexus.Handler) (ctx context.Context, client *nexusrpc.HTTPClient, teardown func()) {
	return setupCustom(t, handler, nil, nil)
}

func setupForCompletion(t *testing.T, handler nexusrpc.CompletionHandler, serializer nexus.Serializer, failureConverter nexus.FailureConverter) (ctx context.Context, callbackURL string, teardown func()) {
	ctx, cancel := context.WithTimeout(context.Background(), testTimeout)

	httpHandler := nexusrpc.NewCompletionHTTPHandler(nexusrpc.CompletionHandlerOptions{
		Handler:          handler,
		Serializer:       serializer,
		FailureConverter: failureConverter,
	})

	listener, err := net.Listen("tcp", "localhost:0")
	require.NoError(t, err)
	callbackURL = fmt.Sprintf("http://%s/callback?a=b", listener.Addr().String())

	go func() {
		// Ignore for test purposes
		_ = http.Serve(listener, httpHandler)
	}()

	return ctx, callbackURL, func() {
		cancel()
		// nolint // ignore error on close in test
		listener.Close()
	}
}
