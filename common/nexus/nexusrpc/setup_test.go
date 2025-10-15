package nexusrpc_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
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

// There's zero chance of concurrent updates in the test where this is used. Don't bother locking.
type customSerializer struct {
	encoded int
	decoded int
}

func (c *customSerializer) Serialize(v any) (*nexus.Content, error) {
	vint := v.(int)
	c.encoded++
	return &nexus.Content{
		Header: map[string]string{
			"custom": strconv.Itoa(vint),
		},
	}, nil
}

func (c *customSerializer) Deserialize(s *nexus.Content, v any) error {
	vintPtr := v.(*int)
	decoded, err := strconv.Atoi(s.Header["custom"])
	if err != nil {
		return err
	}
	*vintPtr = decoded
	c.decoded++
	return nil
}

type customFailureConverter struct{}

var errCustom = errors.New("custom")

// ErrorToFailure implements FailureConverter.
func (c customFailureConverter) ErrorToFailure(err error) nexus.Failure {
	return nexus.Failure{
		Message: err.Error(),
		Metadata: map[string]string{
			"type": "custom",
		},
	}
}

// FailureToError implements FailureConverter.
func (c customFailureConverter) FailureToError(f nexus.Failure) error {
	if f.Metadata["type"] != "custom" {
		return errors.New(f.Message)
	}
	return fmt.Errorf("%w: %s", errCustom, f.Message)
}
