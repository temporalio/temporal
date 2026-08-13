package cluster

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/mock/gomock"
)

func TestFrontendHTTPClientCacheInjectsTraceContext(t *testing.T) {
	var traceparent string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		traceparent = r.Header.Get("traceparent")
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)

	ctrl := gomock.NewController(t)
	metadata := NewMockMetadata(ctrl)
	metadata.EXPECT().RegisterMetadataChangeCallback(gomock.Any(), gomock.Any())
	metadata.EXPECT().GetAllClusterInfo().Return(map[string]ClusterInformation{
		"remote": {HTTPAddress: server.Listener.Addr().String()},
	})
	tp := trace.NewTracerProvider()
	cache := NewFrontendHTTPClientCacheWithTracing(metadata, nil, tp, nil)
	client, err := cache.Get("remote")
	require.NoError(t, err)

	ctx, span := tp.Tracer("test").Start(context.Background(), "parent")
	defer span.End()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, client.BaseURL(), nil)
	require.NoError(t, err)
	resp, err := client.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.NotEmpty(t, traceparent)
}
