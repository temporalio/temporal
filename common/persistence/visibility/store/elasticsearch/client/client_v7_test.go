package client

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/olivere/elastic/v7"
	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/log"
)

func newTestClient(t *testing.T, handler http.HandlerFunc) *clientImpl {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	serverURL, err := url.Parse(server.URL)
	require.NoError(t, err)
	c, err := newClient(&Config{URL: *serverURL, DisableGzip: true}, server.Client(), log.NewNoopLogger())
	require.NoError(t, err)
	return c
}

func TestCount(t *testing.T) {
	tests := []struct {
		name           string
		response       string
		expectedCount  int64
		expectedStatus int // 0 means no error expected
	}{
		{
			name:          "success",
			response:      `{"count": 42, "_shards": {"total": 2, "successful": 2, "skipped": 0, "failed": 0}}`,
			expectedCount: 42,
		},
		{
			name:          "no shards info",
			response:      `{"count": 42}`,
			expectedCount: 42,
		},
		{
			name: "all shard failures are rejections (Elasticsearch)",
			response: `{"count": 10, "_shards": {"total": 2, "successful": 1, "skipped": 0, "failed": 1, "failures": [
				{"shard": 0, "index": "test-index", "node": "node-1", "reason": {"type": "es_rejected_execution_exception", "reason": "rejected"}}
			]}}`,
			expectedStatus: http.StatusTooManyRequests,
		},
		{
			name: "all shard failures are rejections (OpenSearch)",
			response: `{"count": 21, "_shards": {"total": 8, "successful": 5, "skipped": 0, "failed": 3, "failures": [
				{"shard": 3, "index": "test-index", "node": "node-1", "reason": {"type": "rejected_execution_exception", "reason": "rejected"}},
				{"shard": 6, "index": "test-index", "node": "node-2", "reason": {"type": "rejected_execution_exception", "reason": "rejected"}},
				{"shard": 7, "index": "test-index", "node": "node-1", "reason": {"type": "rejected_execution_exception", "reason": "rejected"}}
			]}}`,
			expectedStatus: http.StatusTooManyRequests,
		},
		{
			name: "mixed shard failures",
			response: `{"count": 10, "_shards": {"total": 3, "successful": 1, "skipped": 0, "failed": 2, "failures": [
				{"shard": 0, "index": "test-index", "node": "node-1", "reason": {"type": "rejected_execution_exception", "reason": "rejected"}},
				{"shard": 1, "index": "test-index", "node": "node-2", "reason": {"type": "node_disconnected_exception", "reason": "disconnected"}}
			]}}`,
			expectedStatus: http.StatusServiceUnavailable,
		},
		{
			name:           "shard failures without details",
			response:       `{"count": 10, "_shards": {"total": 2, "successful": 1, "skipped": 0, "failed": 1}}`,
			expectedStatus: http.StatusServiceUnavailable,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var reqMethod, reqPath string
			var reqBody []byte
			c := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
				reqMethod = r.Method
				reqPath = r.URL.Path
				reqBody, _ = io.ReadAll(r.Body)

				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(tc.response))
			})

			count, err := c.Count(context.Background(), "test-index", elastic.NewMatchAllQuery())

			require.Equal(t, http.MethodPost, reqMethod)
			require.Equal(t, "/test-index/_count", reqPath)
			var body map[string]any
			require.NoError(t, json.Unmarshal(reqBody, &body))
			require.Contains(t, body, "query")

			if tc.expectedStatus == 0 {
				require.NoError(t, err)
				require.Equal(t, tc.expectedCount, count)
				return
			}

			require.Zero(t, count)
			var esErr *elastic.Error
			require.ErrorAs(t, err, &esErr)
			require.Equal(t, tc.expectedStatus, esErr.Status)
			require.NotNil(t, esErr.Details)
			require.Equal(t, "shard_failures", esErr.Details.Type)
		})
	}
}
