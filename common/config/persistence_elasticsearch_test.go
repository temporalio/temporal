//go:build !temporal_minimal || temporal_with_elasticsearch

package config

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
)

func TestDataStoreValidate_Elasticsearch(t *testing.T) {
	ds := &DataStore{Elasticsearch: &client.Config{Indices: map[string]string{client.VisibilityAppName: "temporal_visibility_v1"}}}
	require.NoError(t, ds.Validate())
}
