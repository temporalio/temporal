//go:build temporal_minimal && !temporal_with_elasticsearch

package visibility

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/esconfig"
)

func TestNewVisibilityStore_ElasticsearchExcluded(t *testing.T) {
	ds := config.DataStore{Elasticsearch: &esconfig.Config{}}
	visStore, err := newVisibilityStoreFromDataStoreConfig(ds, &ManagerParams{}, &ManagerConfig{})
	require.EqualError(t, err, "elasticsearch visibility is not compiled into this server; rebuild with the temporal_with_elasticsearch build tag")
	require.Nil(t, visStore)
}
