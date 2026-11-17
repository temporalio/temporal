//go:build temporal_minimal && !temporal_with_elasticsearch

package visibility

import (
	"errors"

	"go.temporal.io/server/common/persistence/visibility/store"
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/esconfig"
)

// newElasticsearchVisibilityStore fails, since Elasticsearch visibility is excluded from this build.
func newElasticsearchVisibilityStore(*esconfig.Config, *ManagerParams, *ManagerConfig) (store.VisibilityStore, error) {
	return nil, errors.New("elasticsearch visibility is not compiled into this server; rebuild with the temporal_with_elasticsearch build tag")
}
