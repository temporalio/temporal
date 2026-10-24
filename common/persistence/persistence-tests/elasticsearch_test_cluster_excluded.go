//go:build temporal_minimal && !temporal_with_elasticsearch

package persistencetests

import (
	"errors"

	"go.temporal.io/server/common/log"
	esclient "go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
)

// errElasticsearchExcluded is returned by the Elasticsearch test helpers, since Elasticsearch
// visibility is excluded from this build.
var errElasticsearchExcluded = errors.New("elasticsearch visibility is not compiled into this server; rebuild with the temporal_with_elasticsearch build tag")

func newEsTestCluster(string, int, string, string, string, log.Logger) PersistenceTestCluster {
	panic(errElasticsearchExcluded)
}

// SetupEsIndex fails, since Elasticsearch visibility is excluded from this build.
func SetupEsIndex(*esclient.Config, log.Logger) error {
	return errElasticsearchExcluded
}

// DeleteEsIndex fails, since Elasticsearch visibility is excluded from this build.
func DeleteEsIndex(*esclient.Config, log.Logger) error {
	return errElasticsearchExcluded
}
