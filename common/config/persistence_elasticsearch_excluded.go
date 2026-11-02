//go:build temporal_minimal && !temporal_with_elasticsearch

package config

import (
	"errors"

	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/esconfig"
)

// validateElasticsearch fails if cfg is set, since Elasticsearch visibility is excluded from this build.
func validateElasticsearch(cfg *esconfig.Config) error {
	if cfg != nil {
		return errors.New("elasticsearch: Elasticsearch visibility is not compiled into this server; remove this config or rebuild with the temporal_with_elasticsearch build tag")
	}
	return nil
}
