//go:build !temporal_minimal || temporal_with_elasticsearch

package config

import (
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/esconfig"
)

func validateElasticsearch(*esconfig.Config) error {
	return nil
}
