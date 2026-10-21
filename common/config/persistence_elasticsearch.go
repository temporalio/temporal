//go:build !temporal_minimal || temporal_with_elasticsearch

package config

import (
	"go.temporal.io/server/common/persistence/visibility/store/elasticsearch/client"
)

func validateElasticsearch(*client.Config) error {
	return nil
}
