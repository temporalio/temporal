//go:build temporal_minimal && !temporal_with_elasticsearch

package client

import (
	"errors"
	"net/http"

	"go.temporal.io/server/common/log"
)

// Client is a placeholder: Elasticsearch support is compiled out of this build.
type Client interface{}

// errElasticsearchExcluded is returned by NewClient, since Elasticsearch visibility is excluded from this build.
var errElasticsearchExcluded = errors.New("elasticsearch visibility is not compiled into this server; rebuild with the temporal_with_elasticsearch build tag")

// NewClient fails, since Elasticsearch visibility is excluded from this build.
func NewClient(*Config, *http.Client, log.Logger) (Client, error) {
	return nil, errElasticsearchExcluded
}
