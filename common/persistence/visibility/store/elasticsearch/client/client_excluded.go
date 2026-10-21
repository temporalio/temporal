//go:build temporal_minimal && !temporal_with_elasticsearch

package client

import (
	"errors"
	"net/http"

	"go.temporal.io/server/common/log"
)

// Elasticsearch support is compiled out of this build. The client types remain so that
// configuration and dependency wiring compile unchanged, but no client can be created.

type (
	Client                 interface{}
	CLIClient              interface{}
	IntegrationTestsClient interface{}
)

// errElasticsearchExcluded is returned by every client constructor, since Elasticsearch visibility is
// excluded from this build.
var errElasticsearchExcluded = errors.New("elasticsearch visibility is not compiled into this server; rebuild with the temporal_with_elasticsearch build tag")

func newClient(*Config, *http.Client, log.Logger) (any, error) {
	return nil, errElasticsearchExcluded
}
