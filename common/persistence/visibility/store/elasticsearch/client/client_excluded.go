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

var errElasticsearchExcluded = errors.New("elasticsearch is excluded from this build, see build tag temporal_with_elasticsearch")

func newClient(*Config, *http.Client, log.Logger) (any, error) {
	return nil, errElasticsearchExcluded
}
