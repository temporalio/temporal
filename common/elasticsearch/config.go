package elasticsearch

import (
	"net/url"

	"github.com/temporalio/temporal/common"
)

// Config for connecting to ElasticSearch
type (
	Config struct {
		URL     url.URL           `yaml:url`     //nolint:govet
		Indices map[string]string `yaml:indices` //nolint:govet
	}
)

// GetVisibilityIndex return visibility index name
func (cfg *Config) GetVisibilityIndex() string {
	return cfg.Indices[common.VisibilityAppName]
}
