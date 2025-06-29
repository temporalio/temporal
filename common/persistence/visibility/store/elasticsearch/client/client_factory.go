package client

import (
	"fmt"
	"net/http"

	"go.temporal.io/server/common/log"
)

func NewClient(config *Config, httpClient *http.Client, logger log.Logger) (Client, error) {
	switch config.Version {
	case "v8", "v7", "":
		return newClient(config, httpClient, logger)
	default:
		return nil, fmt.Errorf("not supported Elasticsearch version: %v", config.Version)
	}
}

func NewCLIClient(config *Config, logger log.Logger) (CLIClient, error) {
	switch config.Version {
	case "v8", "v7", "":
		return newClient(config, nil, logger)
	default:
		return nil, fmt.Errorf("not supported Elasticsearch version: %v", config.Version)
	}
}

func NewFunctionalTestsClient(config *Config, logger log.Logger) (IntegrationTestsClient, error) {
	switch config.Version {
	case "v8", "v7", "":
		return newClient(config, nil, logger)
	default:
		return nil, fmt.Errorf("not supported Elasticsearch version: %v", config.Version)
	}
}

// NewV8ElasticClient creates a new v8 Elasticsearch client
func NewV8ElasticClient(config *Config, httpClient *http.Client, logger log.Logger) (ElasticClient, error) {
	switch config.Version {
	case "v8", "official", "go-elasticsearch":
		return newClientV8(config, httpClient, logger)
	case "v7", "":
		// TODO: Create adapter for v7 elastic client
		return nil, fmt.Errorf("v7 adapter not implemented yet")
	default:
		return nil, fmt.Errorf("not supported Elasticsearch version: %v", config.Version)
	}
}
