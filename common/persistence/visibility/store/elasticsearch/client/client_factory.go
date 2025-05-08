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
