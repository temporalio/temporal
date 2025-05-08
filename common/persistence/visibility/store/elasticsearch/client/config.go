package client

import (
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"go.temporal.io/server/common/auth"
)

const (
	// VisibilityAppName is used to find ES indexName for visibility
	VisibilityAppName          = "visibility"
	SecondaryVisibilityAppName = "secondary_visibility"
)

// Config for connecting to Elasticsearch
type (
	Config struct {
		Version                      string                    `yaml:"version"`
		URL                          url.URL                   `yaml:"url"`
		URLs                         []url.URL                 `yaml:"urls"`
		Username                     string                    `yaml:"username"`
		Password                     string                    `yaml:"password"`
		Indices                      map[string]string         `yaml:"indices"`
		LogLevel                     string                    `yaml:"logLevel"`
		AWSRequestSigning            ESAWSRequestSigningConfig `yaml:"aws-request-signing"`
		CloseIdleConnectionsInterval time.Duration             `yaml:"closeIdleConnectionsInterval"`
		EnableSniff                  bool                      `yaml:"enableSniff"`
		EnableHealthcheck            bool                      `yaml:"enableHealthcheck"`
		TLS                          *auth.TLS                 `yaml:"tls"`
		// httpClient is the awsHttpClient to be used for creating esClient
		httpClient *http.Client
	}

	// ESAWSRequestSigningConfig represents configuration for signing ES requests to AWS
	ESAWSRequestSigningConfig struct {
		Enabled bool   `yaml:"enabled"`
		Region  string `yaml:"region"`

		// Possible options for CredentialProvider include:
		//   1) static (fill out static Credential Provider)
		//   2) environment
		//		a) AccessKeyID from either AWS_ACCESS_KEY_ID or AWS_ACCESS_KEY environment variable
		//		b) SecretAccessKey from either AWS_SECRET_ACCESS_KEY or AWS_SECRET_KEY environment variable
		//   3) aws-sdk-default
		//		a) Follows aws-go-sdk default credential resolution for session.NewSession
		CredentialProvider string `yaml:"credentialProvider"`

		Static ESAWSStaticCredentialProvider `yaml:"static"`
	}

	// ESAWSStaticCredentialProvider represents static AWS credentials
	ESAWSStaticCredentialProvider struct {
		AccessKeyID     string `yaml:"accessKeyID"`
		SecretAccessKey string `yaml:"secretAccessKey"`

		// Token only required for temporary security credentials retrieved via STS. Otherwise, this is optional.
		Token string `yaml:"token"`
	}
)

// GetVisibilityIndex return visibility index name from Elasticsearch config or empty string if it is not defined.
func (cfg *Config) GetVisibilityIndex() string {
	if cfg == nil {
		// Empty string is be used as default index name when Elasticsearch is not configured.
		return ""
	}
	return cfg.Indices[VisibilityAppName]
}

func (cfg *Config) GetSecondaryVisibilityIndex() string {
	if cfg == nil {
		return ""
	}
	return cfg.Indices[SecondaryVisibilityAppName]
}

func (cfg *Config) SetHttpClient(httpClient *http.Client) {
	cfg.httpClient = httpClient
}

func (cfg *Config) GetHttpClient() *http.Client {
	if cfg == nil {
		return nil
	}
	return cfg.httpClient
}

func (cfg *Config) Validate() error {
	if cfg == nil {
		return errors.New("elasticsearch config: config not found")
	}
	if len(cfg.Indices) < 1 {
		return errors.New("elasticsearch config: missing indices")
	}
	if cfg.Indices[VisibilityAppName] == "" {
		return fmt.Errorf("elasticsearch config: indices configuration: missing %q key", VisibilityAppName)
	}
	return nil
}
