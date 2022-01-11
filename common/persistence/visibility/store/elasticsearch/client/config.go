// The MIT License
//
// Copyright (c) 2020 Temporal Technologies Inc.  All rights reserved.
//
// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package client

import (
	"fmt"
	"net/url"
	"time"
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
		URL                          url.URL                   `yaml:"url"` //nolint:govet
		Username                     string                    `yaml:"username"`
		Password                     string                    `yaml:"password"`
		Indices                      map[string]string         `yaml:"indices"` //nolint:govet
		LogLevel                     string                    `yaml:"logLevel"`
		AWSRequestSigning            ESAWSRequestSigningConfig `yaml:"aws-request-signing"`
		CloseIdleConnectionsInterval time.Duration             `yaml:"closeIdleConnectionsInterval"`
		EnableSniff                  bool                      `yaml:"enableSniff"`
		EnableHealthcheck            bool                      `yaml:"enableHealthcheck"`
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

func (cfg *Config) Validate(storeName string) error {
	if cfg == nil {
		return fmt.Errorf("persistence config: advanced visibility datastore %q: must provide config for \"elasticsearch\"", storeName)
	}

	if len(cfg.Indices) < 1 {
		return fmt.Errorf("persistence config: advanced visibility datastore %q: missing indices", storeName)

	}
	if cfg.Indices[VisibilityAppName] == "" {
		return fmt.Errorf("persistence config: advanced visibility datastore %q indices configuration: missing %q key", storeName, VisibilityAppName)
	}
	return nil
}
