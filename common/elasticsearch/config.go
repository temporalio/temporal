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

package elasticsearch

import (
	"net/url"

	"go.temporal.io/server/common"
)

// Config for connecting to ElasticSearch
type (
	Config struct {
		Version           string                  `yaml:"version"`
		URL               url.URL                 `yaml:"url"` //nolint:govet
		Username          string                  `yaml:"username"`
		Password          string                  `yaml:"password"`
		Indices           map[string]string       `yaml:"indices"` //nolint:govet
		AWSRequestSigning AWSRequestSigningConfig `yaml:"aws-request-signing"`
	}

	// AWSRequestSigningConfig represents configuration for signing ES requests to AWS
	AWSRequestSigningConfig struct {
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

		Static AWSStaticCredentialProvider `yaml:"static"`
	}

	// AWSStaticCredentialProvider represents static AWS credentials
	AWSStaticCredentialProvider struct {
		AccessKeyID     string `yaml:"accessKeyID"`
		SecretAccessKey string `yaml:"secretAccessKey"`

		// Token only required for temporary security credentials retrieved via STS. Otherwise, this is optional.
		Token string `yaml:"token"`
	}
)

// GetVisibilityIndex return visibility index name
func (cfg *Config) GetVisibilityIndex() string {
	return cfg.Indices[common.VisibilityAppName]
}
