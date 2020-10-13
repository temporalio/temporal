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
		URL      url.URL           `yaml:"url"` //nolint:govet
		Username string            `yaml:"username"`
		Password string            `yaml:"password"`
		Indices  map[string]string `yaml:"indices"` //nolint:govet
	}
)

// GetVisibilityIndex return visibility index name
func (cfg *Config) GetVisibilityIndex() string {
	return cfg.Indices[common.VisibilityAppName]
}

// CompleteUserInfo complete url.URL with username and password for ElasticSearch
func (cfg *Config) CompleteUserInfo() {
	if len(cfg.Username) > 0 && len(cfg.Password) > 0 {
		cfg.URL.User = url.UserPassword(cfg.Username, cfg.Password)
	} else if len(cfg.Username) > 0 {
		cfg.URL.User = url.User(cfg.Username)
	}
}
