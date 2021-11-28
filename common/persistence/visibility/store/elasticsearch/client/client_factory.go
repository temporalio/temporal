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
	"net/http"

	"go.temporal.io/server/common/log"
)

func NewClient(config *Config, httpClient *http.Client, logger log.Logger) (Client, error) {
	switch config.Version {
	case "v6":
		return newClientV6(config, httpClient, logger)
	case "v7", "":
		return newClientV7(config, httpClient, logger)
	default:
		return nil, fmt.Errorf("not supported Elasticsearch version: %v", config.Version)
	}
}

func NewCLIClient(config *Config, logger log.Logger) (CLIClient, error) {
	switch config.Version {
	case "v6":
		return newClientV6(config, nil, logger)
	case "v7", "":
		return newClientV7(config, nil, logger)
	default:
		return nil, fmt.Errorf("not supported Elasticsearch version: %v", config.Version)
	}
}

func NewIntegrationTestsClient(config *Config, logger log.Logger) (IntegrationTestsClient, error) {
	switch config.Version {
	case "v6":
		return newClientV6(config, nil, logger)
	case "v7", "":
		return newClientV7(config, nil, logger)
	default:
		return nil, fmt.Errorf("not supported Elasticsearch version: %v", config.Version)
	}
}
