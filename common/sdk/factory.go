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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination factory_mock.go

package sdk

import (
	"crypto/tls"
	"fmt"

	sdkclient "go.temporal.io/sdk/client"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
)

type (
	ClientFactory interface {
		NewClient(namespaceName string, logger log.Logger) (sdkclient.Client, error)
		NewSystemClient(logger log.Logger) (sdkclient.Client, error)
	}

	clientFactory struct {
		hostPort       string
		tlsConfig      *tls.Config
		metricsHandler *MetricsHandler
	}
)

var _ ClientFactory = (*clientFactory)(nil)

func NewClientFactory(hostPort string, tlsConfig *tls.Config, metricsHandler *MetricsHandler) *clientFactory {
	return &clientFactory{
		hostPort:       hostPort,
		tlsConfig:      tlsConfig,
		metricsHandler: metricsHandler,
	}
}

func (f *clientFactory) NewClient(namespaceName string, logger log.Logger) (sdkclient.Client, error) {
	sdkClient, err := sdkclient.NewClient(sdkclient.Options{
		HostPort:       f.hostPort,
		Namespace:      namespaceName,
		MetricsHandler: f.metricsHandler,
		Logger:         log.NewSdkLogger(logger),
		ConnectionOptions: sdkclient.ConnectionOptions{
			TLS:                f.tlsConfig,
			DisableHealthCheck: true,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create SDK client: %w", err)
	}

	return sdkClient, nil
}

func (f *clientFactory) NewSystemClient(logger log.Logger) (sdkclient.Client, error) {
	return f.NewClient(common.SystemLocalNamespace, logger)
}
