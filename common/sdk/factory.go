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
	"sync"

	sdkclient "go.temporal.io/sdk/client"
	sdkworker "go.temporal.io/sdk/worker"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
)

type (
	ClientFactory interface {
		NewClient(namespaceName string, logger log.Logger) (sdkclient.Client, error)
		GetSystemClient(logger log.Logger) sdkclient.Client
	}

	WorkerFactory interface {
		New(client sdkclient.Client, taskQueue string, options sdkworker.Options) sdkworker.Worker
	}

	clientFactory struct {
		hostPort        string
		tlsConfig       *tls.Config
		metricsHandler  *MetricsHandler
		systemSdkClient sdkclient.Client
		once            *sync.Once
	}

	workerFactory struct{}
)

var (
	_ ClientFactory = (*clientFactory)(nil)
	_ WorkerFactory = (*workerFactory)(nil)
)

func NewClientFactory(hostPort string, tlsConfig *tls.Config, metricsHandler *MetricsHandler) *clientFactory {
	return &clientFactory{
		hostPort:       hostPort,
		tlsConfig:      tlsConfig,
		metricsHandler: metricsHandler,
		once:           &sync.Once{},
	}
}

func (f *clientFactory) NewClient(namespaceName string, logger log.Logger) (sdkclient.Client, error) {
	var client sdkclient.Client

	// Retry for up to 1m, handles frontend service not ready
	err := backoff.ThrottleRetry(func() error {
		sdkClient, err := sdkclient.Dial(sdkclient.Options{
			HostPort:       f.hostPort,
			Namespace:      namespaceName,
			MetricsHandler: f.metricsHandler,
			Logger:         log.NewSdkLogger(logger),
			ConnectionOptions: sdkclient.ConnectionOptions{
				TLS: f.tlsConfig,
			},
		})
		if err != nil {
			return fmt.Errorf("unable to create SDK client: %w", err)
		}

		client = sdkClient
		return nil
	}, common.CreateSdkClientFactoryRetryPolicy(), common.IsContextDeadlineExceededErr)

	return client, err
}

func (f *clientFactory) GetSystemClient(logger log.Logger) sdkclient.Client {
	f.once.Do(func() {
		client, err := f.NewClient(common.SystemLocalNamespace, logger)
		if err != nil {
			logger.Fatal(
				"error getting system sdk client",
				tag.Error(err),
			)
		}

		f.systemSdkClient = client
	})
	return f.systemSdkClient
}

func NewWorkerFactory() *workerFactory {
	return &workerFactory{}
}

func (f *workerFactory) New(
	client sdkclient.Client,
	taskQueue string,
	options sdkworker.Options,
) sdkworker.Worker {
	return sdkworker.New(client, taskQueue, options)
}
