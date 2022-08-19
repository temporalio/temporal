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
	sdklog "go.temporal.io/sdk/log"
	sdkworker "go.temporal.io/sdk/worker"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/backoff"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/primitives"
)

type (
	ClientFactory interface {
		NewClient(namespaceName string) sdkclient.Client
		GetSystemClient() sdkclient.Client
	}

	WorkerFactory interface {
		New(client sdkclient.Client, taskQueue string, options sdkworker.Options) sdkworker.Worker
	}

	clientFactory struct {
		hostPort        string
		tlsConfig       *tls.Config
		metricsHandler  *MetricsHandler
		logger          log.Logger
		sdklogger       sdklog.Logger
		firstSdkClient  sdkclient.Client
		firstOnce       sync.Once
		systemSdkClient sdkclient.Client
		systemOnce      sync.Once
	}

	workerFactory struct{}
)

var (
	_ ClientFactory = (*clientFactory)(nil)
	_ WorkerFactory = (*workerFactory)(nil)
)

func NewClientFactory(
	hostPort string,
	tlsConfig *tls.Config,
	metricsHandler *MetricsHandler,
	logger log.Logger,
) *clientFactory {
	return &clientFactory{
		hostPort:       hostPort,
		tlsConfig:      tlsConfig,
		metricsHandler: metricsHandler,
		logger:         logger,
		sdklogger:      log.NewSdkLogger(logger),
	}
}

func (f *clientFactory) NewClient(namespaceName string) sdkclient.Client {
	var retClient sdkclient.Client

	options := sdkclient.Options{
		HostPort:       f.hostPort,
		Namespace:      namespaceName,
		MetricsHandler: f.metricsHandler,
		Logger:         f.sdklogger,
		ConnectionOptions: sdkclient.ConnectionOptions{
			TLS: f.tlsConfig,
		},
	}

	f.firstOnce.Do(func() {
		err := backoff.ThrottleRetry(func() error {
			sdkClient, err := sdkclient.Dial(options)
			if err != nil {
				return fmt.Errorf("unable to create SDK client: %w", err)
			}
			retClient = sdkClient
			return nil
		}, common.CreateSdkClientFactoryRetryPolicy(), common.IsContextDeadlineExceededErr)

		if err != nil {
			f.logger.Fatal("error creating sdk client", tag.Error(err))
		}

		f.firstSdkClient = retClient
	})

	if retClient == f.firstSdkClient {
		return retClient
	}
	// this shouldn't fail if the existing client was created successfully
	client, err := sdkclient.NewClientFromExisting(f.firstSdkClient, options)
	if err != nil {
		f.logger.Fatal("error creating sdk client", tag.Error(err))
	}
	return client
}

func (f *clientFactory) GetSystemClient() sdkclient.Client {
	f.systemOnce.Do(func() {
		f.systemSdkClient = f.NewClient(primitives.SystemLocalNamespace)
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
