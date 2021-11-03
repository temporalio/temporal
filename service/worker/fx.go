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

package worker

import (
	"context"

	"go.uber.org/fx"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/client/history"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/persistence"
	persistenceClient "go.temporal.io/server/common/persistence/client"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/service"
)

var Module = fx.Options(
	persistenceClient.Module,
	fx.Provide(ParamsExpandProvider),
	fx.Provide(resource.PersistenceConfigProvider),
	fx.Provide(dynamicconfig.NewCollection),
	fx.Provide(ThrottledLoggerRpsFnProvider),
	fx.Provide(NewConfig),
	fx.Provide(PersistenceMaxQpsProvider),
	fx.Provide(ArchivalMetadataProvider),
	fx.Provide(NamespaceReplicationQueueProvider),
	fx.Provide(resource.ClusterMetadataManagerProvider),
	fx.Provide(resource.MetricsClientProvider),
	fx.Provide(resource.ClientBeanProvider),
	fx.Provide(resource.NamespaceCacheProvider),
	fx.Provide(resource.ArchiverProviderProvider),
	fx.Provide(resource.MetricsScopeProvider),
	fx.Provide(resource.MembershipMonitorProvider),
	fx.Provide(resource.PersistenceExecutionManagerProvider),
	fx.Provide(resource.AbstractDatastoreFactoryProvider),
	fx.Provide(resource.PersistenceServiceResolverProvider),
	fx.Provide(resource.SdkClientProvider),
	fx.Provide(resource.ClientFactoryProvider),
	fx.Provide(resource.SnTaggedLoggerProvider),
	fx.Provide(resource.MembershipFactoryProvider),
	fx.Provide(resource.ServiceNameProvider),
	fx.Provide(resource.MetadataManagerProvider),
	fx.Provide(PersistenceTaskManagerProvider),
	fx.Provide(HistoryServiceClientProvider),
	fx.Provide(cluster.NewMetadataFromConfig),
	fx.Provide(NewService),
	fx.Provide(NewWorkerManager),
	fx.Invoke(ServiceLifetimeHooks),
)

func ParamsExpandProvider(params *resource.BootstrapParams) common.RPCFactory {
	return params.RPCFactory
}

func ThrottledLoggerRpsFnProvider(serviceConfig *Config) resource.ThrottledLoggerRpsFn {
	return func() float64 { return float64(serviceConfig.ThrottledLogRPS()) }
}

func PersistenceMaxQpsProvider(
	serviceConfig *Config,
) persistenceClient.PersistenceMaxQps {
	return service.PersistenceMaxQpsFn(serviceConfig.PersistenceMaxQPS, serviceConfig.PersistenceGlobalMaxQPS)
}

func ArchivalMetadataProvider(params *resource.BootstrapParams) archiver.ArchivalMetadata {
	return params.ArchivalMetadata
}

func NamespaceReplicationQueueProvider(bean persistenceClient.Bean) persistence.NamespaceReplicationQueue {
	return bean.GetNamespaceReplicationQueue()
}

func PersistenceTaskManagerProvider(bean persistenceClient.Bean) persistence.TaskManager {
	return bean.GetTaskManager()
}

func HistoryServiceClientProvider(bean client.Bean) historyservice.HistoryServiceClient {
	historyRawClient := bean.GetHistoryClient()
	return history.NewRetryableClient(
		historyRawClient,
		common.CreateHistoryServiceRetryPolicy(),
		common.IsWhitelistServiceTransientError,
	)

}

func ServiceLifetimeHooks(
	lc fx.Lifecycle,
	svcStoppedCh chan struct{},
	svc *Service,
) {
	lc.Append(
		fx.Hook{
			OnStart: func(context.Context) error {
				go func(svc common.Daemon, svcStoppedCh chan<- struct{}) {
					// Start is blocked until Stop() is called.
					svc.Start()
					close(svcStoppedCh)
				}(svc, svcStoppedCh)

				return nil
			},
			OnStop: func(ctx context.Context) error {
				svc.Stop()
				return nil
			},
		},
	)
}
