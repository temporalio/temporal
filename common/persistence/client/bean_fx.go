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
	"context"

	"go.uber.org/fx"

	"go.temporal.io/server/common/persistence"
)

var BeanModule = fx.Options(
	BeanDepsModule,
	fx.Provide(BeanProvider),
	fx.Provide(func(impl *BeanImpl) Bean { return impl }),
	fx.Invoke(BeanLifetimeHooks),
)

var BeanDepsModule = fx.Options(
	fx.Provide(ClusterMetadataManagerProvider),
	fx.Provide(MetadataManagerProvider),
	fx.Provide(TaskManagerProvider),
	fx.Provide(NamespaceReplicationQueueProvider),
	fx.Provide(ShardManagerProvider),
	fx.Provide(ExecutionManagerProvider),
)

func BeanProvider(
	factory Factory,
	clusterMetadataManager persistence.ClusterMetadataManager,
	metadataManager persistence.MetadataManager,
	taskManager persistence.TaskManager,
	namespaceReplicationQueue persistence.NamespaceReplicationQueue,
	shardManager persistence.ShardManager,
	executionManager persistence.ExecutionManager,
) *BeanImpl {
	return NewBean(
		factory,
		clusterMetadataManager,
		metadataManager,
		taskManager,
		namespaceReplicationQueue,
		shardManager,
		executionManager,
	)
}

func ClusterMetadataManagerProvider(factory Factory) (persistence.ClusterMetadataManager, error) {
	return factory.NewClusterMetadataManager()
}

func MetadataManagerProvider(factory Factory) (persistence.MetadataManager, error) {
	return factory.NewMetadataManager()
}
func TaskManagerProvider(factory Factory) (persistence.TaskManager, error) {
	return factory.NewTaskManager()
}

func NamespaceReplicationQueueProvider(factory Factory) (persistence.NamespaceReplicationQueue, error) {
	return factory.NewNamespaceReplicationQueue()
}
func ShardManagerProvider(factory Factory) (persistence.ShardManager, error) {
	return factory.NewShardManager()
}
func ExecutionManagerProvider(factory Factory) (persistence.ExecutionManager, error) {
	return factory.NewExecutionManager()
}

func BeanLifetimeHooks(
	lc fx.Lifecycle,
	bean Bean,
) {
	lc.Append(
		fx.Hook{
			OnStop: func(ctx context.Context) error {
				bean.Close()
				return nil
			},
		},
	)
}
