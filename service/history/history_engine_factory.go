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

package history

import (
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"

	"go.temporal.io/server/client"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/service/history/api"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/replication"
	"go.temporal.io/server/service/history/shard"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.temporal.io/server/service/worker/archiver"
)

type (
	HistoryEngineFactoryParams struct {
		fx.In

		ClientBean                      client.Bean
		MatchingClient                  resource.MatchingClient
		SdkClientFactory                sdk.ClientFactory
		EventNotifier                   events.Notifier
		Config                          *configs.Config
		RawMatchingClient               resource.MatchingRawClient
		NewCacheFn                      wcache.NewCacheFn
		ArchivalClient                  archiver.Client
		EventSerializer                 serialization.Serializer
		QueueFactories                  []QueueFactory `group:"queueFactory"`
		ReplicationTaskFetcherFactory   replication.TaskFetcherFactory
		ReplicationTaskExecutorProvider replication.TaskExecutorProvider
		TracerProvider                  trace.TracerProvider
		PersistenceVisibilityMgr        manager.VisibilityManager
	}

	historyEngineFactory struct {
		HistoryEngineFactoryParams
	}
)

func (f *historyEngineFactory) CreateEngine(
	shard shard.Context,
) shard.Engine {
	workflowCache := f.NewCacheFn(shard)
	workflowConsistencyChecker := api.NewWorkflowConsistencyChecker(shard, workflowCache)
	return NewEngineWithShardContext(
		shard,
		f.ClientBean,
		f.MatchingClient,
		f.SdkClientFactory,
		f.EventNotifier,
		f.Config,
		f.RawMatchingClient,
		workflowCache,
		f.ArchivalClient,
		f.EventSerializer,
		f.QueueFactories,
		f.ReplicationTaskFetcherFactory,
		f.ReplicationTaskExecutorProvider,
		workflowConsistencyChecker,
		f.TracerProvider,
		f.PersistenceVisibilityMgr,
	)
}
