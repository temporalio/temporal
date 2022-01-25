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
	sdkclient "go.temporal.io/sdk/client"
	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/archiver/provider"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

type (
	historyEngineFactory struct {
		visibilityMgr           manager.VisibilityManager
		matchingClient          matchingservice.MatchingServiceClient
		historyClient           historyservice.HistoryServiceClient
		publicClient            sdkclient.Client
		eventNotifier           events.Notifier
		config                  *configs.Config
		replicationTaskFetchers ReplicationTaskFetchers
		rawMatchingClient       matchingservice.MatchingServiceClient
		newCacheFn              workflow.NewCacheFn
		clientBean              client.Bean
		archiverProvider        provider.ArchiverProvider
		registry                namespace.Registry
	}
)

func NewEngineFactory(
	visibilityMgr manager.VisibilityManager,
	matchingClient matchingservice.MatchingServiceClient,
	historyClient historyservice.HistoryServiceClient,
	publicClient sdkclient.Client,
	eventNotifier events.Notifier,
	config *configs.Config,
	replicationTaskFetchers ReplicationTaskFetchers,
	rawMatchingClient matchingservice.MatchingServiceClient,
	newCacheFn workflow.NewCacheFn,
	clientBean client.Bean,
	archiverProvider provider.ArchiverProvider,
	registry namespace.Registry,
) shard.EngineFactory {
	return &historyEngineFactory{
		visibilityMgr:           visibilityMgr,
		matchingClient:          matchingClient,
		historyClient:           historyClient,
		publicClient:            publicClient,
		eventNotifier:           eventNotifier,
		config:                  config,
		replicationTaskFetchers: replicationTaskFetchers,
		rawMatchingClient:       rawMatchingClient,
		newCacheFn:              newCacheFn,
		clientBean:              clientBean,
		archiverProvider:        archiverProvider,
		registry:                registry,
	}
}

func (f *historyEngineFactory) CreateEngine(
	context shard.Context,
) shard.Engine {
	return NewEngineWithShardContext(
		context,
		f.visibilityMgr,
		f.matchingClient,
		f.historyClient,
		f.publicClient,
		f.eventNotifier,
		f.config,
		f.replicationTaskFetchers,
		f.rawMatchingClient,
		f.newCacheFn,
		f.clientBean,
		f.archiverProvider,
		f.registry,
	)
}
