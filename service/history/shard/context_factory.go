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

package shard

import (
	"go.uber.org/fx"

	"go.temporal.io/server/client"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/events"
	"go.temporal.io/server/service/history/hsm"
	"go.temporal.io/server/service/history/tasks"
)

//go:generate mockgen -copyright_file ../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination context_factory_mock.go

type (
	CloseCallback func(ControllableContext)

	ContextFactory interface {
		CreateContext(shardID int32, closeCallback CloseCallback) (ControllableContext, error)
	}

	ContextFactoryParams struct {
		fx.In

		ArchivalMetadata            archiver.ArchivalMetadata
		ClientBean                  client.Bean
		ClusterMetadata             cluster.Metadata
		Config                      *configs.Config
		PersistenceConfig           config.Persistence
		EngineFactory               EngineFactory
		HistoryClient               resource.HistoryClient
		HistoryServiceResolver      membership.ServiceResolver
		HostInfoProvider            membership.HostInfoProvider
		Logger                      log.Logger
		MetricsHandler              metrics.Handler
		NamespaceRegistry           namespace.Registry
		PayloadSerializer           serialization.Serializer
		PersistenceExecutionManager persistence.ExecutionManager
		PersistenceShardManager     persistence.ShardManager
		SaMapperProvider            searchattribute.MapperProvider
		SaProvider                  searchattribute.Provider
		ThrottledLogger             log.ThrottledLogger
		TimeSource                  clock.TimeSource
		TaskCategoryRegistry        tasks.TaskCategoryRegistry
		EventsCache                 events.Cache

		StateMachineRegistry *hsm.Registry
	}

	contextFactoryImpl struct {
		*ContextFactoryParams
	}
)

func ContextFactoryProvider(params ContextFactoryParams) ContextFactory {
	return &contextFactoryImpl{
		ContextFactoryParams: &params,
	}
}

func (c *contextFactoryImpl) CreateContext(
	shardID int32,
	closeCallback CloseCallback,
) (ControllableContext, error) {
	shard, err := newContext(
		shardID,
		c.EngineFactory,
		c.Config,
		c.PersistenceConfig,
		closeCallback,
		c.Logger,
		c.ThrottledLogger,
		c.PersistenceExecutionManager,
		c.PersistenceShardManager,
		c.ClientBean,
		c.HistoryClient,
		c.MetricsHandler,
		c.PayloadSerializer,
		c.TimeSource,
		c.NamespaceRegistry,
		c.SaProvider,
		c.SaMapperProvider,
		c.ClusterMetadata,
		c.ArchivalMetadata,
		c.HostInfoProvider,
		c.TaskCategoryRegistry,
		c.EventsCache,
		c.StateMachineRegistry,
	)
	if err != nil {
		return nil, err
	}
	shard.start()
	return shard, nil
}
