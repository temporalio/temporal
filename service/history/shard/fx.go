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
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/archiver"
	"go.temporal.io/server/common/clock"
	"go.temporal.io/server/common/cluster"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/membership"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/searchattribute"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/consts"
)

var Module = fx.Options(
	fx.Provide(ShardControllerProvider),
)

func ShardControllerProvider(
	config *configs.Config,
	logger log.Logger,
	throttledLogger resource.ThrottledLogger,
	persistenceExecutionManager persistence.ExecutionManager,
	persistenceShardManager persistence.ShardManager,
	clientBean client.Bean,
	historyClient historyservice.HistoryServiceClient,
	historyServiceResolver membership.ServiceResolver,
	metricsClient metrics.Client,
	metricsHandler metrics.MetricsHandler,
	payloadSerializer serialization.Serializer,
	timeSource clock.TimeSource,
	namespaceRegistry namespace.Registry,
	saProvider searchattribute.Provider,
	saMapper searchattribute.Mapper,
	clusterMetadata cluster.Metadata,
	archivalMetadata archiver.ArchivalMetadata,
	hostInfoProvider membership.HostInfoProvider,
	engineFactory EngineFactory,
	tracerProvider trace.TracerProvider,
) *ControllerImpl {
	return &ControllerImpl{
		status:                      common.DaemonStatusInitialized,
		membershipUpdateCh:          make(chan *membership.ChangedEvent, 10),
		historyShards:               make(map[int32]*ContextImpl),
		shutdownCh:                  make(chan struct{}),
		logger:                      logger,
		contextTaggedLogger:         logger,          // will add tags in Start
		throttledLogger:             throttledLogger, // will add tags in Start
		config:                      config,
		metricsScope:                metricsClient.Scope(metrics.HistoryShardControllerScope),
		persistenceExecutionManager: persistenceExecutionManager,
		persistenceShardManager:     persistenceShardManager,
		clientBean:                  clientBean,
		historyClient:               historyClient,
		historyServiceResolver:      historyServiceResolver,
		metricsClient:               metricsClient,
		metricsHandler:              metricsHandler,
		payloadSerializer:           payloadSerializer,
		timeSource:                  timeSource,
		namespaceRegistry:           namespaceRegistry,
		saProvider:                  saProvider,
		saMapper:                    saMapper,
		clusterMetadata:             clusterMetadata,
		archivalMetadata:            archivalMetadata,
		hostInfoProvider:            hostInfoProvider,
		engineFactory:               engineFactory,
		tracer:                      tracerProvider.Tracer(consts.LibraryName),
	}
}
