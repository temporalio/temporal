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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination executor_factory_mock.go

package history

import (
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/common/xdc"
	"go.temporal.io/server/service/history/archival"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/deletemanager"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	wcache "go.temporal.io/server/service/history/workflow/cache"
	"go.temporal.io/server/service/worker/archiver"
	"go.uber.org/fx"
)

type (
	ExecutorFactory interface {
		CreateArchivalExecutor(
			shardCtx shard.Context,
			workflowCache wcache.Cache,
			logger log.Logger,
		) queues.Executor

		CreateTimerActiveExecutor(
			shardCtx shard.Context,
			workflowCache wcache.Cache,
			workflowDeleteManager deletemanager.DeleteManager,
			logger log.Logger,
		) queues.Executor

		CreateTimerStandbyExecutor(
			shardCtx shard.Context,
			workflowCache wcache.Cache,
			workflowDeleteManager deletemanager.DeleteManager,
			nDCHistoryResender xdc.NDCHistoryResender,
			logger log.Logger,
			clusterName string,
		) queues.Executor

		CreateTransferActiveExecutor(
			shardCtx shard.Context,
			workflowCache wcache.Cache,
			logger log.Logger,
		) queues.Executor

		CreateTransferStandbyExecutor(
			shardCtx shard.Context,
			workflowCache wcache.Cache,
			nDCHistoryResender xdc.NDCHistoryResender,
			logger log.Logger,
			clusterName string,
		) queues.Executor

		CreateExecutorWrapper(
			currentClusterName string,
			activeExecutor queues.Executor,
			standbyExecutor queues.Executor,
			logger log.Logger,
		) queues.Executor

		CreateVisibilityExecutor(
			shardCtx shard.Context,
			workflowCache wcache.Cache,
			logger log.Logger,
		) queues.Executor

		GetNewDefaultStandbyNDCHistoryResender(
			shardCtx shard.Context,
			logger log.Logger,
		) xdc.NDCHistoryResender
	}

	ExecutorFactoryBaseParams struct {
		fx.In

		Config         *configs.Config
		MetricsHandler metrics.Handler

		NamespaceRegistry namespace.Registry
		ClientBean        client.Bean
		MatchingClient    resource.MatchingClient
		ArchivalClient    archiver.Client
		SdkClientFactory  sdk.ClientFactory
		VisibilityManager manager.VisibilityManager

		// Archiver is the archival client used to archive history events and visibility records.
		Archiver archival.Archiver
		// RelocatableAttributesFetcher is the client used to fetch the memo and search attributes of a workflow.
		RelocatableAttributesFetcher workflow.RelocatableAttributesFetcher
	}

	ExecutorFactoryBase struct {
		ExecutorFactoryBaseParams
	}
)

func NewExecutorFactoryBase(
	params ExecutorFactoryBaseParams,
) ExecutorFactory {
	return &ExecutorFactoryBase{
		ExecutorFactoryBaseParams: params,
	}
}

func (f *ExecutorFactoryBase) CreateArchivalExecutor(
	shardCtx shard.Context,
	workflowCache wcache.Cache,
	logger log.Logger,
) queues.Executor {
	return NewArchivalQueueTaskExecutor(
		f.Archiver,
		shardCtx,
		workflowCache,
		f.RelocatableAttributesFetcher,
		f.MetricsHandler,
		logger,
	)
}

func (f *ExecutorFactoryBase) CreateTimerActiveExecutor(
	shardCtx shard.Context,
	workflowCache wcache.Cache,
	workflowDeleteManager deletemanager.DeleteManager,
	logger log.Logger,
) queues.Executor {
	return newTimerQueueActiveTaskExecutor(
		shardCtx,
		workflowCache,
		workflowDeleteManager,
		logger,
		f.MetricsHandler,
		f.Config,
		f.MatchingClient,
	)
}

func (f *ExecutorFactoryBase) CreateTimerStandbyExecutor(
	shardCtx shard.Context,
	workflowCache wcache.Cache,
	workflowDeleteManager deletemanager.DeleteManager,
	nDCHistoryResender xdc.NDCHistoryResender,
	logger log.Logger,
	clusterName string,
) queues.Executor {
	return newTimerQueueStandbyTaskExecutor(
		shardCtx,
		workflowCache,
		workflowDeleteManager,
		nDCHistoryResender,
		f.MatchingClient,
		logger,
		f.MetricsHandler,
		clusterName,
		f.Config,
	)
}

func (f *ExecutorFactoryBase) CreateTransferActiveExecutor(
	shardCtx shard.Context,
	workflowCache wcache.Cache,
	logger log.Logger,
) queues.Executor {
	return newTransferQueueActiveTaskExecutor(
		shardCtx,
		workflowCache,
		f.ArchivalClient,
		f.SdkClientFactory,
		logger,
		f.MetricsHandler,
		f.Config,
		f.MatchingClient,
		f.VisibilityManager,
	)
}

func (f *ExecutorFactoryBase) CreateTransferStandbyExecutor(
	shardCtx shard.Context,
	workflowCache wcache.Cache,
	nDCHistoryResender xdc.NDCHistoryResender,
	logger log.Logger,
	clusterName string,
) queues.Executor {
	return newTransferQueueStandbyTaskExecutor(
		shardCtx,
		workflowCache,
		f.ArchivalClient,
		nDCHistoryResender,
		logger,
		f.MetricsHandler,
		clusterName,
		f.MatchingClient,
		f.VisibilityManager,
	)
}

func (f *ExecutorFactoryBase) CreateExecutorWrapper(
	currentClusterName string,
	activeExecutor queues.Executor,
	standbyExecutor queues.Executor,
	logger log.Logger,
) queues.Executor {
	return queues.NewExecutorWrapper(
		currentClusterName,
		f.NamespaceRegistry,
		activeExecutor,
		standbyExecutor,
		logger,
	)
}

func (f *ExecutorFactoryBase) CreateVisibilityExecutor(
	shardCtx shard.Context,
	workflowCache wcache.Cache,
	logger log.Logger,
) queues.Executor {
	return newVisibilityQueueTaskExecutor(
		shardCtx,
		workflowCache,
		f.VisibilityManager,
		logger,
		f.MetricsHandler,
		f.Config.VisibilityProcessorEnsureCloseBeforeDelete,
		f.Config.VisibilityProcessorEnableCloseWorkflowCleanup,
	)
}

func (f *ExecutorFactoryBase) GetNewWorkflowDeleteManager(
	shardCtx shard.Context,
	workflowCache wcache.Cache,
) deletemanager.DeleteManager {
	return deletemanager.NewDeleteManager(
		shardCtx,
		workflowCache,
		f.Config,
		f.ArchivalClient,
		shardCtx.GetTimeSource(),
		f.VisibilityManager,
	)
}

func (f *ExecutorFactoryBase) GetNewDefaultStandbyNDCHistoryResender(
	shardCtx shard.Context,
	logger log.Logger,
) xdc.NDCHistoryResender {
	return xdc.NewNDCHistoryResender(
		f.NamespaceRegistry,
		f.ClientBean,
		func(ctx context.Context, request *historyservice.ReplicateEventsV2Request) error {
			engine, err := shardCtx.GetEngine(ctx)
			if err != nil {
				return err
			}
			return engine.ReplicateEventsV2(ctx, request)
		},
		shardCtx.GetPayloadSerializer(),
		f.Config.StandbyTaskReReplicationContextTimeout,
		logger,
	)
}
