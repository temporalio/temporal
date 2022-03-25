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
	"go.uber.org/fx"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/persistence/visibility/manager"
	"go.temporal.io/server/common/resource"
	"go.temporal.io/server/common/sdk"
	"go.temporal.io/server/service/history/queues"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/worker/archiver"
)

var QueueProcessorModule = fx.Options(
	fx.Provide(
		fx.Annotated{
			Group:  queues.ProcessorFactoryFxGroup,
			Target: NewTransferQueueProcessorFactory,
		},
		fx.Annotated{
			Group:  queues.ProcessorFactoryFxGroup,
			Target: NewTimerQueueProcessorFactory,
		},
		fx.Annotated{
			Group:  queues.ProcessorFactoryFxGroup,
			Target: NewVisibilityQueueProcessorFactory,
		},
	),
)

type (
	transferQueueProcessorFactoryParams struct {
		fx.In

		ArchivalClient   archiver.Client
		SdkClientFactory sdk.ClientFactory
		MatchingClient   resource.MatchingClient
		HistoryClient    historyservice.HistoryServiceClient
	}

	timerQueueProcessorFactoryParams struct {
		fx.In

		ArchivalClient archiver.Client
		MatchingClient resource.MatchingClient
	}

	visibilityQueueProcessorFactoryParams struct {
		fx.In

		VisibilityMgr manager.VisibilityManager
	}

	transferQueueProcessorFactory struct {
		transferQueueProcessorFactoryParams
	}

	timerQueueProcessorFactory struct {
		timerQueueProcessorFactoryParams
	}

	visibilityQueueProcessorFactory struct {
		visibilityQueueProcessorFactoryParams
	}
)

func NewTransferQueueProcessorFactory(
	params transferQueueProcessorFactoryParams,
) queues.ProcessorFactory {
	return &transferQueueProcessorFactory{
		transferQueueProcessorFactoryParams: params,
	}
}

func (f *transferQueueProcessorFactory) CreateProcessor(
	shard shard.Context,
	engine shard.Engine,
	workflowCache workflow.Cache,
) queues.Processor {
	return newTransferQueueProcessor(
		shard,
		engine,
		workflowCache,
		f.ArchivalClient,
		f.SdkClientFactory,
		f.MatchingClient,
		f.HistoryClient,
	)
}

func NewTimerQueueProcessorFactory(
	params timerQueueProcessorFactoryParams,
) queues.ProcessorFactory {
	return &timerQueueProcessorFactory{
		timerQueueProcessorFactoryParams: params,
	}
}

func (f *timerQueueProcessorFactory) CreateProcessor(
	shard shard.Context,
	engine shard.Engine,
	workflowCache workflow.Cache,
) queues.Processor {
	return newTimerQueueProcessor(
		shard,
		engine,
		workflowCache,
		f.ArchivalClient,
		f.MatchingClient,
	)
}

func NewVisibilityQueueProcessorFactory(
	params visibilityQueueProcessorFactoryParams,
) queues.ProcessorFactory {
	return &visibilityQueueProcessorFactory{
		visibilityQueueProcessorFactoryParams: params,
	}
}

func (f *visibilityQueueProcessorFactory) CreateProcessor(
	shard shard.Context,
	engine shard.Engine,
	workflowCache workflow.Cache,
) queues.Processor {
	return newVisibilityQueueProcessor(
		shard,
		workflowCache,
		f.VisibilityMgr,
	)
}
