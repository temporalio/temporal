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
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/api/matchingservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/service/history/configs"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/tasks"
	"go.temporal.io/server/service/history/workflow"
	"go.temporal.io/server/service/worker/parentclosepolicy"
)

type (
	tieredStorageQueueTaskExecutor struct {
		shard                   shard.Context
		historyService          *historyEngineImpl
		cache                   workflow.Cache
		logger                  log.Logger
		metricsClient           metrics.Client
		matchingClient          matchingservice.MatchingServiceClient
		config                  *configs.Config
		historyClient           historyservice.HistoryServiceClient
		parentClosePolicyClient parentclosepolicy.Client
	}
)

func newTieredStorageQueueTaskExecutor(
	shard shard.Context,
	historyService *historyEngineImpl,
	logger log.Logger,
	metricsClient metrics.Client,
	config *configs.Config,
	matchingClient matchingservice.MatchingServiceClient,
) *tieredStorageQueueTaskExecutor {
	return &tieredStorageQueueTaskExecutor{
		shard:          shard,
		historyService: historyService,
		cache:          historyService.historyCache,
		logger:         logger,
		metricsClient:  metricsClient,
		matchingClient: matchingClient,
		config:         config,
		historyClient:  shard.GetHistoryClient(),
		parentClosePolicyClient: parentclosepolicy.NewClient(
			shard.GetMetricsClient(),
			shard.GetLogger(),
			historyService.publicClient,
			config.NumParentClosePolicySystemWorkflows(),
		),
	}
}

func (t *tieredStorageQueueTaskExecutor) execute(
	ctx context.Context,
	taskInfo tasks.Task,
	shouldProcessTask bool,
) error {

	if !shouldProcessTask {
		return nil
	}

	switch task := taskInfo.(type) {
	case *tasks.TieredStorageTask:
		return t.processTieredStorageTask(ctx, task)
	default:
		return errUnknownVisibilityTask
	}
}

func (t *tieredStorageQueueTaskExecutor) processTieredStorageTask(_ context.Context, _ *tasks.TieredStorageTask) error {
	// FIXME implement
	panic("unimplemented")
}
