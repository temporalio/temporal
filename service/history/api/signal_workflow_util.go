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

package api

import (
	"context"

	enumspb "go.temporal.io/api/enums/v1"

	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/rpc/interceptor"
	"go.temporal.io/server/service/history/consts"
	"go.temporal.io/server/service/history/shard"
	"go.temporal.io/server/service/history/workflow"
)

func ValidateSignal(
	ctx context.Context,
	shard shard.Context,
	mutableState workflow.MutableState,
	signalPayloadSize int,
	operation string,
) error {
	config := shard.GetConfig()
	namespaceEntry := mutableState.GetNamespaceEntry()
	namespaceID := namespaceEntry.ID().String()
	namespaceName := namespaceEntry.Name().String()
	workflowID := mutableState.GetExecutionInfo().WorkflowId
	runID := mutableState.GetExecutionState().RunId

	executionInfo := mutableState.GetExecutionInfo()
	maxAllowedSignals := config.MaximumSignalsPerExecution(namespaceName)
	blobSizeLimitWarn := config.BlobSizeLimitWarn(namespaceName)
	blobSizeLimitError := config.BlobSizeLimitError(namespaceName)

	if err := common.CheckEventBlobSizeLimit(
		signalPayloadSize,
		blobSizeLimitWarn,
		blobSizeLimitError,
		namespaceName,
		workflowID,
		runID,
		interceptor.MetricsScope(ctx, shard.GetLogger()).Tagged(
			metrics.CommandTypeTag(enumspb.COMMAND_TYPE_UNSPECIFIED.String()),
		),
		shard.GetThrottledLogger(),
		tag.BlobSizeViolationOperation(operation),
	); err != nil {
		return err
	}

	if maxAllowedSignals > 0 && int(executionInfo.SignalCount) >= maxAllowedSignals {
		shard.GetLogger().Info("Execution limit reached for maximum signals",
			tag.WorkflowNamespaceID(namespaceID),
			tag.WorkflowID(workflowID),
			tag.WorkflowRunID(runID),
			tag.WorkflowSignalCount(executionInfo.SignalCount),
		)
		return consts.ErrSignalsLimitExceeded
	}

	return nil
}
