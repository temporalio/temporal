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

package replication

import (
	"context"

	"go.temporal.io/server/api/historyservice/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/service/history/replication"
)

func SyncWorkflowState(
	ctx context.Context,
	request *historyservice.SyncWorkflowStateRequest,
	replicationProgressCache replication.ProgressCache,
	syncStateRetriever replication.SyncStateRetriever,
	logger log.Logger,
) (_ *historyservice.SyncWorkflowStateResponse, retError error) {
	result, err := syncStateRetriever.GetSyncWorkflowStateArtifact(ctx, request.GetNamespaceId(), request.Execution, request.VersionedTransition, request.VersionHistories)
	if err != nil {
		logger.Error("SyncWorkflowState failed to retrieve sync state artifact", tag.WorkflowNamespaceID(request.NamespaceId),
			tag.WorkflowID(request.Execution.WorkflowId),
			tag.WorkflowRunID(request.Execution.RunId),
			tag.Error(err))
		return nil, err
	}

	err = replicationProgressCache.Update(request.Execution.RunId, request.TargetClusterId, result.VersionedTransitionHistory, result.SyncedVersionHistory.Items)
	if err != nil {
		logger.Error("SyncWorkflowState failed to update progress cache",
			tag.WorkflowNamespaceID(request.NamespaceId),
			tag.WorkflowID(request.Execution.WorkflowId),
			tag.WorkflowRunID(request.Execution.RunId),
			tag.Error(err))
	}

	return &historyservice.SyncWorkflowStateResponse{
		VersionedTransitionArtifact: result.VersionedTransitionArtifact,
	}, nil
}
