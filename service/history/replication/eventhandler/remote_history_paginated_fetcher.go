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

package eventhandler

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"

	"go.temporal.io/server/api/adminservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/client"
	"go.temporal.io/server/common/collection"
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/namespace"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/rpc"
)

const (
	resendContextTimeout = 30 * time.Second
)

//go:generate mockgen -copyright_file ../../../../LICENSE -package $GOPACKAGE -source $GOFILE -destination remote_history_paginated_fetcher_mock.go

type (
	// HistoryPaginatedFetcher is the interface for fetching history from remote cluster
	// Start and End event ID is inclusive.
	HistoryPaginatedFetcher interface {
		GetSingleWorkflowHistoryPaginatedIterator(
			ctx context.Context,
			remoteClusterName string,
			namespaceID namespace.ID,
			workflowID string,
			runID string,
			startEventID int64,
			startEventVersion int64,
			endEventID int64,
			endEventVersion int64,
		) collection.Iterator[HistoryBatch]
	}

	HistoryPaginatedFetcherImpl struct {
		namespaceRegistry    namespace.Registry
		clientBean           client.Bean
		serializer           serialization.Serializer
		rereplicationTimeout dynamicconfig.DurationPropertyFnWithNamespaceIDFilter
		logger               log.Logger
	}

	HistoryBatch struct {
		VersionHistory *historyspb.VersionHistory
		RawEventBatch  *commonpb.DataBlob
	}
)

const (
	defaultPageSize = int32(100)
)

func NewHistoryPaginatedFetcher(
	namespaceRegistry namespace.Registry,
	clientBean client.Bean,
	serializer serialization.Serializer,
	rereplicationTimeout dynamicconfig.DurationPropertyFnWithNamespaceIDFilter,
	logger log.Logger,
) *HistoryPaginatedFetcherImpl {
	return &HistoryPaginatedFetcherImpl{
		namespaceRegistry:    namespaceRegistry,
		clientBean:           clientBean,
		serializer:           serializer,
		rereplicationTimeout: rereplicationTimeout,
		logger:               logger,
	}
}

func (n *HistoryPaginatedFetcherImpl) GetSingleWorkflowHistoryPaginatedIterator(
	ctx context.Context,
	remoteClusterName string,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	startEventID int64,
	startEventVersion int64,
	endEventID int64,
	endEventVersion int64,
) collection.Iterator[HistoryBatch] {

	resendCtx := context.Background()
	var cancel context.CancelFunc
	if n.rereplicationTimeout != nil {
		resendContextTimeout := n.rereplicationTimeout(namespaceID.String())
		if resendContextTimeout > 0 {
			resendCtx, cancel = context.WithTimeout(resendCtx, resendContextTimeout)
			defer cancel()
		}
	}
	resendCtx = rpc.CopyContextValues(resendCtx, ctx)

	return collection.NewPagingIterator(n.getPaginationFn(
		resendCtx,
		remoteClusterName,
		namespaceID,
		workflowID,
		runID,
		startEventID,
		startEventVersion,
		endEventID,
		endEventVersion,
	))
}

func (n *HistoryPaginatedFetcherImpl) getPaginationFn(
	ctx context.Context,
	remoteClusterName string,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	startEventID int64,
	startEventVersion int64,
	endEventID int64,
	endEventVersion int64,
) collection.PaginationFn[HistoryBatch] {

	return func(paginationToken []byte) ([]HistoryBatch, []byte, error) {

		response, err := n.getHistory(
			ctx,
			remoteClusterName,
			namespaceID,
			workflowID,
			runID,
			startEventID,
			startEventVersion,
			endEventID,
			endEventVersion,
			paginationToken,
			defaultPageSize,
		)
		if err != nil {
			return nil, nil, err
		}
		batches := make([]HistoryBatch, 0, len(response.GetHistoryBatches()))
		versionHistory := response.GetVersionHistory()
		for _, history := range response.GetHistoryBatches() {
			batch := HistoryBatch{
				VersionHistory: versionHistory,
				RawEventBatch:  history,
			}
			batches = append(batches, batch)
		}
		return batches, response.NextPageToken, nil
	}
}

func (n *HistoryPaginatedFetcherImpl) getHistory(
	ctx context.Context,
	remoteClusterName string,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	startEventID int64,
	startEventVersion int64,
	endEventID int64,
	endEventVersion int64,
	token []byte,
	pageSize int32,
) (*adminservice.GetWorkflowExecutionRawHistoryResponse, error) {

	logger := log.With(n.logger, tag.WorkflowRunID(runID))

	ctx, cancel := rpc.NewContextFromParentWithTimeoutAndVersionHeaders(ctx, resendContextTimeout)
	defer cancel()

	adminClient, err := n.clientBean.GetRemoteAdminClient(remoteClusterName)
	if err != nil {
		return nil, err
	}

	response, err := adminClient.GetWorkflowExecutionRawHistory(ctx, &adminservice.GetWorkflowExecutionRawHistoryRequest{
		NamespaceId: namespaceID.String(),
		Execution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		StartEventId:      startEventID,
		StartEventVersion: startEventVersion,
		EndEventId:        endEventID,
		EndEventVersion:   endEventVersion,
		MaximumPageSize:   pageSize,
		NextPageToken:     token,
	})
	if err != nil {
		logger.Error("error getting history", tag.Error(err))
		return nil, err
	}

	return response, nil
}
