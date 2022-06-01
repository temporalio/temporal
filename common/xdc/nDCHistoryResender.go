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

//go:generate mockgen -copyright_file ../../LICENSE -package $GOPACKAGE -source $GOFILE -destination nDCHistoryResender_mock.go

package xdc

import (
	"context"
	"time"

	commonpb "go.temporal.io/api/common/v1"

	"go.temporal.io/server/api/adminservice/v1"
	historyspb "go.temporal.io/server/api/history/v1"
	"go.temporal.io/server/api/historyservice/v1"
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

type (
	// nDCHistoryReplicationFn provides the functionality to deliver replication raw history request to history
	// the provided func should be thread safe
	nDCHistoryReplicationFn func(ctx context.Context, request *historyservice.ReplicateEventsV2Request) error

	// NDCHistoryResender is the interface for resending history events to remote
	NDCHistoryResender interface {
		// SendSingleWorkflowHistory sends multiple run IDs's history events to remote
		SendSingleWorkflowHistory(
			remoteClusterName string,
			namespaceID namespace.ID,
			workflowID string,
			runID string,
			startEventID int64,
			startEventVersion int64,
			endEventID int64,
			endEventVersion int64,
		) error
	}

	// NDCHistoryResenderImpl is the implementation of NDCHistoryResender
	NDCHistoryResenderImpl struct {
		namespaceRegistry    namespace.Registry
		clientBean           client.Bean
		historyReplicationFn nDCHistoryReplicationFn
		serializer           serialization.Serializer
		rereplicationTimeout dynamicconfig.DurationPropertyFnWithNamespaceIDFilter
		logger               log.Logger
	}

	historyBatch struct {
		versionHistory *historyspb.VersionHistory
		rawEventBatch  *commonpb.DataBlob
	}
)

const (
	defaultPageSize = int32(100)
)

// NewNDCHistoryResender create a new NDCHistoryResenderImpl
func NewNDCHistoryResender(
	namespaceRegistry namespace.Registry,
	clientBean client.Bean,
	historyReplicationFn nDCHistoryReplicationFn,
	serializer serialization.Serializer,
	rereplicationTimeout dynamicconfig.DurationPropertyFnWithNamespaceIDFilter,
	logger log.Logger,
) *NDCHistoryResenderImpl {

	return &NDCHistoryResenderImpl{
		namespaceRegistry:    namespaceRegistry,
		clientBean:           clientBean,
		historyReplicationFn: historyReplicationFn,
		serializer:           serializer,
		rereplicationTimeout: rereplicationTimeout,
		logger:               logger,
	}
}

// SendSingleWorkflowHistory sends one run IDs's history events to remote
func (n *NDCHistoryResenderImpl) SendSingleWorkflowHistory(
	remoteClusterName string,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	startEventID int64,
	startEventVersion int64,
	endEventID int64,
	endEventVersion int64,
) error {

	ctx := context.Background()
	var cancel context.CancelFunc
	if n.rereplicationTimeout != nil {
		resendContextTimeout := n.rereplicationTimeout(namespaceID.String())
		if resendContextTimeout > 0 {
			ctx, cancel = context.WithTimeout(ctx, resendContextTimeout)
			defer cancel()
		}
	}

	historyIterator := collection.NewPagingIterator(n.getPaginationFn(
		ctx,
		remoteClusterName,
		namespaceID,
		workflowID,
		runID,
		startEventID,
		startEventVersion,
		endEventID,
		endEventVersion,
	))

	for historyIterator.HasNext() {
		batch, err := historyIterator.Next()
		if err != nil {
			n.logger.Error("failed to get history events",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(workflowID),
				tag.WorkflowRunID(runID),
				tag.Error(err))
			return err
		}

		replicationRequest := n.createReplicationRawRequest(
			namespaceID,
			workflowID,
			runID,
			batch.rawEventBatch,
			batch.versionHistory.GetItems())

		err = n.sendReplicationRawRequest(ctx, replicationRequest)
		if err != nil {
			n.logger.Error("failed to replicate events",
				tag.WorkflowNamespaceID(namespaceID.String()),
				tag.WorkflowID(workflowID),
				tag.WorkflowRunID(runID),
				tag.Error(err))
			return err
		}
	}
	return nil
}

func (n *NDCHistoryResenderImpl) getPaginationFn(
	ctx context.Context,
	remoteClusterName string,
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	startEventID int64,
	startEventVersion int64,
	endEventID int64,
	endEventVersion int64,
) collection.PaginationFn[historyBatch] {

	return func(paginationToken []byte) ([]historyBatch, []byte, error) {

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

		batches := make([]historyBatch, 0, len(response.GetHistoryBatches()))
		versionHistory := response.GetVersionHistory()
		for _, history := range response.GetHistoryBatches() {
			batch := historyBatch{
				versionHistory: versionHistory,
				rawEventBatch:  history,
			}
			batches = append(batches, batch)
		}
		return batches, response.NextPageToken, nil
	}
}

func (n *NDCHistoryResenderImpl) createReplicationRawRequest(
	namespaceID namespace.ID,
	workflowID string,
	runID string,
	historyBlob *commonpb.DataBlob,
	versionHistoryItems []*historyspb.VersionHistoryItem,
) *historyservice.ReplicateEventsV2Request {

	request := &historyservice.ReplicateEventsV2Request{
		NamespaceId: namespaceID.String(),
		WorkflowExecution: &commonpb.WorkflowExecution{
			WorkflowId: workflowID,
			RunId:      runID,
		},
		Events:              historyBlob,
		VersionHistoryItems: versionHistoryItems,
	}
	return request
}

func (n *NDCHistoryResenderImpl) sendReplicationRawRequest(
	ctx context.Context,
	request *historyservice.ReplicateEventsV2Request,
) error {

	ctx, cancel := context.WithTimeout(ctx, resendContextTimeout)
	defer cancel()
	return n.historyReplicationFn(ctx, request)
}

func (n *NDCHistoryResenderImpl) getHistory(
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
) (*adminservice.GetWorkflowExecutionRawHistoryV2Response, error) {

	logger := log.With(n.logger, tag.WorkflowRunID(runID))

	ns, err := n.namespaceRegistry.GetNamespaceByID(namespaceID)
	if err != nil {
		return nil, err
	}

	ctx, cancel := rpc.NewContextFromParentWithTimeoutAndHeaders(ctx, resendContextTimeout)
	defer cancel()

	adminClient, err := n.clientBean.GetRemoteAdminClient(remoteClusterName)
	if err != nil {
		return nil, err
	}

	response, err := adminClient.GetWorkflowExecutionRawHistoryV2(ctx, &adminservice.GetWorkflowExecutionRawHistoryV2Request{
		Namespace:   ns.Name().String(),
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
