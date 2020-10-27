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

package persistence

import (
	"fmt"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/service/dynamicconfig"
)

type (
	// historyManagerImpl implements HistoryManager based on HistoryStore and PayloadSerializer
	historyV2ManagerImpl struct {
		historySerializer     PayloadSerializer
		persistence           HistoryStore
		logger                log.Logger
		pagingTokenSerializer *jsonHistoryTokenSerializer
		transactionSizeLimit  dynamicconfig.IntPropertyFn
	}
)

const (
	defaultLastNodeID        = common.FirstEventID - 1
	defaultLastTransactionID = int64(0)
)

var _ HistoryManager = (*historyV2ManagerImpl)(nil)

// NewHistoryV2ManagerImpl returns new HistoryManager
func NewHistoryV2ManagerImpl(
	persistence HistoryStore,
	logger log.Logger,
	transactionSizeLimit dynamicconfig.IntPropertyFn,
) HistoryManager {

	return &historyV2ManagerImpl{
		historySerializer:     NewPayloadSerializer(),
		persistence:           persistence,
		logger:                logger,
		pagingTokenSerializer: newJSONHistoryTokenSerializer(),
		transactionSizeLimit:  transactionSizeLimit,
	}
}

func (m *historyV2ManagerImpl) GetName() string {
	return m.persistence.GetName()
}

// ForkHistoryBranch forks a new branch from a old branch
func (m *historyV2ManagerImpl) ForkHistoryBranch(
	request *ForkHistoryBranchRequest,
) (*ForkHistoryBranchResponse, error) {

	if request.ForkNodeID <= 1 {
		return nil, &InvalidPersistenceRequestError{
			Msg: fmt.Sprintf("ForkNodeID must be > 1"),
		}
	}

	forkBranch, err := serialization.HistoryBranchFromBlob(request.ForkBranchToken, enumspb.ENCODING_TYPE_PROTO3.String())
	if err != nil {
		return nil, err
	}
	shardID, err := getShardID(request.ShardID)
	if err != nil {
		return nil, serviceerror.NewInternal(err.Error())
	}
	req := &InternalForkHistoryBranchRequest{
		ForkBranchInfo: forkBranch,
		ForkNodeID:     request.ForkNodeID,
		NewBranchID:    uuid.New(),
		Info:           request.Info,
		ShardID:        shardID,
	}

	resp, err := m.persistence.ForkHistoryBranch(req)
	if err != nil {
		return nil, err
	}

	datablob, err := serialization.HistoryBranchToBlob(resp.NewBranchInfo)
	if err != nil {
		return nil, err
	}
	token := datablob.Data

	return &ForkHistoryBranchResponse{
		NewBranchToken: token,
	}, nil
}

// DeleteHistoryBranch removes a branch
func (m *historyV2ManagerImpl) DeleteHistoryBranch(
	request *DeleteHistoryBranchRequest,
) error {

	branch, err := serialization.HistoryBranchFromBlob(request.BranchToken, enumspb.ENCODING_TYPE_PROTO3.String())
	if err != nil {
		return err
	}

	shardID, err := getShardID(request.ShardID)
	if err != nil {
		m.logger.Error("shardID is not set in delete history operation", tag.Error(err))
		return serviceerror.NewInternal(err.Error())
	}
	req := &InternalDeleteHistoryBranchRequest{
		BranchInfo: branch,
		ShardID:    shardID,
	}

	return m.persistence.DeleteHistoryBranch(req)
}

// GetHistoryTree returns all branch information of a tree
func (m *historyV2ManagerImpl) GetHistoryTree(
	request *GetHistoryTreeRequest,
) (*GetHistoryTreeResponse, error) {

	if len(request.TreeID) == 0 {
		branch, err := serialization.HistoryBranchFromBlob(request.BranchToken, enumspb.ENCODING_TYPE_PROTO3.String())
		if err != nil {
			return nil, err
		}
		request.TreeID = branch.GetTreeId()
	}
	return m.persistence.GetHistoryTree(request)
}

// AppendHistoryNodes add(or override) a node to a history branch
func (m *historyV2ManagerImpl) AppendHistoryNodes(
	request *AppendHistoryNodesRequest,
) (*AppendHistoryNodesResponse, error) {

	branch, err := serialization.HistoryBranchFromBlob(request.BranchToken, enumspb.ENCODING_TYPE_PROTO3.String())
	if err != nil {
		return nil, err
	}
	if len(request.Events) == 0 {
		return nil, &InvalidPersistenceRequestError{
			Msg: fmt.Sprintf("events to be appended cannot be empty"),
		}
	}
	version := request.Events[0].Version
	nodeID := request.Events[0].EventId
	lastID := nodeID - 1

	if nodeID <= 0 {
		return nil, &InvalidPersistenceRequestError{
			Msg: fmt.Sprintf("eventID cannot be less than 1"),
		}
	}
	for _, e := range request.Events {
		if e.Version != version {
			return nil, &InvalidPersistenceRequestError{
				Msg: fmt.Sprintf("event version must be the same inside a batch"),
			}
		}
		if e.EventId != lastID+1 {
			return nil, &InvalidPersistenceRequestError{
				Msg: fmt.Sprintf("event ID must be continous"),
			}
		}
		lastID++
	}

	// nodeID will be the first eventID
	blob, err := m.historySerializer.SerializeEvents(request.Events, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return nil, err
	}
	size := len(blob.Data)
	sizeLimit := m.transactionSizeLimit()
	if size > sizeLimit {
		return nil, &TransactionSizeLimitError{
			Msg: fmt.Sprintf("transaction size of %v bytes exceeds limit of %v bytes", size, sizeLimit),
		}
	}
	shardID, err := getShardID(request.ShardID)
	if err != nil {
		m.logger.Error("shardID is not set in append history nodes operation", tag.Error(err))
		return nil, serviceerror.NewInternal(err.Error())
	}
	req := &InternalAppendHistoryNodesRequest{
		IsNewBranch:   request.IsNewBranch,
		Info:          request.Info,
		BranchInfo:    branch,
		NodeID:        nodeID,
		Events:        blob,
		TransactionID: request.TransactionID,
		ShardID:       shardID,
	}

	err = m.persistence.AppendHistoryNodes(req)

	return &AppendHistoryNodesResponse{
		Size: size,
	}, err
}

// ReadHistoryBranchByBatch returns history node data for a branch by batch
// Pagination is implemented here, the actual minNodeID passing to persistence layer is calculated along with token's LastNodeID
func (m *historyV2ManagerImpl) ReadHistoryBranchByBatch(
	request *ReadHistoryBranchRequest,
) (*ReadHistoryBranchByBatchResponse, error) {

	resp := &ReadHistoryBranchByBatchResponse{}
	var err error
	_, resp.History, resp.NextPageToken, resp.Size, resp.LastFirstEventID, resp.LastEventID, err = m.readHistoryBranch(true, request)
	return resp, err
}

// ReadHistoryBranch returns history node data for a branch
// Pagination is implemented here, the actual minNodeID passing to persistence layer is calculated along with token's LastNodeID
func (m *historyV2ManagerImpl) ReadHistoryBranch(
	request *ReadHistoryBranchRequest,
) (*ReadHistoryBranchResponse, error) {

	resp := &ReadHistoryBranchResponse{}
	var err error
	resp.HistoryEvents, _, resp.NextPageToken, resp.Size, resp.LastFirstEventID, _, err = m.readHistoryBranch(false, request)
	return resp, err
}

// ReadRawHistoryBranch returns raw history binary data for a branch
// Pagination is implemented here, the actual minNodeID passing to persistence layer is calculated along with token's LastNodeID
// NOTE: this API should only be used by 3+DC
func (m *historyV2ManagerImpl) ReadRawHistoryBranch(
	request *ReadHistoryBranchRequest,
) (*ReadRawHistoryBranchResponse, error) {

	dataBlobs, token, dataSize, _, err := m.readRawHistoryBranch(request)
	if err != nil {
		return nil, err
	}

	nextPageToken, err := m.serializeToken(token)
	if err != nil {
		return nil, err
	}

	return &ReadRawHistoryBranchResponse{
		HistoryEventBlobs: dataBlobs,
		NextPageToken:     nextPageToken,
		Size:              dataSize,
	}, nil
}

func (m *historyV2ManagerImpl) GetAllHistoryTreeBranches(
	request *GetAllHistoryTreeBranchesRequest,
) (*GetAllHistoryTreeBranchesResponse, error) {

	return m.persistence.GetAllHistoryTreeBranches(request)
}

func (m *historyV2ManagerImpl) readRawHistoryBranch(
	request *ReadHistoryBranchRequest,
) ([]*commonpb.DataBlob, *historyV2PagingToken, int, log.Logger, error) {

	branch, err := serialization.HistoryBranchFromBlob(request.BranchToken, enumspb.ENCODING_TYPE_PROTO3.String())
	if err != nil {
		return nil, nil, 0, nil, err
	}
	treeID := branch.TreeId
	branchID := branch.BranchId

	if request.PageSize <= 0 || request.MinEventID >= request.MaxEventID {
		return nil, nil, 0, nil, &InvalidPersistenceRequestError{
			Msg: fmt.Sprintf(
				"no events can be found for pageSize %v, minEventID %v, maxEventID: %v",
				request.PageSize,
				request.MinEventID,
				request.MaxEventID,
			),
		}
	}

	defaultLastEventID := request.MinEventID - 1
	token, err := m.deserializeToken(
		request.NextPageToken,
		defaultLastEventID,
	)
	if err != nil {
		return nil, nil, 0, nil, err
	}

	allBRs := branch.Ancestors
	// We may also query the current branch from beginNodeID
	beginNodeID := common.FirstEventID
	if len(branch.Ancestors) > 0 {
		beginNodeID = branch.Ancestors[len(branch.Ancestors)-1].GetEndNodeId()
	}
	allBRs = append(allBRs, &persistencespb.HistoryBranchRange{
		BranchId:    branchID,
		BeginNodeId: beginNodeID,
		EndNodeId:   request.MaxEventID,
	})

	if token.CurrentRangeIndex == notStartedIndex {
		for idx, br := range allBRs {
			// this range won't contain any nodes needed
			if request.MinEventID >= br.GetEndNodeId() {
				continue
			}
			// similarly, the ranges and the rest won't contain any nodes needed,
			if request.MaxEventID <= br.GetBeginNodeId() {
				break
			}

			if token.CurrentRangeIndex == notStartedIndex {
				token.CurrentRangeIndex = idx
			}
			token.FinalRangeIndex = idx
		}

		if token.CurrentRangeIndex == notStartedIndex {
			return nil, nil, 0, nil, serviceerror.NewInternal(fmt.Sprintf("branchRange is corrupted"))
		}
	}

	minNodeID := request.MinEventID
	maxNodeID := allBRs[token.CurrentRangeIndex].GetEndNodeId()
	if request.MaxEventID < maxNodeID {
		maxNodeID = request.MaxEventID
	}
	pageSize := request.PageSize

	shardID, err := getShardID(request.ShardID)
	if err != nil {
		m.logger.Error("shardID is not set in read history branch operation", tag.Error(err))
		return nil, nil, 0, nil, serviceerror.NewInternal(err.Error())
	}
	req := &InternalReadHistoryBranchRequest{
		TreeID:            treeID,
		BranchID:          allBRs[token.CurrentRangeIndex].GetBranchId(),
		MinNodeID:         minNodeID,
		MaxNodeID:         maxNodeID,
		NextPageToken:     token.StoreToken,
		LastNodeID:        token.LastNodeID,
		LastTransactionID: token.LastTransactionID,
		ShardID:           shardID,
		PageSize:          pageSize,
	}

	resp, err := m.persistence.ReadHistoryBranch(req)
	if err != nil {
		return nil, nil, 0, nil, err
	}
	if len(resp.History) == 0 && len(request.NextPageToken) == 0 {
		return nil, nil, 0, nil, serviceerror.NewNotFound("Workflow execution history not found.")
	}

	dataBlobs := resp.History
	dataSize := 0
	for _, dataBlob := range resp.History {
		dataSize += len(dataBlob.Data)
	}

	token.StoreToken = resp.NextPageToken
	token.LastNodeID = resp.LastNodeID
	token.LastTransactionID = resp.LastTransactionID

	// NOTE: in this method, we need to make sure eventVersion is NOT
	// decreasing(otherwise we skip the events), eventID should be continuous(otherwise return error)
	logger := m.logger.WithTags(tag.WorkflowBranchID(branch.BranchId), tag.WorkflowTreeID(branch.TreeId))

	return dataBlobs, token, dataSize, logger, nil
}

func (m *historyV2ManagerImpl) readHistoryBranch(
	byBatch bool,
	request *ReadHistoryBranchRequest,
) ([]*historypb.HistoryEvent, []*historypb.History, []byte, int, int64, int64, error) {

	dataBlobs, token, dataSize, logger, err := m.readRawHistoryBranch(request)
	if err != nil {
		return nil, nil, nil, 0, 0, 0, err
	}
	defaultLastEventID := request.MinEventID - 1

	historyEvents := make([]*historypb.HistoryEvent, 0, request.PageSize)
	historyEventBatches := make([]*historypb.History, 0, request.PageSize)
	// first_event_id of the last batch
	lastFirstEventID := common.EmptyEventID
	for _, batch := range dataBlobs {
		events, err := m.historySerializer.DeserializeEvents(batch)
		if err != nil {
			return historyEvents, historyEventBatches, nil, dataSize, lastFirstEventID, token.LastEventID, err
		}
		if len(events) == 0 {
			logger.Error("Empty events in a batch")
			return historyEvents, historyEventBatches, nil, dataSize, lastFirstEventID, token.LastEventID,
				serviceerror.NewInternal(fmt.Sprintf("corrupted history event batch, empty events"))
		}

		firstEvent := events[0]           // first
		eventCount := len(events)         // length
		lastEvent := events[eventCount-1] // last

		if firstEvent.GetVersion() != lastEvent.GetVersion() || firstEvent.GetEventId()+int64(eventCount-1) != lastEvent.GetEventId() {
			// in a single batch, version should be the same, and ID should be continuous
			logger.Error("Corrupted event batch",
				tag.FirstEventVersion(firstEvent.GetVersion()), tag.WorkflowFirstEventID(firstEvent.GetEventId()),
				tag.LastEventVersion(lastEvent.GetVersion()), tag.WorkflowNextEventID(lastEvent.GetEventId()),
				tag.Counter(eventCount))
			return historyEvents, historyEventBatches, nil, dataSize, lastFirstEventID, token.LastEventID,
				serviceerror.NewInternal(fmt.Sprintf("corrupted history event batch, wrong version and IDs"))
		}

		if firstEvent.GetVersion() < token.LastEventVersion {
			// version decrease means the this batch are all stale events, we should skip
			logger.Info("Stale event batch with smaller version", tag.FirstEventVersion(firstEvent.GetVersion()), tag.TokenLastEventVersion(token.LastEventVersion))
			continue
		}
		if firstEvent.GetEventId() <= token.LastEventID {
			// we could see it because first batch of next page has a smaller txn_id
			logger.Info("Stale event batch with eventID", tag.WorkflowFirstEventID(firstEvent.GetEventId()), tag.TokenLastEventID(token.LastEventID))
			continue
		}
		if firstEvent.GetEventId() != token.LastEventID+1 {
			// We assume application layer want to read from MinEventID(inclusive)
			// However, for getting history from remote cluster, there is scenario that we have to read from middle without knowing the firstEventID.
			// In that case we don't validate history continuousness for the first page
			// TODO: in this case, some events returned can be invalid(stale). application layer need to make sure it won't make any problems to XDC
			if defaultLastEventID == 0 || token.LastEventID != defaultLastEventID {
				logger.Error("Corrupted non-continuous event batch",
					tag.FirstEventVersion(firstEvent.GetVersion()), tag.WorkflowFirstEventID(firstEvent.GetEventId()),
					tag.LastEventVersion(lastEvent.GetVersion()), tag.WorkflowNextEventID(lastEvent.GetEventId()),
					tag.TokenLastEventVersion(token.LastEventVersion), tag.TokenLastEventID(token.LastEventID),
					tag.Counter(eventCount))
				return historyEvents, historyEventBatches, nil, dataSize, lastFirstEventID, token.LastEventID,
					serviceerror.NewInternal(fmt.Sprintf("corrupted history event batch, eventID is not continuous"))
			}
		}

		token.LastEventVersion = firstEvent.GetVersion()
		token.LastEventID = lastEvent.GetEventId()
		if byBatch {
			historyEventBatches = append(historyEventBatches, &historypb.History{Events: events})
		} else {
			historyEvents = append(historyEvents, events...)
		}
		lastFirstEventID = firstEvent.GetEventId()
	}

	nextPageToken, err := m.serializeToken(token)
	if err != nil {
		return nil, nil, nil, 0, 0, 0, err
	}

	return historyEvents, historyEventBatches, nextPageToken, dataSize, lastFirstEventID, token.LastEventID, nil
}

func (m *historyV2ManagerImpl) deserializeToken(
	token []byte,
	defaultLastEventID int64,
) (*historyV2PagingToken, error) {

	return m.pagingTokenSerializer.Deserialize(
		token,
		defaultLastEventID,
		common.EmptyVersion,
		defaultLastNodeID,
		defaultLastTransactionID,
	)
}

func (m *historyV2ManagerImpl) serializeToken(
	pagingToken *historyV2PagingToken,
) ([]byte, error) {

	if len(pagingToken.StoreToken) == 0 {
		if pagingToken.CurrentRangeIndex == pagingToken.FinalRangeIndex {
			// this means that we have reached the final page of final branchRange
			return nil, nil
		}

		pagingToken.CurrentRangeIndex++
		return m.pagingTokenSerializer.Serialize(pagingToken)
	}

	return m.pagingTokenSerializer.Serialize(pagingToken)
}

func (m *historyV2ManagerImpl) Close() {
	m.persistence.Close()
}
