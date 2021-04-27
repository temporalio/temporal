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
	"go.temporal.io/server/common/dynamicconfig"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence/serialization"
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

	// TrimHistoryBranch will only dump metadata, relatively cheap
	trimHistoryBranchPageSize = 1000
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

	req := &InternalForkHistoryBranchRequest{
		ForkBranchInfo: forkBranch,
		ForkNodeID:     request.ForkNodeID,
		NewBranchID:    uuid.New(),
		Info:           request.Info,
		ShardID:        request.ShardID,
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

	req := &InternalDeleteHistoryBranchRequest{
		BranchInfo: branch,
		ShardID:    request.ShardID,
	}

	return m.persistence.DeleteHistoryBranch(req)
}

// TrimHistoryBranch trims a branch
func (m *historyV2ManagerImpl) TrimHistoryBranch(
	request *TrimHistoryBranchRequest,
) (*TrimHistoryBranchResponse, error) {

	shardID := request.ShardID
	minNodeID := common.FirstEventID
	maxNodeID := request.NodeID + 1
	pageSize := trimHistoryBranchPageSize

	branch, err := serialization.HistoryBranchFromBlob(request.BranchToken, enumspb.ENCODING_TYPE_PROTO3.String())
	if err != nil {
		return nil, err
	}
	treeID := branch.TreeId
	branchID := branch.BranchId
	branchAncestors := branch.Ancestors

	// merge tree ID & branch ID into branch ancestors so the processing logic is simple
	beginNodeID := common.FirstEventID
	if len(branch.Ancestors) > 0 {
		beginNodeID = branch.Ancestors[len(branch.Ancestors)-1].GetEndNodeId()
	}
	branchAncestors = append(branchAncestors, &persistencespb.HistoryBranchRange{
		BranchId:    branchID,
		BeginNodeId: beginNodeID,
		EndNodeId:   maxNodeID,
	})

	var pageToken []byte
	transactionIDToNode := map[int64]historyNodeMetadata{}
	for doContinue := true; doContinue; doContinue = len(pageToken) > 0 {
		token, err := m.deserializeToken(pageToken, minNodeID-1)
		if err != nil {
			return nil, err
		}

		nodes, token, err := m.readRawHistoryBranch(
			shardID,
			treeID,
			branchAncestors,
			minNodeID,
			maxNodeID,
			token,
			pageSize,
			true,
		)
		if err != nil {
			return nil, err
		}

		branchID := branchAncestors[token.CurrentRangeIndex].BranchId
		for _, node := range nodes {
			transactionIDToNode[node.TransactionID] = historyNodeMetadata{
				branchInfo: &persistencespb.HistoryBranch{
					TreeId:    treeID,
					BranchId:  branchID,
					Ancestors: branchAncestors[0:token.CurrentRangeIndex],
				},
				nodeID:            node.NodeID,
				transactionID:     node.TransactionID,
				prevTransactionID: node.PrevTransactionID,
			}
		}

		pageToken, err = m.serializeToken(token)
		if err != nil {
			return nil, err
		}
	}

	nodesToTrim, err := validateNodeChainAndTrim(
		request.NodeID,
		request.TransactionID,
		transactionIDToNode,
	)
	if err != nil {
		m.logger.Debug("unable to trim history branch due to existing history node not fully onboarded", tag.Error(err))
		return &TrimHistoryBranchResponse{}, nil
	}

	for _, node := range nodesToTrim {
		if err := m.persistence.DeleteHistoryNodes(&InternalDeleteHistoryNodesRequest{
			ShardID:       shardID,
			BranchInfo:    node.branchInfo,
			NodeID:        node.nodeID,
			TransactionID: node.transactionID,
		}); err != nil {
			return nil, err
		}
	}

	return &TrimHistoryBranchResponse{}, nil
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

// AppendHistoryNodes add a node to history node table
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

	req := &InternalAppendHistoryNodesRequest{
		IsNewBranch: request.IsNewBranch,
		Info:        request.Info,
		BranchInfo:  branch,
		Node: InternalHistoryNode{
			NodeID:            nodeID,
			Events:            blob,
			PrevTransactionID: request.PrevTransactionID,
			TransactionID:     request.TransactionID,
		},
		ShardID: request.ShardID,
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
	_, resp.History, resp.NextPageToken, resp.Size, err = m.readHistoryBranch(true, request)
	return resp, err
}

// ReadHistoryBranch returns history node data for a branch
// Pagination is implemented here, the actual minNodeID passing to persistence layer is calculated along with token's LastNodeID
func (m *historyV2ManagerImpl) ReadHistoryBranch(
	request *ReadHistoryBranchRequest,
) (*ReadHistoryBranchResponse, error) {

	resp := &ReadHistoryBranchResponse{}
	var err error
	resp.HistoryEvents, _, resp.NextPageToken, resp.Size, err = m.readHistoryBranch(false, request)
	return resp, err
}

// ReadRawHistoryBranch returns raw history binary data for a branch
// Pagination is implemented here, the actual minNodeID passing to persistence layer is calculated along with token's LastNodeID
// NOTE: this API should only be used by 3+DC
func (m *historyV2ManagerImpl) ReadRawHistoryBranch(
	request *ReadHistoryBranchRequest,
) (*ReadRawHistoryBranchResponse, error) {

	dataBlobs, token, dataSize, err := m.readRawHistoryBranchAndFilter(request)
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
	shardID int32,
	treeID string,
	branchAncestors []*persistencespb.HistoryBranchRange,
	minNodeID int64,
	maxNodeID int64,
	token *historyV2PagingToken,
	pageSize int,
	metadataOnly bool,
) ([]InternalHistoryNode, *historyV2PagingToken, error) {

	if token.CurrentRangeIndex == notStartedIndex {
		for idx, br := range branchAncestors {
			// this range won't contain any nodes needed
			if minNodeID >= br.GetEndNodeId() {
				continue
			}
			// similarly, the ranges and the rest won't contain any nodes needed,
			if maxNodeID <= br.GetBeginNodeId() {
				break
			}

			if token.CurrentRangeIndex == notStartedIndex {
				token.CurrentRangeIndex = idx
			}
			token.FinalRangeIndex = idx
		}

		if token.CurrentRangeIndex == notStartedIndex {
			return nil, nil, serviceerror.NewDataLoss("branchRange is corrupted")
		}
	}

	currentBranch := branchAncestors[token.CurrentRangeIndex]
	// minNodeID remains the same, since caller can read from the middle
	// maxNodeID need to be shortened since this branch can contain additional history nodes
	maxNodeID = currentBranch.GetEndNodeId()
	branchID := currentBranch.GetBranchId()
	resp, err := m.persistence.ReadHistoryBranch(&InternalReadHistoryBranchRequest{
		ShardID:       shardID,
		TreeID:        treeID,
		BranchID:      branchID,
		MinNodeID:     minNodeID,
		MaxNodeID:     maxNodeID,
		NextPageToken: token.StoreToken,
		PageSize:      pageSize,
		MetadataOnly:  metadataOnly,
	})
	if err != nil {
		return nil, nil, err
	}
	token.StoreToken = resp.NextPageToken
	return resp.Nodes, token, nil
}

func (m *historyV2ManagerImpl) readRawHistoryBranchAndFilter(
	request *ReadHistoryBranchRequest,
) ([]*commonpb.DataBlob, *historyV2PagingToken, int, error) {

	shardID := request.ShardID
	branchToken := request.BranchToken
	minNodeID := request.MinEventID
	maxNodeID := request.MaxEventID

	branch, err := serialization.HistoryBranchFromBlob(branchToken, enumspb.ENCODING_TYPE_PROTO3.String())
	if err != nil {
		return nil, nil, 0, err
	}
	treeID := branch.TreeId
	branchID := branch.BranchId
	branchAncestors := branch.Ancestors

	// merge tree ID & branch ID into branch ancestors so the processing logic is simple
	beginNodeID := common.FirstEventID
	if len(branch.Ancestors) > 0 {
		beginNodeID = branch.Ancestors[len(branch.Ancestors)-1].GetEndNodeId()
	}
	branchAncestors = append(branchAncestors, &persistencespb.HistoryBranchRange{
		BranchId:    branchID,
		BeginNodeId: beginNodeID,
		EndNodeId:   maxNodeID,
	})

	token, err := m.deserializeToken(
		request.NextPageToken,
		request.MinEventID-1,
	)
	if err != nil {
		return nil, nil, 0, err
	}

	nodes, token, err := m.readRawHistoryBranch(
		shardID,
		treeID,
		branchAncestors,
		minNodeID,
		maxNodeID,
		token,
		request.PageSize,
		false,
	)
	if err != nil {
		return nil, nil, 0, err
	}
	if len(nodes) == 0 && len(request.NextPageToken) == 0 {
		return nil, nil, 0, serviceerror.NewNotFound("Workflow execution history not found.")
	}

	nodes, err = m.filterHistoryNodes(
		token.LastNodeID,
		token.LastTransactionID,
		nodes,
	)
	if err != nil {
		return nil, nil, 0, err
	}

	var dataBlobs []*commonpb.DataBlob
	dataSize := 0
	if len(nodes) > 0 {
		dataBlobs = make([]*commonpb.DataBlob, len(nodes))
		for index, node := range nodes {
			dataBlobs[index] = node.Events
			dataSize += len(node.Events.Data)
		}
		lastNode := nodes[len(nodes)-1]
		token.LastNodeID = lastNode.NodeID
		token.LastTransactionID = lastNode.TransactionID
	}

	return dataBlobs, token, dataSize, nil
}

func (m *historyV2ManagerImpl) readHistoryBranch(
	byBatch bool,
	request *ReadHistoryBranchRequest,
) ([]*historypb.HistoryEvent, []*historypb.History, []byte, int, error) {

	dataBlobs, token, dataSize, err := m.readRawHistoryBranchAndFilter(request)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	historyEvents := make([]*historypb.HistoryEvent, 0, request.PageSize)
	historyEventBatches := make([]*historypb.History, 0, request.PageSize)

	for _, batch := range dataBlobs {
		events, err := m.historySerializer.DeserializeEvents(batch)
		if err != nil {
			return historyEvents, historyEventBatches, nil, dataSize, err
		}
		if len(events) == 0 {
			m.logger.Error("Empty events in a batch")
			return historyEvents, historyEventBatches, nil, dataSize, serviceerror.NewDataLoss(fmt.Sprintf("corrupted history event batch, empty events"))
		}

		firstEvent := events[0]           // first
		eventCount := len(events)         // length
		lastEvent := events[eventCount-1] // last

		if firstEvent.GetVersion() != lastEvent.GetVersion() || firstEvent.GetEventId()+int64(eventCount-1) != lastEvent.GetEventId() {
			// in a single batch, version should be the same, and ID should be contiguous
			m.logger.Error("Corrupted event batch",
				tag.FirstEventVersion(firstEvent.GetVersion()), tag.WorkflowFirstEventID(firstEvent.GetEventId()),
				tag.LastEventVersion(lastEvent.GetVersion()), tag.WorkflowNextEventID(lastEvent.GetEventId()),
				tag.Counter(eventCount))
			return historyEvents, historyEventBatches, nil, dataSize, serviceerror.NewDataLoss("corrupted history event batch, wrong version and IDs")
		}
		if firstEvent.GetEventId() != token.LastEventID+1 {
			m.logger.Error("Corrupted non-contiguous event batch",
				tag.WorkflowFirstEventID(firstEvent.GetEventId()),
				tag.WorkflowNextEventID(lastEvent.GetEventId()),
				tag.TokenLastEventID(token.LastEventID),
				tag.Counter(eventCount))
			return historyEvents, historyEventBatches, nil, dataSize, serviceerror.NewDataLoss("corrupted history event batch, eventID is not contiguous")
		}

		if byBatch {
			historyEventBatches = append(historyEventBatches, &historypb.History{Events: events})
		} else {
			historyEvents = append(historyEvents, events...)
		}
		token.LastEventID = lastEvent.GetEventId()
	}

	nextPageToken, err := m.serializeToken(token)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	return historyEvents, historyEventBatches, nextPageToken, dataSize, nil
}

func (m *historyV2ManagerImpl) filterHistoryNodes(
	lastNodeID int64,
	lastTransactionID int64,
	nodes []InternalHistoryNode,
) ([]InternalHistoryNode, error) {
	var result []InternalHistoryNode
	for _, node := range nodes {
		// assuming that business logic layer is correct and transaction ID only increase
		// thus, valid event batch will come with increasing transaction ID

		// event batches with smaller node ID
		//  -> should not be possible since records are already sorted
		// event batches with same node ID
		//  -> batch with higher transaction ID is valid
		// event batches with larger node ID
		//  -> batch with lower transaction ID is invalid (happens before)
		//  -> batch with higher transaction ID is valid
		if node.TransactionID < lastTransactionID {
			continue
		}

		switch {
		case node.NodeID < lastNodeID:
			return nil, serviceerror.NewInternal(fmt.Sprintf("corrupted data, nodeID cannot decrease"))
		case node.NodeID == lastNodeID:
			return nil, serviceerror.NewInternal(fmt.Sprintf("corrupted data, same nodeID must have smaller txnID"))
		default: // row.NodeID > lastNodeID:
			// NOTE: when row.nodeID > lastNodeID, we expect the one with largest txnID comes first
			lastTransactionID = node.TransactionID
			lastNodeID = node.NodeID
			result = append(result, node)
		}
	}
	return result, nil
}

func (m *historyV2ManagerImpl) deserializeToken(
	token []byte,
	defaultLastEventID int64,
) (*historyV2PagingToken, error) {

	return m.pagingTokenSerializer.Deserialize(
		token,
		defaultLastEventID,
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
