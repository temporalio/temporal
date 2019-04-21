// Copyright (c) 2017 Uber Technologies, Inc.
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
	"github.com/uber/cadence/common/log"
	"github.com/uber/cadence/common/log/tag"

	"github.com/pborman/uuid"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/codec"
)

type (

	// historyManagerImpl implements HistoryManager based on HistoryStore and PayloadSerializer
	historyV2ManagerImpl struct {
		historySerializer     PayloadSerializer
		persistence           HistoryV2Store
		logger                log.Logger
		thrifteEncoder        codec.BinaryEncoder
		pagingTokenSerializer *jsonHistoryTokenSerializer
	}
)

var _ HistoryV2Manager = (*historyV2ManagerImpl)(nil)

//NewHistoryV2ManagerImpl returns new HistoryManager
func NewHistoryV2ManagerImpl(persistence HistoryV2Store, logger log.Logger) HistoryV2Manager {
	return &historyV2ManagerImpl{
		historySerializer:     NewPayloadSerializer(),
		persistence:           persistence,
		logger:                logger,
		thrifteEncoder:        codec.NewThriftRWEncoder(),
		pagingTokenSerializer: newJSONHistoryTokenSerializer(),
	}
}

func (m *historyV2ManagerImpl) GetName() string {
	return m.persistence.GetName()
}

// ForkHistoryBranch forks a new branch from a old branch
func (m *historyV2ManagerImpl) ForkHistoryBranch(request *ForkHistoryBranchRequest) (*ForkHistoryBranchResponse, error) {
	if request.ForkNodeID <= 1 {
		return nil, &InvalidPersistenceRequestError{
			Msg: fmt.Sprintf("ForkNodeID must be > 1"),
		}
	}

	var forkBranch workflow.HistoryBranch
	err := m.thrifteEncoder.Decode(request.ForkBranchToken, &forkBranch)
	if err != nil {
		return nil, err
	}
	shardID, err := getShardID(request.ShardID)
	if err != nil {
		return nil, &workflow.InternalServiceError{
			Message: err.Error(),
		}
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

	token, err := m.thrifteEncoder.Encode(&resp.NewBranchInfo)
	if err != nil {
		return nil, err
	}

	return &ForkHistoryBranchResponse{
		NewBranchToken: token,
	}, nil
}

// DeleteHistoryBranch removes a branch
func (m *historyV2ManagerImpl) DeleteHistoryBranch(request *DeleteHistoryBranchRequest) error {
	var branch workflow.HistoryBranch
	err := m.thrifteEncoder.Decode(request.BranchToken, &branch)
	if err != nil {
		return err
	}

	shardID, err := getShardID(request.ShardID)
	if err != nil {
		m.logger.Error("shardID is not set in delete history operation", tag.Error(err))
		return &workflow.InternalServiceError{
			Message: err.Error(),
		}
	}
	req := &InternalDeleteHistoryBranchRequest{
		BranchInfo: branch,
		ShardID:    shardID,
	}

	return m.persistence.DeleteHistoryBranch(req)
}

// CompleteForkBranch complete the forking process
func (m *historyV2ManagerImpl) CompleteForkBranch(request *CompleteForkBranchRequest) error {
	var branch workflow.HistoryBranch
	err := m.thrifteEncoder.Decode(request.BranchToken, &branch)
	if err != nil {
		return err
	}

	shardID, err := getShardID(request.ShardID)
	if err != nil {
		m.logger.Error("shardID is not set in complete fork branch operation", tag.Error(err))
		return &workflow.InternalServiceError{
			Message: err.Error(),
		}
	}
	req := &InternalCompleteForkBranchRequest{
		BranchInfo: branch,
		Success:    request.Success,
		ShardID:    shardID,
	}

	return m.persistence.CompleteForkBranch(req)
}

// GetHistoryTree returns all branch information of a tree
func (m *historyV2ManagerImpl) GetHistoryTree(request *GetHistoryTreeRequest) (*GetHistoryTreeResponse, error) {
	if len(request.TreeID) == 0 {
		var branch workflow.HistoryBranch
		err := m.thrifteEncoder.Decode(request.BranchToken, &branch)
		if err != nil {
			return nil, err
		}
		request.TreeID = branch.GetTreeID()
	}
	return m.persistence.GetHistoryTree(request)
}

// AppendHistoryNodes add(or override) a node to a history branch
func (m *historyV2ManagerImpl) AppendHistoryNodes(request *AppendHistoryNodesRequest) (*AppendHistoryNodesResponse, error) {
	var branch workflow.HistoryBranch
	err := m.thrifteEncoder.Decode(request.BranchToken, &branch)
	if err != nil {
		return nil, err
	}
	if len(request.Events) == 0 {
		return nil, &InvalidPersistenceRequestError{
			Msg: fmt.Sprintf("events to be appended cannot be empty"),
		}
	}
	version := *request.Events[0].Version
	nodeID := *request.Events[0].EventId
	lastID := nodeID - 1

	if nodeID <= 0 {
		return nil, &InvalidPersistenceRequestError{
			Msg: fmt.Sprintf("eventID cannot be less than 1"),
		}
	}
	for _, e := range request.Events {
		if *e.Version != version {
			return nil, &InvalidPersistenceRequestError{
				Msg: fmt.Sprintf("event version must be the same inside a batch"),
			}
		}
		if *e.EventId != lastID+1 {
			return nil, &InvalidPersistenceRequestError{
				Msg: fmt.Sprintf("event ID must be continous"),
			}
		}
		lastID++
	}

	// nodeID will be the first eventID
	blob, err := m.historySerializer.SerializeBatchEvents(request.Events, request.Encoding)
	size := len(blob.Data)

	shardID, err := getShardID(request.ShardID)
	if err != nil {
		m.logger.Error("shardID is not set in append history nodes operation", tag.Error(err))
		return nil, &workflow.InternalServiceError{
			Message: err.Error(),
		}
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
func (m *historyV2ManagerImpl) ReadHistoryBranchByBatch(request *ReadHistoryBranchRequest) (*ReadHistoryBranchByBatchResponse, error) {
	resp := &ReadHistoryBranchByBatchResponse{}
	var err error
	_, resp.History, resp.NextPageToken, resp.Size, resp.LastFirstEventID, err = m.readHistoryBranch(true, request)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// ReadHistoryBranch returns history node data for a branch
// Pagination is implemented here, the actual minNodeID passing to persistence layer is calculated along with token's LastNodeID
func (m *historyV2ManagerImpl) ReadHistoryBranch(request *ReadHistoryBranchRequest) (*ReadHistoryBranchResponse, error) {
	resp := &ReadHistoryBranchResponse{}
	var err error
	resp.HistoryEvents, _, resp.NextPageToken, resp.Size, resp.LastFirstEventID, err = m.readHistoryBranch(false, request)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func (m *historyV2ManagerImpl) readHistoryBranch(byBatch bool, request *ReadHistoryBranchRequest) ([]*workflow.HistoryEvent, []*workflow.History, []byte, int, int64, error) {
	var branch workflow.HistoryBranch
	err := m.thrifteEncoder.Decode(request.BranchToken, &branch)
	if err != nil {
		return nil, nil, nil, 0, 0, err
	}
	treeID := *branch.TreeID
	branchID := *branch.BranchID

	if request.PageSize <= 0 || request.MinEventID >= request.MaxEventID {
		return nil, nil, nil, 0, 0, &InvalidPersistenceRequestError{
			Msg: fmt.Sprintf("no events can be found for pageSize %v, minEventID %v, maxEventID: %v", request.PageSize, request.MinEventID, request.MaxEventID),
		}
	}

	defaultLastEventID := request.MinEventID - 1
	token, err := m.pagingTokenSerializer.Deserialize(request.NextPageToken, defaultLastEventID, common.EmptyVersion)
	if err != nil {
		return nil, nil, nil, 0, 0, err
	}

	allBRs := branch.Ancestors
	// We may also query the current branch from beginNodeID
	beginNodeID := int64(1)
	if len(branch.Ancestors) > 0 {
		beginNodeID = *branch.Ancestors[len(branch.Ancestors)-1].EndNodeID
	}
	allBRs = append(allBRs, &workflow.HistoryBranchRange{
		BranchID:    &branchID,
		BeginNodeID: common.Int64Ptr(beginNodeID),
		EndNodeID:   common.Int64Ptr(request.MaxEventID),
	})

	if token.CurrentRangeIndex == notStartedIndex {
		for idx, br := range allBRs {
			// this range won't contain any nodes needed
			if request.MinEventID >= *br.EndNodeID {
				continue
			}
			// similarly, the ranges and the rest won't contain any nodes needed,
			if request.MaxEventID <= *br.BeginNodeID {
				break
			}

			if token.CurrentRangeIndex == notStartedIndex {
				token.CurrentRangeIndex = idx
			}
			token.FinalRangeIndex = idx
		}

		if token.CurrentRangeIndex == notStartedIndex {
			return nil, nil, nil, 0, 0, &workflow.InternalServiceError{
				Message: fmt.Sprintf("branchRange is corrupted"),
			}
		}
	}

	maxNodeID := *allBRs[token.CurrentRangeIndex].EndNodeID
	if request.MaxEventID < maxNodeID {
		maxNodeID = request.MaxEventID
	}

	shardID, err := getShardID(request.ShardID)
	if err != nil {
		m.logger.Error("shardID is not set in read history branch operation", tag.Error(err))
		return nil, nil, nil, 0, 0, &workflow.InternalServiceError{
			Message: err.Error(),
		}
	}
	req := &InternalReadHistoryBranchRequest{
		TreeID:        treeID,
		BranchID:      *allBRs[token.CurrentRangeIndex].BranchID,
		MinNodeID:     request.MinEventID,
		MaxNodeID:     maxNodeID,
		PageSize:      request.PageSize,
		NextPageToken: token.StoreToken,
		ShardID:       shardID,
	}

	resp, err := m.persistence.ReadHistoryBranch(req)
	if err != nil {
		return nil, nil, nil, 0, 0, err
	}
	if len(resp.History) == 0 && len(request.NextPageToken) == 0 {
		return nil, nil, nil, 0, 0, &workflow.EntityNotExistsError{Message: "Workflow execution history not found."}
	}

	events := make([]*workflow.HistoryEvent, 0, request.PageSize)
	historyBatches := make([]*workflow.History, 0, request.PageSize)
	dataSize := 0
	// first_event_id of the last batch
	lastFirstEventID := common.EmptyEventID

	//NOTE: in this method, we need to make sure eventVersion is NOT decreasing(otherwise we skip the events), eventID should be continuous(otherwise return error)
	logger := m.logger.WithTags(tag.WorkflowBranchID(*branch.BranchID), tag.WorkflowTreeID(*branch.TreeID))

	for _, b := range resp.History {
		es, err := m.historySerializer.DeserializeBatchEvents(b)
		if err != nil {
			return nil, nil, nil, 0, 0, err
		}
		if len(es) == 0 {
			logger.Error("Empty events in a batch")
			return nil, nil, nil, 0, 0, &workflow.InternalServiceError{
				Message: fmt.Sprintf("corrupted history event batch, empty events"),
			}
		}

		firstEvent := es[0]           // first
		eventCount := len(es)         // length
		lastEvent := es[eventCount-1] // last

		if firstEvent.GetVersion() != lastEvent.GetVersion() || firstEvent.GetEventId()+int64(eventCount-1) != lastEvent.GetEventId() {
			// in a single batch, version should be the same, and ID should be continous
			logger.Error("Corrupted event batch",
				tag.FirstEventVersion(firstEvent.GetVersion()), tag.WorkflowFirstEventID(firstEvent.GetEventId()),
				tag.LastEventVersion(lastEvent.GetVersion()), tag.WorkflowNextEventID(lastEvent.GetEventId()),
				tag.Counter(eventCount))
			return nil, nil, nil, 0, 0, &workflow.InternalServiceError{
				Message: fmt.Sprintf("corrupted history event batch, wrong version and IDs"),
			}
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
				logger.Error("Corrupted incontinouous event batch",
					tag.FirstEventVersion(firstEvent.GetVersion()), tag.WorkflowFirstEventID(firstEvent.GetEventId()),
					tag.LastEventVersion(lastEvent.GetVersion()), tag.WorkflowNextEventID(lastEvent.GetEventId()),
					tag.TokenLastEventVersion(token.LastEventVersion), tag.TokenLastEventID(token.LastEventID),
					tag.Counter(eventCount))
				return nil, nil, nil, 0, 0, &workflow.InternalServiceError{
					Message: fmt.Sprintf("corrupted history event batch, eventID is not continouous"),
				}
			}
		}

		token.LastEventVersion = firstEvent.GetVersion()
		token.LastEventID = lastEvent.GetEventId()
		if byBatch {
			historyBatches = append(historyBatches, &workflow.History{Events: es})
		} else {
			events = append(events, es...)
		}
		dataSize += len(b.Data)
		lastFirstEventID = firstEvent.GetEventId()
	}

	var nextToken []byte
	if len(resp.NextPageToken) == 0 {
		if token.CurrentRangeIndex == token.FinalRangeIndex {
			// this means that we have reached the final page of final branchRange
			nextToken = nil
		} else {
			token.CurrentRangeIndex++
			token.StoreToken = nil
			nextToken, err = m.pagingTokenSerializer.Serialize(token)
			if err != nil {
				return nil, nil, nil, 0, 0, err
			}
		}
	} else {
		token.StoreToken = resp.NextPageToken
		nextToken, err = m.pagingTokenSerializer.Serialize(token)
		if err != nil {
			return nil, nil, nil, 0, 0, err
		}
	}

	return events, historyBatches, nextToken, dataSize, lastFirstEventID, nil
}

func (m *historyV2ManagerImpl) Close() {
	m.persistence.Close()
}
