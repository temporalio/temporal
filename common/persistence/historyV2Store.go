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

	"github.com/pborman/uuid"
	"github.com/uber-common/bark"
	workflow "github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/codec"
	"github.com/uber/cadence/common/logging"
)

type (

	// historyManagerImpl implements HistoryManager based on HistoryStore and HistorySerializer
	historyV2ManagerImpl struct {
		historySerializer     HistorySerializer
		persistence           HistoryV2Store
		logger                bark.Logger
		thrifteEncoder        codec.BinaryEncoder
		pagingTokenSerializer *jsonHistoryTokenSerializer
	}
)

var _ HistoryV2Manager = (*historyV2ManagerImpl)(nil)

//NewHistoryV2ManagerImpl returns new HistoryManager
func NewHistoryV2ManagerImpl(persistence HistoryV2Store, logger bark.Logger) HistoryV2Manager {
	return &historyV2ManagerImpl{
		historySerializer:     NewHistorySerializer(),
		persistence:           persistence,
		logger:                logger,
		thrifteEncoder:        codec.NewThriftRWEncoder(),
		pagingTokenSerializer: newJSONHistoryTokenSerializer(),
	}
}

func (m *historyV2ManagerImpl) GetName() string {
	return m.persistence.GetName()
}

// NewHistoryBranch creates a new branch from tree root. If tree doesn't exist, then create one. Return error if the branch already exists.
func (m *historyV2ManagerImpl) NewHistoryBranch(request *NewHistoryBranchRequest) (*NewHistoryBranchResponse, error) {
	req := &InternalNewHistoryBranchRequest{
		TreeID:   request.TreeID,
		BranchID: uuid.New(),
	}
	resp, err := m.persistence.NewHistoryBranch(req)
	response := &NewHistoryBranchResponse{}
	if err != nil {
		return response, err
	}
	token, err := m.thrifteEncoder.Encode(&resp.BranchInfo)
	if err != nil {
		return response, err
	}
	return &NewHistoryBranchResponse{
		BranchToken: token,
	}, nil
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

	// We read the forking node to validate the forking point.
	// This is mostly correctly. But in some very rarely corner case, it can be incorrect because of corrupted history:
	//		It is possible that the node we read exists, but it is a stale batch of events. Forking incorrectly can cause corrupted history.
	// Therefore the safest way is to read from very beginning to validate the forking point. But it will be very complex and inefficient.
	// Talking to team we can start with this implementation.
	// If a customer run into this bug, we can do another reset(fork) to correct it.
	readReq := &ReadHistoryBranchRequest{
		BranchToken: request.ForkBranchToken,
		MinEventID:  request.ForkNodeID,
		MaxEventID:  request.ForkNodeID + 1,
		PageSize:    1,
	}
	readResp, err := m.ReadHistoryBranch(readReq)
	if err != nil {
		return nil, err
	}
	if len(readResp.History) != 1 {
		return nil, &InvalidPersistenceRequestError{
			Msg: fmt.Sprintf("ForkNodeID is invalid: %v for %+v", request.ForkNodeID, forkBranch),
		}
	}

	req := &InternalForkHistoryBranchRequest{
		ForkBranchInfo: forkBranch,
		ForkNodeID:     request.ForkNodeID,
		NewBranchID:    uuid.New(),
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

	req := &InternalDeleteHistoryBranchRequest{
		BranchInfo: branch,
	}

	return m.persistence.DeleteHistoryBranch(req)
}

// GetHistoryTree returns all branch information of a tree
func (m *historyV2ManagerImpl) GetHistoryTree(request *GetHistoryTreeRequest) (*GetHistoryTreeResponse, error) {
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

	req := &InternalAppendHistoryNodesRequest{
		BranchInfo:    branch,
		NodeID:        nodeID,
		Events:        blob,
		TransactionID: request.TransactionID,
	}

	err = m.persistence.AppendHistoryNodes(req)

	return &AppendHistoryNodesResponse{
		Size: size,
	}, err
}

// ReadHistoryBranch returns history node data for a branch
// Pagination is implemented here, the actual minNodeID passing to persistence layer is calculated along with token's LastNodeID
func (m *historyV2ManagerImpl) ReadHistoryBranch(request *ReadHistoryBranchRequest) (*ReadHistoryBranchResponse, error) {
	var branch workflow.HistoryBranch
	err := m.thrifteEncoder.Decode(request.BranchToken, &branch)
	if err != nil {
		return nil, err
	}
	treeID := *branch.TreeID
	branchID := *branch.BranchID

	if request.PageSize <= 0 || request.MinEventID >= request.MaxEventID {
		return nil, &InvalidPersistenceRequestError{
			Msg: fmt.Sprintf("no events can be found for pageSize %v, minEventID %v, maxEventID: %v", request.PageSize, request.MinEventID, request.MaxEventID),
		}
	}

	token, err := m.pagingTokenSerializer.Deserialize(request.NextPageToken, request.MinEventID-1, request.LastEventVersion)
	if err != nil {
		return nil, err
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
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("branchRange is corrupted"),
			}
		}
	}

	minNodeID := token.LastEventID + 1
	maxNodeID := *allBRs[token.CurrentRangeIndex].EndNodeID
	if request.MaxEventID < maxNodeID {
		maxNodeID = request.MaxEventID
	}

	req := &InternalReadHistoryBranchRequest{
		TreeID:        treeID,
		BranchID:      *allBRs[token.CurrentRangeIndex].BranchID,
		MinNodeID:     minNodeID,
		MaxNodeID:     maxNodeID,
		PageSize:      request.PageSize,
		NextPageToken: token.StoreToken,
	}

	resp, err := m.persistence.ReadHistoryBranch(req)
	if err != nil {
		return nil, err
	}

	events := make([]*workflow.HistoryEvent, 0, request.PageSize)
	dataSize := 0

	//NOTE: in this method, we need to make sure eventVersion is NOT decreasing(otherwise we skip the events), eventID should be continuous(otherwise return error)
	logger := m.logger.WithFields(bark.Fields{
		logging.TagBranchID: *branch.BranchID,
		logging.TagTreeID:   *branch.TreeID,
	})

	for _, b := range resp.History {
		es, err := m.historySerializer.DeserializeBatchEvents(b)
		if err != nil {
			return nil, err
		}
		if len(es) == 0 {
			logger.Error("Empty events in a batch")
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("corrupted history event batch, empty events"),
			}
		}

		fe := es[0]    // first
		el := len(es)  // length
		le := es[el-1] // last

		if *fe.Version != *le.Version || *fe.EventId+int64(el-1) != *le.EventId {
			// in a single batch, version should be the same, and ID should be continous
			logger.Errorf("Corrupted event batch, %v, %v, %v, %v, %v", *fe.Version, *le.Version, *fe.EventId, *le.EventId, el)
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("corrupted history event batch, wrong version and IDs"),
			}
		}

		if *fe.Version < token.LastEventVersion {
			// version decrease means the this batch are all stale events, we should skip
			logger.Infof("Stale event batch with version: %v", *fe.Version)
			continue
		}

		if *fe.EventId == token.LastEventID {
			// we could see it because of batch with smaller txn_id
			logger.Infof("Stale event batch with eventID: %v", *fe.EventId)
			continue
		}
		if *fe.EventId != token.LastEventID+1 {
			logger.Errorf("Corrupted incontinouous event batch, %v, %v, %v, %v, %v", *fe.Version, *le.Version, *fe.EventId, *le.EventId, el)
			return nil, &workflow.InternalServiceError{
				Message: fmt.Sprintf("corrupted history event batch, eventID is not continouous"),
			}
		}

		token.LastEventVersion = *fe.Version
		token.LastEventID = *le.EventId
		events = append(events, es...)
		dataSize += len(b.Data)
	}

	response := &ReadHistoryBranchResponse{
		History: events,
		Size:    dataSize,
	}

	if len(token.StoreToken) == 0 {
		if token.CurrentRangeIndex == token.FinalRangeIndex {
			// this means that we have reached the final page of final branchRange
			response.NextPageToken = nil
		} else {
			token.CurrentRangeIndex++
			token.StoreToken = nil
			response.NextPageToken, err = m.pagingTokenSerializer.Serialize(token)
			if err != nil {
				return nil, err
			}
		}
	} else {
		token.StoreToken = resp.NextPageToken
		response.NextPageToken, err = m.pagingTokenSerializer.Serialize(token)
		if err != nil {
			return nil, err
		}
	}

	return response, nil
}

func (m *historyV2ManagerImpl) Close() {
	m.persistence.Close()
}
