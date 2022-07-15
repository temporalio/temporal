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
	"context"
	"fmt"

	"github.com/pborman/uuid"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	historypb "go.temporal.io/api/history/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives/timestamp"
)

const (
	defaultLastNodeID        = common.FirstEventID - 1
	defaultLastTransactionID = int64(0)

	// TrimHistoryBranch will only dump metadata, relatively cheap
	trimHistoryBranchPageSize = 1000
)

var _ ExecutionManager = (*executionManagerImpl)(nil)

// ForkHistoryBranch forks a new branch from a old branch
func (m *executionManagerImpl) ForkHistoryBranch(
	ctx context.Context,
	request *ForkHistoryBranchRequest,
) (*ForkHistoryBranchResponse, error) {

	if request.ForkNodeID <= 1 {
		return nil, &InvalidPersistenceRequestError{
			Msg: "ForkNodeID must be > 1",
		}
	}

	forkBranch, err := m.serializer.HistoryBranchFromBlob(&commonpb.DataBlob{
		Data:         request.ForkBranchToken,
		EncodingType: enumspb.ENCODING_TYPE_PROTO3,
	})
	if err != nil {
		return nil, err
	}

	newAncestors := make([]*persistencespb.HistoryBranchRange, 0, len(forkBranch.Ancestors)+1)

	beginNodeID := GetBeginNodeID(forkBranch)
	if beginNodeID >= request.ForkNodeID {
		// this is the case that new branch's ancestors doesn't include the forking branch
		for _, br := range forkBranch.Ancestors {
			if br.GetEndNodeId() >= request.ForkNodeID {
				newAncestors = append(newAncestors, &persistencespb.HistoryBranchRange{
					BranchId:    br.GetBranchId(),
					BeginNodeId: br.GetBeginNodeId(),
					EndNodeId:   request.ForkNodeID,
				})
				break
			} else {
				newAncestors = append(newAncestors, br)
			}
		}
	} else {
		// this is the case the new branch will inherit all ancestors from forking branch
		newAncestors = forkBranch.Ancestors
		newAncestors = append(newAncestors, &persistencespb.HistoryBranchRange{
			BranchId:    forkBranch.GetBranchId(),
			BeginNodeId: beginNodeID,
			EndNodeId:   request.ForkNodeID,
		})
	}
	newBranchInfo := &persistencespb.HistoryBranch{
		TreeId:    forkBranch.TreeId,
		BranchId:  uuid.New(),
		Ancestors: newAncestors,
	}
	treeInfo := &persistencespb.HistoryTreeInfo{
		BranchInfo: newBranchInfo,
		ForkTime:   timestamp.TimeNowPtrUtc(),
		Info:       request.Info,
	}

	treeInfoBlob, err := m.serializer.HistoryTreeInfoToBlob(treeInfo, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return nil, err
	}

	req := &InternalForkHistoryBranchRequest{
		ForkBranchInfo: forkBranch,
		TreeInfo:       treeInfoBlob,
		ForkNodeID:     request.ForkNodeID,
		NewBranchID:    newBranchInfo.BranchId,
		Info:           request.Info,
		ShardID:        request.ShardID,
	}

	err = m.persistence.ForkHistoryBranch(ctx, req)
	if err != nil {
		return nil, err
	}

	branchInfoBlob, err := m.serializer.HistoryBranchToBlob(newBranchInfo, enumspb.ENCODING_TYPE_PROTO3)
	if err != nil {
		return nil, err
	}

	return &ForkHistoryBranchResponse{
		NewBranchToken: branchInfoBlob.Data,
	}, nil
}

// DeleteHistoryBranch removes a branch
func (m *executionManagerImpl) DeleteHistoryBranch(
	ctx context.Context,
	request *DeleteHistoryBranchRequest,
) error {

	branch, err := serialization.HistoryBranchFromBlob(request.BranchToken, enumspb.ENCODING_TYPE_PROTO3.String())
	if err != nil {
		return err
	}

	// We need to delete the target branch and its ancestors if they are not referenced by any other branches.
	// However, it is possible that part of the target branch (or its ancestors) is used as ancestors by other branch.
	// We need to avoid deleting those referenced parts. This is similar to reference count in garbage collection.
	brsToDelete := branch.Ancestors
	brsToDelete = append(brsToDelete, &persistencespb.HistoryBranchRange{
		BranchId:    branch.GetBranchId(),
		BeginNodeId: GetBeginNodeID(branch),
	})

	// Get the entire history tree, so we know if any part of the target branch is referenced by other branches.
	historyTreeResp, err := m.GetHistoryTree(context.TODO(), &GetHistoryTreeRequest{
		TreeID:      branch.TreeId,
		ShardID:     &request.ShardID,
		BranchToken: request.BranchToken,
	})
	if err != nil {
		return err
	}

	// usedBranches record branches referenced by others
	usedBranches := map[string]int64{}
	for _, br := range historyTreeResp.Branches {
		if br.BranchId == branch.BranchId {
			// skip the target branch
			continue
		}
		usedBranches[br.BranchId] = common.LastEventID
		for _, ancestor := range br.Ancestors {
			if curr, ok := usedBranches[ancestor.GetBranchId()]; !ok || curr < ancestor.GetEndNodeId() {
				usedBranches[ancestor.GetBranchId()] = ancestor.GetEndNodeId()
			}
		}
	}

	var deleteRanges []InternalDeleteHistoryBranchRange
	// for each branch range to delete, we iterate from bottom up, and stop when the range is also used by others
findDeleteRanges:
	for i := len(brsToDelete) - 1; i >= 0; i-- {
		br := brsToDelete[i]
		if maxEndNode, ok := usedBranches[br.GetBranchId()]; ok {
			// branch is used by others, we can only delete from the maxEndNode
			if maxEndNode != common.LastEventID {
				deleteRanges = append(deleteRanges, InternalDeleteHistoryBranchRange{
					BranchId:    br.BranchId,
					BeginNodeId: maxEndNode,
				})
			}
			// all ancestors are also used, no need to go up further,
			break findDeleteRanges
		} else {
			// No other branch is using this range, we can delete all of it
			deleteRanges = append(deleteRanges, InternalDeleteHistoryBranchRange{
				BranchId:    br.BranchId,
				BeginNodeId: br.BeginNodeId,
			})
		}
	}

	req := &InternalDeleteHistoryBranchRequest{
		ShardID:      request.ShardID,
		TreeId:       branch.TreeId,
		BranchId:     branch.BranchId,
		BranchRanges: deleteRanges,
	}
	return m.persistence.DeleteHistoryBranch(ctx, req)
}

// TrimHistoryBranch trims a branch
func (m *executionManagerImpl) TrimHistoryBranch(
	ctx context.Context,
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
		token, err := m.deserializeToken(pageToken, minNodeID-1, defaultLastTransactionID)
		if err != nil {
			return nil, err
		}

		nodes, token, err := m.readRawHistoryBranch(
			ctx,
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

		pageToken, err = m.serializeToken(token, false)
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
		if err := m.persistence.DeleteHistoryNodes(ctx, &InternalDeleteHistoryNodesRequest{
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
func (m *executionManagerImpl) GetHistoryTree(
	ctx context.Context,
	request *GetHistoryTreeRequest,
) (*GetHistoryTreeResponse, error) {

	if len(request.TreeID) == 0 {
		branch, err := serialization.HistoryBranchFromBlob(request.BranchToken, enumspb.ENCODING_TYPE_PROTO3.String())
		if err != nil {
			return nil, err
		}
		request.TreeID = branch.GetTreeId()
	}
	resp, err := m.persistence.GetHistoryTree(ctx, request)
	if err != nil {
		return nil, err
	}
	branches := make([]*persistencespb.HistoryBranch, 0, len(resp.TreeInfos))
	for _, blob := range resp.TreeInfos {
		br, err := m.serializer.HistoryTreeInfoFromBlob(NewDataBlob(blob.Data, blob.EncodingType.String()))
		if err != nil {
			return nil, err
		}
		branches = append(branches, br.BranchInfo)
	}
	return &GetHistoryTreeResponse{Branches: branches}, nil
}

func (m *executionManagerImpl) serializeAppendHistoryNodesRequest(
	request *AppendHistoryNodesRequest,
) (*InternalAppendHistoryNodesRequest, error) {
	branch, err := m.serializer.HistoryBranchFromBlob(&commonpb.DataBlob{Data: request.BranchToken, EncodingType: enumspb.ENCODING_TYPE_PROTO3})
	if err != nil {
		return nil, err
	}

	if len(request.Events) == 0 {
		return nil, &InvalidPersistenceRequestError{
			Msg: "events to be appended cannot be empty",
		}
	}
	sortAncestors(branch.Ancestors)

	version := request.Events[0].Version
	nodeID := request.Events[0].EventId
	lastID := nodeID - 1

	if nodeID <= 0 {
		return nil, &InvalidPersistenceRequestError{
			Msg: "eventID cannot be less than 1",
		}
	}
	for _, e := range request.Events {
		if e.Version != version {
			return nil, &InvalidPersistenceRequestError{
				Msg: "event version must be the same inside a batch",
			}
		}
		if e.EventId != lastID+1 {
			return nil, &InvalidPersistenceRequestError{
				Msg: "event ID must be continous",
			}
		}
		lastID++
	}

	// nodeID will be the first eventID
	blob, err := m.serializer.SerializeEvents(request.Events, enumspb.ENCODING_TYPE_PROTO3)
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

	if req.IsNewBranch {
		// TreeInfo is only needed for new branch
		treeInfoBlob, err := m.serializer.HistoryTreeInfoToBlob(&persistencespb.HistoryTreeInfo{
			BranchInfo: branch,
			ForkTime:   timestamp.TimeNowPtrUtc(),
			Info:       request.Info,
		}, enumspb.ENCODING_TYPE_PROTO3)
		if err != nil {
			return nil, err
		}
		req.TreeInfo = treeInfoBlob
	}

	if nodeID < GetBeginNodeID(branch) {
		return nil, &InvalidPersistenceRequestError{
			Msg: "cannot append to ancestors' nodes",
		}
	}

	return req, nil
}

func (m *executionManagerImpl) serializeAppendRawHistoryNodesRequest(
	request *AppendRawHistoryNodesRequest,
) (*InternalAppendHistoryNodesRequest, error) {
	branch, err := m.serializer.HistoryBranchFromBlob(&commonpb.DataBlob{Data: request.BranchToken, EncodingType: enumspb.ENCODING_TYPE_PROTO3})
	if err != nil {
		return nil, err
	}

	if len(request.History.Data) == 0 {
		return nil, &InvalidPersistenceRequestError{
			Msg: "events to be appended cannot be empty",
		}
	}
	sortAncestors(branch.Ancestors)

	nodeID := request.NodeID
	if nodeID <= 0 {
		return nil, &InvalidPersistenceRequestError{
			Msg: "eventID cannot be less than 1",
		}
	}
	// nodeID will be the first eventID
	size := len(request.History.Data)
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
			Events:            request.History,
			PrevTransactionID: request.PrevTransactionID,
			TransactionID:     request.TransactionID,
		},
		ShardID: request.ShardID,
	}

	if req.IsNewBranch {
		// TreeInfo is only needed for new branch
		treeInfoBlob, err := m.serializer.HistoryTreeInfoToBlob(&persistencespb.HistoryTreeInfo{
			BranchInfo: branch,
			ForkTime:   timestamp.TimeNowPtrUtc(),
			Info:       request.Info,
		}, enumspb.ENCODING_TYPE_PROTO3)
		if err != nil {
			return nil, err
		}
		req.TreeInfo = treeInfoBlob
	}

	if nodeID < GetBeginNodeID(branch) {
		return nil, &InvalidPersistenceRequestError{
			Msg: "cannot append to ancestors' nodes",
		}
	}

	return req, nil
}

// AppendHistoryNodes add a node to history node table
func (m *executionManagerImpl) AppendHistoryNodes(
	ctx context.Context,
	request *AppendHistoryNodesRequest,
) (*AppendHistoryNodesResponse, error) {

	req, err := m.serializeAppendHistoryNodesRequest(request)

	if err != nil {
		return nil, err
	}

	err = m.persistence.AppendHistoryNodes(ctx, req)

	return &AppendHistoryNodesResponse{
		Size: len(req.Node.Events.Data),
	}, err
}

// AppendRawHistoryNodes add raw history nodes to history node table
func (m *executionManagerImpl) AppendRawHistoryNodes(
	ctx context.Context,
	request *AppendRawHistoryNodesRequest,
) (*AppendHistoryNodesResponse, error) {

	req, err := m.serializeAppendRawHistoryNodesRequest(request)
	if err != nil {
		return nil, err
	}

	err = m.persistence.AppendHistoryNodes(ctx, req)
	return &AppendHistoryNodesResponse{
		Size: len(request.History.Data),
	}, err
}

// ReadHistoryBranchByBatch returns history node data for a branch by batch
// Pagination is implemented here, the actual minNodeID passing to persistence layer is calculated along with token's LastNodeID
func (m *executionManagerImpl) ReadHistoryBranchByBatch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadHistoryBranchByBatchResponse, error) {

	resp := &ReadHistoryBranchByBatchResponse{}
	var err error
	_, resp.History, resp.TransactionIDs, resp.NextPageToken, resp.Size, err = m.readHistoryBranch(ctx, true, request)
	return resp, err
}

// ReadHistoryBranch returns history node data for a branch
// Pagination is implemented here, the actual minNodeID passing to persistence layer is calculated along with token's LastNodeID
func (m *executionManagerImpl) ReadHistoryBranch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadHistoryBranchResponse, error) {

	resp := &ReadHistoryBranchResponse{}
	var err error
	resp.HistoryEvents, _, _, resp.NextPageToken, resp.Size, err = m.readHistoryBranch(ctx, false, request)
	return resp, err
}

// ReadRawHistoryBranch returns raw history binary data for a branch
// Pagination is implemented here, the actual minNodeID passing to persistence layer is calculated along with token's LastNodeID
// NOTE: this API should only be used by 3+DC
func (m *executionManagerImpl) ReadRawHistoryBranch(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) (*ReadRawHistoryBranchResponse, error) {

	dataBlobs, _, nodeIDs, token, dataSize, err := m.readRawHistoryBranchAndFilter(ctx, request)
	if err != nil {
		return nil, err
	}

	nextPageToken, err := m.serializeToken(token, false)
	if err != nil {
		return nil, err
	}

	return &ReadRawHistoryBranchResponse{
		HistoryEventBlobs: dataBlobs,
		NodeIDs:           nodeIDs,
		NextPageToken:     nextPageToken,
		Size:              dataSize,
	}, nil
}

// ReadHistoryBranchReverse returns history node data for a branch
// Pagination is implemented here, the actual minNodeID passing to persistence layer is calculated along with token's LastNodeID
func (m *executionManagerImpl) ReadHistoryBranchReverse(
	ctx context.Context,
	request *ReadHistoryBranchReverseRequest,
) (*ReadHistoryBranchReverseResponse, error) {
	resp := &ReadHistoryBranchReverseResponse{}
	var err error
	resp.HistoryEvents, _, resp.NextPageToken, resp.Size, err = m.readHistoryBranchReverse(ctx, request)
	return resp, err
}

func (m *executionManagerImpl) GetAllHistoryTreeBranches(
	ctx context.Context,
	request *GetAllHistoryTreeBranchesRequest,
) (*GetAllHistoryTreeBranchesResponse, error) {
	resp, err := m.persistence.GetAllHistoryTreeBranches(ctx, request)
	if err != nil {
		return nil, err
	}
	branches := make([]HistoryBranchDetail, 0, len(resp.Branches))
	for _, branch := range resp.Branches {
		blob := NewDataBlob(branch.Data, branch.Encoding)
		treeInfo, err := m.serializer.HistoryTreeInfoFromBlob(blob)
		if err != nil {
			return nil, err
		}

		branchDetail := HistoryBranchDetail{
			TreeID:   branch.TreeID,
			BranchID: branch.BranchID,
			ForkTime: treeInfo.ForkTime,
			Info:     treeInfo.Info,
		}
		branches = append(branches, branchDetail)
	}

	return &GetAllHistoryTreeBranchesResponse{
		NextPageToken: resp.NextPageToken,
		Branches:      branches,
	}, nil
}

func (m *executionManagerImpl) readRawHistoryBranch(
	ctx context.Context,
	shardID int32,
	treeID string,
	branchAncestors []*persistencespb.HistoryBranchRange,
	minNodeID int64,
	maxNodeID int64,
	token *historyPagingToken,
	pageSize int,
	metadataOnly bool,
) ([]InternalHistoryNode, *historyPagingToken, error) {

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
	if currentBranch.GetEndNodeId() < maxNodeID {
		maxNodeID = currentBranch.GetEndNodeId()
	}
	branchID := currentBranch.GetBranchId()
	resp, err := m.persistence.ReadHistoryBranch(ctx, &InternalReadHistoryBranchRequest{
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

func (m *executionManagerImpl) readRawHistoryBranchReverse(
	ctx context.Context,
	shardID int32,
	treeID string,
	branchAncestors []*persistencespb.HistoryBranchRange,
	minNodeID int64,
	maxNodeID int64,
	token *historyPagingToken,
	pageSize int,
	metadataOnly bool,
) ([]InternalHistoryNode, *historyPagingToken, error) {
	if token.CurrentRangeIndex == notStartedIndex {
		for i := range branchAncestors {
			idx := len(branchAncestors) - 1 - i
			br := branchAncestors[idx]
			// Skip branches that don't have relevant nodes
			if maxNodeID <= br.GetBeginNodeId() {
				continue
			}
			if minNodeID >= br.GetEndNodeId() {
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
	if currentBranch.GetEndNodeId() < maxNodeID {
		maxNodeID = currentBranch.GetEndNodeId()
	}
	branchID := currentBranch.GetBranchId()

	resp, err := m.persistence.ReadHistoryBranch(ctx, &InternalReadHistoryBranchRequest{
		ShardID:       shardID,
		TreeID:        treeID,
		BranchID:      branchID,
		MinNodeID:     minNodeID,
		MaxNodeID:     maxNodeID,
		NextPageToken: token.StoreToken,
		PageSize:      pageSize,
		MetadataOnly:  metadataOnly,
		ReverseOrder:  true,
	})
	if err != nil {
		return nil, nil, err
	}
	token.StoreToken = resp.NextPageToken
	return resp.Nodes, token, nil
}

func (m *executionManagerImpl) readRawHistoryBranchAndFilter(
	ctx context.Context,
	request *ReadHistoryBranchRequest,
) ([]*commonpb.DataBlob, []int64, []int64, *historyPagingToken, int, error) {

	shardID := request.ShardID
	branchToken := request.BranchToken
	minNodeID := request.MinEventID
	maxNodeID := request.MaxEventID

	branch, err := serialization.HistoryBranchFromBlob(branchToken, enumspb.ENCODING_TYPE_PROTO3.String())
	if err != nil {
		return nil, nil, nil, nil, 0, err
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
		defaultLastTransactionID,
	)
	if err != nil {
		return nil, nil, nil, nil, 0, err
	}

	nodes, token, err := m.readRawHistoryBranch(
		ctx,
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
		return nil, nil, nil, nil, 0, err
	}
	if len(nodes) == 0 && len(request.NextPageToken) == 0 {
		return nil, nil, nil, nil, 0, serviceerror.NewNotFound("Workflow execution history not found.")
	}

	nodes, err = m.filterHistoryNodes(
		token.LastNodeID,
		token.LastTransactionID,
		nodes,
	)
	if err != nil {
		return nil, nil, nil, nil, 0, err
	}

	var dataBlobs []*commonpb.DataBlob
	transactionIDs := make([]int64, 0, len(nodes))
	nodeIDs := make([]int64, 0, len(nodes))
	dataSize := 0
	if len(nodes) > 0 {
		dataBlobs = make([]*commonpb.DataBlob, len(nodes))
		for index, node := range nodes {
			dataBlobs[index] = node.Events
			dataSize += len(node.Events.Data)
			transactionIDs = append(transactionIDs, node.TransactionID)
			nodeIDs = append(nodeIDs, node.NodeID)
		}
		lastNode := nodes[len(nodes)-1]
		token.LastNodeID = lastNode.NodeID
		token.LastTransactionID = lastNode.TransactionID
	}

	return dataBlobs, transactionIDs, nodeIDs, token, dataSize, nil
}

func (m *executionManagerImpl) readRawHistoryBranchReverseAndFilter(
	ctx context.Context,
	request *ReadHistoryBranchReverseRequest,
) ([]*commonpb.DataBlob, []int64, *historyPagingToken, int, error) {

	shardID := request.ShardID
	branchToken := request.BranchToken
	minNodeID := common.FirstEventID
	maxNodeID := request.MaxEventID
	if maxNodeID == common.EmptyEventID {
		maxNodeID = common.EndEventID
	} else {
		maxNodeID++ // downstream code is exclusive on maxNodeID
	}

	branch, err := serialization.HistoryBranchFromBlob(branchToken, enumspb.ENCODING_TYPE_PROTO3.String())
	if err != nil {
		return nil, nil, nil, 0, err
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
		request.MaxEventID,
		request.LastFirstTransactionID,
	)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	nodes, token, err := m.readRawHistoryBranchReverse(
		ctx,
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
		return nil, nil, nil, 0, err
	}
	if len(nodes) == 0 && len(request.NextPageToken) == 0 {
		return nil, nil, nil, 0, serviceerror.NewNotFound("Workflow execution history not found.")
	}

	nodes, err = m.filterHistoryNodesReverse(
		token.LastNodeID,
		token.LastTransactionID,
		nodes,
	)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	var dataBlobs []*commonpb.DataBlob
	transactionIDs := make([]int64, 0, len(nodes))
	dataSize := 0
	if len(nodes) > 0 {
		dataBlobs = make([]*commonpb.DataBlob, len(nodes))
		for index, node := range nodes {
			dataBlobs[index] = node.Events
			dataSize += len(node.Events.Data)
			transactionIDs = append(transactionIDs, node.TransactionID)
		}
		lastNode := nodes[len(nodes)-1]
		token.LastNodeID = lastNode.NodeID
		token.LastTransactionID = lastNode.PrevTransactionID
	}

	return dataBlobs, transactionIDs, token, dataSize, nil
}

func (m *executionManagerImpl) readHistoryBranch(
	ctx context.Context,
	byBatch bool,
	request *ReadHistoryBranchRequest,
) ([]*historypb.HistoryEvent, []*historypb.History, []int64, []byte, int, error) {

	dataBlobs, transactionIDs, _, token, dataSize, err := m.readRawHistoryBranchAndFilter(ctx, request)
	if err != nil {
		return nil, nil, nil, nil, 0, err
	}

	historyEvents := make([]*historypb.HistoryEvent, 0, request.PageSize)
	historyEventBatches := make([]*historypb.History, 0, request.PageSize)

	for _, batch := range dataBlobs {
		events, err := m.serializer.DeserializeEvents(batch)
		if err != nil {
			return nil, nil, nil, nil, dataSize, err
		}
		if len(events) == 0 {
			m.logger.Error("Empty events in a batch")
			return nil, nil, nil, nil, dataSize, serviceerror.NewDataLoss("corrupted history event batch, empty events")
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
			return historyEvents, historyEventBatches, transactionIDs, nil, dataSize, serviceerror.NewDataLoss("corrupted history event batch, wrong version and IDs")
		}
		if firstEvent.GetEventId() != token.LastEventID+1 {
			m.logger.Error("Corrupted non-contiguous event batch",
				tag.WorkflowFirstEventID(firstEvent.GetEventId()),
				tag.WorkflowNextEventID(lastEvent.GetEventId()),
				tag.TokenLastEventID(token.LastEventID),
				tag.Counter(eventCount))
			return historyEvents, historyEventBatches, transactionIDs, nil, dataSize, serviceerror.NewDataLoss("corrupted history event batch, eventID is not contiguous")
		}

		if byBatch {
			historyEventBatches = append(historyEventBatches, &historypb.History{Events: events})
		} else {
			historyEvents = append(historyEvents, events...)
		}
		token.LastEventID = lastEvent.GetEventId()
	}

	nextPageToken, err := m.serializeToken(token, false)
	if err != nil {
		return nil, nil, nil, nil, 0, err
	}
	return historyEvents, historyEventBatches, transactionIDs, nextPageToken, dataSize, nil
}

func (m *executionManagerImpl) readHistoryBranchReverse(
	ctx context.Context,
	request *ReadHistoryBranchReverseRequest,
) ([]*historypb.HistoryEvent, []int64, []byte, int, error) {

	dataBlobs, transactionIDs, token, dataSize, err := m.readRawHistoryBranchReverseAndFilter(ctx, request)
	if err != nil {
		return nil, nil, nil, 0, err
	}

	historyEvents := make([]*historypb.HistoryEvent, 0, request.PageSize)

	for _, batch := range dataBlobs {
		events, err := m.serializer.DeserializeEvents(batch)
		if err != nil {
			return nil, nil, nil, dataSize, err
		}
		if len(events) == 0 {
			m.logger.Error("Empty events in a batch")
			return nil, nil, nil, dataSize, serviceerror.NewDataLoss("corrupted history event batch, empty events")
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
			return historyEvents, transactionIDs, nil, dataSize, serviceerror.NewDataLoss("corrupted history event batch, wrong version and IDs")
		}
		if (token.LastEventID != common.EmptyEventID) && (lastEvent.GetEventId() != token.LastEventID-1) {
			m.logger.Error("Corrupted non-contiguous event batch",
				tag.WorkflowFirstEventID(firstEvent.GetEventId()),
				tag.WorkflowNextEventID(lastEvent.GetEventId()),
				tag.TokenLastEventID(token.LastEventID),
				tag.Counter(eventCount))
			return historyEvents, transactionIDs, nil, dataSize, serviceerror.NewDataLoss("corrupted history event batch, eventID is not contiguous")
		}

		events = m.reverseSlice(events)

		historyEvents = append(historyEvents, events...)
		token.LastEventID = firstEvent.GetEventId()
	}

	nextPageToken, err := m.serializeToken(token, true)
	if err != nil {
		return nil, nil, nil, 0, err
	}
	return historyEvents, transactionIDs, nextPageToken, dataSize, nil
}

func (m *executionManagerImpl) reverseSlice(events []*historypb.HistoryEvent) []*historypb.HistoryEvent {
	for i, j := 0, len(events)-1; i < j; i, j = i+1, j-1 {
		events[i], events[j] = events[j], events[i]
	}
	return events
}

func (m *executionManagerImpl) filterHistoryNodes(
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
			return nil, serviceerror.NewUnavailable("corrupted data, nodeID cannot decrease")
		case node.NodeID == lastNodeID:
			return nil, serviceerror.NewUnavailable("corrupted data, same nodeID must have smaller txnID")
		default: // row.NodeID > lastNodeID:
			// NOTE: when row.nodeID > lastNodeID, we expect the one with largest txnID comes first
			lastTransactionID = node.TransactionID
			lastNodeID = node.NodeID
			result = append(result, node)
		}
	}
	return result, nil
}

func (m *executionManagerImpl) filterHistoryNodesReverse(
	lastNodeID int64,
	lastTransactionID int64,
	nodes []InternalHistoryNode,
) ([]InternalHistoryNode, error) {
	var result []InternalHistoryNode
	for _, node := range nodes {
		if lastNodeID == defaultLastNodeID {
			lastNodeID = node.NodeID
		}
		if node.TransactionID != lastTransactionID {
			continue
		}

		switch {
		case node.NodeID > lastNodeID:
			return nil, serviceerror.NewUnavailable("corrupted data, nodeID cannot decrease")
		default:
			lastTransactionID = node.PrevTransactionID
			lastNodeID = node.NodeID
			result = append(result, node)
		}
	}
	return result, nil
}

func (m *executionManagerImpl) deserializeToken(
	token []byte,
	defaultLastEventID int64,
	lastTransactionId int64,
) (*historyPagingToken, error) {

	return m.pagingTokenSerializer.Deserialize(
		token,
		defaultLastEventID,
		defaultLastNodeID,
		lastTransactionId,
	)
}

func (m *executionManagerImpl) serializeToken(
	pagingToken *historyPagingToken,
	reverseOrder bool,
) ([]byte, error) {

	if len(pagingToken.StoreToken) == 0 {
		if pagingToken.CurrentRangeIndex == pagingToken.FinalRangeIndex {
			// this means that we have reached the final page of final branchRange
			return nil, nil
		}

		if reverseOrder {
			pagingToken.CurrentRangeIndex--

		} else {
			pagingToken.CurrentRangeIndex++
		}
		return m.pagingTokenSerializer.Serialize(pagingToken)
	}

	return m.pagingTokenSerializer.Serialize(pagingToken)
}
