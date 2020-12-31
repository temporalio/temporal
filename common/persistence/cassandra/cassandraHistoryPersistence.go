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

package cassandra

import (
	"fmt"
	"sort"

	"github.com/gocql/gocql"
	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
)

const (
	// below are templates for history_node table
	v2templateUpsertData = `INSERT INTO history_node (` +
		`tree_id, branch_id, node_id, txn_id, data, data_encoding) ` +
		`VALUES (?, ?, ?, ?, ?, ?) `

	v2templateReadData = `SELECT node_id, txn_id, data, data_encoding FROM history_node ` +
		`WHERE tree_id = ? AND branch_id = ? AND node_id >= ? AND node_id < ? `

	v2templateRangeDeleteData = `DELETE FROM history_node WHERE tree_id = ? AND branch_id = ? AND node_id >= ? `

	// below are templates for history_tree table
	v2templateInsertTree = `INSERT INTO history_tree (` +
		`tree_id, branch_id, branch, branch_encoding) ` +
		`VALUES (?, ?, ?, ?) `

	v2templateReadAllBranches = `SELECT branch_id, branch, branch_encoding FROM history_tree WHERE tree_id = ? `

	v2templateDeleteBranch = `DELETE FROM history_tree WHERE tree_id = ? AND branch_id = ? `

	v2templateScanAllTreeBranches = `SELECT tree_id, branch_id, branch, branch_encoding FROM history_tree `
)

type (
	cassandraHistoryV2Persistence struct {
		cassandraStore
	}
)

// NewHistoryV2PersistenceFromSession returns new HistoryStore
func NewHistoryV2PersistenceFromSession(
	session *gocql.Session,
	logger log.Logger,
) p.HistoryStore {

	return &cassandraHistoryV2Persistence{cassandraStore: cassandraStore{session: session, logger: logger}}
}

// newHistoryPersistence is used to create an instance of HistoryManager implementation
func newHistoryPersistence(
	session *gocql.Session,
	logger log.Logger,
) (p.HistoryStore, error) {
	return &cassandraHistoryV2Persistence{
		cassandraStore: cassandraStore{
			session: session,
			logger:  logger,
		},
	}, nil
}

// AppendHistoryNodes upsert a batch of events as a single node to a history branch
// Note that it's not allowed to append above the branch's ancestors' nodes, which means nodeID >= ForkNodeID
func (h *cassandraHistoryV2Persistence) AppendHistoryNodes(
	request *p.InternalAppendHistoryNodesRequest,
) error {

	branchInfo := request.BranchInfo
	beginNodeID := p.GetBeginNodeID(branchInfo)

	if request.NodeID < beginNodeID {
		return &p.InvalidPersistenceRequestError{
			Msg: fmt.Sprintf("cannot append to ancestors' nodes"),
		}
	}

	var err error
	if request.IsNewBranch {
		h.sortAncestors(branchInfo.Ancestors)
		treeInfoDataBlob, err := serialization.HistoryTreeInfoToBlob(&persistencespb.HistoryTreeInfo{
			BranchInfo: branchInfo,
			ForkTime:   timestamp.TimeNowPtrUtc(),
			Info:       request.Info,
		})

		if err != nil {
			return convertCommonErrors("AppendHistoryNodes", err)
		}

		batch := h.session.NewBatch(gocql.LoggedBatch)
		batch.Query(v2templateInsertTree,
			branchInfo.TreeId, branchInfo.BranchId, treeInfoDataBlob.Data, treeInfoDataBlob.EncodingType.String())
		batch.Query(v2templateUpsertData,
			branchInfo.TreeId, branchInfo.BranchId, request.NodeID, request.TransactionID, request.Events.Data, request.Events.EncodingType.String())
		err = h.session.ExecuteBatch(batch)
		h.logger.Info(fmt.Sprintf("####### BATCH QUERY: %v", prettyPrint(batch.Entries)), tag.Error(err))
	} else {
		query := h.session.Query(v2templateUpsertData,
			branchInfo.TreeId, branchInfo.BranchId, request.NodeID, request.TransactionID, request.Events.Data, request.Events.EncodingType.String())
		err = query.Exec()
	}

	if err != nil {
		return convertCommonErrors("AppendHistoryNodes", err)
	}
	return nil
}

// ReadHistoryBranch returns history node data for a branch
// NOTE: For branch that has ancestors, we need to query Cassandra multiple times, because it doesn't support OR/UNION operator
func (h *cassandraHistoryV2Persistence) ReadHistoryBranch(
	request *p.InternalReadHistoryBranchRequest,
) (*p.InternalReadHistoryBranchResponse, error) {

	treeID, err := primitives.ValidateUUID(request.TreeID)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ReadHistoryBranch - Gocql TreeId UUID cast failed. Error: %v", err))
	}

	branchID, err := primitives.ValidateUUID(request.BranchID)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ReadHistoryBranch - Gocql BranchId UUID cast failed. Error: %v", err))
	}

	lastNodeID := request.LastNodeID
	lastTxnID := request.LastTransactionID

	query := h.session.Query(v2templateReadData, treeID, branchID, request.MinNodeID, request.MaxNodeID)

	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()
	if iter == nil {
		return nil, serviceerror.NewInternal("ReadHistoryBranch operation failed.  Not able to create query iterator.")
	}
	pagingToken := iter.PageState()

	history := make([]*commonpb.DataBlob, 0, request.PageSize)

	for {
		var data []byte
		var encoding string
		nodeID := int64(0)
		txnID := int64(0)
		if !iter.Scan(&nodeID, &txnID, &data, &encoding) {
			break
		}
		if txnID < lastTxnID {
			// assuming that business logic layer is correct and transaction ID only increase
			// thus, valid event batch will come with increasing transaction ID

			// event batches with smaller node ID
			//  -> should not be possible since records are already sorted
			// event batches with same node ID
			//  -> batch with higher transaction ID is valid
			// event batches with larger node ID
			//  -> batch with lower transaction ID is invalid (happens before)
			//  -> batch with higher transaction ID is valid
			continue
		}

		switch {
		case nodeID < lastNodeID:
			return nil, serviceerror.NewInternal(fmt.Sprintf("corrupted data, nodeID cannot decrease"))
		case nodeID == lastNodeID:
			return nil, serviceerror.NewInternal(fmt.Sprintf("corrupted data, same nodeID must have smaller txnID"))
		default: // row.NodeID > lastNodeID:
			// NOTE: when row.nodeID > lastNodeID, we expect the one with largest txnID comes first
			lastTxnID = txnID
			lastNodeID = nodeID
			eventBlob := p.NewDataBlob(data, encoding)
			history = append(history, eventBlob)
		}
	}

	if err := iter.Close(); err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ReadHistoryBranch. Close operation failed. Error: %v", err))
	}

	return &p.InternalReadHistoryBranchResponse{
		History:           history,
		NextPageToken:     pagingToken,
		LastNodeID:        lastNodeID,
		LastTransactionID: lastTxnID,
	}, nil
}

// ForkHistoryBranch forks a new branch from an existing branch
// Note that application must provide a void forking nodeID, it must be a valid nodeID in that branch.
// A valid forking nodeID can be an ancestor from the existing branch.
// For example, we have branch B1 with three nodes(1[1,2], 3[3,4,5] and 6[6,7,8]. 1, 3 and 6 are nodeIDs (first eventID of the batch).
// So B1 looks like this:
//           1[1,2]
//           /
//         3[3,4,5]
//        /
//      6[6,7,8]
//
// Assuming we have branch B2 which contains one ancestor B1 stopping at 6 (exclusive). So B2 inherit nodeID 1 and 3 from B1, and have its own nodeID 6 and 8.
// Branch B2 looks like this:
//           1[1,2]
//           /
//         3[3,4,5]
//          \
//           6[6,7]
//           \
//            8[8]
//
// Now we want to fork a new branch B3 from B2.
// The only valid forking nodeIDs are 3,6 or 8.
// 1 is not valid because we can't fork from first node.
// 2/4/5 is NOT valid either because they are inside a batch.
//
// Case #1: If we fork from nodeID 6, then B3 will have an ancestor B1 which stops at 6(exclusive).
// As we append a batch of events[6,7,8,9] to B3, it will look like :
//           1[1,2]
//           /
//         3[3,4,5]
//          \
//         6[6,7,8,9]
//
// Case #2: If we fork from node 8, then B3 will have two ancestors: B1 stops at 6(exclusive) and ancestor B2 stops at 8(exclusive)
// As we append a batch of events[8,9] to B3, it will look like:
//           1[1,2]
//           /
//         3[3,4,5]
//        /
//      6[6,7]
//       \
//       8[8,9]
//
func (h *cassandraHistoryV2Persistence) ForkHistoryBranch(
	request *p.InternalForkHistoryBranchRequest,
) (*p.InternalForkHistoryBranchResponse, error) {

	forkB := request.ForkBranchInfo
	newAncestors := make([]*persistencespb.HistoryBranchRange, 0, len(forkB.Ancestors)+1)

	beginNodeID := p.GetBeginNodeID(forkB)
	if beginNodeID >= request.ForkNodeID {
		// this is the case that new branch's ancestors doesn't include the forking branch
		for _, br := range forkB.Ancestors {
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
		newAncestors = forkB.Ancestors
		newAncestors = append(newAncestors, &persistencespb.HistoryBranchRange{
			BranchId:    forkB.GetBranchId(),
			BeginNodeId: beginNodeID,
			EndNodeId:   request.ForkNodeID,
		})
	}

	hti := &persistencespb.HistoryTreeInfo{
		BranchInfo: &persistencespb.HistoryBranch{
			TreeId:    forkB.TreeId,
			BranchId:  request.NewBranchID,
			Ancestors: newAncestors,
		},
		ForkTime: timestamp.TimeNowPtrUtc(),
		Info:     request.Info,
	}

	datablob, err := serialization.HistoryTreeInfoToBlob(hti)
	if err != nil {
		return nil, err
	}

	cqlTreeID, err := primitives.ValidateUUID(forkB.TreeId)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ForkHistoryBranch - Gocql TreeId UUID cast failed. Error: %v", err))
	}

	cqlNewBranchID, err := primitives.ValidateUUID(request.NewBranchID)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ForkHistoryBranch - Gocql NewBranchID UUID cast failed. Error: %v", err))
	}
	query := h.session.Query(v2templateInsertTree, cqlTreeID, cqlNewBranchID, datablob.Data, datablob.EncodingType.String())
	err = query.Exec()
	if err != nil {
		return nil, convertCommonErrors("ForkHistoryBranch", err)
	}

	return &p.InternalForkHistoryBranchResponse{
		NewBranchInfo: hti.BranchInfo,
	}, nil
}

// DeleteHistoryBranch removes a branch
func (h *cassandraHistoryV2Persistence) DeleteHistoryBranch(
	request *p.InternalDeleteHistoryBranchRequest,
) error {

	branch := request.BranchInfo
	treeID := branch.TreeId

	brsToDelete := branch.Ancestors
	beginNodeID := p.GetBeginNodeID(branch)
	brsToDelete = append(brsToDelete, &persistencespb.HistoryBranchRange{
		BranchId:    branch.GetBranchId(),
		BeginNodeId: beginNodeID,
	})

	rsp, err := h.GetHistoryTree(&p.GetHistoryTreeRequest{
		TreeID: treeID,
	})
	if err != nil {
		return err
	}

	batch := h.session.NewBatch(gocql.LoggedBatch)
	batch.Query(v2templateDeleteBranch, treeID, branch.BranchId)

	// validBRsMaxEndNode is to know each branch range that is being used, we want to know what is the max nodeID referred by other valid branch
	validBRsMaxEndNode := map[string]int64{}
	for _, b := range rsp.Branches {
		for _, br := range b.Ancestors {
			// These string casts are safe and optimized away -- https://github.com/golang/go/issues/3512
			curr, ok := validBRsMaxEndNode[string(br.GetBranchId())]
			if !ok || curr < br.GetEndNodeId() {
				validBRsMaxEndNode[string(br.GetBranchId())] = br.GetEndNodeId()
			}
		}
	}

	// for each branch range to delete, we iterate from bottom to up, and delete up to the point according to validBRsEndNode
	for i := len(brsToDelete) - 1; i >= 0; i-- {
		br := brsToDelete[i]
		maxReferredEndNodeID, ok := validBRsMaxEndNode[string(br.GetBranchId())]
		if ok {
			// we can only delete from the maxEndNode and stop here
			h.deleteBranchRangeNodes(batch, treeID, br.GetBranchId(), maxReferredEndNodeID)
			break
		} else {
			// No any branch is using this range, we can delete all of it
			h.deleteBranchRangeNodes(batch, treeID, br.GetBranchId(), br.GetBeginNodeId())
		}
	}

	err = h.session.ExecuteBatch(batch)
	if err != nil {
		return convertCommonErrors("DeleteHistoryBranch", err)
	}
	return nil
}

func (h *cassandraHistoryV2Persistence) deleteBranchRangeNodes(
	batch *gocql.Batch,
	treeID string,
	branchID string,
	beginNodeID int64,
) {

	batch.Query(v2templateRangeDeleteData,
		treeID,
		branchID,
		beginNodeID)
}

func (h *cassandraHistoryV2Persistence) GetAllHistoryTreeBranches(
	request *p.GetAllHistoryTreeBranchesRequest,
) (*p.GetAllHistoryTreeBranchesResponse, error) {

	query := h.session.Query(v2templateScanAllTreeBranches)

	iter := query.PageSize(int(request.PageSize)).PageState(request.NextPageToken).Iter()
	if iter == nil {
		return nil, serviceerror.NewInternal("GetAllHistoryTreeBranches operation failed.  Not able to create query iterator.")
	}
	pagingToken := iter.PageState()

	branches := make([]p.HistoryBranchDetail, 0, request.PageSize)
	treeUUID := gocql.UUID{}
	branchUUID := gocql.UUID{}
	var data []byte
	var encoding string

	for iter.Scan(&treeUUID, &branchUUID, &data, &encoding) {
		hti, err := serialization.HistoryTreeInfoFromBlob(data, encoding)
		if err != nil {
			return nil, convertCommonErrors("GetAllHistoryTreeBranches", err)
		}

		branchDetail := p.HistoryBranchDetail{
			TreeID:   treeUUID.String(),
			BranchID: branchUUID.String(),
			ForkTime: hti.ForkTime,
			Info:     hti.Info,
		}
		branches = append(branches, branchDetail)
	}

	if err := iter.Close(); err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("GetAllHistoryTreeBranches. Close operation failed. Error: %v", err))
	}

	response := &p.GetAllHistoryTreeBranchesResponse{
		Branches:      branches,
		NextPageToken: pagingToken,
	}

	return response, nil
}

// GetHistoryTree returns all branch information of a tree
func (h *cassandraHistoryV2Persistence) GetHistoryTree(
	request *p.GetHistoryTreeRequest,
) (*p.GetHistoryTreeResponse, error) {

	treeID, err := primitives.ValidateUUID(request.TreeID)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ReadHistoryBranch. Gocql TreeId UUID cast failed. Error: %v", err))
	}
	query := h.session.Query(v2templateReadAllBranches, treeID)

	pageSize := 100
	pagingToken := []byte{}
	branches := make([]*persistencespb.HistoryBranch, 0, pageSize)

	var iter *gocql.Iter
	for {
		iter = query.PageSize(pageSize).PageState(pagingToken).Iter()
		if iter == nil {
			return nil, serviceerror.NewInternal("GetHistoryTree operation failed.  Not able to create query iterator.")
		}
		pagingToken = iter.PageState()

		branchUUID := gocql.UUID{}
		var data []byte
		var encoding string
		for iter.Scan(&branchUUID, &data, &encoding) {
			br, err := serialization.HistoryTreeInfoFromBlob(data, encoding)
			if err != nil {
				return nil, convertCommonErrors("GetHistoryTree", err)
			}

			branches = append(branches, br.BranchInfo)

			branchUUID = gocql.UUID{}
			data = []byte{}
			encoding = ""
		}

		if err := iter.Close(); err != nil {
			return nil, convertCommonErrors("GetHistoryTree", err)
		}

		if len(pagingToken) == 0 {
			break
		}
	}

	return &p.GetHistoryTreeResponse{
		Branches: branches,
	}, nil
}

func (h *cassandraHistoryV2Persistence) sortAncestors(
	ans []*persistencespb.HistoryBranchRange,
) {
	if len(ans) > 0 {
		// sort ans based onf EndNodeID so that we can set BeginNodeID
		sort.Slice(ans, func(i, j int) bool { return (ans)[i].GetEndNodeId() < (ans)[j].GetEndNodeId() })
		(ans)[0].BeginNodeId = int64(1)
		for i := 1; i < len(ans); i++ {
			(ans)[i].BeginNodeId = (ans)[i-1].GetEndNodeId()
		}
	}
}
