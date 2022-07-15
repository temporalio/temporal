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
	"context"
	"fmt"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/nosql/nosqlplugin/cassandra/gocql"
	"go.temporal.io/server/common/primitives"
)

const (
	// below are templates for history_node table
	v2templateUpsertHistoryNode = `INSERT INTO history_node (` +
		`tree_id, branch_id, node_id, prev_txn_id, txn_id, data, data_encoding) ` +
		`VALUES (?, ?, ?, ?, ?, ?, ?) `

	v2templateReadHistoryNode = `SELECT node_id, prev_txn_id, txn_id, data, data_encoding FROM history_node ` +
		`WHERE tree_id = ? AND branch_id = ? AND node_id >= ? AND node_id < ? `

	v2templateReadHistoryNodeReverse = `SELECT node_id, prev_txn_id, txn_id, data, data_encoding FROM history_node ` +
		`WHERE tree_id = ? AND branch_id = ? AND node_id >= ? AND node_id < ? ORDER BY branch_id DESC, node_id DESC `

	v2templateReadHistoryNodeMetadata = `SELECT node_id, prev_txn_id, txn_id FROM history_node ` +
		`WHERE tree_id = ? AND branch_id = ? AND node_id >= ? AND node_id < ? `

	v2templateDeleteHistoryNode = `DELETE FROM history_node WHERE tree_id = ? AND branch_id = ? AND node_id = ? AND txn_id = ? `

	v2templateRangeDeleteHistoryNode = `DELETE FROM history_node WHERE tree_id = ? AND branch_id = ? AND node_id >= ? `

	// below are templates for history_tree table
	v2templateInsertTree = `INSERT INTO history_tree (` +
		`tree_id, branch_id, branch, branch_encoding) ` +
		`VALUES (?, ?, ?, ?) `

	v2templateReadAllBranches = `SELECT branch_id, branch, branch_encoding FROM history_tree WHERE tree_id = ? `

	v2templateDeleteBranch = `DELETE FROM history_tree WHERE tree_id = ? AND branch_id = ? `

	v2templateScanAllTreeBranches = `SELECT tree_id, branch_id, branch, branch_encoding FROM history_tree `
)

type (
	HistoryStore struct {
		Session gocql.Session
		Logger  log.Logger
	}
)

func NewHistoryStore(
	session gocql.Session,
	logger log.Logger,
) *HistoryStore {
	return &HistoryStore{
		Session: session,
		Logger:  logger,
	}
}

// AppendHistoryNodes upsert a batch of events as a single node to a history branch
// Note that it's not allowed to append above the branch's ancestors' nodes, which means nodeID >= ForkNodeID
func (h *HistoryStore) AppendHistoryNodes(
	ctx context.Context,
	request *p.InternalAppendHistoryNodesRequest,
) error {
	branchInfo := request.BranchInfo
	node := request.Node

	if !request.IsNewBranch {
		query := h.Session.Query(v2templateUpsertHistoryNode,
			branchInfo.TreeId,
			branchInfo.BranchId,
			node.NodeID,
			node.PrevTransactionID,
			node.TransactionID,
			node.Events.Data,
			node.Events.EncodingType.String(),
		).WithContext(ctx)
		if err := query.Exec(); err != nil {
			return gocql.ConvertError("AppendHistoryNodes", err)
		}
		return nil
	}

	treeInfoDataBlob := request.TreeInfo
	batch := h.Session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	batch.Query(v2templateInsertTree,
		branchInfo.TreeId,
		branchInfo.BranchId,
		treeInfoDataBlob.Data,
		treeInfoDataBlob.EncodingType.String(),
	)
	batch.Query(v2templateUpsertHistoryNode,
		branchInfo.TreeId,
		branchInfo.BranchId,
		node.NodeID,
		node.PrevTransactionID,
		node.TransactionID,
		node.Events.Data,
		node.Events.EncodingType.String(),
	)
	if err := h.Session.ExecuteBatch(batch); err != nil {
		return gocql.ConvertError("AppendHistoryNodes", err)
	}
	return nil
}

// DeleteHistoryNodes delete a history node
func (h *HistoryStore) DeleteHistoryNodes(
	ctx context.Context,
	request *p.InternalDeleteHistoryNodesRequest,
) error {
	branchInfo := request.BranchInfo
	treeID := branchInfo.TreeId
	branchID := branchInfo.BranchId
	nodeID := request.NodeID
	txnID := request.TransactionID

	if nodeID < p.GetBeginNodeID(branchInfo) {
		return &p.InvalidPersistenceRequestError{
			Msg: "cannot delete from ancestors' nodes",
		}
	}

	query := h.Session.Query(v2templateDeleteHistoryNode,
		treeID,
		branchID,
		nodeID,
		txnID,
	).WithContext(ctx)
	if err := query.Exec(); err != nil {
		return gocql.ConvertError("DeleteHistoryNodes", err)
	}
	return nil
}

// ReadHistoryBranch returns history node data for a branch
// NOTE: For branch that has ancestors, we need to query Cassandra multiple times, because it doesn't support OR/UNION operator
func (h *HistoryStore) ReadHistoryBranch(
	ctx context.Context,
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

	var queryString string
	if request.MetadataOnly {
		queryString = v2templateReadHistoryNodeMetadata
	} else if request.ReverseOrder {
		queryString = v2templateReadHistoryNodeReverse
	} else {
		queryString = v2templateReadHistoryNode
	}

	query := h.Session.Query(queryString, treeID, branchID, request.MinNodeID, request.MaxNodeID).WithContext(ctx)

	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()
	var pagingToken []byte
	if len(iter.PageState()) > 0 {
		pagingToken = iter.PageState()
	}

	nodes := make([]p.InternalHistoryNode, 0, request.PageSize)
	message := make(map[string]interface{})
	for iter.MapScan(message) {
		nodes = append(nodes, convertHistoryNode(message))
		message = make(map[string]interface{})
	}

	if err := iter.Close(); err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("ReadHistoryBranch. Close operation failed. Error: %v", err))
	}

	return &p.InternalReadHistoryBranchResponse{
		Nodes:         nodes,
		NextPageToken: pagingToken,
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
func (h *HistoryStore) ForkHistoryBranch(
	ctx context.Context,
	request *p.InternalForkHistoryBranchRequest,
) error {

	forkB := request.ForkBranchInfo
	datablob := request.TreeInfo

	cqlTreeID, err := primitives.ValidateUUID(forkB.TreeId)
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("ForkHistoryBranch - Gocql TreeId UUID cast failed. Error: %v", err))
	}

	cqlNewBranchID, err := primitives.ValidateUUID(request.NewBranchID)
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("ForkHistoryBranch - Gocql NewBranchID UUID cast failed. Error: %v", err))
	}
	query := h.Session.Query(v2templateInsertTree, cqlTreeID, cqlNewBranchID, datablob.Data, datablob.EncodingType.String()).WithContext(ctx)
	err = query.Exec()
	if err != nil {
		return gocql.ConvertError("ForkHistoryBranch", err)
	}

	return nil
}

// DeleteHistoryBranch removes a branch
func (h *HistoryStore) DeleteHistoryBranch(
	ctx context.Context,
	request *p.InternalDeleteHistoryBranchRequest,
) error {
	batch := h.Session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	batch.Query(v2templateDeleteBranch, request.TreeId, request.BranchId)

	// delete each branch range
	for _, br := range request.BranchRanges {
		h.deleteBranchRangeNodes(batch, request.TreeId, br.BranchId, br.BeginNodeId)
	}

	err := h.Session.ExecuteBatch(batch)
	if err != nil {
		return gocql.ConvertError("DeleteHistoryBranch", err)
	}
	return nil
}

func (h *HistoryStore) deleteBranchRangeNodes(
	batch gocql.Batch,
	treeID string,
	branchID string,
	beginNodeID int64,
) {

	batch.Query(v2templateRangeDeleteHistoryNode,
		treeID,
		branchID,
		beginNodeID)
}

func (h *HistoryStore) GetAllHistoryTreeBranches(
	ctx context.Context,
	request *p.GetAllHistoryTreeBranchesRequest,
) (*p.InternalGetAllHistoryTreeBranchesResponse, error) {

	query := h.Session.Query(v2templateScanAllTreeBranches).WithContext(ctx)

	iter := query.PageSize(request.PageSize).PageState(request.NextPageToken).Iter()

	var pagingToken []byte
	if len(iter.PageState()) > 0 {
		pagingToken = iter.PageState()
	}

	branches := make([]p.InternalHistoryBranchDetail, 0, request.PageSize)
	treeUUID := ""
	branchUUID := ""
	var data []byte
	var encoding string

	for iter.Scan(&treeUUID, &branchUUID, &data, &encoding) {
		branch := p.InternalHistoryBranchDetail{
			TreeID:   treeUUID,
			BranchID: branchUUID,
			Data:     data,
			Encoding: encoding,
		}
		branches = append(branches, branch)

		treeUUID = ""
		branchUUID = ""
		data = nil
		encoding = ""
	}

	if err := iter.Close(); err != nil {
		return nil, serviceerror.NewUnavailable(fmt.Sprintf("GetAllHistoryTreeBranches. Close operation failed. Error: %v", err))
	}

	response := &p.InternalGetAllHistoryTreeBranchesResponse{
		Branches:      branches,
		NextPageToken: pagingToken,
	}

	return response, nil
}

// GetHistoryTree returns all branch information of a tree
func (h *HistoryStore) GetHistoryTree(
	ctx context.Context,
	request *p.GetHistoryTreeRequest,
) (*p.InternalGetHistoryTreeResponse, error) {

	treeID, err := primitives.ValidateUUID(request.TreeID)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ReadHistoryBranch. Gocql TreeId UUID cast failed. Error: %v", err))
	}
	query := h.Session.Query(v2templateReadAllBranches, treeID).WithContext(ctx)

	pageSize := 100
	var pagingToken []byte
	treeInfos := make([]*commonpb.DataBlob, 0, pageSize)

	var iter gocql.Iter
	for {
		iter = query.PageSize(pageSize).PageState(pagingToken).Iter()
		pagingToken = iter.PageState()

		branchUUID := ""
		var data []byte
		var encoding string
		for iter.Scan(&branchUUID, &data, &encoding) {
			treeInfos = append(treeInfos, p.NewDataBlob(data, encoding))

			branchUUID = ""
			data = []byte{}
			encoding = ""
		}

		if err := iter.Close(); err != nil {
			return nil, gocql.ConvertError("GetHistoryTree", err)
		}

		if len(pagingToken) == 0 {
			break
		}
	}

	return &p.InternalGetHistoryTreeResponse{
		TreeInfos: treeInfos,
	}, nil
}

func convertHistoryNode(
	message map[string]interface{},
) p.InternalHistoryNode {
	nodeID := message["node_id"].(int64)
	prevTxnID := message["prev_txn_id"].(int64)
	txnID := message["txn_id"].(int64)

	var data []byte
	var dataEncoding string
	if _, ok := message["data"]; ok {
		data = message["data"].([]byte)
		dataEncoding = message["data_encoding"].(string)
	}
	return p.InternalHistoryNode{
		NodeID:            nodeID,
		PrevTransactionID: prevTxnID,
		TransactionID:     txnID,
		Events:            p.NewDataBlob(data, dataEncoding),
	}
}
