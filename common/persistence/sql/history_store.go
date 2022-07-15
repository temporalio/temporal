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

package sql

import (
	"context"
	"database/sql"
	"fmt"
	"math"

	commonpb "go.temporal.io/api/common/v1"
	"go.temporal.io/api/serviceerror"

	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
)

const (
	// NOTE: transaction ID is *= -1 in DB
	MinTxnID int64 = math.MaxInt64
	MaxTxnID int64 = math.MinInt64 + 1 // int overflow
)

// AppendHistoryNodes add(or override) a node to a history branch
func (m *sqlExecutionStore) AppendHistoryNodes(
	ctx context.Context,
	request *p.InternalAppendHistoryNodesRequest,
) error {
	branchInfo := request.BranchInfo
	node := request.Node

	treeIDBytes, err := primitives.ParseUUID(branchInfo.GetTreeId())
	if err != nil {
		return err
	}
	branchIDBytes, err := primitives.ParseUUID(branchInfo.GetBranchId())
	if err != nil {
		return err
	}

	nodeRow := &sqlplugin.HistoryNodeRow{
		TreeID:       treeIDBytes,
		BranchID:     branchIDBytes,
		NodeID:       node.NodeID,
		PrevTxnID:    node.PrevTransactionID,
		TxnID:        node.TransactionID,
		Data:         node.Events.Data,
		DataEncoding: node.Events.EncodingType.String(),
		ShardID:      request.ShardID,
	}

	if !request.IsNewBranch {
		_, err = m.Db.InsertIntoHistoryNode(ctx, nodeRow)
		if err != nil {
			if m.Db.IsDupEntryError(err) {
				return &p.ConditionFailedError{Msg: fmt.Sprintf("AppendHistoryNodes: row already exist: %v", err)}
			}
			return serviceerror.NewUnavailable(fmt.Sprintf("AppendHistoryNodes: %v", err))
		}
		return nil
	}

	treeInfoBlob := request.TreeInfo
	treeRow := &sqlplugin.HistoryTreeRow{
		ShardID:      request.ShardID,
		TreeID:       treeIDBytes,
		BranchID:     branchIDBytes,
		Data:         treeInfoBlob.Data,
		DataEncoding: treeInfoBlob.EncodingType.String(),
	}

	return m.txExecute(ctx, "AppendHistoryNodes", func(tx sqlplugin.Tx) error {
		result, err := tx.InsertIntoHistoryNode(ctx, nodeRow)
		if err != nil {
			return err
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			return err
		}
		if !(rowsAffected == 1 || rowsAffected == 2) {
			return fmt.Errorf("expected 1 or 2 row to be affected for node table, got %v", rowsAffected)
		}

		result, err = tx.InsertIntoHistoryTree(ctx, treeRow)
		if err != nil {
			return err
		}
		rowsAffected, err = result.RowsAffected()
		if err != nil {
			return err
		}
		if !(rowsAffected == 1 || rowsAffected == 2) {
			return fmt.Errorf("expected 1 or 2 rows to be affected for tree table as we allow upserts, got %v", rowsAffected)
		}
		return nil
	})
}

func (m *sqlExecutionStore) DeleteHistoryNodes(
	ctx context.Context,
	request *p.InternalDeleteHistoryNodesRequest,
) error {
	branchInfo := request.BranchInfo
	nodeID := request.NodeID
	txnID := request.TransactionID
	shardID := request.ShardID

	if nodeID < p.GetBeginNodeID(branchInfo) {
		return &p.InvalidPersistenceRequestError{
			Msg: "cannot append to ancestors' nodes",
		}
	}

	treeIDBytes, err := primitives.ParseUUID(branchInfo.GetTreeId())
	if err != nil {
		return err
	}
	branchIDBytes, err := primitives.ParseUUID(branchInfo.GetBranchId())
	if err != nil {
		return err
	}

	nodeRow := &sqlplugin.HistoryNodeRow{
		TreeID:   treeIDBytes,
		BranchID: branchIDBytes,
		NodeID:   nodeID,
		TxnID:    txnID,
		ShardID:  shardID,
	}

	_, err = m.Db.DeleteFromHistoryNode(ctx, nodeRow)
	if err != nil {
		return serviceerror.NewUnavailable(fmt.Sprintf("DeleteHistoryNodes: %v", err))
	}
	return nil
}

// ReadHistoryBranch returns history node data for a branch
func (m *sqlExecutionStore) ReadHistoryBranch(
	ctx context.Context,
	request *p.InternalReadHistoryBranchRequest,
) (*p.InternalReadHistoryBranchResponse, error) {
	branchIDBytes, err := primitives.ParseUUID(request.BranchID)
	if err != nil {
		return nil, err
	}
	treeIDBytes, err := primitives.ParseUUID(request.TreeID)
	if err != nil {
		return nil, err
	}

	var token historyNodePaginationToken
	if len(request.NextPageToken) == 0 {
		if request.ReverseOrder {
			token = newHistoryNodePaginationToken(request.MaxNodeID, MaxTxnID)
		} else {
			token = newHistoryNodePaginationToken(request.MinNodeID, MinTxnID)
		}
	} else {
		token, err = deserializeHistoryNodePaginationToken(request.NextPageToken)
		if err != nil {
			return nil, err
		}
	}

	minNodeId, maxNodeId := request.MinNodeID, request.MaxNodeID
	minTxnId, maxTxnId := MinTxnID, MaxTxnID
	if request.ReverseOrder {
		maxNodeId = token.LastNodeID
		maxTxnId = token.LastTxnID
	} else {
		minNodeId = token.LastNodeID
		minTxnId = token.LastTxnID
	}

	rows, err := m.Db.RangeSelectFromHistoryNode(ctx, sqlplugin.HistoryNodeSelectFilter{
		ShardID:      request.ShardID,
		TreeID:       treeIDBytes,
		BranchID:     branchIDBytes,
		MinNodeID:    minNodeId,
		MinTxnID:     minTxnId,
		MaxNodeID:    maxNodeId,
		MaxTxnID:     maxTxnId,
		PageSize:     request.PageSize,
		MetadataOnly: request.MetadataOnly,
		ReverseOrder: request.ReverseOrder,
	})
	switch err {
	case nil:
		// noop
	case sql.ErrNoRows:
		// noop
	default:
		return nil, err
	}

	nodes := make([]p.InternalHistoryNode, 0, len(rows))
	for _, row := range rows {
		nodes = append(nodes, p.InternalHistoryNode{
			NodeID:            row.NodeID,
			PrevTransactionID: row.PrevTxnID,
			TransactionID:     row.TxnID,
			Events:            p.NewDataBlob(row.Data, row.DataEncoding),
		})
	}

	var pagingToken []byte
	if len(rows) < request.PageSize {
		pagingToken = nil
	} else {
		lastRow := rows[len(rows)-1]
		pagingToken, err = serializeHistoryNodePaginationToken(
			newHistoryNodePaginationToken(lastRow.NodeID, lastRow.TxnID),
		)
		if err != nil {
			return nil, err
		}
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
func (m *sqlExecutionStore) ForkHistoryBranch(
	ctx context.Context,
	request *p.InternalForkHistoryBranchRequest,
) error {
	forkB := request.ForkBranchInfo
	treeInfoBlob := request.TreeInfo
	newBranchIdBytes, err := primitives.ParseUUID(request.NewBranchID)
	if err != nil {
		return err
	}
	treeIDBytes, err := primitives.ParseUUID(forkB.GetTreeId())
	if err != nil {
		return err
	}

	row := &sqlplugin.HistoryTreeRow{
		ShardID:      request.ShardID,
		TreeID:       treeIDBytes,
		BranchID:     newBranchIdBytes,
		Data:         treeInfoBlob.Data,
		DataEncoding: treeInfoBlob.EncodingType.String(),
	}

	result, err := m.Db.InsertIntoHistoryTree(ctx, row)
	if err != nil {
		return err
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if !(rowsAffected == 1 || rowsAffected == 2) {
		return fmt.Errorf("expected 1 or 2 row to be affected for tree table, got %v", rowsAffected)
	}
	return nil
}

// DeleteHistoryBranch removes a branch
func (m *sqlExecutionStore) DeleteHistoryBranch(
	ctx context.Context,
	request *p.InternalDeleteHistoryBranchRequest,
) error {
	branchIDBytes, err := primitives.ParseUUID(request.BranchId)
	if err != nil {
		return err
	}
	treeIDBytes, err := primitives.ParseUUID(request.TreeId)
	if err != nil {
		return err
	}

	return m.txExecute(ctx, "DeleteHistoryBranch", func(tx sqlplugin.Tx) error {
		_, err = tx.DeleteFromHistoryTree(ctx, sqlplugin.HistoryTreeDeleteFilter{
			TreeID:   treeIDBytes,
			BranchID: branchIDBytes,
			ShardID:  request.ShardID,
		})
		if err != nil {
			return err
		}

		// delete each branch range
		for _, br := range request.BranchRanges {
			branchIDBytes, err := primitives.ParseUUID(br.BranchId)
			if err != nil {
				return err
			}

			deleteFilter := sqlplugin.HistoryNodeDeleteFilter{
				ShardID:   request.ShardID,
				TreeID:    treeIDBytes,
				BranchID:  branchIDBytes,
				MinNodeID: br.BeginNodeId,
			}
			_, err = tx.RangeDeleteFromHistoryNode(ctx, deleteFilter)
			if err != nil {
				return err
			}
		}
		return nil
	})
}

func (m *sqlExecutionStore) GetAllHistoryTreeBranches(
	_ context.Context,
	request *p.GetAllHistoryTreeBranchesRequest,
) (*p.InternalGetAllHistoryTreeBranchesResponse, error) {

	// TODO https://github.com/uber/cadence/issues/2458
	// Implement it when we need
	panic("not implemented yet")
}

// GetHistoryTree returns all branch information of a tree
func (m *sqlExecutionStore) GetHistoryTree(
	ctx context.Context,
	request *p.GetHistoryTreeRequest,
) (*p.InternalGetHistoryTreeResponse, error) {
	treeID, err := primitives.ParseUUID(request.TreeID)
	if err != nil {
		return nil, err
	}

	rows, err := m.Db.SelectFromHistoryTree(ctx, sqlplugin.HistoryTreeSelectFilter{
		TreeID:  treeID,
		ShardID: *request.ShardID,
	})
	if err == sql.ErrNoRows || (err == nil && len(rows) == 0) {
		return &p.InternalGetHistoryTreeResponse{}, nil
	}
	treeInfos := make([]*commonpb.DataBlob, 0, len(rows))
	for _, row := range rows {
		treeInfos = append(treeInfos, p.NewDataBlob(row.Data, row.DataEncoding))
	}

	return &p.InternalGetHistoryTreeResponse{
		TreeInfos: treeInfos,
	}, nil
}
