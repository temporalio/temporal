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
	"database/sql"
	"fmt"
	"math"

	"go.temporal.io/api/serviceerror"

	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/convert"
	"go.temporal.io/server/common/log"
	p "go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/serialization"
	"go.temporal.io/server/common/persistence/sql/sqlplugin"
	"go.temporal.io/server/common/primitives"
	"go.temporal.io/server/common/primitives/timestamp"
)

type (
	sqlHistoryV2Manager struct {
		sqlStore
	}
)

const (
	// NOTE: transaction ID is *= -1 in DB
	MinTxnID = math.MaxInt64
)

// newHistoryV2Persistence creates an instance of HistoryManager
func newHistoryV2Persistence(
	db sqlplugin.DB,
	logger log.Logger,
) (p.HistoryStore, error) {

	return &sqlHistoryV2Manager{
		sqlStore: sqlStore{
			db:     db,
			logger: logger,
		},
	}, nil
}

// AppendHistoryNodes add(or override) a node to a history branch
func (m *sqlHistoryV2Manager) AppendHistoryNodes(
	request *p.InternalAppendHistoryNodesRequest,
) error {
	ctx, cancel := newExecutionContext()
	defer cancel()
	branchInfo := request.BranchInfo
	beginNodeID := p.GetBeginNodeID(branchInfo)
	node := request.Node

	branchIDBytes, err := primitives.ParseUUID(branchInfo.GetBranchId())
	if err != nil {
		return err
	}
	treeIDBytes, err := primitives.ParseUUID(branchInfo.GetTreeId())
	if err != nil {
		return err
	}

	if node.NodeID < beginNodeID {
		return &p.InvalidPersistenceRequestError{
			Msg: fmt.Sprintf("cannot append to ancestors' nodes"),
		}
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

	if request.IsNewBranch {
		treeInfo := &persistencespb.HistoryTreeInfo{
			BranchInfo: branchInfo,
			Info:       request.Info,
			ForkTime:   timestamp.TimeNowPtrUtc(),
		}

		blob, err := serialization.HistoryTreeInfoToBlob(treeInfo)
		if err != nil {
			return err
		}

		treeRow := &sqlplugin.HistoryTreeRow{
			ShardID:      request.ShardID,
			TreeID:       treeIDBytes,
			BranchID:     branchIDBytes,
			Data:         blob.Data,
			DataEncoding: blob.EncodingType.String(),
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
			if rowsAffected != 1 {
				return fmt.Errorf("expected 1 row to be affected for node table, got %v", rowsAffected)
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

	_, err = m.db.InsertIntoHistoryNode(ctx, nodeRow)
	if err != nil {
		if m.db.IsDupEntryError(err) {
			return &p.ConditionFailedError{Msg: fmt.Sprintf("AppendHistoryNodes: row already exist: %v", err)}
		}
		return serviceerror.NewInternal(fmt.Sprintf("AppendHistoryEvents: %v", err))
	}
	return nil
}

// ReadHistoryBranch returns history node data for a branch
func (m *sqlHistoryV2Manager) ReadHistoryBranch(
	request *p.InternalReadHistoryBranchRequest,
) (*p.InternalReadHistoryBranchResponse, error) {
	ctx, cancel := newExecutionContext()
	defer cancel()
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
		token = newHistoryNodePaginationToken(request.MinNodeID, MinTxnID)
	} else if len(request.NextPageToken) == 8 {
		// TODO @wxing1292 remove this block in 1.10.x
		//  this else if block exists to handle forward / backwards compatibility
		lastNodeID, err := deserializePageToken(request.NextPageToken)
		if err != nil {
			return nil, err
		}
		token = newHistoryNodePaginationToken(lastNodeID+1, MinTxnID)
	} else {
		token, err = deserializeHistoryNodePaginationToken(request.NextPageToken)
		if err != nil {
			return nil, err
		}
	}

	rows, err := m.db.SelectFromHistoryNode(ctx, sqlplugin.HistoryNodeSelectFilter{
		ShardID:      request.ShardID,
		TreeID:       treeIDBytes,
		BranchID:     branchIDBytes,
		MinNodeID:    token.LastNodeID,
		MinTxnID:     token.LastTxnID,
		MaxNodeID:    request.MaxNodeID,
		PageSize:     request.PageSize,
		MetadataOnly: request.MetadataOnly,
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
//Now we want to fork a new branch B3 from B2.
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
func (m *sqlHistoryV2Manager) ForkHistoryBranch(
	request *p.InternalForkHistoryBranchRequest,
) (*p.InternalForkHistoryBranchResponse, error) {
	ctx, cancel := newExecutionContext()
	defer cancel()
	forkB := request.ForkBranchInfo
	treeID := forkB.TreeId

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
			BranchId:    forkB.BranchId,
			BeginNodeId: beginNodeID,
			EndNodeId:   request.ForkNodeID,
		})
	}

	treeInfo := &persistencespb.HistoryTreeInfo{
		BranchInfo: &persistencespb.HistoryBranch{
			TreeId:    treeID,
			BranchId:  request.NewBranchID,
			Ancestors: newAncestors,
		},
		Info:     request.Info,
		ForkTime: timestamp.TimeNowPtrUtc(),
	}

	blob, err := serialization.HistoryTreeInfoToBlob(treeInfo)
	if err != nil {
		return nil, err
	}

	newBranchIdBytes, err := primitives.ParseUUID(request.NewBranchID)
	if err != nil {
		return nil, err
	}
	treeIDBytes, err := primitives.ParseUUID(forkB.GetTreeId())
	if err != nil {
		return nil, err
	}

	row := &sqlplugin.HistoryTreeRow{
		ShardID:      request.ShardID,
		TreeID:       treeIDBytes,
		BranchID:     newBranchIdBytes,
		Data:         blob.Data,
		DataEncoding: blob.EncodingType.String(),
	}

	result, err := m.db.InsertIntoHistoryTree(ctx, row)
	if err != nil {
		return nil, err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return nil, err
	}
	if rowsAffected != 1 {
		return nil, fmt.Errorf("expected 1 row to be affected for tree table, got %v", rowsAffected)
	}
	return &p.InternalForkHistoryBranchResponse{
		NewBranchInfo: treeInfo.BranchInfo,
	}, nil
}

// DeleteHistoryBranch removes a branch
func (m *sqlHistoryV2Manager) DeleteHistoryBranch(
	request *p.InternalDeleteHistoryBranchRequest,
) error {
	ctx, cancel := newExecutionContext()
	defer cancel()
	branch := request.BranchInfo
	treeID := branch.TreeId
	branchIDBytes, err := primitives.ParseUUID(branch.GetBranchId())
	if err != nil {
		return err
	}
	treeIDBytes, err := primitives.ParseUUID(branch.GetTreeId())
	if err != nil {
		return err
	}

	brsToDelete := branch.Ancestors
	beginNodeID := p.GetBeginNodeID(branch)
	brsToDelete = append(brsToDelete, &persistencespb.HistoryBranchRange{
		BranchId:    branch.BranchId,
		BeginNodeId: beginNodeID,
	})

	rsp, err := m.GetHistoryTree(&p.GetHistoryTreeRequest{
		TreeID:  treeID,
		ShardID: convert.Int32Ptr(request.ShardID),
	})
	if err != nil {
		return err
	}

	// validBRsMaxEndNode is to for each branch range that is being used, we want to know what is the max nodeID referred by other valid branch
	validBRsMaxEndNode := map[string]int64{}
	for _, b := range rsp.Branches {
		for _, br := range b.Ancestors {
			curr, ok := validBRsMaxEndNode[br.GetBranchId()]
			if !ok || curr < br.GetEndNodeId() {
				validBRsMaxEndNode[br.GetBranchId()] = br.GetEndNodeId()
			}
		}
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

		done := false
		// for each branch range to delete, we iterate from bottom to up, and delete up to the point according to validBRsEndNode
		for i := len(brsToDelete) - 1; i >= 0; i-- {
			br := brsToDelete[i]
			maxReferredEndNodeID, ok := validBRsMaxEndNode[br.GetBranchId()]
			deleteFilter := sqlplugin.HistoryNodeDeleteFilter{
				ShardID:  request.ShardID,
				TreeID:   treeIDBytes,
				BranchID: branchIDBytes,
			}

			if ok {
				// we can only delete from the maxEndNode and stop here
				deleteFilter.MinNodeID = maxReferredEndNodeID
				done = true
			} else {
				// No any branch is using this range, we can delete all of it
				deleteFilter.MinNodeID = br.BeginNodeId
			}
			_, err := tx.DeleteFromHistoryNode(ctx, deleteFilter)
			if err != nil {
				return err
			}
			if done {
				break
			}
		}
		return nil
	})
}

func (m *sqlHistoryV2Manager) GetAllHistoryTreeBranches(
	request *p.GetAllHistoryTreeBranchesRequest,
) (*p.GetAllHistoryTreeBranchesResponse, error) {

	// TODO https://github.com/uber/cadence/issues/2458
	// Implement it when we need
	panic("not implemented yet")
}

// GetHistoryTree returns all branch information of a tree
func (m *sqlHistoryV2Manager) GetHistoryTree(
	request *p.GetHistoryTreeRequest,
) (*p.GetHistoryTreeResponse, error) {
	ctx, cancel := newExecutionContext()
	defer cancel()
	treeID, err := primitives.ParseUUID(request.TreeID)
	if err != nil {
		return nil, err
	}

	rows, err := m.db.SelectFromHistoryTree(ctx, sqlplugin.HistoryTreeSelectFilter{
		TreeID:  treeID,
		ShardID: *request.ShardID,
	})
	if err == sql.ErrNoRows || (err == nil && len(rows) == 0) {
		return &p.GetHistoryTreeResponse{}, nil
	}
	branches := make([]*persistencespb.HistoryBranch, 0, len(rows))
	for _, row := range rows {
		treeInfo, err := serialization.HistoryTreeInfoFromBlob(row.Data, row.DataEncoding)
		if err != nil {
			return nil, err
		}
		branches = append(branches, treeInfo.BranchInfo)
	}

	return &p.GetHistoryTreeResponse{
		Branches: branches,
	}, nil
}
