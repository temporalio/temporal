// Copyright (c) 2018 Uber Technologies, Inc.
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

	"github.com/uber/cadence/.gen/go/shared"
	"github.com/uber/cadence/.gen/go/sqlblobs"
	"github.com/uber/cadence/common"
	"github.com/uber/cadence/common/log"
	p "github.com/uber/cadence/common/persistence"
	"github.com/uber/cadence/common/persistence/sql/sqlplugin"
)

type sqlHistoryV2Manager struct {
	sqlStore
}

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

	branchInfo := request.BranchInfo
	beginNodeID := p.GetBeginNodeID(branchInfo)

	if request.NodeID < beginNodeID {
		return &p.InvalidPersistenceRequestError{
			Msg: fmt.Sprintf("cannot append to ancestors' nodes"),
		}
	}

	nodeRow := &sqlplugin.HistoryNodeRow{
		TreeID:       sqlplugin.MustParseUUID(branchInfo.GetTreeID()),
		BranchID:     sqlplugin.MustParseUUID(branchInfo.GetBranchID()),
		NodeID:       request.NodeID,
		TxnID:        &request.TransactionID,
		Data:         request.Events.Data,
		DataEncoding: string(request.Events.Encoding),
		ShardID:      request.ShardID,
	}

	if request.IsNewBranch {
		var ancestors []*shared.HistoryBranchRange
		for _, anc := range branchInfo.Ancestors {
			ancestors = append(ancestors, anc)
		}

		treeInfo := &sqlblobs.HistoryTreeInfo{
			Ancestors:        ancestors,
			Info:             &request.Info,
			CreatedTimeNanos: common.TimeNowNanosPtr(),
		}

		blob, err := historyTreeInfoToBlob(treeInfo)
		if err != nil {
			return err
		}

		treeRow := &sqlplugin.HistoryTreeRow{
			ShardID:      request.ShardID,
			TreeID:       sqlplugin.MustParseUUID(branchInfo.GetTreeID()),
			BranchID:     sqlplugin.MustParseUUID(branchInfo.GetBranchID()),
			Data:         blob.Data,
			DataEncoding: string(blob.Encoding),
		}

		return m.txExecute("AppendHistoryNodes", func(tx sqlplugin.Tx) error {
			result, err := tx.InsertIntoHistoryNode(nodeRow)
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
			result, err = tx.InsertIntoHistoryTree(treeRow)
			if err != nil {
				return err
			}
			rowsAffected, err = result.RowsAffected()
			if err != nil {
				return err
			}
			if rowsAffected != 1 {
				return fmt.Errorf("expected 1 row to be affected for tree table, got %v", rowsAffected)
			}
			return nil
		})
	}

	_, err := m.db.InsertIntoHistoryNode(nodeRow)
	if err != nil {
		if m.db.IsDupEntryError(err) {
			return &p.ConditionFailedError{Msg: fmt.Sprintf("AppendHistoryNodes: row already exist: %v", err)}
		}
		return &shared.InternalServiceError{Message: fmt.Sprintf("AppendHistoryEvents: %v", err)}
	}
	return nil
}

// ReadHistoryBranch returns history node data for a branch
func (m *sqlHistoryV2Manager) ReadHistoryBranch(
	request *p.InternalReadHistoryBranchRequest,
) (*p.InternalReadHistoryBranchResponse, error) {

	minNodeID := request.MinNodeID
	maxNodeID := request.MaxNodeID

	lastNodeID := request.LastNodeID
	lastTxnID := request.LastTransactionID

	if request.NextPageToken != nil && len(request.NextPageToken) > 0 {
		var lastNodeID int64
		var err error
		// TODO the inner pagination token can be replaced by a dummy token
		//  since lastNodeID & lastTxnID are both provided
		if lastNodeID, err = deserializePageToken(request.NextPageToken); err != nil {
			return nil, &shared.InternalServiceError{
				Message: fmt.Sprintf("invalid next page token %v", request.NextPageToken)}
		}
		minNodeID = lastNodeID + 1
	}

	filter := &sqlplugin.HistoryNodeFilter{
		TreeID:    sqlplugin.MustParseUUID(request.TreeID),
		BranchID:  sqlplugin.MustParseUUID(request.BranchID),
		MinNodeID: &minNodeID,
		MaxNodeID: &maxNodeID,
		PageSize:  &request.PageSize,
		ShardID:   request.ShardID,
	}

	rows, err := m.db.SelectFromHistoryNode(filter)
	if err == sql.ErrNoRows || (err == nil && len(rows) == 0) {
		return &p.InternalReadHistoryBranchResponse{}, nil
	}

	history := make([]*p.DataBlob, 0, int(request.PageSize))
	eventBlob := &p.DataBlob{}

	for _, row := range rows {
		eventBlob.Data = row.Data
		eventBlob.Encoding = common.EncodingType(row.DataEncoding)

		if *row.TxnID < lastTxnID {
			// assuming that business logic layer is correct and transaction ID only increase
			// thus, valid event batch will come with increasing transaction ID

			// event batches with smaller node ID
			//  -> should not be possible since records are already sorted
			// event batches with same node ID
			//  -> batch with higher transaction ID is valid
			// event batches with larger node ID
			//  -> batch with lower transaction ID is invalid (happens before)
			//  -> batch with higher transaction ID is valid
			if row.NodeID < lastNodeID {
				return nil, &shared.InternalServiceError{
					Message: fmt.Sprintf("corrupted data, nodeID cannot decrease"),
				}
			} else if row.NodeID > lastNodeID {
				// update lastNodeID so that our pagination can make progress in the corner case that
				// the page are all rows with smaller txnID
				// because next page we always have minNodeID = lastNodeID+1
				lastNodeID = row.NodeID
			}
			continue
		}

		switch {
		case row.NodeID < lastNodeID:
			return nil, &shared.InternalServiceError{
				Message: fmt.Sprintf("corrupted data, nodeID cannot decrease"),
			}
		case row.NodeID == lastNodeID:
			return nil, &shared.InternalServiceError{
				Message: fmt.Sprintf("corrupted data, same nodeID must have smaller txnID"),
			}
		default: // row.NodeID > lastNodeID:
			// NOTE: when row.nodeID > lastNodeID, we expect the one with largest txnID comes first
			lastTxnID = *row.TxnID
			lastNodeID = row.NodeID
			history = append(history, eventBlob)
			eventBlob = &p.DataBlob{}
		}
	}

	var pagingToken []byte
	if len(rows) >= request.PageSize {
		pagingToken = serializePageToken(lastNodeID)
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

	forkB := request.ForkBranchInfo
	treeID := *forkB.TreeID
	newAncestors := make([]*shared.HistoryBranchRange, 0, len(forkB.Ancestors)+1)

	beginNodeID := p.GetBeginNodeID(forkB)
	if beginNodeID >= request.ForkNodeID {
		// this is the case that new branch's ancestors doesn't include the forking branch
		for _, br := range forkB.Ancestors {
			if *br.EndNodeID >= request.ForkNodeID {
				newAncestors = append(newAncestors, &shared.HistoryBranchRange{
					BranchID:    br.BranchID,
					BeginNodeID: br.BeginNodeID,
					EndNodeID:   common.Int64Ptr(request.ForkNodeID),
				})
				break
			} else {
				newAncestors = append(newAncestors, br)
			}
		}
	} else {
		// this is the case the new branch will inherit all ancestors from forking branch
		newAncestors = forkB.Ancestors
		newAncestors = append(newAncestors, &shared.HistoryBranchRange{
			BranchID:    forkB.BranchID,
			BeginNodeID: common.Int64Ptr(beginNodeID),
			EndNodeID:   common.Int64Ptr(request.ForkNodeID),
		})
	}

	resp := &p.InternalForkHistoryBranchResponse{
		NewBranchInfo: shared.HistoryBranch{
			TreeID:    &treeID,
			BranchID:  &request.NewBranchID,
			Ancestors: newAncestors,
		}}

	treeInfo := &sqlblobs.HistoryTreeInfo{
		Ancestors:        newAncestors,
		Info:             &request.Info,
		CreatedTimeNanos: common.TimeNowNanosPtr(),
	}

	blob, err := historyTreeInfoToBlob(treeInfo)
	if err != nil {
		return nil, err
	}

	row := &sqlplugin.HistoryTreeRow{
		ShardID:      request.ShardID,
		TreeID:       sqlplugin.MustParseUUID(treeID),
		BranchID:     sqlplugin.MustParseUUID(request.NewBranchID),
		Data:         blob.Data,
		DataEncoding: string(blob.Encoding),
	}
	result, err := m.db.InsertIntoHistoryTree(row)
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
	return resp, nil
}

// DeleteHistoryBranch removes a branch
func (m *sqlHistoryV2Manager) DeleteHistoryBranch(
	request *p.InternalDeleteHistoryBranchRequest,
) error {

	branch := request.BranchInfo
	treeID := *branch.TreeID
	brsToDelete := branch.Ancestors
	beginNodeID := p.GetBeginNodeID(branch)
	brsToDelete = append(brsToDelete, &shared.HistoryBranchRange{
		BranchID:    branch.BranchID,
		BeginNodeID: common.Int64Ptr(beginNodeID),
	})

	rsp, err := m.GetHistoryTree(&p.GetHistoryTreeRequest{
		TreeID:  treeID,
		ShardID: common.IntPtr(request.ShardID),
	})
	if err != nil {
		return err
	}

	// validBRsMaxEndNode is to for each branch range that is being used, we want to know what is the max nodeID referred by other valid branch
	validBRsMaxEndNode := map[string]int64{}
	for _, b := range rsp.Branches {
		for _, br := range b.Ancestors {
			curr, ok := validBRsMaxEndNode[*br.BranchID]
			if !ok || curr < *br.EndNodeID {
				validBRsMaxEndNode[*br.BranchID] = *br.EndNodeID
			}
		}
	}

	return m.txExecute("DeleteHistoryBranch", func(tx sqlplugin.Tx) error {
		branchID := sqlplugin.MustParseUUID(*branch.BranchID)
		treeFilter := &sqlplugin.HistoryTreeFilter{
			TreeID:   sqlplugin.MustParseUUID(treeID),
			BranchID: &branchID,
			ShardID:  request.ShardID,
		}
		_, err = tx.DeleteFromHistoryTree(treeFilter)
		if err != nil {
			return err
		}

		done := false
		// for each branch range to delete, we iterate from bottom to up, and delete up to the point according to validBRsEndNode
		for i := len(brsToDelete) - 1; i >= 0; i-- {
			br := brsToDelete[i]
			maxReferredEndNodeID, ok := validBRsMaxEndNode[*br.BranchID]
			nodeFilter := &sqlplugin.HistoryNodeFilter{
				TreeID:   sqlplugin.MustParseUUID(treeID),
				BranchID: sqlplugin.MustParseUUID(*br.BranchID),
				ShardID:  request.ShardID,
			}

			if ok {
				// we can only delete from the maxEndNode and stop here
				nodeFilter.MinNodeID = &maxReferredEndNodeID
				done = true
			} else {
				// No any branch is using this range, we can delete all of it
				nodeFilter.MinNodeID = br.BeginNodeID
			}
			_, err := tx.DeleteFromHistoryNode(nodeFilter)
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

	treeID := sqlplugin.MustParseUUID(request.TreeID)
	branches := make([]*shared.HistoryBranch, 0)

	treeFilter := &sqlplugin.HistoryTreeFilter{
		TreeID:  treeID,
		ShardID: *request.ShardID,
	}
	rows, err := m.db.SelectFromHistoryTree(treeFilter)
	if err == sql.ErrNoRows || (err == nil && len(rows) == 0) {
		return &p.GetHistoryTreeResponse{}, nil
	}
	for _, row := range rows {
		treeInfo, err := historyTreeInfoFromBlob(row.Data, row.DataEncoding)
		if err != nil {
			return nil, err
		}
		br := &shared.HistoryBranch{
			TreeID:    &request.TreeID,
			BranchID:  common.StringPtr(row.BranchID.String()),
			Ancestors: treeInfo.Ancestors,
		}
		branches = append(branches, br)
	}

	return &p.GetHistoryTreeResponse{
		Branches: branches,
	}, nil
}
