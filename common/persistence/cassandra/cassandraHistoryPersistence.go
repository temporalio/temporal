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

package cassandra

import (
	"fmt"
	"sort"

	"github.com/gocql/gocql"
	"github.com/gogo/protobuf/types"
	"go.temporal.io/temporal-proto/serviceerror"

	"github.com/temporalio/temporal/.gen/proto/persistenceblobs"
	"github.com/temporalio/temporal/common/cassandra"
	"github.com/temporalio/temporal/common/log"
	p "github.com/temporalio/temporal/common/persistence"
	"github.com/temporalio/temporal/common/persistence/serialization"
	"github.com/temporalio/temporal/common/service/config"
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
func newHistoryV2Persistence(
	cfg config.Cassandra,
	logger log.Logger,
) (p.HistoryStore, error) {

	cluster := cassandra.NewCassandraCluster(cfg)
	cluster.ProtoVersion = cassandraProtoVersion
	cluster.Consistency = gocql.LocalQuorum
	cluster.SerialConsistency = gocql.LocalSerial
	cluster.Timeout = defaultSessionTimeout
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &cassandraHistoryV2Persistence{cassandraStore: cassandraStore{session: session, logger: logger}}, nil
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
		h.sortAncestors(&branchInfo.Ancestors)
		treeInfoDataBlob, err := serialization.HistoryTreeInfoToBlob(&persistenceblobs.HistoryTreeInfo{
			BranchInfo: branchInfo,
			ForkTime:   types.TimestampNow(),
			Info:       request.Info,
		})

		if err != nil {
			return convertCommonErrors("AppendHistoryNodes", err)
		}

		batch := h.session.NewBatch(gocql.LoggedBatch)
		batch.Query(v2templateInsertTree,
			branchInfo.TreeID, branchInfo.BranchID, treeInfoDataBlob.Data, treeInfoDataBlob.Encoding)
		batch.Query(v2templateUpsertData,
			branchInfo.TreeID, branchInfo.BranchID, request.NodeID, request.TransactionID, request.Events.Data, request.Events.Encoding)
		err = h.session.ExecuteBatch(batch)
	} else {
		query := h.session.Query(v2templateUpsertData,
			branchInfo.TreeID, branchInfo.BranchID, request.NodeID, request.TransactionID, request.Events.Data, request.Events.Encoding)
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

	treeID, err := gocql.UUIDFromBytes(request.TreeID)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ReadHistoryBranch - Gocql TreeId UUID cast failed. Error: %v", err))
	}

	branchID, err := gocql.UUIDFromBytes(request.BranchID)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ReadHistoryBranch - Gocql BranchId UUID cast failed. Error: %v", err))
	}

	lastNodeID := request.LastNodeID
	lastTxnID := request.LastTransactionID

	query := h.session.Query(v2templateReadData, treeID, branchID, request.MinNodeID, request.MaxNodeID)

	iter := query.PageSize(int(request.PageSize)).PageState(request.NextPageToken).Iter()
	if iter == nil {
		return nil, serviceerror.NewInternal("ReadHistoryBranch operation failed.  Not able to create query iterator.")
	}
	pagingToken := iter.PageState()

	history := make([]*serialization.DataBlob, 0, int(request.PageSize))

	eventBlob := &serialization.DataBlob{}
	nodeID := int64(0)
	txnID := int64(0)

	for iter.Scan(&nodeID, &txnID, &eventBlob.Data, &eventBlob.Encoding) {
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
			history = append(history, eventBlob)
			eventBlob = &serialization.DataBlob{}
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
func (h *cassandraHistoryV2Persistence) ForkHistoryBranch(
	request *p.InternalForkHistoryBranchRequest,
) (*p.InternalForkHistoryBranchResponse, error) {

	forkB := request.ForkBranchInfo
	newAncestors := make([]*persistenceblobs.HistoryBranchRange, 0, len(forkB.Ancestors)+1)

	beginNodeID := p.GetBeginNodeID(forkB)
	if beginNodeID >= request.ForkNodeID {
		// this is the case that new branch's ancestors doesn't include the forking branch
		for _, br := range forkB.Ancestors {
			if br.EndNodeID >= request.ForkNodeID {
				newAncestors = append(newAncestors, &persistenceblobs.HistoryBranchRange{
					BranchID:    br.BranchID,
					BeginNodeID: br.BeginNodeID,
					EndNodeID:   request.ForkNodeID,
				})
				break
			} else {
				newAncestors = append(newAncestors, br)
			}
		}
	} else {
		// this is the case the new branch will inherit all ancestors from forking branch
		newAncestors = forkB.Ancestors
		newAncestors = append(newAncestors, &persistenceblobs.HistoryBranchRange{
			BranchID:    forkB.BranchID,
			BeginNodeID: beginNodeID,
			EndNodeID:   request.ForkNodeID,
		})
	}

	hti := &persistenceblobs.HistoryTreeInfo{
		BranchInfo: &persistenceblobs.HistoryBranch{
			TreeID:    forkB.TreeID,
			BranchID:  request.NewBranchID,
			Ancestors: newAncestors,
		},
		ForkTime: types.TimestampNow(),
		Info:     request.Info,
	}

	datablob, err := serialization.HistoryTreeInfoToBlob(hti)
	if err != nil {
		return nil, err
	}

	cqlTreeID, err := gocql.UUIDFromBytes(forkB.TreeID)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ForkHistoryBranch - Gocql TreeId UUID cast failed. Error: %v", err))
	}

	cqlNewBranchID, err := gocql.UUIDFromBytes(request.NewBranchID)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ForkHistoryBranch - Gocql NewBranchID UUID cast failed. Error: %v", err))
	}
	query := h.session.Query(v2templateInsertTree, cqlTreeID, cqlNewBranchID, datablob.Data, datablob.Encoding)
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
	treeID, err := gocql.UUIDFromBytes(branch.TreeID)
	if err != nil {
		return serviceerror.NewInternal(fmt.Sprintf("DeleteHistoryBranch - Gocql TreeId UUID cast failed. Error: %v", err))
	}
	brsToDelete := branch.Ancestors
	beginNodeID := p.GetBeginNodeID(branch)
	brsToDelete = append(brsToDelete, &persistenceblobs.HistoryBranchRange{
		BranchID:    branch.BranchID,
		BeginNodeID: beginNodeID,
	})

	rsp, err := h.GetHistoryTree(&p.GetHistoryTreeRequest{
		TreeID: treeID[:],
	})
	if err != nil {
		return err
	}

	batch := h.session.NewBatch(gocql.LoggedBatch)
	batch.Query(v2templateDeleteBranch, treeID, branch.BranchID)

	// validBRsMaxEndNode is to know each branch range that is being used, we want to know what is the max nodeID referred by other valid branch
	validBRsMaxEndNode := map[string]int64{}
	for _, b := range rsp.Branches {
		for _, br := range b.Ancestors {
			// These string casts are safe and optimized away -- https://github.com/golang/go/issues/3512
			curr, ok := validBRsMaxEndNode[string(br.BranchID)]
			if !ok || curr < br.EndNodeID {
				validBRsMaxEndNode[string(br.BranchID)] = br.EndNodeID
			}
		}
	}

	// for each branch range to delete, we iterate from bottom to up, and delete up to the point according to validBRsEndNode
	for i := len(brsToDelete) - 1; i >= 0; i-- {
		br := brsToDelete[i]
		maxReferredEndNodeID, ok := validBRsMaxEndNode[string(br.BranchID)]
		if ok {
			// we can only delete from the maxEndNode and stop here
			h.deleteBranchRangeNodes(batch, treeID[:], br.BranchID, maxReferredEndNodeID)
			break
		} else {
			// No any branch is using this range, we can delete all of it
			h.deleteBranchRangeNodes(batch, treeID[:], br.BranchID, br.BeginNodeID)
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
	treeID []byte,
	branchID []byte,
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

	branches := make([]p.HistoryBranchDetail, 0, int(request.PageSize))
	treeUUID := gocql.UUID{}
	branchUUID := gocql.UUID{}
	var data []byte
	var encoding string

	for iter.Scan(&treeUUID, &branchUUID, &data, &encoding) {
		hti, err := serialization.HistoryTreeInfoFromBlob(data, encoding)
		if err != nil {
			return nil, convertCommonErrors("GetAllHistoryTreeBranches", err)
		}

		goForkTime, err := types.TimestampFromProto(hti.ForkTime)
		if err != nil {
			return nil, convertCommonErrors("GetAllHistoryTreeBranches", err)
		}

		branchDetail := p.HistoryBranchDetail{
			TreeID:   treeUUID.String(),
			BranchID: branchUUID.String(),
			ForkTime: goForkTime,
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

	treeID, err := gocql.UUIDFromBytes(request.TreeID)
	if err != nil {
		return nil, serviceerror.NewInternal(fmt.Sprintf("ReadHistoryBranch. Gocql TreeId UUID cast failed. Error: %v", err))
	}
	query := h.session.Query(v2templateReadAllBranches, treeID)

	pagingToken := []byte{}
	branches := make([]*persistenceblobs.HistoryBranch, 0)

	var iter *gocql.Iter
	for {
		iter = query.PageSize(100).PageState(pagingToken).Iter()
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
	ans *[]*persistenceblobs.HistoryBranchRange,
) {
	if len(*ans) > 0 {
		// sort ans based onf EndNodeID so that we can set BeginNodeID
		sort.Slice(ans, func(i, j int) bool { return (*ans)[i].EndNodeID < (*ans)[j].EndNodeID })
		(*ans)[0].BeginNodeID = int64(1)
		for i := 1; i < len(*ans); i++ {
			(*ans)[i].BeginNodeID = (*ans)[i-1].EndNodeID
		}
	}
}
