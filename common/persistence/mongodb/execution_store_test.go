package mongodb

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	enumspb "go.temporal.io/api/enums/v1"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
	"go.temporal.io/server/common/persistence/serialization"
	taskspkg "go.temporal.io/server/service/history/tasks"
)

func TestNewExecutionStore_Constructs(t *testing.T) {
	store, err := NewExecutionStore(
		&noopDatabase{},
		&fakeMongoClient{},
		config.MongoDB{},
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
		true,
	)
	require.NoError(t, err)
	require.NotNil(t, store)
	require.Equal(t, "mongodb", store.GetName())
}

func TestNewExecutionStoreRequiresTransactions(t *testing.T) {
	_, err := NewExecutionStore(
		&noopDatabase{},
		&fakeMongoClient{},
		config.MongoDB{},
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
		false,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires transactions-enabled topology")
}

func TestNewExecutionStoreRequiresClient(t *testing.T) {
	_, err := NewExecutionStore(
		&noopDatabase{},
		nil,
		config.MongoDB{},
		log.NewTestLogger(),
		metrics.NoopMetricsHandler,
		true,
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "requires client with session support")
}

func TestConflictResolveWorkflowExecution_WithCurrentMutationAndNewWorkflow(t *testing.T) {
	nsID := "namespace"
	wfID := "workflow"
	resetRunID := "reset-run"

	currentRunID := "current-run"
	newRunID := "new-run"

	execCol := newFakeCollection(t)
	currCol := newFakeCollection(t)

	resetDoc := &executionDocument{
		ID:              executionDocID(nsID, wfID, resetRunID),
		NamespaceID:     nsID,
		WorkflowID:      wfID,
		RunID:           resetRunID,
		DBRecordVersion: 4,
		NextEventID:     10,
		Condition:       10,
	}
	mutationDoc := &executionDocument{
		ID:              executionDocID(nsID, wfID, currentRunID),
		NamespaceID:     nsID,
		WorkflowID:      wfID,
		RunID:           currentRunID,
		DBRecordVersion: 5,
		NextEventID:     20,
		Condition:       20,
	}

	execCol.enqueueFind(resetDoc)
	execCol.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1})
	execCol.enqueueFind(mutationDoc)
	execCol.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1})
	currCol.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1})

	store := &executionStore{
		transactionalStore:  transactionalStore{client: &fakeMongoClient{}, metricsHandler: metrics.NoopMetricsHandler},
		serializer:          serialization.NewSerializer(),
		executionsCol:       execCol,
		currentExecsCol:     currCol,
		transactionsEnabled: true,
	}

	resetSnapshot := persistence.InternalWorkflowSnapshot{
		NamespaceID:      nsID,
		WorkflowID:       wfID,
		RunID:            resetRunID,
		ExecutionInfo:    &persistencespb.WorkflowExecutionInfo{StateTransitionCount: 1},
		ExecutionState:   &persistencespb.WorkflowExecutionState{RunId: resetRunID, CreateRequestId: "reset", State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
		NextEventID:      10,
		LastWriteVersion: 11,
		DBRecordVersion:  5,
		Condition:        10,
	}
	currentMutation := &persistence.InternalWorkflowMutation{
		NamespaceID:      nsID,
		WorkflowID:       wfID,
		ExecutionInfo:    &persistencespb.WorkflowExecutionInfo{StateTransitionCount: 2},
		ExecutionState:   &persistencespb.WorkflowExecutionState{RunId: currentRunID, CreateRequestId: "current", State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED},
		NextEventID:      30,
		LastWriteVersion: 12,
		DBRecordVersion:  6,
		Condition:        20,
	}
	newSnapshot := &persistence.InternalWorkflowSnapshot{
		NamespaceID:      nsID,
		WorkflowID:       wfID,
		RunID:            newRunID,
		ExecutionInfo:    &persistencespb.WorkflowExecutionInfo{StateTransitionCount: 3},
		ExecutionState:   &persistencespb.WorkflowExecutionState{RunId: newRunID, CreateRequestId: "new", State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
		NextEventID:      40,
		LastWriteVersion: 13,
		DBRecordVersion:  7,
		Condition:        40,
	}

	request := &persistence.InternalConflictResolveWorkflowExecutionRequest{
		ShardID:                 1,
		RangeID:                 1,
		ResetWorkflowSnapshot:   resetSnapshot,
		CurrentWorkflowMutation: currentMutation,
		NewWorkflowSnapshot:     newSnapshot,
		Mode:                    persistence.ConflictResolveWorkflowModeUpdateCurrent,
	}

	err := store.applyConflictResolveWorkflowExecution(context.Background(), request)
	require.NoError(t, err)

	require.Len(t, execCol.findFilters, 2)
	require.Equal(t, executionDocID(nsID, wfID, resetRunID), execCol.findFilters[0].(bson.M)["_id"])
	require.Equal(t, executionDocID(nsID, wfID, currentRunID), execCol.findFilters[1].(bson.M)["_id"])

	require.Len(t, execCol.updateFilters, 2)
	require.Equal(t, executionDocID(nsID, wfID, resetRunID), execCol.updateFilters[0].(bson.M)["_id"])
	require.Equal(t, executionDocID(nsID, wfID, currentRunID), execCol.updateFilters[1].(bson.M)["_id"])

	require.Len(t, execCol.insertDocs, 1)
	insertedDoc, ok := execCol.insertDocs[0].(*executionDocument)
	require.True(t, ok)
	require.Equal(t, newRunID, insertedDoc.RunID)

	require.Len(t, currCol.updateFilters, 1)
	currFilter, ok := currCol.updateFilters[0].(bson.M)
	require.True(t, ok)
	require.Equal(t, currentRunID, currFilter["run_id"])

	updateSet := currCol.updateUpdates[0].(bson.M)["$set"].(bson.M)
	require.Equal(t, newRunID, updateSet["run_id"])
}

func TestUpdateWorkflowExecution_ContinueAsNew(t *testing.T) {
	nsID := "namespace"
	wfID := "workflow"
	oldRunID := "old-run"
	newRunID := "new-run"

	execCol := newFakeCollection(t)
	currCol := newFakeCollection(t)

	existingDoc := &executionDocument{
		ID:              executionDocID(nsID, wfID, oldRunID),
		NamespaceID:     nsID,
		WorkflowID:      wfID,
		RunID:           oldRunID,
		DBRecordVersion: 2,
		NextEventID:     20,
		Condition:       20,
	}

	currentDoc := &currentExecutionDocument{
		ID:               currentExecutionDocID(nsID, wfID),
		ShardID:          1,
		NamespaceID:      nsID,
		WorkflowID:       wfID,
		RunID:            oldRunID,
		CreateRequestID:  "old",
		State:            int32(enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED),
		Status:           int32(enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED),
		LastWriteVersion: 11,
		DBRecordVersion:  3,
		UpdateTime:       time.Unix(0, 0).UTC(),
	}
	currCol.enqueueFind(currentDoc)

	execCol.enqueueFind(existingDoc)
	execCol.enqueueFind(nil)
	execCol.enqueueFind(nil)
	execCol.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1})
	currCol.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1})

	store := &executionStore{
		transactionalStore:  transactionalStore{client: &fakeMongoClient{}, metricsHandler: metrics.NoopMetricsHandler},
		serializer:          serialization.NewSerializer(),
		executionsCol:       execCol,
		currentExecsCol:     currCol,
		transactionsEnabled: true,
	}

	mutation := persistence.InternalWorkflowMutation{
		NamespaceID:      nsID,
		WorkflowID:       wfID,
		ExecutionInfo:    &persistencespb.WorkflowExecutionInfo{StateTransitionCount: 2},
		ExecutionState:   &persistencespb.WorkflowExecutionState{RunId: oldRunID, CreateRequestId: "old", State: enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED, Status: enumspb.WORKFLOW_EXECUTION_STATUS_COMPLETED},
		NextEventID:      30,
		LastWriteVersion: 11,
		DBRecordVersion:  3,
		Condition:        20,
	}
	newSnapshot := persistence.InternalWorkflowSnapshot{
		NamespaceID:      nsID,
		WorkflowID:       wfID,
		RunID:            newRunID,
		ExecutionInfo:    &persistencespb.WorkflowExecutionInfo{StateTransitionCount: 1},
		ExecutionState:   &persistencespb.WorkflowExecutionState{RunId: newRunID, CreateRequestId: "new", State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
		NextEventID:      4,
		LastWriteVersion: 12,
		DBRecordVersion:  1,
		Condition:        4,
	}

	req := &persistence.InternalUpdateWorkflowExecutionRequest{
		ShardID:                1,
		RangeID:                1,
		Mode:                   persistence.UpdateWorkflowModeUpdateCurrent,
		UpdateWorkflowMutation: mutation,
		NewWorkflowSnapshot:    &newSnapshot,
	}

	err := store.applyUpdateWorkflowExecution(context.Background(), req)
	require.NoError(t, err)

	require.Len(t, execCol.findFilters, 1)
	require.Equal(t, executionDocID(nsID, wfID, oldRunID), execCol.findFilters[0].(bson.M)["_id"])

	require.Len(t, execCol.updateFilters, 1)
	require.Equal(t, executionDocID(nsID, wfID, oldRunID), execCol.updateFilters[0].(bson.M)["_id"])

	require.Len(t, execCol.insertDocs, 1)
	insertedDoc, ok := execCol.insertDocs[0].(*executionDocument)
	require.True(t, ok)
	require.Equal(t, newRunID, insertedDoc.RunID)

	require.Len(t, currCol.updateFilters, 1)
	filter, ok := currCol.updateFilters[0].(bson.M)
	require.True(t, ok)
	require.Equal(t, oldRunID, filter["run_id"])

	set := currCol.updateUpdates[0].(bson.M)["$set"].(bson.M)
	require.Equal(t, newRunID, set["run_id"])
	require.Equal(t, int64(12), set["last_write_version"])
	require.Equal(t, int64(1), set["db_record_version"])
}

func TestGetWorkflowExecution_Success(t *testing.T) {
	nsID := "namespace"
	wfID := "workflow"
	runID := "run"

	execCol := newFakeCollection(t)
	stateBlob := persistence.NewDataBlob([]byte("state"), enumspb.ENCODING_TYPE_PROTO3.String())
	infoBlob := persistence.NewDataBlob([]byte("info"), enumspb.ENCODING_TYPE_PROTO3.String())

	doc := &executionDocument{
		ID:                 executionDocID(nsID, wfID, runID),
		NamespaceID:        nsID,
		WorkflowID:         wfID,
		RunID:              runID,
		DBRecordVersion:    9,
		NextEventID:        42,
		ExecutionInfo:      dataBlobToDoc(infoBlob),
		ExecutionState:     dataBlobToDoc(stateBlob),
		Checksum:           dataBlobToDoc(persistence.NewDataBlob([]byte("checksum"), enumspb.ENCODING_TYPE_PROTO3.String())),
		SignalRequestedIDs: []string{"sig-1", "sig-2"},
	}
	doc.ActivityInfos = []int64BlobDoc{{ID: 7, Blob: dataBlobToDoc(persistence.NewDataBlob([]byte("activity"), enumspb.ENCODING_TYPE_PROTO3.String()))}}

	execCol.enqueueFind(doc)

	store := &executionStore{
		transactionalStore:  transactionalStore{client: &fakeMongoClient{}, metricsHandler: metrics.NoopMetricsHandler},
		transactionsEnabled: true,
		executionsCol:       execCol,
	}

	resp, err := store.GetWorkflowExecution(context.Background(), &persistence.GetWorkflowExecutionRequest{
		NamespaceID: nsID,
		WorkflowID:  wfID,
		RunID:       runID,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.NotNil(t, resp.State)
	require.Equal(t, int64(42), resp.State.NextEventID)
	require.Equal(t, int64(9), resp.State.DBRecordVersion)
	require.Contains(t, resp.State.ActivityInfos, int64(7))
	require.Equal(t, []byte("activity"), resp.State.ActivityInfos[7].GetData())
	require.Equal(t, []string{"sig-1", "sig-2"}, resp.State.SignalRequestedIDs)
	require.Equal(t, []byte("info"), resp.State.ExecutionInfo.GetData())
	require.Equal(t, []byte("state"), resp.State.ExecutionState.GetData())
	require.Equal(t, []byte("checksum"), resp.State.Checksum.GetData())
}

func TestGetCurrentExecution_Success(t *testing.T) {
	nsID := "namespace"
	wfID := "workflow"
	runID := "run"

	currCol := newFakeCollection(t)
	now := time.Now().UTC()
	doc := &currentExecutionDocument{
		ID:              currentExecutionDocID(nsID, wfID),
		NamespaceID:     nsID,
		WorkflowID:      wfID,
		RunID:           runID,
		CreateRequestID: "req",
		State:           int32(enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING),
		Status:          int32(enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING),
		StartTime:       &now,
	}

	currCol.enqueueFind(doc)

	store := &executionStore{
		transactionalStore:  transactionalStore{client: &fakeMongoClient{}, metricsHandler: metrics.NoopMetricsHandler},
		transactionsEnabled: true,
		currentExecsCol:     currCol,
	}

	resp, err := store.GetCurrentExecution(context.Background(), &persistence.GetCurrentExecutionRequest{
		NamespaceID: nsID,
		WorkflowID:  wfID,
	})
	require.NoError(t, err)
	require.NotNil(t, resp)
	require.Equal(t, runID, resp.RunID)
	require.Equal(t, "req", resp.ExecutionState.GetCreateRequestId())
	require.Equal(t, enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, resp.ExecutionState.GetState())
	require.Equal(t, enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING, resp.ExecutionState.GetStatus())
	require.NotNil(t, resp.ExecutionState.GetStartTime())
}

func TestListConcreteExecutions_Pagination(t *testing.T) {
	ctx := context.Background()
	nsID := "namespace"
	shardID := int32(1)

	doc1 := &executionDocument{
		ID:              executionDocID(nsID, "wf1", "run1"),
		ShardID:         shardID,
		NamespaceID:     nsID,
		WorkflowID:      "wf1",
		RunID:           "run1",
		DBRecordVersion: 1,
		NextEventID:     5,
		ExecutionInfo:   dataBlobToDoc(persistence.NewDataBlob([]byte("info1"), enumspb.ENCODING_TYPE_PROTO3.String())),
		ExecutionState:  dataBlobToDoc(persistence.NewDataBlob([]byte("state1"), enumspb.ENCODING_TYPE_PROTO3.String())),
	}
	doc2 := &executionDocument{
		ID:              executionDocID(nsID, "wf2", "run2"),
		ShardID:         shardID,
		NamespaceID:     nsID,
		WorkflowID:      "wf2",
		RunID:           "run2",
		DBRecordVersion: 2,
		NextEventID:     7,
		ExecutionInfo:   dataBlobToDoc(persistence.NewDataBlob([]byte("info2"), enumspb.ENCODING_TYPE_PROTO3.String())),
		ExecutionState:  dataBlobToDoc(persistence.NewDataBlob([]byte("state2"), enumspb.ENCODING_TYPE_PROTO3.String())),
	}
	doc3 := &executionDocument{
		ID:              executionDocID(nsID, "wf3", "run3"),
		ShardID:         shardID,
		NamespaceID:     nsID,
		WorkflowID:      "wf3",
		RunID:           "run3",
		DBRecordVersion: 3,
		NextEventID:     9,
		ExecutionInfo:   dataBlobToDoc(persistence.NewDataBlob([]byte("info3"), enumspb.ENCODING_TYPE_PROTO3.String())),
		ExecutionState:  dataBlobToDoc(persistence.NewDataBlob([]byte("state3"), enumspb.ENCODING_TYPE_PROTO3.String())),
	}

	col := newFakeCollection(t)
	col.enqueueFindMany(doc1, doc2, doc3)

	store := &executionStore{
		transactionalStore:  transactionalStore{client: &fakeMongoClient{}, metricsHandler: metrics.NoopMetricsHandler},
		transactionsEnabled: true,
		executionsCol:       col,
	}

	resp, err := store.ListConcreteExecutions(ctx, &persistence.ListConcreteExecutionsRequest{
		ShardID:  shardID,
		PageSize: 2,
	})
	require.NoError(t, err)
	require.Len(t, resp.States, 2)
	require.NotNil(t, resp.NextPageToken)
	lastID, err := decodeListExecutionsToken(resp.NextPageToken)
	require.NoError(t, err)
	require.Equal(t, doc2.ID, lastID)

	col.enqueueFindMany(doc3)
	resp2, err := store.ListConcreteExecutions(ctx, &persistence.ListConcreteExecutionsRequest{
		ShardID:   shardID,
		PageSize:  2,
		PageToken: resp.NextPageToken,
	})
	require.NoError(t, err)
	require.Len(t, resp2.States, 1)
	require.Nil(t, resp2.NextPageToken)
}

func TestListConcreteExecutions_Empty(t *testing.T) {
	col := newFakeCollection(t)
	col.enqueueFindMany()
	store := &executionStore{
		transactionalStore:  transactionalStore{client: &fakeMongoClient{}, metricsHandler: metrics.NoopMetricsHandler},
		transactionsEnabled: true,
		executionsCol:       col,
	}
	resp, err := store.ListConcreteExecutions(context.Background(), &persistence.ListConcreteExecutionsRequest{ShardID: 1, PageSize: 10})
	require.NoError(t, err)
	require.Empty(t, resp.States)
	require.Nil(t, resp.NextPageToken)
}

func TestCreateWorkflowExecution_AppendsNewEvents(t *testing.T) {
	ctx := context.Background()
	historyCol := newFakeCollection(t)
	execCol := newFakeCollection(t)
	currCol := newFakeCollection(t)
	store := &executionStore{
		transactionalStore:  transactionalStore{client: &fakeMongoClient{}, metricsHandler: metrics.NoopMetricsHandler},
		transactionsEnabled: true,
		historyNodesCol:     historyCol,
		executionsCol:       execCol,
		currentExecsCol:     currCol,
		historyBranchesCol:  newFakeCollection(t),
		serializer:          serialization.NewSerializer(),
	}
	shardCol := newFakeCollection(t)
	shardCol.enqueueFindResult(&struct {
		RangeID int64 `bson:"range_id"`
	}{RangeID: 2})
	shardDB := newFakeMongoDatabase(t)
	shardDB.collections[collectionShards] = shardCol
	store.db = shardDB

	branchInfo := &persistencespb.HistoryBranch{TreeId: "tree", BranchId: "branch"}
	appendReq := &persistence.InternalAppendHistoryNodesRequest{
		ShardID:    1,
		BranchInfo: branchInfo,
		Node: persistence.InternalHistoryNode{
			NodeID:        5,
			TransactionID: 7,
		},
	}

	snapshot := persistence.InternalWorkflowSnapshot{
		NamespaceID:      "ns",
		WorkflowID:       "wf",
		RunID:            "run",
		ExecutionInfo:    &persistencespb.WorkflowExecutionInfo{},
		ExecutionState:   &persistencespb.WorkflowExecutionState{RunId: "run", CreateRequestId: "req", State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
		NextEventID:      10,
		LastWriteVersion: 11,
		DBRecordVersion:  1,
	}

	req := &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID:              1,
		RangeID:              2,
		Mode:                 persistence.CreateWorkflowModeBrandNew,
		NewWorkflowSnapshot:  snapshot,
		NewWorkflowNewEvents: []*persistence.InternalAppendHistoryNodesRequest{appendReq},
	}

	_, err := store.CreateWorkflowExecution(ctx, req)
	require.NoError(t, err)
	require.Len(t, historyCol.insertDocs, 1)
}

func TestUpdateWorkflowExecution_AppendsNewEvents(t *testing.T) {
	ctx := context.Background()
	historyCol := newFakeCollection(t)
	execCol := newFakeCollection(t)
	store := &executionStore{
		transactionalStore:  transactionalStore{client: &fakeMongoClient{}, metricsHandler: metrics.NoopMetricsHandler},
		transactionsEnabled: true,
		historyNodesCol:     historyCol,
		executionsCol:       execCol,
		historyBranchesCol:  newFakeCollection(t),
		serializer:          serialization.NewSerializer(),
	}
	shardCol := newFakeCollection(t)
	shardCol.enqueueFindResult(&struct {
		RangeID int64 `bson:"range_id"`
	}{RangeID: 2})
	shardDB := newFakeMongoDatabase(t)
	shardDB.collections[collectionShards] = shardCol
	store.db = shardDB

	branchInfo := &persistencespb.HistoryBranch{TreeId: "tree", BranchId: "branch"}
	appendReq1 := &persistence.InternalAppendHistoryNodesRequest{
		ShardID:    1,
		BranchInfo: branchInfo,
		Node:       persistence.InternalHistoryNode{NodeID: 11, TransactionID: 21},
	}
	appendReq2 := &persistence.InternalAppendHistoryNodesRequest{
		ShardID:    1,
		BranchInfo: branchInfo,
		Node:       persistence.InternalHistoryNode{NodeID: 12, TransactionID: 22},
	}

	existingDoc := &executionDocument{
		ID:              executionDocID("ns", "wf", "run"),
		NamespaceID:     "ns",
		WorkflowID:      "wf",
		RunID:           "run",
		ShardID:         1,
		NextEventID:     20,
		DBRecordVersion: 4,
		ExecutionInfo:   dataBlobToDoc(persistence.NewDataBlob([]byte("info"), enumspb.ENCODING_TYPE_PROTO3.String())),
		ExecutionState:  dataBlobToDoc(persistence.NewDataBlob([]byte("state"), enumspb.ENCODING_TYPE_PROTO3.String())),
	}

	execCol.enqueueFind(existingDoc)
	execCol.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1})

	mutation := persistence.InternalWorkflowMutation{
		NamespaceID:      "ns",
		WorkflowID:       "wf",
		ExecutionInfo:    &persistencespb.WorkflowExecutionInfo{},
		ExecutionState:   &persistencespb.WorkflowExecutionState{RunId: "run", CreateRequestId: "req", State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
		NextEventID:      22,
		Condition:        existingDoc.NextEventID,
		DBRecordVersion:  existingDoc.DBRecordVersion + 1,
		LastWriteVersion: 33,
	}

	req := &persistence.InternalUpdateWorkflowExecutionRequest{
		ShardID:                 1,
		RangeID:                 2,
		Mode:                    persistence.UpdateWorkflowModeIgnoreCurrent,
		UpdateWorkflowMutation:  mutation,
		UpdateWorkflowNewEvents: []*persistence.InternalAppendHistoryNodesRequest{appendReq1},
		NewWorkflowNewEvents:    []*persistence.InternalAppendHistoryNodesRequest{appendReq2},
	}

	require.NoError(t, store.UpdateWorkflowExecution(ctx, req))
	require.Len(t, historyCol.insertDocs, 2)
}

func TestConflictResolveWorkflowExecution_AppendsNewEvents(t *testing.T) {
	ctx := context.Background()
	historyCol := newFakeCollection(t)
	execCol := newFakeCollection(t)
	currCol := newFakeCollection(t)
	store := &executionStore{
		transactionalStore:  transactionalStore{client: &fakeMongoClient{}, metricsHandler: metrics.NoopMetricsHandler},
		transactionsEnabled: true,
		historyNodesCol:     historyCol,
		executionsCol:       execCol,
		historyBranchesCol:  newFakeCollection(t),
		currentExecsCol:     currCol,
		serializer:          serialization.NewSerializer(),
	}
	shardCol := newFakeCollection(t)
	shardCol.enqueueFindResult(&struct {
		RangeID int64 `bson:"range_id"`
	}{RangeID: 2})
	shardDB := newFakeMongoDatabase(t)
	shardDB.collections[collectionShards] = shardCol
	store.db = shardDB

	branchInfo := &persistencespb.HistoryBranch{TreeId: "tree", BranchId: "branch"}
	appendCurrent := &persistence.InternalAppendHistoryNodesRequest{ShardID: 1, BranchInfo: branchInfo, Node: persistence.InternalHistoryNode{NodeID: 30, TransactionID: 40}}
	appendReset := &persistence.InternalAppendHistoryNodesRequest{ShardID: 1, BranchInfo: branchInfo, Node: persistence.InternalHistoryNode{NodeID: 31, TransactionID: 41}}
	appendNew := &persistence.InternalAppendHistoryNodesRequest{ShardID: 1, BranchInfo: branchInfo, Node: persistence.InternalHistoryNode{NodeID: 32, TransactionID: 42}}

	resetSnapshot := persistence.InternalWorkflowSnapshot{
		NamespaceID:      "ns",
		WorkflowID:       "wf",
		RunID:            "reset-run",
		ExecutionInfo:    &persistencespb.WorkflowExecutionInfo{},
		ExecutionState:   &persistencespb.WorkflowExecutionState{RunId: "reset-run", CreateRequestId: "reset", State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
		NextEventID:      40,
		DBRecordVersion:  7,
		LastWriteVersion: 70,
	}

	existingDoc := &executionDocument{
		ID:              executionDocID("ns", "wf", "reset-run"),
		NamespaceID:     "ns",
		WorkflowID:      "wf",
		RunID:           "reset-run",
		ShardID:         1,
		NextEventID:     40,
		DBRecordVersion: resetSnapshot.DBRecordVersion - 1,
		ExecutionInfo:   dataBlobToDoc(persistence.NewDataBlob([]byte("info"), enumspb.ENCODING_TYPE_PROTO3.String())),
		ExecutionState:  dataBlobToDoc(persistence.NewDataBlob([]byte("state"), enumspb.ENCODING_TYPE_PROTO3.String())),
	}

	execCol.enqueueFind(existingDoc)
	execCol.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1})
	execCol.enqueueInsertError(nil)
	currCol.enqueueUpdateResult(&mongo.UpdateResult{MatchedCount: 1})

	newSnapshot := persistence.InternalWorkflowSnapshot{
		NamespaceID:      "ns",
		WorkflowID:       "wf",
		RunID:            "new-run",
		ExecutionInfo:    &persistencespb.WorkflowExecutionInfo{},
		ExecutionState:   &persistencespb.WorkflowExecutionState{RunId: "new-run", CreateRequestId: "new", State: enumsspb.WORKFLOW_EXECUTION_STATE_RUNNING, Status: enumspb.WORKFLOW_EXECUTION_STATUS_RUNNING},
		NextEventID:      50,
		DBRecordVersion:  1,
		LastWriteVersion: 80,
	}

	req := &persistence.InternalConflictResolveWorkflowExecutionRequest{
		ShardID:                        1,
		RangeID:                        2,
		Mode:                           persistence.ConflictResolveWorkflowModeUpdateCurrent,
		ResetWorkflowSnapshot:          resetSnapshot,
		ResetWorkflowEventsNewEvents:   []*persistence.InternalAppendHistoryNodesRequest{appendReset},
		NewWorkflowSnapshot:            &newSnapshot,
		NewWorkflowEventsNewEvents:     []*persistence.InternalAppendHistoryNodesRequest{appendNew},
		CurrentWorkflowEventsNewEvents: []*persistence.InternalAppendHistoryNodesRequest{appendCurrent},
	}

	require.NoError(t, store.ConflictResolveWorkflowExecution(ctx, req))
	require.Len(t, historyCol.insertDocs, 3)
}

func TestAppendHistoryNodes_NewBranch(t *testing.T) {
	ctx := context.Background()
	nodesCol := newFakeCollection(t)
	branchesCol := newFakeCollection(t)
	store := &executionStore{
		historyNodesCol:    nodesCol,
		historyBranchesCol: branchesCol,
		serializer:         serialization.NewSerializer(),
	}

	eventsBlob := persistence.NewDataBlob([]byte("events"), enumspb.ENCODING_TYPE_PROTO3.String())
	treeInfo := persistence.NewDataBlob([]byte("tree"), enumspb.ENCODING_TYPE_PROTO3.String())
	branchInfo := &persistencespb.HistoryBranch{TreeId: "tree", BranchId: "branch"}

	req := &persistence.InternalAppendHistoryNodesRequest{
		ShardID:     1,
		IsNewBranch: true,
		Info:        "branch-info",
		BranchInfo:  branchInfo,
		TreeInfo:    treeInfo,
		Node: persistence.InternalHistoryNode{
			NodeID:            5,
			TransactionID:     7,
			PrevTransactionID: 3,
			Events:            eventsBlob,
		},
	}

	require.NoError(t, store.AppendHistoryNodes(ctx, req))
	require.Len(t, nodesCol.insertDocs, 1)
	nodeDoc, ok := nodesCol.insertDocs[0].(historyNodeDocument)
	require.True(t, ok)
	require.Equal(t, int64(5), nodeDoc.NodeID)
	require.Equal(t, int64(7), nodeDoc.TxnID)
	require.NotNil(t, nodeDoc.Events)
	require.Equal(t, []byte("events"), nodeDoc.Events.Data)

	require.Len(t, branchesCol.updateFilters, 1)
	filter, ok := branchesCol.updateFilters[0].(bson.M)
	require.True(t, ok)
	require.Equal(t, historyBranchDocID("tree", "branch"), filter["_id"])
	require.Len(t, branchesCol.updateUpdates, 1)
	updateSet := branchesCol.updateUpdates[0].(bson.M)["$set"].(bson.M)
	require.Equal(t, "branch-info", updateSet["info"])
	require.NotNil(t, updateSet["tree_info"])
}

func TestGetHistoryTasks_ImmediatePagination(t *testing.T) {
	ctx := context.Background()
	transferCol := newFakeCollection(t)
	store := &executionStore{
		transferTasksCol: transferCol,
	}

	shardID := int32(3)
	categoryID := taskspkg.CategoryTransfer.ID()
	key1 := taskspkg.NewImmediateKey(11)
	key2 := taskspkg.NewImmediateKey(15)
	blob1 := persistence.NewDataBlob([]byte("task-1"), enumspb.ENCODING_TYPE_PROTO3.String())
	blob2 := persistence.NewDataBlob([]byte("task-2"), enumspb.ENCODING_TYPE_PROTO3.String())

	doc1 := &immediateTaskDocument{
		ID:         immediateDocIDForKey(shardID, categoryID, key1),
		ShardID:    shardID,
		CategoryID: categoryID,
		TaskID:     key1.TaskID,
		FireTime:   key1.FireTime,
		Blob:       dataBlobToDoc(blob1),
	}
	doc2 := &immediateTaskDocument{
		ID:         immediateDocIDForKey(shardID, categoryID, key2),
		ShardID:    shardID,
		CategoryID: categoryID,
		TaskID:     key2.TaskID,
		FireTime:   key2.FireTime,
		Blob:       dataBlobToDoc(blob2),
	}

	transferCol.enqueueFindMany(doc1, doc2)

	resp, err := store.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID:             shardID,
		TaskCategory:        taskspkg.CategoryTransfer,
		InclusiveMinTaskKey: taskspkg.MinimumKey,
		BatchSize:           1,
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 1)
	require.NotNil(t, resp.NextPageToken)
	require.Equal(t, key1, resp.Tasks[0].Key)
	require.Equal(t, []byte("task-1"), resp.Tasks[0].Blob.GetData())

	decodedID, err := decodeHistoryTaskPageToken(resp.NextPageToken)
	require.NoError(t, err)
	require.Equal(t, doc1.ID, decodedID)

	transferCol.enqueueFindMany(doc2)
	resp2, err := store.GetHistoryTasks(ctx, &persistence.GetHistoryTasksRequest{
		ShardID:             shardID,
		TaskCategory:        taskspkg.CategoryTransfer,
		InclusiveMinTaskKey: taskspkg.MinimumKey,
		BatchSize:           1,
		NextPageToken:       resp.NextPageToken,
	})
	require.NoError(t, err)
	require.Len(t, resp2.Tasks, 1)
	require.Nil(t, resp2.NextPageToken)
	require.Equal(t, key2, resp2.Tasks[0].Key)
	require.Equal(t, []byte("task-2"), resp2.Tasks[0].Blob.GetData())
}

func TestPutReplicationTaskToDLQ_Deduplicates(t *testing.T) {
	ctx := context.Background()
	replicationCol := newFakeCollection(t)
	store := &executionStore{
		replicationDLQCol: replicationCol,
	}

	taskInfo := &persistencespb.ReplicationTaskInfo{TaskId: 42}
	req := &persistence.PutReplicationTaskToDLQRequest{
		ShardID:           1,
		SourceClusterName: "source",
		TaskInfo:          taskInfo,
	}

	replicationCol.enqueueInsertError(nil)
	replicationCol.enqueueInsertError(mongo.WriteException{WriteErrors: []mongo.WriteError{{Code: 11000}}})
	require.NoError(t, store.PutReplicationTaskToDLQ(ctx, req))
	require.NoError(t, store.PutReplicationTaskToDLQ(ctx, req))

}

func TestGetReplicationTasksFromDLQ_Pagination(t *testing.T) {
	ctx := context.Background()
	replicationCol := newFakeCollection(t)
	store := &executionStore{
		replicationDLQCol: replicationCol,
	}

	shardID := int32(7)
	doc1 := &replicationDLQDocument{
		ID:            replicationDLQDocID(shardID, "remote", 21),
		ShardID:       shardID,
		SourceCluster: "remote",
		TaskID:        21,
		Blob:          dataBlobToDoc(persistence.NewDataBlob([]byte("dlq-1"), enumspb.ENCODING_TYPE_PROTO3.String())),
	}
	doc2 := &replicationDLQDocument{
		ID:            replicationDLQDocID(shardID, "remote", 22),
		ShardID:       shardID,
		SourceCluster: "remote",
		TaskID:        22,
		Blob:          dataBlobToDoc(persistence.NewDataBlob([]byte("dlq-2"), enumspb.ENCODING_TYPE_PROTO3.String())),
	}

	replicationCol.enqueueFindMany(doc1, doc2)

	resp, err := store.GetReplicationTasksFromDLQ(ctx, &persistence.GetReplicationTasksFromDLQRequest{
		GetHistoryTasksRequest: persistence.GetHistoryTasksRequest{
			ShardID:             shardID,
			TaskCategory:        taskspkg.CategoryReplication,
			InclusiveMinTaskKey: taskspkg.MinimumKey,
			BatchSize:           1,
		},
		SourceClusterName: "remote",
	})
	require.NoError(t, err)
	require.Len(t, resp.Tasks, 1)
	require.NotNil(t, resp.NextPageToken)
	require.Equal(t, int64(21), resp.Tasks[0].Key.TaskID)
	require.Equal(t, []byte("dlq-1"), resp.Tasks[0].Blob.GetData())

	replicationCol.enqueueFindMany(doc2)
	resp2, err := store.GetReplicationTasksFromDLQ(ctx, &persistence.GetReplicationTasksFromDLQRequest{
		GetHistoryTasksRequest: persistence.GetHistoryTasksRequest{
			ShardID:             shardID,
			TaskCategory:        taskspkg.CategoryReplication,
			InclusiveMinTaskKey: taskspkg.MinimumKey,
			BatchSize:           1,
			NextPageToken:       resp.NextPageToken,
		},
		SourceClusterName: "remote",
	})
	require.NoError(t, err)
	require.Len(t, resp2.Tasks, 1)
	require.Nil(t, resp2.NextPageToken)
	require.Equal(t, int64(22), resp2.Tasks[0].Key.TaskID)
	require.Equal(t, []byte("dlq-2"), resp2.Tasks[0].Blob.GetData())
}

func TestDeleteReplicationTaskFromDLQ(t *testing.T) {
	replicationCol := newFakeCollection(t)
	store := &executionStore{
		replicationDLQCol: replicationCol,
	}

	req := &persistence.DeleteReplicationTaskFromDLQRequest{
		CompleteHistoryTaskRequest: persistence.CompleteHistoryTaskRequest{
			ShardID: 1,
			TaskKey: taskspkg.NewImmediateKey(55),
		},
		SourceClusterName: "remote",
	}

	require.NoError(t, store.DeleteReplicationTaskFromDLQ(context.Background(), req))
	require.Len(t, replicationCol.deleteFilters, 1)
	filter, ok := replicationCol.deleteFilters[0].(bson.M)
	require.True(t, ok)
	require.Equal(t, replicationDLQDocID(1, "remote", 55), filter["_id"])
}

func TestRangeDeleteReplicationTaskFromDLQ(t *testing.T) {
	replicationCol := newFakeCollection(t)
	store := &executionStore{
		replicationDLQCol: replicationCol,
	}

	req := &persistence.RangeDeleteReplicationTaskFromDLQRequest{
		RangeCompleteHistoryTasksRequest: persistence.RangeCompleteHistoryTasksRequest{
			ShardID:             1,
			TaskCategory:        taskspkg.CategoryReplication,
			InclusiveMinTaskKey: taskspkg.NewImmediateKey(10),
			ExclusiveMaxTaskKey: taskspkg.NewImmediateKey(20),
		},
		SourceClusterName: "remote",
	}

	require.NoError(t, store.RangeDeleteReplicationTaskFromDLQ(context.Background(), req))
	require.Len(t, replicationCol.deleteManyFilters, 1)
	filter, ok := replicationCol.deleteManyFilters[0].(bson.M)
	require.True(t, ok)
	idCond, ok := filter["_id"].(bson.M)
	require.True(t, ok)
	require.Equal(t, replicationDLQDocID(1, "remote", 10), idCond["$gte"])
	require.Equal(t, replicationDLQDocID(1, "remote", 20), idCond["$lt"])
}

func TestIsReplicationDLQEmpty(t *testing.T) {
	ctx := context.Background()
	replicationCol := newFakeCollection(t)
	store := &executionStore{
		replicationDLQCol: replicationCol,
	}

	doc := &replicationDLQDocument{ID: replicationDLQDocID(1, "remote", 1)}
	replicationCol.enqueueFindMany(doc)
	empty, err := store.IsReplicationDLQEmpty(ctx, &persistence.GetReplicationTasksFromDLQRequest{
		GetHistoryTasksRequest: persistence.GetHistoryTasksRequest{ShardID: 1},
		SourceClusterName:      "remote",
	})
	require.NoError(t, err)
	require.False(t, empty)

	replicationCol.enqueueFindMany()
	empty2, err := store.IsReplicationDLQEmpty(ctx, &persistence.GetReplicationTasksFromDLQRequest{
		GetHistoryTasksRequest: persistence.GetHistoryTasksRequest{ShardID: 1},
		SourceClusterName:      "remote",
	})
	require.NoError(t, err)
	require.True(t, empty2)
}

type fakeMongoClient struct {
	sessions []*fakeMongoSession
	startErr error
}

type fakeMongoDatabase struct {
	t           *testing.T
	collections map[string]*fakeCollection
}

func (f *fakeMongoClient) Connect(context.Context) error { return nil }

func (f *fakeMongoClient) Close(context.Context) error { return nil }

func (f *fakeMongoClient) Database(string) client.Database { return nil }

func newFakeMongoDatabase(t *testing.T) *fakeMongoDatabase {
	return &fakeMongoDatabase{t: t, collections: make(map[string]*fakeCollection)}
}

func (d *fakeMongoDatabase) Collection(name string) client.Collection {
	if col, ok := d.collections[name]; ok {
		return col
	}
	col := newFakeCollection(d.t)
	d.collections[name] = col
	return col
}

func (d *fakeMongoDatabase) RunCommand(context.Context, interface{}) client.SingleResult {
	return fakeSingleResult{}
}

func (f *fakeMongoClient) StartSession(context.Context) (client.Session, error) {
	if f.startErr != nil {
		return nil, f.startErr
	}
	sess := &fakeMongoSession{}
	f.sessions = append(f.sessions, sess)
	return sess, nil
}

func (f *fakeMongoClient) Ping(context.Context) error { return nil }

func (f *fakeMongoClient) GetTopology(context.Context) (*client.TopologyInfo, error) { return nil, nil }

func (f *fakeMongoClient) NumberSessionsInProgress() int { return len(f.sessions) }

type fakeMongoSession struct {
	withTransactionCalls int
}

func (s *fakeMongoSession) StartTransaction(...*options.TransactionOptions) error { return nil }

func (s *fakeMongoSession) CommitTransaction(context.Context) error { return nil }

func (s *fakeMongoSession) AbortTransaction(context.Context) error { return nil }

func (s *fakeMongoSession) WithTransaction(ctx context.Context, fn func(context.Context) (interface{}, error), _ ...*options.TransactionOptions) (interface{}, error) {
	s.withTransactionCalls++
	return fn(ctx)
}

func (s *fakeMongoSession) EndSession(context.Context) {}

type fakeCollection struct {
	t *testing.T

	findQueue         []client.SingleResult
	findFilters       []interface{}
	updateQueue       []*mongo.UpdateResult
	updateErrors      []error
	updateFilters     []interface{}
	updateUpdates     []interface{}
	insertDocs        []interface{}
	insertErrors      []error
	findManyQueue     [][]interface{}
	deleteFilters     []interface{}
	deleteManyFilters []interface{}
	deleteResults     []*mongo.DeleteResult
}

type fakeCursor struct {
	items []interface{}
	idx   int
}

func (c *fakeCursor) Next(context.Context) bool {
	if c.idx >= len(c.items) {
		return false
	}
	c.idx++
	return true
}

func (c *fakeCursor) Decode(v interface{}) error {
	if c.idx == 0 || c.idx > len(c.items) {
		return mongo.ErrNoDocuments
	}
	current := c.items[c.idx-1]
	if current == nil {
		return mongo.ErrNoDocuments
	}
	val := reflect.ValueOf(current)
	if val.Kind() != reflect.Ptr || val.IsNil() {
		return fmt.Errorf("invalid cursor item type %T", current)
	}
	copyPtr := reflect.New(val.Elem().Type())
	copyPtr.Elem().Set(val.Elem())
	reflect.ValueOf(v).Elem().Set(copyPtr.Elem())
	return nil
}

func (c *fakeCursor) All(_ context.Context, results interface{}) error {
	val := reflect.ValueOf(results)
	if val.Kind() != reflect.Ptr {
		return errors.New("results must be pointer")
	}
	slice := val.Elem()
	for _, item := range c.items {
		iv := reflect.ValueOf(item)
		if iv.Kind() != reflect.Ptr || iv.IsNil() {
			continue
		}
		copyPtr := reflect.New(iv.Elem().Type())
		copyPtr.Elem().Set(iv.Elem())
		slice = reflect.Append(slice, copyPtr.Elem())
	}
	val.Elem().Set(slice)
	return nil
}

func (c *fakeCursor) Close(_ context.Context) error {
	return nil
}

func (c *fakeCursor) Err() error {
	return nil
}

func newFakeCollection(t *testing.T) *fakeCollection {
	return &fakeCollection{t: t}
}

func (c *fakeCollection) enqueueFind(doc interface{}) {
	if doc == nil {
		c.findQueue = append(c.findQueue, fakeSingleResult{})
		return
	}
	val := reflect.ValueOf(doc)
	if val.Kind() != reflect.Ptr {
		c.t.Fatalf("enqueueFind expects pointer, got %T", doc)
	}
	copyPtr := reflect.New(val.Elem().Type())
	copyPtr.Elem().Set(val.Elem())
	c.findQueue = append(c.findQueue, fakeSingleResult{doc: copyPtr.Interface()})
}

func (c *fakeCollection) enqueueFindMany(docs ...interface{}) {
	copyDocs := make([]interface{}, len(docs))
	copy(copyDocs, docs)
	c.findManyQueue = append(c.findManyQueue, copyDocs)
}

func (c *fakeCollection) enqueueInsertError(err error) {
	c.insertErrors = append(c.insertErrors, err)
}

func (c *fakeCollection) enqueueUpdateResult(res *mongo.UpdateResult) {
	c.updateQueue = append(c.updateQueue, res)
	c.updateErrors = append(c.updateErrors, nil)
}

func (c *fakeCollection) enqueueFindResult(doc interface{}) {
	c.findQueue = append(c.findQueue, fakeSingleResult{doc: doc})
}

func (c *fakeCollection) enqueueDeleteResult(deletedCount int64) {
	c.deleteResults = append(c.deleteResults, &mongo.DeleteResult{DeletedCount: deletedCount})
}

func (c *fakeCollection) FindOne(_ context.Context, filter interface{}, _ ...*options.FindOneOptions) client.SingleResult {
	c.findFilters = append(c.findFilters, filter)
	if len(c.findQueue) == 0 {
		c.t.Fatalf("unexpected FindOne call with filter %v", filter)
	}
	res := c.findQueue[0]
	c.findQueue = c.findQueue[1:]
	return res
}

func (c *fakeCollection) Find(context.Context, interface{}, ...*options.FindOptions) (client.Cursor, error) {
	if len(c.findManyQueue) == 0 {
		return &fakeCursor{}, nil
	}
	items := c.findManyQueue[0]
	c.findManyQueue = c.findManyQueue[1:]
	return &fakeCursor{items: items}, nil
}

func (c *fakeCollection) InsertOne(_ context.Context, document interface{}, _ ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
	c.insertDocs = append(c.insertDocs, document)
	if len(c.insertErrors) > 0 {
		err := c.insertErrors[0]
		c.insertErrors = c.insertErrors[1:]
		return &mongo.InsertOneResult{}, err
	}
	return &mongo.InsertOneResult{}, nil
}

func (c *fakeCollection) InsertMany(context.Context, []interface{}, ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
	return nil, nil
}

func (c *fakeCollection) UpdateOne(_ context.Context, filter interface{}, update interface{}, _ ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	c.updateFilters = append(c.updateFilters, filter)
	c.updateUpdates = append(c.updateUpdates, update)
	if len(c.updateQueue) == 0 {
		return &mongo.UpdateResult{}, nil
	}
	res := c.updateQueue[0]
	err := c.updateErrors[0]
	c.updateQueue = c.updateQueue[1:]
	c.updateErrors = c.updateErrors[1:]
	return res, err
}

func (c *fakeCollection) UpdateMany(context.Context, interface{}, interface{}, ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	return &mongo.UpdateResult{}, nil
}

func (c *fakeCollection) DeleteOne(_ context.Context, filter interface{}, _ ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	c.deleteFilters = append(c.deleteFilters, filter)
	if len(c.deleteResults) == 0 {
		return &mongo.DeleteResult{}, nil
	}
	res := c.deleteResults[0]
	c.deleteResults = c.deleteResults[1:]
	return res, nil
}

func (c *fakeCollection) DeleteMany(_ context.Context, filter interface{}, _ ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	c.deleteManyFilters = append(c.deleteManyFilters, filter)
	return &mongo.DeleteResult{}, nil
}

func (c *fakeCollection) CountDocuments(context.Context, interface{}, ...*options.CountOptions) (int64, error) {
	return 0, nil
}

func (c *fakeCollection) Aggregate(context.Context, interface{}, ...*options.AggregateOptions) (client.Cursor, error) {
	return &noopCursor{}, nil
}

func (c *fakeCollection) Indexes() client.IndexView {
	return &noopIndexView{}
}

type fakeSingleResult struct {
	doc interface{}
	err error
}

func (r fakeSingleResult) Decode(v interface{}) error {
	if r.err != nil {
		return r.err
	}
	if r.doc == nil {
		return mongo.ErrNoDocuments
	}
	dest := reflect.ValueOf(v).Elem()
	src := reflect.ValueOf(r.doc).Elem()
	dest.Set(src)
	return nil
}

func (r fakeSingleResult) Err() error {
	if r.err != nil {
		return r.err
	}
	if r.doc == nil {
		return mongo.ErrNoDocuments
	}
	return nil
}

// noopDatabase implements client.Database returning noop collections for testing the stub constructor.
type noopDatabase struct{}

func (d *noopDatabase) Collection(string) client.Collection {
	return &noopCollection{}
}

func (d *noopDatabase) RunCommand(context.Context, interface{}) client.SingleResult {
	return noopSingleResult{}
}

type noopCollection struct{}

func (c *noopCollection) FindOne(context.Context, interface{}, ...*options.FindOneOptions) client.SingleResult {
	return noopSingleResult{}
}

func (c *noopCollection) Find(context.Context, interface{}, ...*options.FindOptions) (client.Cursor, error) {
	return &noopCursor{}, nil
}

func (c *noopCollection) InsertOne(context.Context, interface{}, ...*options.InsertOneOptions) (*mongo.InsertOneResult, error) {
	return nil, nil
}

func (c *noopCollection) InsertMany(context.Context, []interface{}, ...*options.InsertManyOptions) (*mongo.InsertManyResult, error) {
	return nil, nil
}

func (c *noopCollection) UpdateOne(context.Context, interface{}, interface{}, ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	return nil, nil
}

func (c *noopCollection) UpdateMany(context.Context, interface{}, interface{}, ...*options.UpdateOptions) (*mongo.UpdateResult, error) {
	return nil, nil
}

func (c *noopCollection) DeleteOne(context.Context, interface{}, ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	return nil, nil
}

func (c *noopCollection) DeleteMany(context.Context, interface{}, ...*options.DeleteOptions) (*mongo.DeleteResult, error) {
	return nil, nil
}

func (c *noopCollection) CountDocuments(context.Context, interface{}, ...*options.CountOptions) (int64, error) {
	return 0, nil
}

func (c *noopCollection) Aggregate(context.Context, interface{}, ...*options.AggregateOptions) (client.Cursor, error) {
	return &noopCursor{}, nil
}

func (c *noopCollection) Indexes() client.IndexView {
	return &noopIndexView{}
}

type noopSingleResult struct{}

func (noopSingleResult) Decode(interface{}) error { return nil }

func (noopSingleResult) Err() error { return nil }

type noopCursor struct{}

func (noopCursor) Next(context.Context) bool { return false }

func (noopCursor) Decode(interface{}) error { return nil }

func (noopCursor) All(context.Context, interface{}) error { return nil }

func (noopCursor) Close(context.Context) error { return nil }

func (noopCursor) Err() error { return nil }

type noopIndexView struct{}

func (noopIndexView) List(context.Context, ...*options.ListIndexesOptions) (client.Cursor, error) {
	return &noopCursor{}, nil
}

func (noopIndexView) CreateOne(context.Context, mongo.IndexModel, ...*options.CreateIndexesOptions) (string, error) {
	return "", nil
}

func (noopIndexView) DropOne(context.Context, string, ...*options.DropIndexesOptions) error {
	return nil
}
