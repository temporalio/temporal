package mongodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	commonpb "go.temporal.io/api/common/v1"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	enumsspb "go.temporal.io/server/api/enums/v1"
	persistencespb "go.temporal.io/server/api/persistence/v1"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/log/tag"
	"go.temporal.io/server/common/metrics"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
	"go.temporal.io/server/common/persistence/serialization"
	taskspkg "go.temporal.io/server/service/history/tasks"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	collectionExecutions        = "executions"
	collectionCurrentExecutions = "current_executions"
	collectionHistoryBranches   = "history_branches"
	collectionHistoryNodes      = "history_nodes"
	collectionTransferTasks     = "transfer_tasks"
	collectionTimerTasks        = "timer_tasks"
	collectionReplicationTasks  = "replication_tasks"
	collectionVisibilityTasks   = "visibility_tasks"
	collectionReplicationDLQ    = "replication_dlq_tasks"
)

type (
	executionStore struct {
		persistence.HistoryBranchUtilImpl

		transactionalStore

		db                  client.Database
		cfg                 config.MongoDB
		logger              log.Logger
		transactionsEnabled bool

		serializer serialization.Serializer

		executionsCol       client.Collection
		currentExecsCol     client.Collection
		historyBranchesCol  client.Collection
		historyNodesCol     client.Collection
		transferTasksCol    client.Collection
		timerTasksCol       client.Collection
		replicationTasksCol client.Collection
		visibilityTasksCol  client.Collection
		replicationDLQCol   client.Collection
	}

	dataBlobDoc struct {
		Data     []byte `bson:"data,omitempty"`
		Encoding string `bson:"encoding,omitempty"`
	}

	int64BlobDoc struct {
		ID   int64        `bson:"id"`
		Blob *dataBlobDoc `bson:"blob,omitempty"`
	}

	stringBlobDoc struct {
		Key  string       `bson:"key"`
		Blob *dataBlobDoc `bson:"blob,omitempty"`
	}

	chasmNodeDoc struct {
		Key           string       `bson:"key"`
		Metadata      *dataBlobDoc `bson:"metadata,omitempty"`
		Data          *dataBlobDoc `bson:"data,omitempty"`
		CassandraBlob *dataBlobDoc `bson:"cassandra_blob,omitempty"`
	}

	historyTaskDoc struct {
		CategoryID int          `bson:"category_id"`
		FireTime   time.Time    `bson:"fire_time"`
		TaskID     int64        `bson:"task_id"`
		Blob       *dataBlobDoc `bson:"blob,omitempty"`
	}

	historyBranchDocument struct {
		ID       string       `bson:"_id"`
		ShardID  int32        `bson:"shard_id"`
		TreeID   string       `bson:"tree_id"`
		BranchID string       `bson:"branch_id"`
		Info     string       `bson:"info,omitempty"`
		TreeInfo *dataBlobDoc `bson:"tree_info,omitempty"`
	}

	historyNodeDocument struct {
		ID        string       `bson:"_id"`
		ShardID   int32        `bson:"shard_id"`
		TreeID    string       `bson:"tree_id"`
		BranchID  string       `bson:"branch_id"`
		NodeID    int64        `bson:"node_id"`
		TxnID     int64        `bson:"txn_id"`
		PrevTxnID int64        `bson:"prev_txn_id"`
		Events    *dataBlobDoc `bson:"events,omitempty"`
	}

	immediateTaskDocument struct {
		ID         string       `bson:"_id"`
		ShardID    int32        `bson:"shard_id"`
		CategoryID int          `bson:"category_id"`
		TaskID     int64        `bson:"task_id"`
		FireTime   time.Time    `bson:"fire_time"`
		Blob       *dataBlobDoc `bson:"blob,omitempty"`
	}

	scheduledTaskDocument struct {
		ID         string       `bson:"_id"`
		ShardID    int32        `bson:"shard_id"`
		CategoryID int          `bson:"category_id"`
		FireTime   time.Time    `bson:"fire_time"`
		TaskID     int64        `bson:"task_id"`
		Blob       *dataBlobDoc `bson:"blob,omitempty"`
	}

	replicationDLQDocument struct {
		ID            string       `bson:"_id"`
		ShardID       int32        `bson:"shard_id"`
		SourceCluster string       `bson:"source_cluster_name"`
		TaskID        int64        `bson:"task_id"`
		Blob          *dataBlobDoc `bson:"blob,omitempty"`
	}

	executionDocument struct {
		ID                   string           `bson:"_id"`
		ShardID              int32            `bson:"shard_id"`
		NamespaceID          string           `bson:"namespace_id"`
		WorkflowID           string           `bson:"workflow_id"`
		RunID                string           `bson:"run_id"`
		NextEventID          int64            `bson:"next_event_id"`
		DBRecordVersion      int64            `bson:"db_record_version"`
		LastWriteVersion     int64            `bson:"last_write_version"`
		StartVersion         int64            `bson:"start_version"`
		State                int32            `bson:"state"`
		Status               int32            `bson:"status"`
		Condition            int64            `bson:"condition"`
		StateTransitionCount int64            `bson:"state_transition_count,omitempty"`
		CreateRequestID      string           `bson:"create_request_id,omitempty"`
		UpdateTime           time.Time        `bson:"update_time"`
		StartTime            *time.Time       `bson:"start_time,omitempty"`
		ExecutionTime        *time.Time       `bson:"execution_time,omitempty"`
		VisibilityTime       *time.Time       `bson:"visibility_time,omitempty"`
		ExecutionInfo        *dataBlobDoc     `bson:"execution_info"`
		ExecutionState       *dataBlobDoc     `bson:"execution_state"`
		ActivityInfos        []int64BlobDoc   `bson:"activity_infos,omitempty"`
		TimerInfos           []stringBlobDoc  `bson:"timer_infos,omitempty"`
		ChildExecutionInfos  []int64BlobDoc   `bson:"child_execution_infos,omitempty"`
		RequestCancelInfos   []int64BlobDoc   `bson:"request_cancel_infos,omitempty"`
		SignalInfos          []int64BlobDoc   `bson:"signal_infos,omitempty"`
		SignalRequestedIDs   []string         `bson:"signal_requested_ids,omitempty"`
		ChasmNodes           []chasmNodeDoc   `bson:"chasm_nodes,omitempty"`
		BufferedEvents       *dataBlobDoc     `bson:"buffered_events,omitempty"`
		Tasks                []historyTaskDoc `bson:"tasks,omitempty"`
		Checksum             *dataBlobDoc     `bson:"checksum,omitempty"`
	}

	currentExecutionDocument struct {
		ID               string     `bson:"_id"`
		ShardID          int32      `bson:"shard_id"`
		NamespaceID      string     `bson:"namespace_id"`
		WorkflowID       string     `bson:"workflow_id"`
		RunID            string     `bson:"run_id"`
		CreateRequestID  string     `bson:"create_request_id,omitempty"`
		State            int32      `bson:"state"`
		Status           int32      `bson:"status"`
		LastWriteVersion int64      `bson:"last_write_version"`
		DBRecordVersion  int64      `bson:"db_record_version"`
		StartTime        *time.Time `bson:"start_time,omitempty"`
		UpdateTime       time.Time  `bson:"update_time"`
	}

	currentExecutionWriteConflictError struct {
		namespaceID string
		workflowID  string
		message     string
	}
)

func (e *currentExecutionWriteConflictError) Error() string {
	return e.message
}

var _ persistence.ExecutionStore = (*executionStore)(nil)

// NewExecutionStore returns a MongoDB-backed implementation of persistence.ExecutionStore.
func NewExecutionStore(
	db client.Database,
	mongoClient client.Client,
	cfg config.MongoDB,
	logger log.Logger,
	metricsHandler metrics.Handler,
	transactionsEnabled bool,
) (persistence.ExecutionStore, error) {
	if !transactionsEnabled {
		return nil, errors.New("mongodb execution store requires transactions-enabled topology")
	}
	if mongoClient == nil {
		return nil, errors.New("mongodb execution store requires client with session support")
	}

	store := &executionStore{
		transactionalStore:  newTransactionalStore(mongoClient, metricsHandler),
		db:                  db,
		cfg:                 cfg,
		logger:              logger,
		transactionsEnabled: transactionsEnabled,
		serializer:          serialization.NewSerializer(),
		executionsCol:       db.Collection(collectionExecutions),
		currentExecsCol:     db.Collection(collectionCurrentExecutions),
		historyBranchesCol:  db.Collection(collectionHistoryBranches),
		historyNodesCol:     db.Collection(collectionHistoryNodes),
		transferTasksCol:    db.Collection(collectionTransferTasks),
		timerTasksCol:       db.Collection(collectionTimerTasks),
		replicationTasksCol: db.Collection(collectionReplicationTasks),
		visibilityTasksCol:  db.Collection(collectionVisibilityTasks),
		replicationDLQCol:   db.Collection(collectionReplicationDLQ),
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := store.ensureIndexes(ctx); err != nil {
		return nil, err
	}

	return store, nil
}

func (s *executionStore) GetName() string {
	return "mongodb"
}

func (s *executionStore) Close() {
	// Factory manages client lifecycle.
}

func (s *executionStore) ensureIndexes(ctx context.Context) error {
	if err := s.ensureExecutionIndexes(ctx); err != nil {
		return err
	}
	return s.ensureCurrentExecutionIndexes(ctx)
}

func executionDocID(namespaceID, workflowID, runID string) string {
	return fmt.Sprintf("%s|%s|%s", namespaceID, workflowID, runID)
}

func currentExecutionDocID(namespaceID, workflowID string) string {
	return fmt.Sprintf("%s|%s", namespaceID, workflowID)
}

func timePtrFromProto(ts *timestamppb.Timestamp) *time.Time {
	if ts == nil {
		return nil
	}
	t := ts.AsTime().UTC()
	return &t
}

func dataBlobToDoc(blob *commonpb.DataBlob) *dataBlobDoc {
	if blob == nil {
		return nil
	}
	encoding := blob.GetEncodingType().String()
	if encoding == "" {
		encoding = enumspb.ENCODING_TYPE_PROTO3.String()
	}
	return &dataBlobDoc{
		Data:     append([]byte(nil), blob.GetData()...),
		Encoding: encoding,
	}
}

func mapInt64Blobs(m map[int64]*commonpb.DataBlob) []int64BlobDoc {
	if len(m) == 0 {
		return nil
	}
	ids := make([]int64, 0, len(m))
	for id := range m {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	docs := make([]int64BlobDoc, 0, len(ids))
	for _, id := range ids {
		docs = append(docs, int64BlobDoc{ID: id, Blob: dataBlobToDoc(m[id])})
	}
	return docs
}

func mapStringBlobs(m map[string]*commonpb.DataBlob) []stringBlobDoc {
	if len(m) == 0 {
		return nil
	}
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	docs := make([]stringBlobDoc, 0, len(keys))
	for _, key := range keys {
		docs = append(docs, stringBlobDoc{Key: key, Blob: dataBlobToDoc(m[key])})
	}
	return docs
}

func mapChasmNodes(nodes map[string]persistence.InternalChasmNode) []chasmNodeDoc {
	if len(nodes) == 0 {
		return nil
	}
	keys := make([]string, 0, len(nodes))
	for key := range nodes {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	docs := make([]chasmNodeDoc, 0, len(keys))
	for _, key := range keys {
		node := nodes[key]
		docs = append(docs, chasmNodeDoc{
			Key:           key,
			Metadata:      dataBlobToDoc(node.Metadata),
			Data:          dataBlobToDoc(node.Data),
			CassandraBlob: dataBlobToDoc(node.CassandraBlob),
		})
	}
	return docs
}

func mapSignalRequestedIDs(ids map[string]struct{}) []string {
	if len(ids) == 0 {
		return nil
	}
	result := make([]string, 0, len(ids))
	for id := range ids {
		result = append(result, id)
	}
	sort.Strings(result)
	return result
}

func mapHistoryTasks(tasksByCategory map[taskspkg.Category][]persistence.InternalHistoryTask) []historyTaskDoc {
	if len(tasksByCategory) == 0 {
		return nil
	}
	docs := make([]historyTaskDoc, 0)
	for category, taskList := range tasksByCategory {
		categoryID := category.ID()
		for _, task := range taskList {
			docs = append(docs, historyTaskDoc{
				CategoryID: categoryID,
				FireTime:   task.Key.FireTime.UTC(),
				TaskID:     task.Key.TaskID,
				Blob:       dataBlobToDoc(task.Blob),
			})
		}
	}
	sort.Slice(docs, func(i, j int) bool {
		if docs[i].CategoryID != docs[j].CategoryID {
			return docs[i].CategoryID < docs[j].CategoryID
		}
		if docs[i].FireTime.Equal(docs[j].FireTime) {
			return docs[i].TaskID < docs[j].TaskID
		}
		return docs[i].FireTime.Before(docs[j].FireTime)
	})
	return docs
}

func dataBlobDocToBlob(doc *dataBlobDoc) *commonpb.DataBlob {
	if doc == nil {
		return nil
	}
	encoding := doc.Encoding
	if encoding == "" {
		encoding = enumspb.ENCODING_TYPE_PROTO3.String()
	}
	data := append([]byte(nil), doc.Data...)
	return persistence.NewDataBlob(data, encoding)
}

func docsToInt64Map(docs []int64BlobDoc) map[int64]*commonpb.DataBlob {
	result := make(map[int64]*commonpb.DataBlob, len(docs))
	for _, doc := range docs {
		result[doc.ID] = dataBlobDocToBlob(doc.Blob)
	}
	return result
}

func docsToStringMap(docs []stringBlobDoc) map[string]*commonpb.DataBlob {
	result := make(map[string]*commonpb.DataBlob, len(docs))
	for _, doc := range docs {
		result[doc.Key] = dataBlobDocToBlob(doc.Blob)
	}
	return result
}

func docsToChasmMap(docs []chasmNodeDoc) map[string]persistence.InternalChasmNode {
	result := make(map[string]persistence.InternalChasmNode, len(docs))
	for _, doc := range docs {
		result[doc.Key] = persistence.InternalChasmNode{
			Metadata:      dataBlobDocToBlob(doc.Metadata),
			Data:          dataBlobDocToBlob(doc.Data),
			CassandraBlob: dataBlobDocToBlob(doc.CassandraBlob),
		}
	}
	return result
}

func docsToSignalSet(ids []string) map[string]struct{} {
	set := make(map[string]struct{}, len(ids))
	for _, id := range ids {
		set[id] = struct{}{}
	}
	return set
}

func copyStringSlice(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

func sortableInt64(n int64) string {
	return fmt.Sprintf("%020d", uint64(n)^uint64(1<<63))
}

func replicationDLQDocID(shardID int32, source string, taskID int64) string {
	return fmt.Sprintf("%d|%s|%s", shardID, source, sortableInt64(taskID))
}

func historyBranchDocID(treeID, branchID string) string {
	return fmt.Sprintf("%s|%s", treeID, branchID)
}

func historyNodeDocID(treeID, branchID string, nodeID, txnID int64) string {
	return fmt.Sprintf("%s|%s|%s|%s", treeID, branchID, sortableInt64(nodeID), sortableInt64(txnID))
}

func immediateTaskDocID(shardID int32, categoryID int, taskID int64) string {
	return fmt.Sprintf("%d|%d|%s", shardID, categoryID, sortableInt64(taskID))
}

func scheduledTaskDocID(shardID int32, categoryID int, fireTime time.Time, taskID int64) string {
	return fmt.Sprintf("%d|%d|%s|%s", shardID, categoryID, fireTime.UTC().Format("2006-01-02T15:04:05.000000000Z07:00"), sortableInt64(taskID))
}

func immediateDocIDForKey(shardID int32, categoryID int, key taskspkg.Key) string {
	return immediateTaskDocID(shardID, categoryID, key.TaskID)
}

func scheduledDocIDForKey(shardID int32, categoryID int, key taskspkg.Key) string {
	return scheduledTaskDocID(shardID, categoryID, key.FireTime, key.TaskID)
}

func (s *executionStore) taskCollectionForCategory(
	category taskspkg.Category,
) (client.Collection, int, taskspkg.CategoryType, error) {
	switch category.ID() {
	case taskspkg.CategoryIDTransfer:
		return s.transferTasksCol, category.ID(), taskspkg.CategoryTypeImmediate, nil
	case taskspkg.CategoryIDTimer:
		return s.timerTasksCol, category.ID(), taskspkg.CategoryTypeScheduled, nil
	case taskspkg.CategoryIDReplication:
		return s.replicationTasksCol, category.ID(), taskspkg.CategoryTypeImmediate, nil
	case taskspkg.CategoryIDVisibility:
		return s.visibilityTasksCol, category.ID(), taskspkg.CategoryTypeImmediate, nil
	default:
		// For testing purposes, we need to support fake categories
		if category.Type() == taskspkg.CategoryTypeImmediate {
			return s.transferTasksCol, category.ID(), taskspkg.CategoryTypeImmediate, nil
		}
		if category.Type() == taskspkg.CategoryTypeScheduled {
			return s.timerTasksCol, category.ID(), taskspkg.CategoryTypeScheduled, nil
		}
		return nil, 0, 0, serviceerror.NewUnimplemented(fmt.Sprintf("MongoDB ExecutionStore: unsupported task category %v", category))
	}
}

func taskKeySpecified(key taskspkg.Key) bool {
	return !key.FireTime.IsZero() || key.TaskID != 0
}

func (s *executionStore) insertHistoryTaskDoc(
	ctx context.Context,
	col client.Collection,
	categoryType taskspkg.CategoryType,
	categoryID int,
	shardID int32,
	task persistence.InternalHistoryTask,
) error {
	if err := taskspkg.ValidateKey(task.Key); err != nil {
		return serviceerror.NewInvalidArgument(fmt.Sprintf("invalid task key: %v", err))
	}

	blobDoc := dataBlobToDoc(task.Blob)
	switch categoryType {
	case taskspkg.CategoryTypeImmediate:
		doc := immediateTaskDocument{
			ID:         immediateDocIDForKey(shardID, categoryID, task.Key),
			ShardID:    shardID,
			CategoryID: categoryID,
			TaskID:     task.Key.TaskID,
			FireTime:   task.Key.FireTime.UTC(),
			Blob:       blobDoc,
		}
		if _, err := col.InsertOne(ctx, doc); err != nil {
			if mongo.IsDuplicateKeyError(err) {
				return nil
			}
			return serviceerror.NewUnavailablef("failed to insert immediate task: %v", err)
		}
	case taskspkg.CategoryTypeScheduled:
		doc := scheduledTaskDocument{
			ID:         scheduledDocIDForKey(shardID, categoryID, task.Key),
			ShardID:    shardID,
			CategoryID: categoryID,
			FireTime:   task.Key.FireTime.UTC(),
			TaskID:     task.Key.TaskID,
			Blob:       blobDoc,
		}
		if _, err := col.InsertOne(ctx, doc); err != nil {
			if mongo.IsDuplicateKeyError(err) {
				return nil
			}
			return serviceerror.NewUnavailablef("failed to insert scheduled task: %v", err)
		}
	default:
		return serviceerror.NewInternalf("unknown task category type: %v", categoryType)
	}
	return nil
}

func (s *executionStore) insertWorkflowTasks(
	ctx context.Context,
	shardID int32,
	tasksByCategory map[taskspkg.Category][]persistence.InternalHistoryTask,
) error {
	if len(tasksByCategory) == 0 {
		return nil
	}

	for category, taskList := range tasksByCategory {
		if len(taskList) == 0 {
			continue
		}

		col, categoryID, categoryType, err := s.taskCollectionForCategory(category)
		if err != nil {
			return err
		}

		for _, task := range taskList {
			if err := s.insertHistoryTaskDoc(ctx, col, categoryType, categoryID, shardID, task); err != nil {
				return err
			}
		}
	}

	return nil
}

func buildHistoryTaskFilter(
	shardID int32,
	categoryID int,
	categoryType taskspkg.CategoryType,
	tokenID string,
	minKey taskspkg.Key,
	maxKey taskspkg.Key,
) (bson.M, error) {
	filter := bson.M{
		"shard_id":    shardID,
		"category_id": categoryID,
	}

	idCond := bson.M{}
	if tokenID != "" {
		idCond["$gt"] = tokenID
	}

	switch categoryType {
	case taskspkg.CategoryTypeImmediate:
		if tokenID == "" && taskKeySpecified(minKey) {
			idCond["$gte"] = immediateDocIDForKey(shardID, categoryID, minKey)
		}
		if taskKeySpecified(maxKey) {
			idCond["$lt"] = immediateDocIDForKey(shardID, categoryID, maxKey)
		}
	case taskspkg.CategoryTypeScheduled:
		if tokenID == "" && taskKeySpecified(minKey) {
			idCond["$gte"] = scheduledDocIDForKey(shardID, categoryID, minKey)
		}
		if taskKeySpecified(maxKey) {
			idCond["$lt"] = scheduledDocIDForKey(shardID, categoryID, maxKey)
		}
	default:
		return nil, serviceerror.NewInternalf("unknown task category type: %v", categoryType)
	}

	if len(idCond) > 0 {
		filter["_id"] = idCond
	}

	return filter, nil
}

func decodeImmediateHistoryTasks(
	ctx context.Context,
	cursor client.Cursor,
	pageSize int,
) ([]persistence.InternalHistoryTask, []byte, error) {
	docs := make([]immediateTaskDocument, 0, pageSize+1)
	for cursor.Next(ctx) {
		var doc immediateTaskDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, nil, serviceerror.NewUnavailablef("failed to decode immediate task: %v", err)
		}
		docs = append(docs, doc)
	}
	if err := cursor.Err(); err != nil {
		return nil, nil, serviceerror.NewUnavailablef("iteration error reading immediate tasks: %v", err)
	}
	if len(docs) == 0 {
		return nil, nil, nil
	}

	var nextToken []byte
	if len(docs) > pageSize {
		last := docs[pageSize-1]
		token, err := encodeHistoryTaskPageToken(last.ID)
		if err != nil {
			return nil, nil, serviceerror.NewUnavailablef("failed to encode history task page token: %v", err)
		}
		nextToken = token
		docs = docs[:pageSize]
	}

	tasks := make([]persistence.InternalHistoryTask, len(docs))
	for i, doc := range docs {
		tasks[i] = persistence.InternalHistoryTask{
			Key:  taskspkg.NewKey(doc.FireTime.UTC(), doc.TaskID),
			Blob: dataBlobDocToBlob(doc.Blob),
		}
	}
	return tasks, nextToken, nil
}

func decodeScheduledHistoryTasks(
	ctx context.Context,
	cursor client.Cursor,
	pageSize int,
) ([]persistence.InternalHistoryTask, []byte, error) {
	docs := make([]scheduledTaskDocument, 0, pageSize+1)
	for cursor.Next(ctx) {
		var doc scheduledTaskDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, nil, serviceerror.NewUnavailablef("failed to decode scheduled task: %v", err)
		}
		docs = append(docs, doc)
	}
	if err := cursor.Err(); err != nil {
		return nil, nil, serviceerror.NewUnavailablef("iteration error reading scheduled tasks: %v", err)
	}
	if len(docs) == 0 {
		return nil, nil, nil
	}

	var nextToken []byte
	if len(docs) > pageSize {
		last := docs[pageSize-1]
		token, err := encodeHistoryTaskPageToken(last.ID)
		if err != nil {
			return nil, nil, serviceerror.NewUnavailablef("failed to encode history task page token: %v", err)
		}
		nextToken = token
		docs = docs[:pageSize]
	}

	tasks := make([]persistence.InternalHistoryTask, len(docs))
	for i, doc := range docs {
		fireTime := doc.FireTime.UTC()
		// Try to parse high-precision time from ID
		parts := strings.Split(doc.ID, "|")
		if len(parts) >= 3 {
			if parsed, err := time.Parse(time.RFC3339Nano, parts[2]); err == nil {
				fireTime = parsed.UTC()
			}
		}

		tasks[i] = persistence.InternalHistoryTask{
			Key:  taskspkg.NewKey(fireTime, doc.TaskID),
			Blob: dataBlobDocToBlob(doc.Blob),
		}
	}
	return tasks, nextToken, nil
}

type listExecutionsPageToken struct {
	LastID string `json:"last_id"`
}

func encodeListExecutionsToken(lastID string) ([]byte, error) {
	if lastID == "" {
		return nil, nil
	}
	return json.Marshal(listExecutionsPageToken{LastID: lastID})
}

func decodeListExecutionsToken(token []byte) (string, error) {
	if len(token) == 0 {
		return "", nil
	}
	var page listExecutionsPageToken
	if err := json.Unmarshal(token, &page); err != nil {
		return "", err
	}
	return page.LastID, nil
}

type historyTaskPageToken struct {
	LastDocID string `json:"last_doc_id"`
}

func encodeHistoryTaskPageToken(lastID string) ([]byte, error) {
	if lastID == "" {
		return nil, nil
	}
	return json.Marshal(historyTaskPageToken{LastDocID: lastID})
}

func decodeHistoryTaskPageToken(token []byte) (string, error) {
	if len(token) == 0 {
		return "", nil
	}
	var page historyTaskPageToken
	if err := json.Unmarshal(token, &page); err != nil {
		return "", err
	}
	return page.LastDocID, nil
}

type historyNodePageToken struct {
	LastNodeID int64 `json:"last_node_id"`
	LastTxnID  int64 `json:"last_txn_id"`
}

func encodeHistoryNodePageToken(token historyNodePageToken) ([]byte, error) {
	if token.LastNodeID == 0 && token.LastTxnID == 0 {
		return nil, nil
	}
	return json.Marshal(token)
}

func decodeHistoryNodePageToken(data []byte) (historyNodePageToken, error) {
	var token historyNodePageToken
	if len(data) == 0 {
		return token, nil
	}
	if err := json.Unmarshal(data, &token); err != nil {
		return historyNodePageToken{}, err
	}
	return token, nil
}

func historyNodeFilter(
	shardID int32,
	treeID, branchID string,
	req *persistence.InternalReadHistoryBranchRequest,
	token historyNodePageToken,
) bson.M {
	filter := bson.M{
		"shard_id":  shardID,
		"tree_id":   treeID,
		"branch_id": branchID,
	}

	if req.ReverseOrder {
		idCond := bson.M{}
		if token.LastNodeID != 0 {
			idCond["$lt"] = historyNodeDocID(treeID, branchID, token.LastNodeID, token.LastTxnID)
		} else if req.MaxNodeID != 0 {
			idCond["$lt"] = historyNodeDocID(treeID, branchID, req.MaxNodeID, 0)
		}
		if req.MinNodeID != 0 {
			idCond["$gte"] = historyNodeDocID(treeID, branchID, req.MinNodeID, 0)
		}
		if len(idCond) > 0 {
			filter["_id"] = idCond
		}
	} else {
		// Forward order: NodeID ASC, TxnID DESC
		// SQL uses: ((node_id = ? AND txn_id > ?) OR node_id > ?) AND node_id < ?
		// Since MongoDB stores TxnID normally (not negated like SQL), we use TxnID < for same NodeID.
		if token.LastNodeID != 0 {
			// Pagination with token: get records after the last one
			// Use $and to combine $or pagination with MaxNodeID constraint
			orCond := []bson.M{
				{"node_id": bson.M{"$gt": token.LastNodeID}},
				{"node_id": token.LastNodeID, "txn_id": bson.M{"$lt": token.LastTxnID}},
			}
			if req.MaxNodeID != 0 {
				filter["$and"] = []bson.M{
					{"$or": orCond},
					{"node_id": bson.M{"$lt": req.MaxNodeID}},
				}
			} else {
				filter["$or"] = orCond
			}
		} else {
			// No token: use _id range query
			idCond := bson.M{}
			if req.MinNodeID != 0 {
				idCond["$gte"] = historyNodeDocID(treeID, branchID, req.MinNodeID, 0)
			}
			if req.MaxNodeID != 0 {
				idCond["$lt"] = historyNodeDocID(treeID, branchID, req.MaxNodeID, 0)
			}
			if len(idCond) > 0 {
				filter["_id"] = idCond
			}
		}
	}
	return filter
}

type historyBranchListPageToken struct {
	ShardID  int32  `json:"shard_id"`
	TreeID   string `json:"tree_id"`
	BranchID string `json:"branch_id"`
}

func encodeHistoryBranchListToken(token historyBranchListPageToken) ([]byte, error) {
	if token.ShardID == 0 && token.TreeID == "" && token.BranchID == "" {
		return nil, nil
	}
	return json.Marshal(token)
}

func decodeHistoryBranchListToken(data []byte) (historyBranchListPageToken, error) {
	var token historyBranchListPageToken
	if len(data) == 0 {
		return token, nil
	}
	if err := json.Unmarshal(data, &token); err != nil {
		return historyBranchListPageToken{}, err
	}
	return token, nil
}

func executionDocumentUpdateSet(doc *executionDocument) bson.M {
	return bson.M{
		"run_id":                 doc.RunID,
		"next_event_id":          doc.NextEventID,
		"last_write_version":     doc.LastWriteVersion,
		"db_record_version":      doc.DBRecordVersion,
		"start_version":          doc.StartVersion,
		"condition":              doc.Condition,
		"execution_info":         doc.ExecutionInfo,
		"execution_state":        doc.ExecutionState,
		"state":                  doc.State,
		"status":                 doc.Status,
		"create_request_id":      doc.CreateRequestID,
		"update_time":            doc.UpdateTime,
		"start_time":             doc.StartTime,
		"execution_time":         doc.ExecutionTime,
		"visibility_time":        doc.VisibilityTime,
		"activity_infos":         doc.ActivityInfos,
		"timer_infos":            doc.TimerInfos,
		"child_execution_infos":  doc.ChildExecutionInfos,
		"request_cancel_infos":   doc.RequestCancelInfos,
		"signal_infos":           doc.SignalInfos,
		"signal_requested_ids":   doc.SignalRequestedIDs,
		"chasm_nodes":            doc.ChasmNodes,
		"buffered_events":        doc.BufferedEvents,
		"tasks":                  doc.Tasks,
		"checksum":               doc.Checksum,
		"state_transition_count": doc.StateTransitionCount,
	}
}

func (s *executionStore) loadExecutionDocument(ctx context.Context, namespaceID, workflowID, runID string) (*executionDocument, error) {
	filter := bson.M{"_id": executionDocID(namespaceID, workflowID, runID)}
	var doc executionDocument
	if err := s.executionsCol.FindOne(ctx, filter).Decode(&doc); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, serviceerror.NewUnavailablef("failed to load execution document: %v", err)
	}
	return &doc, nil
}

func (s *executionStore) ensureExecutionInfo(mutation *persistence.InternalWorkflowMutation) (*persistencespb.WorkflowExecutionInfo, *commonpb.DataBlob, error) {
	info := mutation.ExecutionInfo
	blob := mutation.ExecutionInfoBlob
	var err error
	if info == nil && blob == nil {
		return nil, nil, serviceerror.NewInvalidArgument("UpdateWorkflowExecution mutation missing execution info")
	}
	if info == nil && blob != nil {
		info, err = s.serializer.WorkflowExecutionInfoFromBlob(blob)
		if err != nil {
			return nil, nil, serviceerror.NewUnavailablef("failed to deserialize execution info: %v", err)
		}
	}
	if blob == nil {
		blob, err = s.serializer.WorkflowExecutionInfoToBlob(info)
		if err != nil {
			return nil, nil, serviceerror.NewUnavailablef("failed to serialize execution info: %v", err)
		}
	}
	return info, blob, nil
}

func (s *executionStore) ensureExecutionState(mutation *persistence.InternalWorkflowMutation) (*persistencespb.WorkflowExecutionState, *commonpb.DataBlob, error) {
	state := mutation.ExecutionState
	blob := mutation.ExecutionStateBlob
	var err error
	if state == nil && blob == nil {
		return nil, nil, serviceerror.NewInvalidArgument("UpdateWorkflowExecution mutation missing execution state")
	}
	if state == nil && blob != nil {
		state, err = s.serializer.WorkflowExecutionStateFromBlob(blob)
		if err != nil {
			return nil, nil, serviceerror.NewUnavailablef("failed to deserialize execution state: %v", err)
		}
	}
	if blob == nil {
		blob, err = s.serializer.WorkflowExecutionStateToBlob(state)
		if err != nil {
			return nil, nil, serviceerror.NewUnavailablef("failed to serialize execution state: %v", err)
		}
	}
	return state, blob, nil
}

func (s *executionStore) applyMutationToExecutionDocument(
	doc *executionDocument,
	mutation *persistence.InternalWorkflowMutation,
	info *persistencespb.WorkflowExecutionInfo,
	infoBlob *commonpb.DataBlob,
	state *persistencespb.WorkflowExecutionState,
	stateBlob *commonpb.DataBlob,
) {
	// Update scalar metadata.
	doc.RunID = state.GetRunId()
	doc.NextEventID = mutation.NextEventID
	doc.LastWriteVersion = mutation.LastWriteVersion
	doc.DBRecordVersion = mutation.DBRecordVersion
	doc.Condition = mutation.Condition
	doc.ExecutionInfo = dataBlobToDoc(infoBlob)
	doc.ExecutionState = dataBlobToDoc(stateBlob)
	doc.State = int32(state.GetState())
	doc.Status = int32(state.GetStatus())
	doc.CreateRequestID = state.GetCreateRequestId()
	doc.StartTime = timePtrFromProto(state.GetStartTime())
	doc.UpdateTime = time.Now().UTC()

	if info != nil {
		doc.ExecutionTime = timePtrFromProto(info.GetExecutionTime())
		doc.VisibilityTime = timePtrFromProto(info.GetLastUpdateTime())
		doc.StateTransitionCount = info.GetStateTransitionCount()
	}

	// Activity infos.
	activityInfos := docsToInt64Map(doc.ActivityInfos)
	for id := range mutation.DeleteActivityInfos {
		delete(activityInfos, id)
	}
	for id, blob := range mutation.UpsertActivityInfos {
		activityInfos[id] = blob
	}
	doc.ActivityInfos = mapInt64Blobs(activityInfos)

	// Timer infos.
	timerInfos := docsToStringMap(doc.TimerInfos)
	for key := range mutation.DeleteTimerInfos {
		delete(timerInfos, key)
	}
	for key, blob := range mutation.UpsertTimerInfos {
		timerInfos[key] = blob
	}
	doc.TimerInfos = mapStringBlobs(timerInfos)

	// Child execution infos.
	childInfos := docsToInt64Map(doc.ChildExecutionInfos)
	for id := range mutation.DeleteChildExecutionInfos {
		delete(childInfos, id)
	}
	for id, blob := range mutation.UpsertChildExecutionInfos {
		childInfos[id] = blob
	}
	doc.ChildExecutionInfos = mapInt64Blobs(childInfos)

	// Request cancel infos.
	cancelInfos := docsToInt64Map(doc.RequestCancelInfos)
	for id := range mutation.DeleteRequestCancelInfos {
		delete(cancelInfos, id)
	}
	for id, blob := range mutation.UpsertRequestCancelInfos {
		cancelInfos[id] = blob
	}
	doc.RequestCancelInfos = mapInt64Blobs(cancelInfos)

	// Signal infos.
	signalInfos := docsToInt64Map(doc.SignalInfos)
	for id := range mutation.DeleteSignalInfos {
		delete(signalInfos, id)
	}
	for id, blob := range mutation.UpsertSignalInfos {
		signalInfos[id] = blob
	}
	doc.SignalInfos = mapInt64Blobs(signalInfos)

	// Signal requested IDs.
	signalIDs := docsToSignalSet(doc.SignalRequestedIDs)
	for id := range mutation.DeleteSignalRequestedIDs {
		delete(signalIDs, id)
	}
	for id := range mutation.UpsertSignalRequestedIDs {
		signalIDs[id] = struct{}{}
	}
	doc.SignalRequestedIDs = mapSignalRequestedIDs(signalIDs)

	// Chasm nodes.
	chasmNodes := docsToChasmMap(doc.ChasmNodes)
	for key := range mutation.DeleteChasmNodes {
		delete(chasmNodes, key)
	}
	for key, node := range mutation.UpsertChasmNodes {
		chasmNodes[key] = node
	}
	doc.ChasmNodes = mapChasmNodes(chasmNodes)

	// Buffered events.
	if mutation.ClearBufferedEvents {
		doc.BufferedEvents = nil
	} else if mutation.NewBufferedEvents != nil {
		doc.BufferedEvents = dataBlobToDoc(mutation.NewBufferedEvents)
	}

	// Tasks (best effort only tracked for observability in this stub).
	if len(mutation.Tasks) > 0 {
		doc.Tasks = mapHistoryTasks(mutation.Tasks)
	}

	doc.Checksum = dataBlobToDoc(mutation.Checksum)
}

func (s *executionStore) updateCurrentExecutionForMutation(
	ctx context.Context,
	namespaceID, workflowID string,
	state *persistencespb.WorkflowExecutionState,
	lastWriteVersion int64,
	dbRecordVersion int64,
) error {
	filter := bson.M{
		"_id":    currentExecutionDocID(namespaceID, workflowID),
		"run_id": state.GetRunId(),
	}
	set := bson.M{
		"create_request_id":  state.GetCreateRequestId(),
		"state":              int32(state.GetState()),
		"status":             int32(state.GetStatus()),
		"last_write_version": lastWriteVersion,
		"update_time":        time.Now().UTC(),
		"db_record_version":  dbRecordVersion,
	}
	if ts := state.GetStartTime(); ts != nil {
		set["start_time"] = ts.AsTime().UTC()
	} else {
		set["start_time"] = nil
	}

	result, err := s.currentExecsCol.UpdateOne(ctx, filter, bson.M{"$set": set})
	if err != nil {
		return serviceerror.NewUnavailablef("failed to update current execution document: %v", err)
	}
	if result.MatchedCount == 0 {
		existing, loadErr := s.loadCurrentExecution(ctx, namespaceID, workflowID)
		if loadErr != nil {
			return loadErr
		}
		return s.currentWorkflowConflictError(ctx, existing, "current execution update condition failed")
	}
	return nil
}

func (s *executionStore) updateCurrentExecutionForConflict(
	ctx context.Context,
	namespaceID, workflowID, previousRunID string,
	newState *persistencespb.WorkflowExecutionState,
	lastWriteVersion int64,
	dbRecordVersion int64,
) error {
	if newState == nil {
		return serviceerror.NewInvalidArgument("conflict resolve current workflow update missing execution state")
	}

	filter := bson.M{
		"_id":    currentExecutionDocID(namespaceID, workflowID),
		"run_id": previousRunID,
	}

	set := bson.M{
		"run_id":             newState.GetRunId(),
		"create_request_id":  newState.GetCreateRequestId(),
		"state":              int32(newState.GetState()),
		"status":             int32(newState.GetStatus()),
		"last_write_version": lastWriteVersion,
		"update_time":        time.Now().UTC(),
		"db_record_version":  dbRecordVersion,
	}
	if ts := newState.GetStartTime(); ts != nil {
		set["start_time"] = ts.AsTime().UTC()
	} else {
		set["start_time"] = nil
	}

	result, err := s.currentExecsCol.UpdateOne(ctx, filter, bson.M{"$set": set})
	if err != nil {
		return serviceerror.NewUnavailablef("failed to update current execution document: %v", err)
	}
	if result.MatchedCount == 0 {
		existing, loadErr := s.loadCurrentExecution(ctx, namespaceID, workflowID)
		if loadErr != nil {
			return loadErr
		}
		return s.currentWorkflowConflictError(ctx, existing, "current execution update condition failed")
	}

	return nil
}

func (s *executionStore) updateCurrentExecutionForSnapshot(
	ctx context.Context,
	namespaceID, workflowID string,
	snapshot *persistence.InternalWorkflowSnapshot,
) error {
	if snapshot == nil || snapshot.ExecutionState == nil {
		return serviceerror.NewInvalidArgument("conflict resolve snapshot missing execution state")
	}
	return s.updateCurrentExecutionForMutation(ctx, namespaceID, workflowID, snapshot.ExecutionState, snapshot.LastWriteVersion, snapshot.DBRecordVersion)
}

func (s *executionStore) assertNotCurrentExecution(ctx context.Context, namespaceID, workflowID, runID string) error {
	current, err := s.loadCurrentExecution(ctx, namespaceID, workflowID)
	if err != nil {
		return err
	}
	if current != nil && current.RunID == runID {
		message := fmt.Sprintf("Workflow execution update condition failed. workflow ID: %v, run ID: %v currently current", workflowID, runID)
		return s.currentWorkflowConflictError(ctx, current, message)
	}
	return nil
}

func (s *executionStore) applyWorkflowMutation(
	ctx context.Context,
	shardID int32,
	mutation *persistence.InternalWorkflowMutation,
) (*persistencespb.WorkflowExecutionState, error) {
	if mutation == nil {
		return nil, serviceerror.NewInvalidArgument("workflow mutation is nil")
	}

	infoProto, infoBlob, err := s.ensureExecutionInfo(mutation)
	if err != nil {
		return nil, err
	}
	stateProto, stateBlob, err := s.ensureExecutionState(mutation)
	if err != nil {
		return nil, err
	}

	doc, err := s.loadExecutionDocument(ctx, mutation.NamespaceID, mutation.WorkflowID, stateProto.GetRunId())
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, &persistence.WorkflowConditionFailedError{
			Msg:             "workflow execution not found",
			NextEventID:     mutation.Condition,
			DBRecordVersion: mutation.DBRecordVersion,
		}
	}

	// SQL-style conditional logic: If DBRecordVersion==0 check Condition(NextEventID), else check DBRecordVersion-1
	if mutation.DBRecordVersion == 0 {
		if mutation.Condition != 0 && doc.NextEventID != mutation.Condition {
			return nil, &persistence.WorkflowConditionFailedError{
				Msg:             fmt.Sprintf("workflow execution update condition failed. expected next event id %v got %v", mutation.Condition, doc.NextEventID),
				NextEventID:     doc.NextEventID,
				DBRecordVersion: doc.DBRecordVersion,
			}
		}
	} else {
		// Check DBRecordVersion-1 against current version
		if doc.DBRecordVersion != mutation.DBRecordVersion-1 {
			return nil, &persistence.WorkflowConditionFailedError{
				Msg:             fmt.Sprintf("workflow execution DB record version mismatch. expected %v got %v", mutation.DBRecordVersion-1, doc.DBRecordVersion),
				NextEventID:     doc.NextEventID,
				DBRecordVersion: doc.DBRecordVersion,
			}
		}
	}

	oldVersion := doc.DBRecordVersion
	s.applyMutationToExecutionDocument(doc, mutation, infoProto, infoBlob, stateProto, stateBlob)

	filter := bson.M{
		"_id":               doc.ID,
		"db_record_version": oldVersion,
	}
	updateSet := executionDocumentUpdateSet(doc)
	result, err := s.executionsCol.UpdateOne(ctx, filter, bson.M{"$set": updateSet})
	if err != nil {
		return nil, serviceerror.NewUnavailablef("failed to update execution document: %v", err)
	}
	if result.MatchedCount == 0 {
		return nil, &persistence.ConditionFailedError{Msg: "workflow execution update conflict"}
	}

	if err := s.insertWorkflowTasks(ctx, shardID, mutation.Tasks); err != nil {
		return nil, err
	}

	return stateProto, nil
}

//nolint:revive // cognitive complexity is driven by persistence contract coverage.
func (s *executionStore) applyUpdateWorkflowExecution(
	ctx context.Context,
	request *persistence.InternalUpdateWorkflowExecutionRequest,
) error {
	if request.NewWorkflowSnapshot != nil {
		if request.NewWorkflowSnapshot.ExecutionState == nil {
			return serviceerror.NewInvalidArgument("UpdateWorkflowExecution new snapshot missing execution state")
		}
	}

	mutation := request.UpdateWorkflowMutation
	stateProto, _, stateErr := s.ensureExecutionState(&mutation)
	if stateErr != nil {
		return stateErr
	}

	// For UpdateCurrent, fail fast on current-run mismatch even if the target execution doesn't exist.
	// This matches the cross-store contract tested in TestUpdate_NotZombie_CurrentConflict.
	if request.Mode == persistence.UpdateWorkflowModeUpdateCurrent {
		current, err := s.loadCurrentExecution(ctx, mutation.NamespaceID, mutation.WorkflowID)
		if err != nil {
			return err
		}
		if current != nil && current.RunID != stateProto.GetRunId() {
			message := fmt.Sprintf(
				"current execution update condition failed. expected run_id %v got %v",
				stateProto.GetRunId(),
				current.RunID,
			)
			return s.currentWorkflowConflictError(ctx, current, message)
		}
		if current == nil {
			return s.currentWorkflowConflictError(ctx, nil, "current execution update condition failed")
		}
	}

	stateProto, err := s.applyWorkflowMutation(ctx, request.ShardID, &mutation)
	if err != nil {
		return err
	}

	var (
		newDoc   *executionDocument
		newState *persistencespb.WorkflowExecutionState
	)
	if request.NewWorkflowSnapshot != nil {
		newDocReq := &persistence.InternalCreateWorkflowExecutionRequest{
			ShardID:             request.ShardID,
			RangeID:             request.RangeID,
			NewWorkflowSnapshot: *request.NewWorkflowSnapshot,
		}
		builtDoc, buildErr := s.buildExecutionDocument(newDocReq)
		if buildErr != nil {
			return buildErr
		}
		newDoc = builtDoc
		newState = request.NewWorkflowSnapshot.ExecutionState
	}

	switch request.Mode {
	case persistence.UpdateWorkflowModeIgnoreCurrent:
		// no-op
	case persistence.UpdateWorkflowModeBypassCurrent:
		if err := s.assertNotCurrentExecution(ctx, mutation.NamespaceID, mutation.WorkflowID, stateProto.GetRunId()); err != nil {
			return err
		}
	case persistence.UpdateWorkflowModeUpdateCurrent:
		if newState != nil {
			if err := s.updateCurrentExecutionForConflict(
				ctx,
				mutation.NamespaceID,
				mutation.WorkflowID,
				stateProto.GetRunId(),
				newState,
				request.NewWorkflowSnapshot.LastWriteVersion,
				request.NewWorkflowSnapshot.DBRecordVersion,
			); err != nil {
				return err
			}
		} else {
			if err := s.updateCurrentExecutionForMutation(ctx, mutation.NamespaceID, mutation.WorkflowID, stateProto, mutation.LastWriteVersion, mutation.DBRecordVersion); err != nil {
				return err
			}
		}
	default:
		return serviceerror.NewInternalf("UpdateWorkflowExecution: unknown mode: %v", request.Mode)
	}

	if newDoc != nil {
		if err := s.insertExecutionDocument(ctx, newDoc); err != nil {
			return err
		}
		if request.NewWorkflowSnapshot != nil {
			if err := s.insertWorkflowTasks(ctx, request.ShardID, request.NewWorkflowSnapshot.Tasks); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *executionStore) buildExecutionDocument(request *persistence.InternalCreateWorkflowExecutionRequest) (*executionDocument, error) {
	snapshot := request.NewWorkflowSnapshot
	if snapshot.ExecutionInfo == nil || snapshot.ExecutionState == nil {
		return nil, serviceerror.NewInvalidArgument("CreateWorkflowExecution requires execution info and state")
	}

	infoBlob := snapshot.ExecutionInfoBlob
	var err error
	if infoBlob == nil {
		infoBlob, err = s.serializer.WorkflowExecutionInfoToBlob(snapshot.ExecutionInfo)
		if err != nil {
			return nil, serviceerror.NewUnavailablef("failed to serialize execution info: %v", err)
		}
	}

	stateBlob := snapshot.ExecutionStateBlob
	if stateBlob == nil {
		stateBlob, err = s.serializer.WorkflowExecutionStateToBlob(snapshot.ExecutionState)
		if err != nil {
			return nil, serviceerror.NewUnavailablef("failed to serialize execution state: %v", err)
		}
	}

	doc := &executionDocument{
		ID:                  executionDocID(snapshot.NamespaceID, snapshot.WorkflowID, snapshot.RunID),
		ShardID:             request.ShardID,
		NamespaceID:         snapshot.NamespaceID,
		WorkflowID:          snapshot.WorkflowID,
		RunID:               snapshot.RunID,
		NextEventID:         snapshot.NextEventID,
		DBRecordVersion:     snapshot.DBRecordVersion,
		LastWriteVersion:    snapshot.LastWriteVersion,
		StartVersion:        snapshot.StartVersion,
		State:               int32(snapshot.ExecutionState.State),
		Status:              int32(snapshot.ExecutionState.Status),
		Condition:           snapshot.Condition,
		CreateRequestID:     snapshot.ExecutionState.CreateRequestId,
		UpdateTime:          time.Now().UTC(),
		ExecutionInfo:       dataBlobToDoc(infoBlob),
		ExecutionState:      dataBlobToDoc(stateBlob),
		ActivityInfos:       mapInt64Blobs(snapshot.ActivityInfos),
		TimerInfos:          mapStringBlobs(snapshot.TimerInfos),
		ChildExecutionInfos: mapInt64Blobs(snapshot.ChildExecutionInfos),
		RequestCancelInfos:  mapInt64Blobs(snapshot.RequestCancelInfos),
		SignalInfos:         mapInt64Blobs(snapshot.SignalInfos),
		SignalRequestedIDs:  mapSignalRequestedIDs(snapshot.SignalRequestedIDs),
		ChasmNodes:          mapChasmNodes(snapshot.ChasmNodes),
		Tasks:               mapHistoryTasks(snapshot.Tasks),
		Checksum:            dataBlobToDoc(snapshot.Checksum),
	}

	if snapshot.ExecutionInfo != nil {
		doc.StateTransitionCount = snapshot.ExecutionInfo.GetStateTransitionCount()
		if ts := snapshot.ExecutionInfo.GetExecutionTime(); ts != nil {
			doc.ExecutionTime = timePtrFromProto(ts)
		}
		if ts := snapshot.ExecutionInfo.GetLastUpdateTime(); ts != nil {
			doc.VisibilityTime = timePtrFromProto(ts)
		}
	}

	if snapshot.ExecutionState.StartTime != nil {
		doc.StartTime = timePtrFromProto(snapshot.ExecutionState.StartTime)
	}

	return doc, nil
}

func (s *executionStore) buildCurrentExecutionDocument(request *persistence.InternalCreateWorkflowExecutionRequest) *currentExecutionDocument {
	snapshot := request.NewWorkflowSnapshot
	state := snapshot.ExecutionState
	var startTime *time.Time
	if state != nil && state.StartTime != nil {
		startTime = timePtrFromProto(state.StartTime)
	}
	return &currentExecutionDocument{
		ID:               currentExecutionDocID(snapshot.NamespaceID, snapshot.WorkflowID),
		ShardID:          request.ShardID,
		NamespaceID:      snapshot.NamespaceID,
		WorkflowID:       snapshot.WorkflowID,
		RunID:            state.GetRunId(),
		CreateRequestID:  state.GetCreateRequestId(),
		State:            int32(state.GetState()),
		Status:           int32(state.GetStatus()),
		LastWriteVersion: snapshot.LastWriteVersion,
		DBRecordVersion:  snapshot.DBRecordVersion,
		StartTime:        startTime,
		UpdateTime:       time.Now().UTC(),
	}
}

func (s *executionStore) applyCreateCurrentExecution(
	ctx context.Context,
	request *persistence.InternalCreateWorkflowExecutionRequest,
	currentDoc *currentExecutionDocument,
) error {
	switch request.Mode {
	case persistence.CreateWorkflowModeBrandNew:
		return s.insertNewCurrentExecution(ctx, request, currentDoc)
	case persistence.CreateWorkflowModeUpdateCurrent:
		return s.updateExistingCurrentExecution(ctx, request, currentDoc)
	case persistence.CreateWorkflowModeBypassCurrent:
		return s.ensureRunIDMismatch(ctx, request, currentDoc)
	default:
		return serviceerror.NewInternalf("CreateWorkflowExecution: unknown mode %v", request.Mode)
	}
}

func (s *executionStore) insertNewCurrentExecution(
	ctx context.Context,
	request *persistence.InternalCreateWorkflowExecutionRequest,
	doc *currentExecutionDocument,
) error {
	if doc == nil {
		return nil
	}

	_, err := s.currentExecsCol.InsertOne(ctx, doc)
	if err == nil {
		return nil
	}

	if mongo.IsDuplicateKeyError(err) {
		message := fmt.Sprintf(
			"Workflow execution creation condition failed. workflow ID: %v, current run ID: %v, request run ID: %v",
			doc.WorkflowID,
			"",
			request.PreviousRunID,
		)
		return &currentExecutionWriteConflictError{
			namespaceID: doc.NamespaceID,
			workflowID:  doc.WorkflowID,
			message:     message,
		}
	}

	return serviceerror.NewUnavailablef("failed to insert current execution document: %v", err)
}

func (s *executionStore) updateExistingCurrentExecution(
	ctx context.Context,
	request *persistence.InternalCreateWorkflowExecutionRequest,
	doc *currentExecutionDocument,
) error {
	if doc == nil {
		return s.currentWorkflowConflictError(ctx, nil, "current execution not found")
	}

	filter := bson.M{
		"_id":                doc.ID,
		"run_id":             request.PreviousRunID,
		"last_write_version": request.PreviousLastWriteVersion,
		"state":              enumsspb.WORKFLOW_EXECUTION_STATE_COMPLETED,
	}

	setFields := bson.M{
		"run_id":             doc.RunID,
		"create_request_id":  doc.CreateRequestID,
		"state":              doc.State,
		"status":             doc.Status,
		"last_write_version": doc.LastWriteVersion,
		"update_time":        doc.UpdateTime,
		"db_record_version":  doc.DBRecordVersion,
	}
	if doc.StartTime != nil {
		setFields["start_time"] = *doc.StartTime
	} else {
		setFields["start_time"] = nil
	}

	update := bson.M{"$set": setFields}

	result, err := s.currentExecsCol.UpdateOne(ctx, filter, update)
	if err != nil {
		return serviceerror.NewUnavailablef("failed to update current execution document: %v", err)
	}
	if result.MatchedCount == 0 {
		existing, loadErr := s.loadCurrentExecution(ctx, doc.NamespaceID, doc.WorkflowID)
		if loadErr != nil {
			return loadErr
		}
		return s.currentWorkflowConflictError(ctx, existing, "current execution update condition failed")
	}

	return nil
}

func (s *executionStore) ensureRunIDMismatch(
	ctx context.Context,
	_ *persistence.InternalCreateWorkflowExecutionRequest,
	doc *currentExecutionDocument,
) error {
	if doc == nil {
		return nil
	}
	current, err := s.loadCurrentExecution(ctx, doc.NamespaceID, doc.WorkflowID)
	if err != nil {
		return err
	}
	if current != nil && current.RunID == doc.RunID {
		message := fmt.Sprintf("Workflow execution creation condition failed. workflow ID: %v, duplicate run ID: %v", doc.WorkflowID, doc.RunID)
		return s.currentWorkflowConflictError(ctx, current, message)
	}
	return nil
}

func (s *executionStore) loadCurrentExecution(ctx context.Context, namespaceID, workflowID string) (*currentExecutionDocument, error) {
	filter := bson.M{"_id": currentExecutionDocID(namespaceID, workflowID)}
	var doc currentExecutionDocument
	if err := s.currentExecsCol.FindOne(ctx, filter).Decode(&doc); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil
		}
		return nil, serviceerror.NewUnavailablef("failed to load current execution: %v", err)
	}
	return &doc, nil
}

func (doc *currentExecutionDocument) getRunIDOrEmpty() string {
	if doc == nil {
		return ""
	}
	return doc.RunID
}

func (s *executionStore) currentWorkflowConflictError(
	ctx context.Context,
	doc *currentExecutionDocument,
	message string,
) error {
	if doc == nil {
		return &persistence.CurrentWorkflowConditionFailedError{Msg: message}
	}

	var requestIDs map[string]*persistencespb.RequestIDInfo
	execDoc, err := s.loadExecutionDocument(ctx, doc.NamespaceID, doc.WorkflowID, doc.RunID)
	if err != nil {
		return err
	}
	if execDoc != nil {
		stateBlob := dataBlobDocToBlob(execDoc.ExecutionState)
		if stateBlob != nil {
			executionState, stateErr := s.serializer.WorkflowExecutionStateFromBlob(stateBlob)
			if stateErr != nil {
				return serviceerror.NewUnavailablef("failed to deserialize execution state for current workflow conflict: %v", stateErr)
			}
			requestIDs = executionState.RequestIds
		}
	}

	return &persistence.CurrentWorkflowConditionFailedError{
		Msg:              message,
		RequestIDs:       requestIDs,
		RunID:            doc.RunID,
		State:            enumsspb.WorkflowExecutionState(doc.State),
		Status:           enumspb.WorkflowExecutionStatus(doc.Status),
		LastWriteVersion: doc.LastWriteVersion,
		StartTime:        doc.StartTime,
	}
}

func (s *executionStore) insertExecutionDocument(ctx context.Context, doc *executionDocument) error {
	if doc == nil {
		return serviceerror.NewUnavailable("execution document is nil")
	}
	_, err := s.executionsCol.InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return &persistence.WorkflowConditionFailedError{Msg: "workflow execution already exists"}
		}
		return serviceerror.NewUnavailablef("failed to insert execution document: %v", err)
	}
	return nil
}

func (s *executionStore) ensureExecutionIndexes(ctx context.Context) error {
	idxView := s.executionsCol.Indexes()

	indexes := []struct {
		name  string
		model mongo.IndexModel
	}{
		{
			name: "executions_shard_namespace_workflow_run",
			model: mongo.IndexModel{
				Keys: bson.D{
					{Key: "shard_id", Value: 1},
					{Key: "namespace_id", Value: 1},
					{Key: "workflow_id", Value: 1},
					{Key: "run_id", Value: 1},
				},
				Options: options.Index().SetName("executions_shard_namespace_workflow_run"),
			},
		},
		{
			name: "executions_namespace_workflow_run",
			model: mongo.IndexModel{
				Keys: bson.D{
					{Key: "namespace_id", Value: 1},
					{Key: "workflow_id", Value: 1},
					{Key: "run_id", Value: 1},
				},
				Options: options.Index().SetName("executions_namespace_workflow_run"),
			},
		},
	}

	for _, spec := range indexes {
		if _, err := idxView.CreateOne(ctx, spec.model); err != nil && !isDuplicateIndexError(err) {
			return serviceerror.NewUnavailablef("failed to ensure executions index %q: %v", spec.name, err)
		}
	}

	return nil
}

func (s *executionStore) ensureCurrentExecutionIndexes(ctx context.Context) error {
	idxView := s.currentExecsCol.Indexes()

	indexes := []struct {
		name  string
		model mongo.IndexModel
	}{
		{
			name: "current_executions_namespace_workflow",
			model: mongo.IndexModel{
				Keys: bson.D{
					{Key: "namespace_id", Value: 1},
					{Key: "workflow_id", Value: 1},
				},
				Options: options.Index().SetName("current_executions_namespace_workflow").SetUnique(true),
			},
		},
		{
			name: "current_executions_shard",
			model: mongo.IndexModel{
				Keys: bson.D{
					{Key: "shard_id", Value: 1},
					{Key: "namespace_id", Value: 1},
					{Key: "workflow_id", Value: 1},
				},
				Options: options.Index().SetName("current_executions_shard"),
			},
		},
	}

	for _, spec := range indexes {
		if _, err := idxView.CreateOne(ctx, spec.model); err != nil && !isDuplicateIndexError(err) {
			return serviceerror.NewUnavailablef("failed to ensure current executions index %q: %v", spec.name, err)
		}
	}

	return nil
}

//nolint:revive // cognitive complexity reflects workflow creation flow.
func (s *executionStore) CreateWorkflowExecution(
	ctx context.Context,
	request *persistence.InternalCreateWorkflowExecutionRequest,
) (*persistence.InternalCreateWorkflowExecutionResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("CreateWorkflowExecution request is nil")
	}

	fn := func(sessCtx context.Context) (interface{}, error) {
		// Write fence: verify shard ownership before any writes
		if err := s.assertShardRangeID(sessCtx, request.ShardID, request.RangeID); err != nil {
			return nil, err
		}

		execDoc, err := s.buildExecutionDocument(request)
		if err != nil {
			return nil, err
		}

		currentDoc := s.buildCurrentExecutionDocument(request)

		if err := s.applyCreateCurrentExecution(sessCtx, request, currentDoc); err != nil {
			return nil, err
		}

		if err := s.insertExecutionDocument(sessCtx, execDoc); err != nil {
			return nil, err
		}

		if err := s.insertWorkflowTasks(sessCtx, request.ShardID, request.NewWorkflowSnapshot.Tasks); err != nil {
			return nil, err
		}

		for _, appendReq := range request.NewWorkflowNewEvents {
			if appendReq == nil {
				continue
			}
			if err := s.AppendHistoryNodes(sessCtx, appendReq); err != nil {
				return nil, err
			}
		}

		return &persistence.InternalCreateWorkflowExecutionResponse{}, nil
	}

	result, err := s.executeTransaction(ctx, fn)
	if err != nil {
		var conflictErr *currentExecutionWriteConflictError
		if errors.As(err, &conflictErr) {
			existing, loadErr := s.loadCurrentExecution(ctx, conflictErr.namespaceID, conflictErr.workflowID)
			if loadErr != nil {
				return nil, loadErr
			}
			return nil, s.currentWorkflowConflictError(ctx, existing, conflictErr.message)
		}
		return nil, err
	}
	resp, ok := result.(*persistence.InternalCreateWorkflowExecutionResponse)
	if !ok {
		return nil, serviceerror.NewInternal("unexpected create workflow transaction result type")
	}
	return resp, nil
}

func (s *executionStore) UpdateWorkflowExecution(
	ctx context.Context,
	request *persistence.InternalUpdateWorkflowExecutionRequest,
) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("UpdateWorkflowExecution request is nil")
	}

	for _, appendReq := range request.UpdateWorkflowNewEvents {
		if appendReq == nil {
			continue
		}
		if err := s.AppendHistoryNodes(ctx, appendReq); err != nil {
			return err
		}
	}
	for _, appendReq := range request.NewWorkflowNewEvents {
		if appendReq == nil {
			continue
		}
		if err := s.AppendHistoryNodes(ctx, appendReq); err != nil {
			return err
		}
	}

	_, err := s.executeTransaction(ctx, func(sessCtx context.Context) (interface{}, error) {
		// Write fence: verify shard ownership before any writes
		if err := s.assertShardRangeID(sessCtx, request.ShardID, request.RangeID); err != nil {
			return nil, err
		}
		return nil, s.applyUpdateWorkflowExecution(sessCtx, request)
	})
	return err
}

func (s *executionStore) executionDocumentToMutableState(doc *executionDocument) *persistence.InternalWorkflowMutableState {
	if doc == nil {
		return nil
	}

	bufferedEventsBlob := dataBlobDocToBlob(doc.BufferedEvents)
	var bufferedEvents []*commonpb.DataBlob
	if bufferedEventsBlob != nil {
		bufferedEvents = []*commonpb.DataBlob{bufferedEventsBlob}
	}

	state := &persistence.InternalWorkflowMutableState{
		ActivityInfos:       docsToInt64Map(doc.ActivityInfos),
		TimerInfos:          docsToStringMap(doc.TimerInfos),
		ChildExecutionInfos: docsToInt64Map(doc.ChildExecutionInfos),
		RequestCancelInfos:  docsToInt64Map(doc.RequestCancelInfos),
		SignalInfos:         docsToInt64Map(doc.SignalInfos),
		ChasmNodes:          docsToChasmMap(doc.ChasmNodes),
		SignalRequestedIDs:  copyStringSlice(doc.SignalRequestedIDs),
		ExecutionInfo:       dataBlobDocToBlob(doc.ExecutionInfo),
		ExecutionState:      dataBlobDocToBlob(doc.ExecutionState),
		BufferedEvents:      bufferedEvents,
		NextEventID:         doc.NextEventID,
		Checksum:            dataBlobDocToBlob(doc.Checksum),
		DBRecordVersion:     doc.DBRecordVersion,
	}
	return state
}

func (s *executionStore) ConflictResolveWorkflowExecution(
	ctx context.Context,
	request *persistence.InternalConflictResolveWorkflowExecutionRequest,
) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("ConflictResolveWorkflowExecution request is nil")
	}

	for _, appendReq := range request.CurrentWorkflowEventsNewEvents {
		if appendReq == nil {
			continue
		}
		if err := s.AppendHistoryNodes(ctx, appendReq); err != nil {
			return err
		}
	}
	for _, appendReq := range request.ResetWorkflowEventsNewEvents {
		if appendReq == nil {
			continue
		}
		if err := s.AppendHistoryNodes(ctx, appendReq); err != nil {
			return err
		}
	}
	for _, appendReq := range request.NewWorkflowEventsNewEvents {
		if appendReq == nil {
			continue
		}
		if err := s.AppendHistoryNodes(ctx, appendReq); err != nil {
			return err
		}
	}

	_, err := s.executeTransaction(ctx, func(sessCtx context.Context) (interface{}, error) {
		// Write fence: verify shard ownership before any writes
		if err := s.assertShardRangeID(sessCtx, request.ShardID, request.RangeID); err != nil {
			return nil, err
		}
		return nil, s.applyConflictResolveWorkflowExecution(sessCtx, request)
	})
	return err
}

//nolint:revive // cyclomatic complexity reflects persistence conflict resolution flow.
func (s *executionStore) applyConflictResolveWorkflowExecution(
	ctx context.Context,
	request *persistence.InternalConflictResolveWorkflowExecutionRequest,
) error {
	resetSnapshot := request.ResetWorkflowSnapshot
	if resetSnapshot.ExecutionState == nil {
		return serviceerror.NewInvalidArgument("ConflictResolveWorkflowExecution snapshot missing execution state")
	}

	docReq := &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID:             request.ShardID,
		RangeID:             request.RangeID,
		NewWorkflowSnapshot: resetSnapshot,
	}
	resetDoc, err := s.buildExecutionDocument(docReq)
	if err != nil {
		return err
	}
	resetDoc.UpdateTime = time.Now().UTC()

	runID := resetSnapshot.ExecutionState.GetRunId()
	existing, err := s.loadExecutionDocument(ctx, resetSnapshot.NamespaceID, resetSnapshot.WorkflowID, runID)
	if err != nil {
		return err
	}
	if existing == nil {
		return &persistence.WorkflowConditionFailedError{Msg: "workflow execution not found for conflict resolution"}
	}

	// Validate DBRecordVersion if set, advance version
	if resetSnapshot.DBRecordVersion > 0 && existing.DBRecordVersion != resetSnapshot.DBRecordVersion-1 {
		return &persistence.WorkflowConditionFailedError{
			Msg:             fmt.Sprintf("workflow execution DB record version mismatch during reset. expected %v got %v", resetSnapshot.DBRecordVersion-1, existing.DBRecordVersion),
			NextEventID:     existing.NextEventID,
			DBRecordVersion: existing.DBRecordVersion,
		}
	}

	filter := bson.M{
		"_id":               resetDoc.ID,
		"db_record_version": existing.DBRecordVersion,
	}
	updateSet := executionDocumentUpdateSet(resetDoc)
	result, err := s.executionsCol.UpdateOne(ctx, filter, bson.M{"$set": updateSet})
	if err != nil {
		return serviceerror.NewUnavailablef("failed to reset execution document: %v", err)
	}
	if result.MatchedCount == 0 {
		return &persistence.WorkflowConditionFailedError{
			Msg:             "workflow execution conflict during reset",
			NextEventID:     existing.NextEventID,
			DBRecordVersion: existing.DBRecordVersion,
		}
	}

	if err := s.insertWorkflowTasks(ctx, request.ShardID, resetSnapshot.Tasks); err != nil {
		return err
	}

	var currentState *persistencespb.WorkflowExecutionState
	if request.CurrentWorkflowMutation != nil {
		stateProto, err := s.applyWorkflowMutation(ctx, request.ShardID, request.CurrentWorkflowMutation)
		if err != nil {
			// Convert "not found" errors for current workflow to CurrentWorkflowConditionFailedError
			if request.Mode == persistence.ConflictResolveWorkflowModeUpdateCurrent {
				if workflowErr, ok := err.(*persistence.WorkflowConditionFailedError); ok {
					if strings.Contains(workflowErr.Msg, "not found") {
						return &persistence.CurrentWorkflowConditionFailedError{
							Msg:              workflowErr.Msg,
							RunID:            request.CurrentWorkflowMutation.ExecutionState.GetRunId(),
							State:            request.CurrentWorkflowMutation.ExecutionState.GetState(),
							Status:           request.CurrentWorkflowMutation.ExecutionState.GetStatus(),
							LastWriteVersion: request.CurrentWorkflowMutation.LastWriteVersion,
						}
					}
				}
			}
			return err
		}
		currentState = stateProto
	}

	var (
		newDoc   *executionDocument
		newState *persistencespb.WorkflowExecutionState
	)
	if request.NewWorkflowSnapshot != nil {
		if request.NewWorkflowSnapshot.ExecutionState == nil {
			return serviceerror.NewInvalidArgument("ConflictResolveWorkflowExecution new snapshot missing execution state")
		}
		newDocReq := &persistence.InternalCreateWorkflowExecutionRequest{
			ShardID:             request.ShardID,
			RangeID:             request.RangeID,
			NewWorkflowSnapshot: *request.NewWorkflowSnapshot,
		}
		builtDoc, err := s.buildExecutionDocument(newDocReq)
		if err != nil {
			return err
		}
		newDoc = builtDoc
		newState = request.NewWorkflowSnapshot.ExecutionState
	}

	prevRunID := resetSnapshot.ExecutionState.GetRunId()
	if currentState != nil {
		prevRunID = currentState.GetRunId()
	}

	switch request.Mode {
	case persistence.ConflictResolveWorkflowModeBypassCurrent:
		if err := s.assertNotCurrentExecution(ctx, resetSnapshot.NamespaceID, resetSnapshot.WorkflowID, runID); err != nil {
			return err
		}
	case persistence.ConflictResolveWorkflowModeUpdateCurrent:
		targetState := resetSnapshot.ExecutionState
		targetLastWriteVersion := resetSnapshot.LastWriteVersion
		targetDBRecordVersion := resetSnapshot.DBRecordVersion
		if newState != nil {
			targetState = newState
			targetLastWriteVersion = request.NewWorkflowSnapshot.LastWriteVersion
			targetDBRecordVersion = request.NewWorkflowSnapshot.DBRecordVersion
		}
		if err := s.updateCurrentExecutionForConflict(ctx, resetSnapshot.NamespaceID, resetSnapshot.WorkflowID, prevRunID, targetState, targetLastWriteVersion, targetDBRecordVersion); err != nil {
			return err
		}
	default:
		return serviceerror.NewInternalf("ConflictResolveWorkflowExecution: unknown mode: %v", request.Mode)
	}

	if newDoc != nil {
		if err := s.insertExecutionDocument(ctx, newDoc); err != nil {
			return err
		}
		if request.NewWorkflowSnapshot != nil {
			if err := s.insertWorkflowTasks(ctx, request.ShardID, request.NewWorkflowSnapshot.Tasks); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *executionStore) DeleteWorkflowExecution(
	ctx context.Context,
	request *persistence.DeleteWorkflowExecutionRequest,
) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("DeleteWorkflowExecution request is nil")
	}

	filter := bson.M{"_id": executionDocID(request.NamespaceID, request.WorkflowID, request.RunID)}
	_, err := s.executionsCol.DeleteOne(ctx, filter)
	if err != nil {
		return serviceerror.NewUnavailablef("failed to delete execution document: %v", err)
	}
	return nil
}

func (s *executionStore) DeleteCurrentWorkflowExecution(
	ctx context.Context,
	request *persistence.DeleteCurrentWorkflowExecutionRequest,
) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("DeleteCurrentWorkflowExecution request is nil")
	}

	filter := bson.M{
		"_id":    currentExecutionDocID(request.NamespaceID, request.WorkflowID),
		"run_id": request.RunID,
	}
	_, err := s.currentExecsCol.DeleteOne(ctx, filter)
	if err != nil {
		return serviceerror.NewUnavailablef("failed to delete current execution document: %v", err)
	}
	return nil
}

func (s *executionStore) GetCurrentExecution(
	ctx context.Context,
	request *persistence.GetCurrentExecutionRequest,
) (*persistence.InternalGetCurrentExecutionResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("GetCurrentExecution request is nil")
	}
	doc, err := s.loadCurrentExecution(ctx, request.NamespaceID, request.WorkflowID)
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, serviceerror.NewNotFound("current execution not found")
	}
	state := &persistencespb.WorkflowExecutionState{
		CreateRequestId: doc.CreateRequestID,
		State:           enumsspb.WorkflowExecutionState(doc.State),
		Status:          enumspb.WorkflowExecutionStatus(doc.Status),
	}
	if doc.StartTime != nil {
		state.StartTime = timestamppb.New(*doc.StartTime)
	}
	return &persistence.InternalGetCurrentExecutionResponse{
		RunID:          doc.RunID,
		ExecutionState: state,
	}, nil
}

func (s *executionStore) GetWorkflowExecution(
	ctx context.Context,
	request *persistence.GetWorkflowExecutionRequest,
) (*persistence.InternalGetWorkflowExecutionResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("GetWorkflowExecution request is nil")
	}
	doc, err := s.loadExecutionDocument(ctx, request.NamespaceID, request.WorkflowID, request.RunID)
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return nil, serviceerror.NewNotFoundf("workflow execution not found. workflowID: %v, runID: %v", request.WorkflowID, request.RunID)
	}
	state := s.executionDocumentToMutableState(doc)
	return &persistence.InternalGetWorkflowExecutionResponse{
		State:           state,
		DBRecordVersion: doc.DBRecordVersion,
	}, nil
}

func (s *executionStore) SetWorkflowExecution(
	ctx context.Context,
	request *persistence.InternalSetWorkflowExecutionRequest,
) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("SetWorkflowExecution request is nil")
	}

	snapshot := request.SetWorkflowSnapshot
	if snapshot.ExecutionState == nil {
		return serviceerror.NewInvalidArgument("SetWorkflowExecution snapshot missing execution state")
	}

	docReq := &persistence.InternalCreateWorkflowExecutionRequest{
		ShardID:             request.ShardID,
		RangeID:             request.RangeID,
		NewWorkflowSnapshot: snapshot,
	}

	newDoc, err := s.buildExecutionDocument(docReq)
	if err != nil {
		return err
	}
	newDoc.UpdateTime = time.Now().UTC()

	_, err = s.executeTransaction(ctx, func(sessCtx context.Context) (interface{}, error) {
		// Write fence: verify shard ownership before any writes
		if err := s.assertShardRangeID(sessCtx, request.ShardID, request.RangeID); err != nil {
			return nil, err
		}

		runID := snapshot.ExecutionState.GetRunId()
		existing, err := s.loadExecutionDocument(sessCtx, snapshot.NamespaceID, snapshot.WorkflowID, runID)
		if err != nil {
			return nil, err
		}
		if existing == nil {
			return nil, &persistence.ConditionFailedError{Msg: "workflow execution not found"}
		}

		if snapshot.DBRecordVersion == 0 {
			if existing.NextEventID != snapshot.Condition {
				return nil, &persistence.WorkflowConditionFailedError{
					Msg:             fmt.Sprintf("workflow execution set condition failed. expected next event id %v got %v", snapshot.Condition, existing.NextEventID),
					NextEventID:     existing.NextEventID,
					DBRecordVersion: existing.DBRecordVersion,
				}
			}
		} else {
			expectedVersion := snapshot.DBRecordVersion - 1
			if existing.DBRecordVersion != expectedVersion {
				return nil, &persistence.WorkflowConditionFailedError{
					Msg:             fmt.Sprintf("workflow execution set DB record version mismatch. expected %v got %v", expectedVersion, existing.DBRecordVersion),
					NextEventID:     existing.NextEventID,
					DBRecordVersion: existing.DBRecordVersion,
				}
			}
		}

		filter := bson.M{
			"_id":               newDoc.ID,
			"db_record_version": existing.DBRecordVersion,
		}
		update := bson.M{"$set": executionDocumentUpdateSet(newDoc)}
		result, err := s.executionsCol.UpdateOne(sessCtx, filter, update)
		if err != nil {
			return nil, serviceerror.NewUnavailablef("failed to set workflow execution: %v", err)
		}
		if result.MatchedCount == 0 {
			return nil, &persistence.ConditionFailedError{Msg: "workflow execution set conflict"}
		}

		if err := s.insertWorkflowTasks(sessCtx, request.ShardID, snapshot.Tasks); err != nil {
			return nil, err
		}

		return nil, nil
	})

	return err
}

func (s *executionStore) ListConcreteExecutions(
	ctx context.Context,
	request *persistence.ListConcreteExecutionsRequest,
) (*persistence.InternalListConcreteExecutionsResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("ListConcreteExecutions request is nil")
	}

	pageSize := request.PageSize
	if pageSize <= 0 {
		pageSize = 100
	}

	filter := bson.M{"shard_id": request.ShardID}
	if len(request.PageToken) > 0 {
		lastID, err := decodeListExecutionsToken(request.PageToken)
		if err != nil {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("invalid page token: %v", err))
		}
		if lastID != "" {
			filter["_id"] = bson.M{"$gt": lastID}
		}
	}

	limit := int64(pageSize + 1)
	findOpts := options.Find().
		SetSort(bson.D{{Key: "_id", Value: 1}}).
		SetLimit(limit)

	cursor, err := s.executionsCol.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("failed to list execution documents: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	docs := make([]*executionDocument, 0, pageSize+1)
	for cursor.Next(ctx) {
		var doc executionDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, serviceerror.NewUnavailablef("failed to decode execution document: %v", err)
		}
		docs = append(docs, &doc)
	}
	if err := cursor.Err(); err != nil {
		return nil, serviceerror.NewUnavailablef("failed during execution listing: %v", err)
	}

	response := &persistence.InternalListConcreteExecutionsResponse{}
	if len(docs) == 0 {
		return response, nil
	}

	if len(docs) > pageSize {
		lastDoc := docs[pageSize-1]
		token, err := encodeListExecutionsToken(lastDoc.ID)
		if err != nil {
			return nil, serviceerror.NewUnavailablef("failed to encode page token: %v", err)
		}
		response.NextPageToken = token
		docs = docs[:pageSize]
	}

	states := make([]*persistence.InternalWorkflowMutableState, 0, len(docs))
	for _, doc := range docs {
		states = append(states, s.executionDocumentToMutableState(doc))
	}
	response.States = states
	return response, nil
}

func (s *executionStore) AddHistoryTasks(
	ctx context.Context,
	request *persistence.InternalAddHistoryTasksRequest,
) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("AddHistoryTasks request is nil")
	}
	if len(request.Tasks) == 0 {
		return nil
	}

	for category, taskList := range request.Tasks {
		if len(taskList) == 0 {
			continue
		}

		col, categoryID, categoryType, err := s.taskCollectionForCategory(category)
		if err != nil {
			return err
		}

		for _, task := range taskList {
			if err := s.insertHistoryTaskDoc(ctx, col, categoryType, categoryID, request.ShardID, task); err != nil {
				return err
			}
		}
	}

	return nil
}

func (s *executionStore) GetHistoryTasks(
	ctx context.Context,
	request *persistence.GetHistoryTasksRequest,
) (*persistence.InternalGetHistoryTasksResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("GetHistoryTasks request is nil")
	}

	col, categoryID, categoryType, err := s.taskCollectionForCategory(request.TaskCategory)
	if err != nil {
		return nil, err
	}

	pageSize := request.BatchSize
	if pageSize <= 0 {
		pageSize = 100
	}

	tokenID, err := decodeHistoryTaskPageToken(request.NextPageToken)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("invalid history task page token: %v", err))
	}

	filter, err := buildHistoryTaskFilter(
		request.ShardID,
		categoryID,
		categoryType,
		tokenID,
		request.InclusiveMinTaskKey,
		request.ExclusiveMaxTaskKey,
	)
	if err != nil {
		return nil, err
	}

	findOpts := options.Find().
		SetSort(bson.D{{Key: "_id", Value: 1}}).
		SetLimit(int64(pageSize + 1))

	cursor, err := col.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("failed to read history tasks: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var (
		tasks     []persistence.InternalHistoryTask
		nextToken []byte
	)
	switch categoryType {
	case taskspkg.CategoryTypeImmediate:
		tasks, nextToken, err = decodeImmediateHistoryTasks(ctx, cursor, pageSize)
	case taskspkg.CategoryTypeScheduled:
		tasks, nextToken, err = decodeScheduledHistoryTasks(ctx, cursor, pageSize)
	default:
		err = serviceerror.NewInternalf("unknown task category type: %v", categoryType)
	}
	if err != nil {
		return nil, err
	}

	return &persistence.InternalGetHistoryTasksResponse{
		Tasks:         tasks,
		NextPageToken: nextToken,
	}, nil
}

func (s *executionStore) CompleteHistoryTask(
	ctx context.Context,
	request *persistence.CompleteHistoryTaskRequest,
) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("CompleteHistoryTask request is nil")
	}
	if request.BestEffort {
		return nil
	}

	col, categoryID, categoryType, err := s.taskCollectionForCategory(request.TaskCategory)
	if err != nil {
		return err
	}

	var docID string
	switch categoryType {
	case taskspkg.CategoryTypeImmediate:
		docID = immediateDocIDForKey(request.ShardID, categoryID, request.TaskKey)
	case taskspkg.CategoryTypeScheduled:
		docID = scheduledDocIDForKey(request.ShardID, categoryID, request.TaskKey)
	default:
		return serviceerror.NewInternalf("unknown task category type: %v", categoryType)
	}

	filter := bson.M{"_id": docID}
	if _, err := col.DeleteOne(ctx, filter); err != nil {
		return serviceerror.NewUnavailablef("failed to delete history task: %v", err)
	}
	return nil
}

func (s *executionStore) RangeCompleteHistoryTasks(
	ctx context.Context,
	request *persistence.RangeCompleteHistoryTasksRequest,
) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("RangeCompleteHistoryTasks request is nil")
	}

	col, categoryID, categoryType, err := s.taskCollectionForCategory(request.TaskCategory)
	if err != nil {
		return err
	}

	filter := bson.M{
		"shard_id":    request.ShardID,
		"category_id": categoryID,
	}

	idCond := bson.M{}
	switch categoryType {
	case taskspkg.CategoryTypeImmediate:
		if taskKeySpecified(request.InclusiveMinTaskKey) {
			idCond["$gte"] = immediateDocIDForKey(request.ShardID, categoryID, request.InclusiveMinTaskKey)
		}
		if taskKeySpecified(request.ExclusiveMaxTaskKey) {
			idCond["$lt"] = immediateDocIDForKey(request.ShardID, categoryID, request.ExclusiveMaxTaskKey)
		}
	case taskspkg.CategoryTypeScheduled:
		if taskKeySpecified(request.InclusiveMinTaskKey) {
			idCond["$gte"] = scheduledDocIDForKey(request.ShardID, categoryID, request.InclusiveMinTaskKey)
		}
		if taskKeySpecified(request.ExclusiveMaxTaskKey) {
			idCond["$lt"] = scheduledDocIDForKey(request.ShardID, categoryID, request.ExclusiveMaxTaskKey)
		}
	default:
		return serviceerror.NewInternalf("unknown task category type: %v", categoryType)
	}

	if len(idCond) > 0 {
		filter["_id"] = idCond
	}

	if _, err := col.DeleteMany(ctx, filter); err != nil {
		return serviceerror.NewUnavailablef("failed to range delete history tasks: %v", err)
	}
	return nil
}

func (s *executionStore) PutReplicationTaskToDLQ(
	ctx context.Context,
	request *persistence.PutReplicationTaskToDLQRequest,
) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("PutReplicationTaskToDLQ request is nil")
	}
	if request.TaskInfo == nil {
		return serviceerror.NewInvalidArgument("PutReplicationTaskToDLQ missing task info")
	}

	blob, err := serialization.ReplicationTaskInfoToBlob(request.TaskInfo)
	if err != nil {
		return err
	}

	doc := replicationDLQDocument{
		ID:            replicationDLQDocID(request.ShardID, request.SourceClusterName, request.TaskInfo.GetTaskId()),
		ShardID:       request.ShardID,
		SourceCluster: request.SourceClusterName,
		TaskID:        request.TaskInfo.GetTaskId(),
		Blob:          dataBlobToDoc(blob),
	}

	if _, err := s.replicationDLQCol.InsertOne(ctx, doc); err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return nil
		}
		return serviceerror.NewUnavailablef("failed to insert replication DLQ task: %v", err)
	}
	return nil
}

func (s *executionStore) GetReplicationTasksFromDLQ(
	ctx context.Context,
	request *persistence.GetReplicationTasksFromDLQRequest,
) (*persistence.InternalGetReplicationTasksFromDLQResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("GetReplicationTasksFromDLQ request is nil")
	}

	pageSize := request.BatchSize
	if pageSize <= 0 {
		pageSize = 100
	}

	tokenID, err := decodeHistoryTaskPageToken(request.NextPageToken)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("invalid replication DLQ page token: %v", err))
	}

	filter := bson.M{
		"shard_id":            request.ShardID,
		"source_cluster_name": request.SourceClusterName,
	}
	idCond := bson.M{}
	if tokenID != "" {
		idCond["$gt"] = tokenID
	}
	if tokenID == "" && taskKeySpecified(request.InclusiveMinTaskKey) {
		idCond["$gte"] = replicationDLQDocID(request.ShardID, request.SourceClusterName, request.InclusiveMinTaskKey.TaskID)
	}
	if taskKeySpecified(request.ExclusiveMaxTaskKey) {
		idCond["$lt"] = replicationDLQDocID(request.ShardID, request.SourceClusterName, request.ExclusiveMaxTaskKey.TaskID)
	}
	if len(idCond) > 0 {
		filter["_id"] = idCond
	}

	findOpts := options.Find().
		SetSort(bson.D{{Key: "_id", Value: 1}}).
		SetLimit(int64(pageSize + 1))

	cursor, err := s.replicationDLQCol.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("failed to read replication DLQ tasks: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	docs := make([]replicationDLQDocument, 0, pageSize+1)
	for cursor.Next(ctx) {
		var doc replicationDLQDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, serviceerror.NewUnavailablef("failed to decode replication DLQ task: %v", err)
		}
		docs = append(docs, doc)
	}
	if err := cursor.Err(); err != nil {
		return nil, serviceerror.NewUnavailablef("iteration error reading replication DLQ tasks: %v", err)
	}

	response := &persistence.InternalGetReplicationTasksFromDLQResponse{}
	if len(docs) == 0 {
		return response, nil
	}

	if len(docs) > pageSize {
		last := docs[pageSize-1]
		token, err := encodeHistoryTaskPageToken(last.ID)
		if err != nil {
			return nil, serviceerror.NewUnavailablef("failed to encode replication DLQ page token: %v", err)
		}
		response.NextPageToken = token
		docs = docs[:pageSize]
	}

	tasks := make([]persistence.InternalHistoryTask, len(docs))
	for i, doc := range docs {
		tasks[i] = persistence.InternalHistoryTask{
			Key:  taskspkg.NewImmediateKey(doc.TaskID),
			Blob: dataBlobDocToBlob(doc.Blob),
		}
	}
	response.Tasks = tasks
	return response, nil
}

func (s *executionStore) DeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *persistence.DeleteReplicationTaskFromDLQRequest,
) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("DeleteReplicationTaskFromDLQ request is nil")
	}

	filter := bson.M{
		"_id": replicationDLQDocID(request.ShardID, request.SourceClusterName, request.TaskKey.TaskID),
	}
	if _, err := s.replicationDLQCol.DeleteOne(ctx, filter); err != nil {
		return serviceerror.NewUnavailablef("failed to delete replication DLQ task: %v", err)
	}
	return nil
}

func (s *executionStore) RangeDeleteReplicationTaskFromDLQ(
	ctx context.Context,
	request *persistence.RangeDeleteReplicationTaskFromDLQRequest,
) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("RangeDeleteReplicationTaskFromDLQ request is nil")
	}

	filter := bson.M{
		"shard_id":            request.ShardID,
		"source_cluster_name": request.SourceClusterName,
	}
	idCond := bson.M{}
	if taskKeySpecified(request.InclusiveMinTaskKey) {
		idCond["$gte"] = replicationDLQDocID(request.ShardID, request.SourceClusterName, request.InclusiveMinTaskKey.TaskID)
	}
	if taskKeySpecified(request.ExclusiveMaxTaskKey) {
		idCond["$lt"] = replicationDLQDocID(request.ShardID, request.SourceClusterName, request.ExclusiveMaxTaskKey.TaskID)
	}
	if len(idCond) > 0 {
		filter["_id"] = idCond
	}

	if _, err := s.replicationDLQCol.DeleteMany(ctx, filter); err != nil {
		return serviceerror.NewUnavailablef("failed to range delete replication DLQ tasks: %v", err)
	}
	return nil
}

func (s *executionStore) IsReplicationDLQEmpty(
	ctx context.Context,
	request *persistence.GetReplicationTasksFromDLQRequest,
) (bool, error) {
	if request == nil {
		return false, serviceerror.NewInvalidArgument("IsReplicationDLQEmpty request is nil")
	}

	filter := bson.M{
		"shard_id":            request.ShardID,
		"source_cluster_name": request.SourceClusterName,
	}
	idCond := bson.M{}
	if taskKeySpecified(request.InclusiveMinTaskKey) {
		idCond["$gte"] = replicationDLQDocID(request.ShardID, request.SourceClusterName, request.InclusiveMinTaskKey.TaskID)
	}
	if taskKeySpecified(request.ExclusiveMaxTaskKey) {
		idCond["$lt"] = replicationDLQDocID(request.ShardID, request.SourceClusterName, request.ExclusiveMaxTaskKey.TaskID)
	}
	if len(idCond) > 0 {
		filter["_id"] = idCond
	}

	cursor, err := s.replicationDLQCol.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "_id", Value: 1}}).SetLimit(1))
	if err != nil {
		return false, serviceerror.NewUnavailablef("failed to query replication DLQ: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	if cursor.Next(ctx) {
		if err := cursor.Err(); err != nil {
			return false, serviceerror.NewUnavailablef("iteration error checking replication DLQ: %v", err)
		}
		return false, nil
	}
	if err := cursor.Err(); err != nil {
		return false, serviceerror.NewUnavailablef("iteration error checking replication DLQ: %v", err)
	}
	return true, nil
}

func (s *executionStore) AppendHistoryNodes(
	ctx context.Context,
	request *persistence.InternalAppendHistoryNodesRequest,
) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("AppendHistoryNodes request is nil")
	}
	branchInfo := request.BranchInfo
	if branchInfo == nil {
		return serviceerror.NewInvalidArgument("AppendHistoryNodes missing branch info")
	}
	if request.IsNewBranch && request.TreeInfo == nil {
		return serviceerror.NewInvalidArgument("AppendHistoryNodes requires tree info for new branch")
	}

	node := request.Node
	treeID := branchInfo.GetTreeId()
	branchID := branchInfo.GetBranchId()

	nodeDoc := historyNodeDocument{
		ID:        historyNodeDocID(treeID, branchID, node.NodeID, node.TransactionID),
		ShardID:   request.ShardID,
		TreeID:    treeID,
		BranchID:  branchID,
		NodeID:    node.NodeID,
		TxnID:     node.TransactionID,
		PrevTxnID: node.PrevTransactionID,
		Events:    dataBlobToDoc(node.Events),
	}

	if _, err := s.historyNodesCol.InsertOne(ctx, nodeDoc); err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded), errors.Is(err, context.Canceled):
			return &persistence.AppendHistoryTimeoutError{Msg: err.Error()}
		case mongo.IsDuplicateKeyError(err):
			return &persistence.ConditionFailedError{Msg: "history node already exists"}
		default:
			return serviceerror.NewUnavailablef("failed to append history node: %v", err)
		}
	}

	if request.IsNewBranch {
		branchDoc := historyBranchDocument{
			ID:       historyBranchDocID(treeID, branchID),
			ShardID:  request.ShardID,
			TreeID:   treeID,
			BranchID: branchID,
			Info:     request.Info,
			TreeInfo: dataBlobToDoc(request.TreeInfo),
		}
		update := bson.M{
			"$set": bson.M{
				"shard_id":  branchDoc.ShardID,
				"tree_id":   branchDoc.TreeID,
				"branch_id": branchDoc.BranchID,
				"info":      branchDoc.Info,
				"tree_info": branchDoc.TreeInfo,
			},
		}
		opts := options.Update().SetUpsert(true)
		if _, err := s.historyBranchesCol.UpdateOne(ctx, bson.M{"_id": branchDoc.ID}, update, opts); err != nil {
			if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				return &persistence.AppendHistoryTimeoutError{Msg: err.Error()}
			}
			return serviceerror.NewUnavailablef("failed to upsert history branch: %v", err)
		}
	}

	return nil
}

func (s *executionStore) DeleteHistoryNodes(
	ctx context.Context,
	request *persistence.InternalDeleteHistoryNodesRequest,
) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("DeleteHistoryNodes request is nil")
	}
	branchInfo := request.BranchInfo
	if branchInfo == nil {
		return serviceerror.NewInvalidArgument("DeleteHistoryNodes missing branch info")
	}
	if request.NodeID < persistence.GetBeginNodeID(branchInfo) {
		return &persistence.InvalidPersistenceRequestError{Msg: "cannot delete ancestor history nodes"}
	}

	treeID := branchInfo.GetTreeId()
	branchID := branchInfo.GetBranchId()
	filter := bson.M{"_id": historyNodeDocID(treeID, branchID, request.NodeID, request.TransactionID)}

	if _, err := s.historyNodesCol.DeleteOne(ctx, filter); err != nil {
		return serviceerror.NewUnavailablef("failed to delete history node: %v", err)
	}
	return nil
}

func (s *executionStore) ReadHistoryBranch(
	ctx context.Context,
	request *persistence.InternalReadHistoryBranchRequest,
) (*persistence.InternalReadHistoryBranchResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("ReadHistoryBranch request is nil")
	}
	branchInfo, err := s.GetHistoryBranchUtil().ParseHistoryBranchInfo(request.BranchToken)
	if err != nil {
		return nil, err
	}

	treeID := branchInfo.GetTreeId()
	branchID := request.BranchID
	if branchID == "" {
		branchID = branchInfo.GetBranchId()
	}

	limit := request.PageSize
	if limit <= 0 {
		limit = 100
	}

	token, err := decodeHistoryNodePageToken(request.NextPageToken)
	if err != nil {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("invalid history node page token: %v", err))
	}

	filter := historyNodeFilter(request.ShardID, treeID, branchID, request, token)

	var sortSpec bson.D
	if request.ReverseOrder {
		// Reverse: NodeID DESC, TxnID DESC
		sortSpec = bson.D{
			{Key: "node_id", Value: -1},
			{Key: "txn_id", Value: -1},
		}
	} else {
		// Forward: NodeID ASC, TxnID DESC (to satisfy history_manager.filterHistoryNodes)
		sortSpec = bson.D{
			{Key: "node_id", Value: 1},
			{Key: "txn_id", Value: -1},
		}
	}

	findOpts := options.Find().
		SetSort(sortSpec).
		SetLimit(int64(limit + 1))

	cursor, err := s.historyNodesCol.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("failed to read history nodes: %v", err)
	}
	defer func() {
		if closeErr := cursor.Close(ctx); closeErr != nil {
			s.logger.Warn("history nodes cursor close failed", tag.Error(closeErr))
		}
	}()

	var docs []historyNodeDocument
	for cursor.Next(ctx) {
		var doc historyNodeDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, serviceerror.NewUnavailablef("failed to decode history node: %v", err)
		}

		docs = append(docs, doc)
	}
	if err := cursor.Err(); err != nil {
		return nil, serviceerror.NewUnavailablef("iteration error reading history nodes: %v", err)
	}

	response := &persistence.InternalReadHistoryBranchResponse{}
	if len(docs) == 0 {
		return response, nil
	}

	if len(docs) > limit {
		last := docs[limit-1]
		tokenBytes, err := encodeHistoryNodePageToken(historyNodePageToken{LastNodeID: last.NodeID, LastTxnID: last.TxnID})
		if err != nil {
			return nil, serviceerror.NewUnavailablef("failed to encode history node page token: %v", err)
		}
		response.NextPageToken = tokenBytes
		docs = docs[:limit]
	}

	nodes := make([]persistence.InternalHistoryNode, len(docs))
	for i, doc := range docs {
		node := persistence.InternalHistoryNode{
			NodeID:            doc.NodeID,
			TransactionID:     doc.TxnID,
			PrevTransactionID: doc.PrevTxnID,
		}
		if !request.MetadataOnly && doc.Events != nil {
			node.Events = dataBlobDocToBlob(doc.Events)
		}
		nodes[i] = node
	}

	// Note: No need to reverse nodes here. MongoDB sort already returns
	// results in the correct order (DESC for ReverseOrder, ASC for forward).
	// This matches Cassandra behavior which uses ORDER BY DESC directly.

	response.Nodes = nodes
	return response, nil
}

func (s *executionStore) ForkHistoryBranch(
	ctx context.Context,
	request *persistence.InternalForkHistoryBranchRequest,
) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("ForkHistoryBranch request is nil")
	}
	if request.ForkBranchInfo == nil {
		return serviceerror.NewInvalidArgument("ForkHistoryBranch missing base branch info")
	}
	if request.TreeInfo == nil {
		return serviceerror.NewInvalidArgument("ForkHistoryBranch missing tree info")
	}

	base := request.ForkBranchInfo
	branchDoc := historyBranchDocument{
		ID:       historyBranchDocID(base.GetTreeId(), request.NewBranchID),
		ShardID:  request.ShardID,
		TreeID:   base.GetTreeId(),
		BranchID: request.NewBranchID,
		Info:     request.Info,
		TreeInfo: dataBlobToDoc(request.TreeInfo),
	}

	update := bson.M{
		"$set": bson.M{
			"shard_id":  branchDoc.ShardID,
			"tree_id":   branchDoc.TreeID,
			"branch_id": branchDoc.BranchID,
			"info":      branchDoc.Info,
			"tree_info": branchDoc.TreeInfo,
		},
	}
	if _, err := s.historyBranchesCol.UpdateOne(ctx, bson.M{"_id": branchDoc.ID}, update, options.Update().SetUpsert(true)); err != nil {
		return serviceerror.NewUnavailablef("failed to upsert history branch: %v", err)
	}
	return nil
}

func (s *executionStore) DeleteHistoryBranch(
	ctx context.Context,
	request *persistence.InternalDeleteHistoryBranchRequest,
) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("DeleteHistoryBranch request is nil")
	}
	if request.BranchInfo == nil {
		return serviceerror.NewInvalidArgument("DeleteHistoryBranch missing branch info")
	}

	branchInfo := request.BranchInfo
	filter := bson.M{"_id": historyBranchDocID(branchInfo.GetTreeId(), branchInfo.GetBranchId())}
	if _, err := s.historyBranchesCol.DeleteOne(ctx, filter); err != nil {
		return serviceerror.NewUnavailablef("failed to delete history branch: %v", err)
	}

	for _, br := range request.BranchRanges {
		rangeFilter := bson.M{
			"shard_id":  request.ShardID,
			"tree_id":   branchInfo.GetTreeId(),
			"branch_id": br.BranchId,
			"node_id": bson.M{
				"$gte": br.BeginNodeId,
			},
		}
		if _, err := s.historyNodesCol.DeleteMany(ctx, rangeFilter); err != nil {
			return serviceerror.NewUnavailablef("failed to delete history nodes: %v", err)
		}
	}

	return nil
}

func (s *executionStore) GetHistoryTreeContainingBranch(
	ctx context.Context,
	request *persistence.InternalGetHistoryTreeContainingBranchRequest,
) (*persistence.InternalGetHistoryTreeContainingBranchResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("GetHistoryTreeContainingBranch request is nil")
	}

	branch, err := s.GetHistoryBranchUtil().ParseHistoryBranchInfo(request.BranchToken)
	if err != nil {
		return nil, err
	}

	filter := bson.M{
		"shard_id": request.ShardID,
		"tree_id":  branch.GetTreeId(),
	}

	cursor, err := s.historyBranchesCol.Find(ctx, filter, options.Find().SetSort(bson.D{{Key: "branch_id", Value: 1}}))
	if err != nil {
		return nil, serviceerror.NewUnavailablef("failed to read history branches: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	response := &persistence.InternalGetHistoryTreeContainingBranchResponse{}

	for cursor.Next(ctx) {
		var doc historyBranchDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, serviceerror.NewUnavailablef("failed to decode history branch: %v", err)
		}
		response.TreeInfos = append(response.TreeInfos, dataBlobDocToBlob(doc.TreeInfo))
	}
	if err := cursor.Err(); err != nil {
		return nil, serviceerror.NewUnavailablef("iteration error reading history tree: %v", err)
	}

	return response, nil
}

func (s *executionStore) GetAllHistoryTreeBranches(
	ctx context.Context,
	request *persistence.GetAllHistoryTreeBranchesRequest,
) (*persistence.InternalGetAllHistoryTreeBranchesResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("GetAllHistoryTreeBranches request is nil")
	}
	if request.PageSize <= 0 {
		return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("PageSize must be greater than 0, but was %d", request.PageSize))
	}

	filter := bson.M{}
	if len(request.NextPageToken) > 0 {
		token, err := decodeHistoryBranchListToken(request.NextPageToken)
		if err != nil {
			return nil, serviceerror.NewInvalidArgument(fmt.Sprintf("invalid history branch page token: %v", err))
		}
		filter["$or"] = []bson.M{
			{"shard_id": bson.M{"$gt": token.ShardID}},
			{
				"$and": []bson.M{
					{"shard_id": token.ShardID},
					{"tree_id": bson.M{"$gt": token.TreeID}},
				},
			},
			{
				"$and": []bson.M{
					{"shard_id": token.ShardID},
					{"tree_id": token.TreeID},
					{"branch_id": bson.M{"$gt": token.BranchID}},
				},
			},
		}
	}

	findOpts := options.Find().
		SetSort(bson.D{{Key: "shard_id", Value: 1}, {Key: "tree_id", Value: 1}, {Key: "branch_id", Value: 1}}).
		SetLimit(int64(request.PageSize + 1))

	cursor, err := s.historyBranchesCol.Find(ctx, filter, findOpts)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("failed to list history tree branches: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var docs []historyBranchDocument
	for cursor.Next(ctx) {
		var doc historyBranchDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, serviceerror.NewUnavailablef("failed to decode history branch: %v", err)
		}
		docs = append(docs, doc)
	}
	if err := cursor.Err(); err != nil {
		return nil, serviceerror.NewUnavailablef("iteration error listing history branches: %v", err)
	}

	response := &persistence.InternalGetAllHistoryTreeBranchesResponse{}
	if len(docs) == 0 {
		return response, nil
	}

	if len(docs) > request.PageSize {
		last := docs[request.PageSize-1]
		tokenBytes, err := encodeHistoryBranchListToken(historyBranchListPageToken{
			ShardID:  last.ShardID,
			TreeID:   last.TreeID,
			BranchID: last.BranchID,
		})
		if err != nil {
			return nil, serviceerror.NewUnavailablef("failed to encode history branch page token: %v", err)
		}
		response.NextPageToken = tokenBytes
		docs = docs[:request.PageSize]
	}

	branches := make([]persistence.InternalHistoryBranchDetail, 0, len(docs))
	for _, doc := range docs {
		var data []byte
		encoding := ""
		if doc.TreeInfo != nil {
			data = doc.TreeInfo.Data
			encoding = doc.TreeInfo.Encoding
		}
		branches = append(branches, persistence.InternalHistoryBranchDetail{
			TreeID:   doc.TreeID,
			BranchID: doc.BranchID,
			Data:     data,
			Encoding: encoding,
		})
	}
	response.Branches = branches
	return response, nil
}

func (s *executionStore) notImplemented(method string) error {
	return serviceerror.NewUnimplemented(fmt.Sprintf("MongoDB ExecutionStore: %s not implemented", method))
}

// assertShardRangeID verifies that the shard's range_id matches the expected value.
// This provides write fencing to prevent stale shard owners from writing.
// Must be called within a transaction context for atomicity.
func (s *executionStore) assertShardRangeID(
	ctx context.Context,
	shardID int32,
	expectedRangeID int64,
) error {
	collection := s.db.Collection(collectionShards)

	filter := bson.M{"_id": shardID}
	var result struct {
		RangeID int64 `bson:"range_id"`
	}

	err := collection.FindOne(ctx, filter).Decode(&result)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return serviceerror.NewUnavailablef("shard %d does not exist", shardID)
		}
		return serviceerror.NewUnavailablef("failed to read shard %d: %v", shardID, err)
	}

	if result.RangeID != expectedRangeID {
		return &persistence.ShardOwnershipLostError{
			ShardID: shardID,
			Msg: fmt.Sprintf(
				"Shard ownership lost. Expected RangeID: %v, actual RangeID: %v",
				expectedRangeID,
				result.RangeID,
			),
		}
	}

	return nil
}
