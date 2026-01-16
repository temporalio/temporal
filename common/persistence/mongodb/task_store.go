package mongodb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/server/common/config"
	"go.temporal.io/server/common/log"
	"go.temporal.io/server/common/persistence"
	"go.temporal.io/server/common/persistence/mongodb/client"
)

const (
	collectionTaskQueues           = "task_queues"
	collectionTasks                = "tasks"
	collectionTaskQueueUserData    = "task_queue_user_data"
	collectionTaskQueueBuildIDMaps = "task_queue_build_id_map"
)

type (
	taskStore struct {
		db                  client.Database
		mongoClient         client.Client
		cfg                 config.MongoDB
		logger              log.Logger
		transactionsEnabled bool
		fairnessEnabled     bool
	}

	taskQueueDocument struct {
		ID          string    `bson:"_id"`
		NamespaceID string    `bson:"namespace_id"`
		TaskQueue   string    `bson:"task_queue"`
		TaskType    int32     `bson:"task_type"`
		Subqueue    int       `bson:"subqueue"`
		RangeID     int64     `bson:"range_id"`
		Data        []byte    `bson:"data"`
		Encoding    string    `bson:"data_encoding"`
		UpdatedAt   time.Time `bson:"updated_at"`
	}

	taskDocument struct {
		ID          string     `bson:"_id"`
		NamespaceID string     `bson:"namespace_id"`
		TaskQueue   string     `bson:"task_queue"`
		TaskType    int32      `bson:"task_type"`
		Subqueue    int        `bson:"subqueue"`
		TaskPass    int64      `bson:"task_pass"`
		TaskID      int64      `bson:"task_id"`
		Task        []byte     `bson:"task"`
		Encoding    string     `bson:"task_encoding"`
		ExpiryTime  *time.Time `bson:"expiry_time,omitempty"`
		CreatedAt   time.Time  `bson:"created_at"`
	}

	userDataDocument struct {
		ID        string    `bson:"_id"`
		Namespace string    `bson:"namespace_id"`
		TaskQueue string    `bson:"task_queue"`
		Version   int64     `bson:"version"`
		Data      []byte    `bson:"data"`
		Encoding  string    `bson:"data_encoding"`
		UpdatedAt time.Time `bson:"updated_at"`
	}

	taskQueueBuildIDDocument struct {
		ID          string `bson:"_id"`
		NamespaceID string `bson:"namespace_id"`
		BuildID     string `bson:"build_id"`
		TaskQueue   string `bson:"task_queue"`
	}
)

func newMongoTaskStore(
	db client.Database,
	mongoClient client.Client,
	cfg config.MongoDB,
	logger log.Logger,
	transactionsEnabled bool,
	fairnessEnabled bool,
) (persistence.TaskStore, error) {
	store := &taskStore{
		db:                  db,
		mongoClient:         mongoClient,
		cfg:                 cfg,
		logger:              logger,
		transactionsEnabled: transactionsEnabled,
		fairnessEnabled:     fairnessEnabled,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := store.ensureIndexes(ctx); err != nil {
		return nil, err
	}

	return store, nil
}

// NewTaskStore returns a new MongoDB TaskStore implementation without fairness enabled.
func NewTaskStore(
	db client.Database,
	mongoClient client.Client,
	cfg config.MongoDB,
	logger log.Logger,
	transactionsEnabled bool,
) (persistence.TaskStore, error) {
	return newMongoTaskStore(db, mongoClient, cfg, logger, transactionsEnabled, false)
}

// NewFairTaskStore returns a new MongoDB TaskStore implementation with fairness enabled.
func NewFairTaskStore(
	db client.Database,
	mongoClient client.Client,
	cfg config.MongoDB,
	logger log.Logger,
	transactionsEnabled bool,
) (persistence.TaskStore, error) {
	return newMongoTaskStore(db, mongoClient, cfg, logger, transactionsEnabled, true)
}

func (s *taskStore) GetName() string {
	return "mongodb"
}

func (s *taskStore) Close() {
	// Factory manages the underlying client lifecycle
}

func (s *taskStore) ensureIndexes(ctx context.Context) error {
	// Note: Task queue documents use _id as the primary key, which includes version (v1/v2) suffix.
	// This allows v1 and v2 task queues to coexist for the same queue name during migration.
	// The unique index below is kept for backwards compatibility queries but is NOT unique
	// because different versions of the same queue can exist.
	queueIndexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "namespace_id", Value: 1}, {Key: "task_queue", Value: 1}, {Key: "task_type", Value: 1}, {Key: "subqueue", Value: 1}},
			Options: options.Index().SetName("task_queue_lookup"),
		},
	}
	idxView := s.db.Collection(collectionTaskQueues).Indexes()
	for _, model := range queueIndexes {
		if _, err := idxView.CreateOne(ctx, model); err != nil && !isDuplicateIndexError(err) {
			return serviceerror.NewUnavailablef("failed to ensure task queue indexes: %v", err)
		}
	}

	taskIndexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "namespace_id", Value: 1}, {Key: "task_queue", Value: 1}, {Key: "task_type", Value: 1}, {Key: "subqueue", Value: 1}, {Key: "task_pass", Value: 1}, {Key: "task_id", Value: 1}},
			Options: options.Index().SetName("tasks_lookup"),
		},
		{
			Keys:    bson.D{{Key: "expiry_time", Value: 1}},
			Options: options.Index().SetExpireAfterSeconds(0).SetName("tasks_ttl"),
		},
	}
	taskIdxView := s.db.Collection(collectionTasks).Indexes()
	for _, model := range taskIndexes {
		if _, err := taskIdxView.CreateOne(ctx, model); err != nil && !isDuplicateIndexError(err) {
			return serviceerror.NewUnavailablef("failed to ensure task indexes: %v", err)
		}
	}

	userDataIndexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "namespace_id", Value: 1}, {Key: "task_queue", Value: 1}},
			Options: options.Index().SetUnique(true).SetName("task_queue_user_data_unique"),
		},
	}
	userDataIdxView := s.db.Collection(collectionTaskQueueUserData).Indexes()
	for _, model := range userDataIndexes {
		if _, err := userDataIdxView.CreateOne(ctx, model); err != nil && !isDuplicateIndexError(err) {
			return serviceerror.NewUnavailablef("failed to ensure task queue user data indexes: %v", err)
		}
	}

	buildIDIndexes := []mongo.IndexModel{
		{
			Keys:    bson.D{{Key: "namespace_id", Value: 1}, {Key: "build_id", Value: 1}, {Key: "task_queue", Value: 1}},
			Options: options.Index().SetUnique(true).SetName("task_queue_build_id_unique"),
		},
		{
			Keys:    bson.D{{Key: "namespace_id", Value: 1}, {Key: "build_id", Value: 1}},
			Options: options.Index().SetName("task_queue_build_id_lookup"),
		},
	}
	buildIDIdxView := s.db.Collection(collectionTaskQueueBuildIDMaps).Indexes()
	for _, model := range buildIDIndexes {
		if _, err := buildIDIdxView.CreateOne(ctx, model); err != nil && !isDuplicateIndexError(err) {
			return serviceerror.NewUnavailablef("failed to ensure task queue build id indexes: %v", err)
		}
	}

	return nil
}

// -------- Task Queue management --------

func (s *taskStore) CreateTaskQueue(
	ctx context.Context,
	request *persistence.InternalCreateTaskQueueRequest,
) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("create task queue request is nil")
	}

	if request.TaskQueueInfo == nil {
		return serviceerror.NewInvalidArgument("task queue info is nil")
	}

	doc := taskQueueDocument{
		ID:          taskQueueDocID(request.NamespaceID, request.TaskQueue, request.TaskType, persistence.SubqueueZero, s.fairnessEnabled),
		NamespaceID: request.NamespaceID,
		TaskQueue:   request.TaskQueue,
		TaskType:    int32(request.TaskType),
		Subqueue:    persistence.SubqueueZero,
		RangeID:     request.RangeID,
		Data:        request.TaskQueueInfo.Data,
		Encoding:    request.TaskQueueInfo.EncodingType.String(),
		UpdatedAt:   time.Now().UTC(),
	}

	_, err := s.db.Collection(collectionTaskQueues).InsertOne(ctx, doc)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return &persistence.ConditionFailedError{Msg: "task queue already exists"}
		}
		return serviceerror.NewUnavailablef("CreateTaskQueue failed: %v", err)
	}

	return nil
}

func (s *taskStore) GetTaskQueue(
	ctx context.Context,
	request *persistence.InternalGetTaskQueueRequest,
) (*persistence.InternalGetTaskQueueResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("get task queue request is nil")
	}

	filter := bson.M{"_id": taskQueueDocID(request.NamespaceID, request.TaskQueue, request.TaskType, persistence.SubqueueZero, s.fairnessEnabled)}

	var doc taskQueueDocument
	if err := s.db.Collection(collectionTaskQueues).FindOne(ctx, filter).Decode(&doc); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, serviceerror.NewNotFoundf("task queue %s not found", request.TaskQueue)
		}
		return nil, serviceerror.NewUnavailablef("GetTaskQueue failed: %v", err)
	}

	if doc.RangeID == 0 {
		return nil, serviceerror.NewNotFoundf("task queue %s not found (rangeID=0)", request.TaskQueue)
	}

	return &persistence.InternalGetTaskQueueResponse{
		RangeID:       doc.RangeID,
		TaskQueueInfo: persistence.NewDataBlob(doc.Data, doc.Encoding),
	}, nil
}

func (s *taskStore) UpdateTaskQueue(
	ctx context.Context,
	request *persistence.InternalUpdateTaskQueueRequest,
) (*persistence.UpdateTaskQueueResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("update task queue request is nil")
	}

	filter := bson.M{
		"_id":      taskQueueDocID(request.NamespaceID, request.TaskQueue, request.TaskType, persistence.SubqueueZero, s.fairnessEnabled),
		"range_id": request.PrevRangeID,
	}

	update := bson.M{
		"$set": bson.M{
			"range_id":      request.RangeID,
			"data":          request.TaskQueueInfo.Data,
			"data_encoding": request.TaskQueueInfo.EncodingType.String(),
			"updated_at":    time.Now().UTC(),
		},
	}

	res, err := s.db.Collection(collectionTaskQueues).UpdateOne(ctx, filter, update)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("UpdateTaskQueue failed: %v", err)
	}
	if res.MatchedCount == 0 {
		return nil, &persistence.ConditionFailedError{Msg: "task queue range mismatch"}
	}

	return &persistence.UpdateTaskQueueResponse{}, nil
}

func (s *taskStore) applyFairGetTasksFilter(filter bson.M, request *persistence.GetTasksRequest) error {
	if request.InclusiveMinPass < 1 {
		return serviceerror.NewInternal("invalid GetTasks request on fair queue: InclusiveMinPass must be >= 1")
	}
	if request.ExclusiveMaxTaskID != math.MaxInt64 {
		return serviceerror.NewInternal("invalid GetTasks request on fair queue: ExclusiveMaxTaskID is not supported")
	}

	minPass := request.InclusiveMinPass
	minTaskID := request.InclusiveMinTaskID
	if len(request.NextPageToken) > 0 {
		token, err := decodeTaskPageToken(request.NextPageToken)
		if err != nil {
			return serviceerror.NewInvalidArgument(fmt.Sprintf("invalid next page token: %v", err))
		}
		if token.TaskPass < 1 {
			return serviceerror.NewInternal("invalid token: missing TaskPass")
		}
		minPass = token.TaskPass
		minTaskID = token.TaskID
	}

	filter["$or"] = []bson.M{
		{"task_pass": bson.M{"$gt": minPass}},
		{"task_pass": minPass, "task_id": bson.M{"$gte": minTaskID}},
	}
	return nil
}

func (s *taskStore) applyStandardGetTasksFilter(filter bson.M, request *persistence.GetTasksRequest) error {
	if request.InclusiveMinPass != 0 {
		return serviceerror.NewInternal("fair task queues are not supported in this task store")
	}

	filter["task_pass"] = int64(0)

	if len(request.NextPageToken) > 0 {
		token, err := decodeTaskPageToken(request.NextPageToken)
		if err != nil {
			return serviceerror.NewInvalidArgument(fmt.Sprintf("invalid next page token: %v", err))
		}
		filter["task_id"] = bson.M{"$gt": token.TaskID}
	} else if request.InclusiveMinTaskID > 0 {
		filter["task_id"] = bson.M{"$gte": request.InclusiveMinTaskID}
	}

	if request.ExclusiveMaxTaskID > 0 {
		if existing, ok := filter["task_id"]; ok {
			if m, ok := existing.(bson.M); ok {
				m["$lt"] = request.ExclusiveMaxTaskID
			} else {
				filter["task_id"] = bson.M{"$lt": request.ExclusiveMaxTaskID}
			}
		} else {
			filter["task_id"] = bson.M{"$lt": request.ExclusiveMaxTaskID}
		}
	}

	return nil
}

func (s *taskStore) findTasks(
	ctx context.Context,
	filter bson.M,
	limit int64,
	fairnessEnabled bool,
) (*persistence.InternalGetTasksResponse, error) {
	opts := options.Find().
		SetSort(bson.D{{Key: "task_pass", Value: 1}, {Key: "task_id", Value: 1}}).
		SetLimit(limit)

	cursor, err := s.db.Collection(collectionTasks).Find(ctx, filter, opts)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("GetTasks failed: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	response := &persistence.InternalGetTasksResponse{}
	var last taskDocument

	for cursor.Next(ctx) {
		var doc taskDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, serviceerror.NewUnavailablef("GetTasks decode failed: %v", err)
		}
		response.Tasks = append(response.Tasks, persistence.NewDataBlob(doc.Task, doc.Encoding))
		last = doc
	}

	if err := cursor.Err(); err != nil {
		return nil, serviceerror.NewUnavailablef("GetTasks cursor error: %v", err)
	}

	if len(response.Tasks) == int(limit) && last.ID != "" {
		nextToken := taskPageToken{TaskID: last.TaskID}
		if fairnessEnabled {
			nextToken.TaskPass = last.TaskPass
			nextToken.TaskID = last.TaskID + 1
		}
		token, err := encodeTaskPageToken(nextToken)
		if err != nil {
			return nil, serviceerror.NewInternal(fmt.Sprintf("encode page token failed: %v", err))
		}
		response.NextPageToken = token
	}

	return response, nil
}

func (s *taskStore) ListTaskQueue(
	ctx context.Context,
	request *persistence.ListTaskQueueRequest,
) (*persistence.InternalListTaskQueueResponse, error) {
	limit := int64(request.PageSize)
	if limit <= 0 {
		limit = 100
	}

	filter := bson.M{}
	if len(request.PageToken) > 0 {
		filter["_id"] = bson.M{"$gt": string(request.PageToken)}
	}

	opts := options.Find().SetLimit(limit).SetSort(bson.D{{Key: "_id", Value: 1}})

	cursor, err := s.db.Collection(collectionTaskQueues).Find(ctx, filter, opts)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("ListTaskQueue failed: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var (
		items  []*persistence.InternalListTaskQueueItem
		lastID string
	)

	for cursor.Next(ctx) {
		var doc taskQueueDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, serviceerror.NewUnavailablef("ListTaskQueue decode failed: %v", err)
		}
		items = append(items, &persistence.InternalListTaskQueueItem{
			TaskQueue: persistence.NewDataBlob(doc.Data, doc.Encoding),
			RangeID:   doc.RangeID,
		})
		lastID = doc.ID
	}
	if err := cursor.Err(); err != nil {
		return nil, serviceerror.NewUnavailablef("ListTaskQueue cursor error: %v", err)
	}

	resp := &persistence.InternalListTaskQueueResponse{Items: items}
	if len(items) == int(limit) && lastID != "" {
		resp.NextPageToken = []byte(lastID)
	}

	return resp, nil
}

func (s *taskStore) DeleteTaskQueue(
	ctx context.Context,
	request *persistence.DeleteTaskQueueRequest,
) error {
	if request == nil || request.TaskQueue == nil {
		return serviceerror.NewInvalidArgument("delete task queue request is invalid")
	}
	key := request.TaskQueue
	filter := bson.M{
		"_id":      taskQueueDocID(key.NamespaceID, key.TaskQueueName, key.TaskQueueType, persistence.SubqueueZero, s.fairnessEnabled),
		"range_id": request.RangeID,
	}
	res, err := s.db.Collection(collectionTaskQueues).DeleteOne(ctx, filter)
	if err != nil {
		return serviceerror.NewUnavailablef("DeleteTaskQueue failed: %v", err)
	}
	if res.DeletedCount == 0 {
		return &persistence.ConditionFailedError{Msg: "task queue not found or range mismatch"}
	}
	return nil
}

// -------- Task management --------

func (s *taskStore) CreateTasks(
	ctx context.Context,
	request *persistence.InternalCreateTasksRequest,
) (*persistence.CreateTasksResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("create tasks request is nil")
	}

	if len(request.Tasks) == 0 {
		return &persistence.CreateTasksResponse{UpdatedMetadata: false}, nil
	}

	// Validate tasks
	for _, task := range request.Tasks {
		if s.fairnessEnabled {
			if task.TaskPass < 1 {
				return nil, serviceerror.NewInternal("invalid fair queue task missing pass number")
			}
		} else if task.TaskPass != 0 {
			return nil, serviceerror.NewInternal("fair task queues are not supported in this task store")
		}
	}

	queueID := taskQueueDocID(request.NamespaceID, request.TaskQueue, request.TaskType, persistence.SubqueueZero, s.fairnessEnabled)
	// Build all task documents upfront
	taskDocs := make([]interface{}, 0, len(request.Tasks))
	for _, task := range request.Tasks {
		taskPass := task.TaskPass
		if !s.fairnessEnabled {
			taskPass = 0
		}
		docID := taskDocumentID(request.NamespaceID, request.TaskQueue, request.TaskType, persistence.SubqueueZero, taskPass, task.TaskId)
		var expiry *time.Time
		if task.ExpiryTime != nil {
			e := task.ExpiryTime.AsTime()
			expiry = &e
		}
		doc := taskDocument{
			ID:          docID,
			NamespaceID: request.NamespaceID,
			TaskQueue:   request.TaskQueue,
			TaskType:    int32(request.TaskType),
			Subqueue:    task.Subqueue,
			TaskPass:    taskPass,
			TaskID:      task.TaskId,
			Task:        task.Task.Data,
			Encoding:    task.Task.EncodingType.String(),
			ExpiryTime:  expiry,
			CreatedAt:   time.Now().UTC(),
		}
		taskDocs = append(taskDocs, doc)
	}

	// Execute in a transaction for atomicity
	sess, err := s.mongoClient.StartSession(ctx)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("CreateTasks: failed to start session: %v", err)
	}
	defer sess.EndSession(ctx)

	var response *persistence.CreateTasksResponse
	_, txErr := sess.WithTransaction(ctx, func(sessCtx context.Context) (interface{}, error) {
		// Step 1: Verify RangeID FIRST (write fence)
		queueFilter := bson.M{"_id": queueID, "range_id": request.RangeID}
		queueUpdate := bson.M{"$set": bson.M{
			"data":          request.TaskQueueInfo.Data,
			"data_encoding": request.TaskQueueInfo.EncodingType.String(),
			"updated_at":    time.Now().UTC(),
		}}

		res, err := s.db.Collection(collectionTaskQueues).UpdateOne(sessCtx, queueFilter, queueUpdate)
		if err != nil {
			return nil, serviceerror.NewUnavailablef("CreateTasks: failed to update queue: %v", err)
		}
		if res.MatchedCount == 0 {
			// RangeID mismatch - transaction will abort, no tasks written
			return nil, &persistence.ConditionFailedError{Msg: "task queue range mismatch"}
		}

		// Step 2: Insert all tasks in one batch operation
		tasksColl := s.db.Collection(collectionTasks)
		_, err = tasksColl.InsertMany(sessCtx, taskDocs)
		if err != nil {
			if mongo.IsDuplicateKeyError(err) {
				return nil, &persistence.ConditionFailedError{Msg: "task already exists"}
			}
			return nil, serviceerror.NewUnavailablef("CreateTasks: batch insert failed: %v", err)
		}

		response = &persistence.CreateTasksResponse{UpdatedMetadata: true}
		return nil, nil
	})

	if txErr != nil {
		return nil, txErr
	}

	return response, nil
}

func (s *taskStore) GetTasks(
	ctx context.Context,
	request *persistence.GetTasksRequest,
) (*persistence.InternalGetTasksResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("get tasks request is nil")
	}

	filter := bson.M{
		"namespace_id": request.NamespaceID,
		"task_queue":   request.TaskQueue,
		"task_type":    int32(request.TaskType),
		"subqueue":     request.Subqueue,
	}

	limit := int64(request.PageSize)
	if limit <= 0 {
		limit = 100
	}

	if s.fairnessEnabled {
		if request.InclusiveMinPass == 0 {
			if err := s.applyStandardGetTasksFilter(filter, request); err != nil {
				return nil, err
			}
		} else {
			if err := s.applyFairGetTasksFilter(filter, request); err != nil {
				return nil, err
			}
		}
		return s.findTasks(ctx, filter, limit, true)
	}

	if err := s.applyStandardGetTasksFilter(filter, request); err != nil {
		return nil, err
	}
	return s.findTasks(ctx, filter, limit, false)
}

func (s *taskStore) CompleteTasksLessThan(
	ctx context.Context,
	request *persistence.CompleteTasksLessThanRequest,
) (int, error) {
	if request == nil {
		return 0, serviceerror.NewInvalidArgument("complete tasks request is nil")
	}

	filter := bson.M{
		"namespace_id": request.NamespaceID,
		"task_queue":   request.TaskQueueName,
		"task_type":    int32(request.TaskType),
		"subqueue":     request.Subqueue,
	}

	if s.fairnessEnabled {
		if request.ExclusiveMaxPass == 0 {
			// V1 request - delete tasks with TaskPass=0
			filter["task_pass"] = int64(0)
			if request.ExclusiveMaxTaskID > 0 {
				filter["task_id"] = bson.M{"$lt": request.ExclusiveMaxTaskID}
			}
		} else {
			// V2 request - delete tasks less than <Pass, TaskID>
			filter["$or"] = []bson.M{
				{"task_pass": bson.M{"$lt": request.ExclusiveMaxPass}},
				{"task_pass": request.ExclusiveMaxPass, "task_id": bson.M{"$lt": request.ExclusiveMaxTaskID}},
			}
		}
	} else {
		if request.ExclusiveMaxPass != 0 {
			return 0, serviceerror.NewInternal("fair task queues are not supported in this task store")
		}
		filter["task_pass"] = int64(0)
		if request.ExclusiveMaxTaskID > 0 {
			filter["task_id"] = bson.M{"$lt": request.ExclusiveMaxTaskID}
		}
	}

	res, err := s.db.Collection(collectionTasks).DeleteMany(ctx, filter)
	if err != nil {
		return 0, serviceerror.NewUnavailablef("CompleteTasksLessThan failed: %v", err)
	}

	if request.Limit > 0 && res.DeletedCount > int64(request.Limit) {
		return request.Limit, nil
	}

	return int(res.DeletedCount), nil
}

// -------- Task queue user data --------

func (s *taskStore) GetTaskQueueUserData(
	ctx context.Context,
	request *persistence.GetTaskQueueUserDataRequest,
) (*persistence.InternalGetTaskQueueUserDataResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("get task queue user data request is nil")
	}

	filter := bson.M{"_id": taskQueueUserDataID(request.NamespaceID, request.TaskQueue)}
	var doc userDataDocument
	if err := s.db.Collection(collectionTaskQueueUserData).FindOne(ctx, filter).Decode(&doc); err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, serviceerror.NewNotFoundf("task queue user data not found for %v.%v", request.NamespaceID, request.TaskQueue)
		}
		return nil, serviceerror.NewUnavailablef("GetTaskQueueUserData failed: %v", err)
	}

	return &persistence.InternalGetTaskQueueUserDataResponse{
		Version:  doc.Version,
		UserData: persistence.NewDataBlob(doc.Data, doc.Encoding),
	}, nil
}

//nolint:revive // cognitive complexity reflects multi-queue transactional workflow requirements.
func (s *taskStore) UpdateTaskQueueUserData(
	ctx context.Context,
	request *persistence.InternalUpdateTaskQueueUserDataRequest,
) error {
	if request == nil {
		return serviceerror.NewInvalidArgument("update task queue user data request is nil")
	}

	// Default to not applied / not conflicting. Callers depend on these being set even on error.
	for _, update := range request.Updates {
		if update == nil {
			continue
		}
		if update.Applied != nil {
			*update.Applied = false
		}
		if update.Conflicting != nil {
			*update.Conflicting = false
		}
	}

	if !s.transactionsEnabled || s.mongoClient == nil {
		// This store requires transactions-enabled topology for correct multi-queue atomicity.
		return serviceerror.NewUnavailable("mongodb task queue user data updates require transactions-enabled topology")
	}

	session, err := s.mongoClient.StartSession(ctx)
	if err != nil {
		return serviceerror.NewUnavailablef("UpdateTaskQueueUserData failed to start session: %v", err)
	}
	defer session.EndSession(ctx)

	_, err = session.WithTransaction(ctx, func(txCtx context.Context) (interface{}, error) {
		for queue, update := range request.Updates {
			if update == nil {
				continue
			}
			if err := s.applyUserDataUpdate(txCtx, request.NamespaceID, queue, update); err != nil {
				if errors.Is(err, errUserDataVersionConflict) {
					if update.Conflicting != nil {
						*update.Conflicting = true
					}
					return nil, &persistence.ConditionFailedError{Msg: err.Error()}
				}
				return nil, err
			}
		}
		return nil, nil
	})
	if err != nil {
		return err
	}

	for _, update := range request.Updates {
		if update == nil {
			continue
		}
		if update.Applied != nil {
			*update.Applied = true
		}
	}

	return nil
}

func (s *taskStore) applyUserDataUpdate(ctx context.Context, namespaceID, taskQueue string, update *persistence.InternalSingleTaskQueueUserDataUpdate) error {
	filter := bson.M{"_id": taskQueueUserDataID(namespaceID, taskQueue)}

	if update.Version == 0 {
		doc := userDataDocument{
			ID:        taskQueueUserDataID(namespaceID, taskQueue),
			Namespace: namespaceID,
			TaskQueue: taskQueue,
			Version:   1,
			Data:      update.UserData.Data,
			Encoding:  update.UserData.EncodingType.String(),
			UpdatedAt: time.Now().UTC(),
		}
		_, err := s.db.Collection(collectionTaskQueueUserData).InsertOne(ctx, doc)
		if err != nil {
			if mongo.IsDuplicateKeyError(err) {
				return errUserDataVersionConflict
			}
			return serviceerror.NewUnavailablef("UpdateTaskQueueUserData insert failed: %v", err)
		}
		if err := s.applyBuildIDUpdates(ctx, namespaceID, taskQueue, update); err != nil {
			_, _ = s.db.Collection(collectionTaskQueueUserData).DeleteOne(ctx, bson.M{"_id": doc.ID})
			return err
		}
		return nil
	}

	set := bson.M{
		"data":          update.UserData.Data,
		"data_encoding": update.UserData.EncodingType.String(),
		"updated_at":    time.Now().UTC(),
		"version":       update.Version + 1,
	}
	updateRes, err := s.db.Collection(collectionTaskQueueUserData).UpdateOne(ctx, bson.M{"_id": filter["_id"], "version": update.Version}, bson.M{"$set": set})
	if err != nil {
		return serviceerror.NewUnavailablef("UpdateTaskQueueUserData failed: %v", err)
	}
	if updateRes.MatchedCount == 0 {
		return errUserDataVersionConflict
	}
	return s.applyBuildIDUpdates(ctx, namespaceID, taskQueue, update)
}

func (s *taskStore) applyBuildIDUpdates(ctx context.Context, namespaceID, taskQueue string, update *persistence.InternalSingleTaskQueueUserDataUpdate) error {
	if len(update.BuildIdsAdded) == 0 && len(update.BuildIdsRemoved) == 0 {
		return nil
	}

	coll := s.db.Collection(collectionTaskQueueBuildIDMaps)
	var inserted []string

	for _, buildID := range update.BuildIdsAdded {
		doc := taskQueueBuildIDDocument{
			ID:          taskQueueBuildIDDocID(namespaceID, buildID, taskQueue),
			NamespaceID: namespaceID,
			BuildID:     buildID,
			TaskQueue:   taskQueue,
		}
		_, err := coll.InsertOne(ctx, doc)
		if err != nil {
			if mongo.IsDuplicateKeyError(err) {
				continue
			}
			for _, id := range inserted {
				_, _ = coll.DeleteOne(ctx, bson.M{"_id": id})
			}
			return serviceerror.NewUnavailablef("UpdateTaskQueueUserData build id insert failed: %v", err)
		}
		inserted = append(inserted, doc.ID)
	}

	for _, buildID := range update.BuildIdsRemoved {
		_, err := coll.DeleteOne(ctx, bson.M{"_id": taskQueueBuildIDDocID(namespaceID, buildID, taskQueue)})
		if err != nil {
			for _, id := range inserted {
				_, _ = coll.DeleteOne(ctx, bson.M{"_id": id})
			}
			return serviceerror.NewUnavailablef("UpdateTaskQueueUserData build id delete failed: %v", err)
		}
	}

	return nil
}

func (s *taskStore) ListTaskQueueUserDataEntries(
	ctx context.Context,
	request *persistence.ListTaskQueueUserDataEntriesRequest,
) (*persistence.InternalListTaskQueueUserDataEntriesResponse, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("list task queue user data request is nil")
	}

	filter := bson.M{"namespace_id": request.NamespaceID}
	if len(request.NextPageToken) > 0 {
		filter["task_queue"] = bson.M{"$gt": string(request.NextPageToken)}
	}

	limit := int64(request.PageSize)
	if limit <= 0 {
		limit = 100
	}

	opts := options.Find().SetSort(bson.D{{Key: "task_queue", Value: 1}}).SetLimit(limit)

	cursor, err := s.db.Collection(collectionTaskQueueUserData).Find(ctx, filter, opts)
	if err != nil {
		return nil, serviceerror.NewUnavailablef("ListTaskQueueUserDataEntries failed: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	resp := &persistence.InternalListTaskQueueUserDataEntriesResponse{}
	var lastQueue string

	for cursor.Next(ctx) {
		var doc userDataDocument
		if err := cursor.Decode(&doc); err != nil {
			return nil, serviceerror.NewUnavailablef("ListTaskQueueUserDataEntries decode failed: %v", err)
		}
		resp.Entries = append(resp.Entries, persistence.InternalTaskQueueUserDataEntry{
			TaskQueue: doc.TaskQueue,
			Data:      persistence.NewDataBlob(doc.Data, doc.Encoding),
			Version:   doc.Version,
		})
		lastQueue = doc.TaskQueue
	}

	if err := cursor.Err(); err != nil {
		return nil, serviceerror.NewUnavailablef("ListTaskQueueUserDataEntries cursor error: %v", err)
	}

	if len(resp.Entries) == int(limit) && lastQueue != "" {
		resp.NextPageToken = []byte(lastQueue)
	}

	return resp, nil
}

//lint:ignore ST1003 method name constrained by persistence.TaskStore interface
//nolint:staticcheck // method name constrained by persistence.TaskStore interface
func (s *taskStore) GetTaskQueuesByBuildId(
	ctx context.Context,
	request *persistence.GetTaskQueuesByBuildIdRequest,
) ([]string, error) {
	if request == nil {
		return nil, serviceerror.NewInvalidArgument("get task queues by build id request is nil")
	}

	cursor, err := s.db.Collection(collectionTaskQueueBuildIDMaps).Find(ctx, bson.M{
		"namespace_id": request.NamespaceID,
		"build_id":     request.BuildID,
	}, options.Find().SetProjection(bson.M{"task_queue": 1}).SetSort(bson.D{{Key: "task_queue", Value: 1}}))
	if err != nil {
		return nil, serviceerror.NewUnavailablef("GetTaskQueuesByBuildId failed: %v", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var queues []string
	for cursor.Next(ctx) {
		var doc struct {
			TaskQueue string `bson:"task_queue"`
		}
		if err := cursor.Decode(&doc); err != nil {
			return nil, serviceerror.NewUnavailablef("GetTaskQueuesByBuildId decode failed: %v", err)
		}
		queues = append(queues, doc.TaskQueue)
	}
	if err := cursor.Err(); err != nil {
		return nil, serviceerror.NewUnavailablef("GetTaskQueuesByBuildId cursor error: %v", err)
	}

	return queues, nil
}

//lint:ignore ST1003 method name constrained by persistence.TaskStore interface
//nolint:staticcheck // method name constrained by persistence.TaskStore interface
func (s *taskStore) CountTaskQueuesByBuildId(
	ctx context.Context,
	request *persistence.CountTaskQueuesByBuildIdRequest,
) (int, error) {
	if request == nil {
		return 0, serviceerror.NewInvalidArgument("count task queues by build id request is nil")
	}

	count, err := s.db.Collection(collectionTaskQueueBuildIDMaps).CountDocuments(ctx, bson.M{
		"namespace_id": request.NamespaceID,
		"build_id":     request.BuildID,
	})
	if err != nil {
		return 0, serviceerror.NewUnavailablef("CountTaskQueuesByBuildId failed: %v", err)
	}
	if count > int64(math.MaxInt) {
		return 0, serviceerror.NewUnavailablef("CountTaskQueuesByBuildId count overflow: %d", count)
	}
	return int(count), nil
}

// -------- helpers --------

// taskQueueDocID generates a unique document ID for task queue metadata.
// The fairnessEnabled flag differentiates between v1 (priority/classic) and v2 (fair) task queues.
// This separation is necessary because during migration, both v1 and v2 backlog managers need
// independent lease management (rangeID) to avoid race conditions where they steal leases from
// each other. In Cassandra, this is achieved by using separate tables (tasks vs tasks_v2).
func taskQueueDocID(namespaceID, taskQueue string, taskType enumspb.TaskQueueType, subqueue int, fairnessEnabled bool) string {
	version := "v1"
	if fairnessEnabled {
		version = "v2"
	}
	return fmt.Sprintf("%s|%s|%d|%d|%s", namespaceID, taskQueue, taskType, subqueue, version)
}

func taskDocumentID(namespaceID, taskQueue string, taskType enumspb.TaskQueueType, subqueue int, taskPass int64, taskID int64) string {
	return fmt.Sprintf("%s|%s|%d|%d|%d|%d", namespaceID, taskQueue, taskType, subqueue, taskPass, taskID)
}

func taskQueueUserDataID(namespaceID, taskQueue string) string {
	return fmt.Sprintf("%s|%s", namespaceID, taskQueue)
}

func taskQueueBuildIDDocID(namespaceID, buildID, taskQueue string) string {
	return fmt.Sprintf("%s|%s|%s", namespaceID, buildID, taskQueue)
}

type taskPageToken struct {
	TaskPass int64 `json:"task_pass,omitempty"`
	TaskID   int64 `json:"task_id"`
}

func encodeTaskPageToken(token taskPageToken) ([]byte, error) {
	if token == (taskPageToken{}) {
		return nil, nil
	}
	return json.Marshal(token)
}

func decodeTaskPageToken(data []byte) (taskPageToken, error) {
	var token taskPageToken
	if len(data) == 0 {
		return token, nil
	}
	if err := json.Unmarshal(data, &token); err != nil {
		return taskPageToken{}, err
	}
	return token, nil
}

func boolPtr(v bool) *bool {
	return &v
}

var errUserDataVersionConflict = errors.New("task queue user data version conflict")
