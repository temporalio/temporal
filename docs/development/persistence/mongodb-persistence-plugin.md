# MongoDB Persistence Plugin Implementation Guide

**Prerequisites**: This guide assumes you have read the [Persistence Plugin Development Guide](persistence-plugin-development-guide.md) and understand Temporal's persistence architecture.

This document provides MongoDB-specific implementation details for creating a MongoDB persistence plugin for Temporal Server.

---

## Table of Contents

1. [MongoDB Plugin Overview](#1-mongodb-plugin-overview)
2. [Client Layer Implementation](#2-client-layer-implementation)
3. [Factory Implementation](#3-factory-implementation)
4. [ShardStore Implementation](#4-shardstore-implementation)
5. [ExecutionStore Implementation](#5-executionstore-implementation)
6. [TaskStore Implementation](#6-taskstore-implementation)
7. [MetadataStore Implementation](#7-metadatastore-implementation)
8. [Schema Design](#8-schema-design)
9. [Configuration](#9-configuration)
10. [Testing](#10-testing)
11. [Performance Considerations](#11-performance-considerations)
12. [Metrics and Monitoring](#12-metrics-and-monitoring)
13. [Future Work](#13-future-work)

---

## 1. MongoDB Plugin Overview

MongoDB uses the **Direct Factory** approach (similar to Cassandra), implementing `DataStoreFactory` without a plugin registry.

### 1.1 Key Characteristics

- **Document-oriented**: Unlike Cassandra's wide-column model, MongoDB uses documents with embedded data
- **ACID transactions**: Multi-document transactions available with replica sets
- **Flexible schema**: Schema can evolve without migrations
- **Rich indexing**: Compound indexes, text indexes, geospatial indexes
- **Replica set or sharded deployment required**: Temporal relies on multi-document transactions for atomic persistence. Standalone MongoDB instances are **not supported**.

### 1.2 Directory Structure

```text
common/persistence/mongodb/
├── client/
│   ├── interfaces.go      # MongoDB client interfaces
│   ├── client.go          # Client implementation
│   └── session.go         # Transaction/session support
├── factory.go             # DataStoreFactory implementation
├── shard_store.go         # ShardStore implementation
├── execution_store.go     # ExecutionStore implementation
├── task_store.go          # TaskStore implementation
├── metadata_store.go      # MetadataStore implementation
├── cluster_metadata_store.go
├── queue.go               # Queue implementations
├── nexus_endpoint_store.go
├── errors.go              # Error conversion utilities
├── common.go              # Shared constants and helpers
└── README.md              # Plugin-specific documentation
```

---

## 2. Client Layer Implementation

### 2.1 Client Interfaces

```go
// common/persistence/mongodb/client/interfaces.go
package client

import (
    "context"
    "fmt"
    "strings"
    "time"

    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
)

type Client interface {
    Connect(ctx context.Context) error
    Close(ctx context.Context) error
    Database(name string) Database
    StartSession(ctx context.Context) (Session, error)
    Ping(ctx context.Context) error
}

type Database interface {
    Collection(name string) Collection
    RunCommand(ctx context.Context, runCommand interface{}) SingleResult
}

type Collection interface {
    FindOne(ctx context.Context, filter interface{}) SingleResult
    Find(ctx context.Context, filter interface{}) (Cursor, error)
    InsertOne(ctx context.Context, document interface{}) (*mongo.InsertOneResult, error)
    UpdateOne(ctx context.Context, filter interface{}, update interface{}) (*mongo.UpdateResult, error)
    DeleteOne(ctx context.Context, filter interface{}) (*mongo.DeleteResult, error)
    CountDocuments(ctx context.Context, filter interface{}) (int64, error)
    CreateIndex(ctx context.Context, model mongo.IndexModel) (string, error)
    Aggregate(ctx context.Context, pipeline interface{}) (Cursor, error)
}

type SingleResult interface {
    Decode(v interface{}) error
    Err() error
}

type Cursor interface {
    Next(ctx context.Context) bool
    Decode(v interface{}) error
    All(ctx context.Context, results interface{}) error
    Close(ctx context.Context) error
    Err() error
}

type Session interface {
    StartTransaction(opts ...*options.TransactionOptions) error
    CommitTransaction(ctx context.Context) error
    AbortTransaction(ctx context.Context) error
    WithTransaction(ctx context.Context, fn func(ctx context.Context) (interface{}, error)) (interface{}, error)
    EndSession(ctx context.Context)
}
```

### 2.2 Client Implementation

```go
// common/persistence/mongodb/client/client.go
package client

import (
    "context"
    "fmt"
    "strings"
    "time"

    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    "go.temporal.io/server/common/config"
    "go.temporal.io/server/common/log"
    "go.temporal.io/server/common/log/tag"
)

type clientImpl struct {
    client *mongo.Client
    logger log.Logger
    cfg    *config.MongoDB
}

func NewClient(cfg *config.MongoDB, logger log.Logger) Client {
    return &clientImpl{
        cfg:    cfg,
        logger: logger.WithTags(tag.ComponentPersistenceClient),
    }
}

func (c *clientImpl) Connect(ctx context.Context) error {
    clientOpts := options.Client().
        ApplyURI(c.buildConnectionString()).
        SetMaxPoolSize(uint64(c.cfg.MaxConns)).
        SetConnectTimeout(c.cfg.ConnectTimeout).
        SetSocketTimeout(30 * time.Second).
        SetHeartbeatInterval(10 * time.Second)

    if c.cfg.TLS != nil && c.cfg.TLS.Enabled {
        tlsConfig, err := c.cfg.TLS.ToTLSConfig()
        if err != nil {
            return fmt.Errorf("failed to create TLS config: %w", err)
        }
        clientOpts.SetTLSConfig(tlsConfig)
    }

    client, err := mongo.Connect(ctx, clientOpts)
    if err != nil {
        return fmt.Errorf("failed to connect to MongoDB: %w", err)
    }

    // Verify connection
    if err := client.Ping(ctx, nil); err != nil {
        return fmt.Errorf("failed to ping MongoDB: %w", err)
    }

    c.client = client
    c.logger.Info("Connected to MongoDB successfully")
    return nil
}

func (c *clientImpl) Close(ctx context.Context) error {
    if c.client != nil {
        return c.client.Disconnect(ctx)
    }
    return nil
}

func (c *clientImpl) Database(name string) Database {
    return &databaseImpl{
        db: c.client.Database(name),
    }
}

func (c *clientImpl) StartSession(ctx context.Context) (Session, error) {
    session, err := c.client.StartSession()
    if err != nil {
        return nil, err
    }
    return &sessionImpl{session: session}, nil
}

func (c *clientImpl) buildConnectionString() string {
    // Build MongoDB connection string from config
    uri := "mongodb://"
    if c.cfg.User != "" && c.cfg.Password != "" {
        uri += fmt.Sprintf("%s:%s@", c.cfg.User, c.cfg.Password)
    }
    uri += strings.Join(c.cfg.Hosts, ",")

    // Add replica set if specified (enables transactions)
    if c.cfg.ReplicaSet != "" {
        uri += fmt.Sprintf("/%s?replicaSet=%s", c.cfg.DatabaseName, c.cfg.ReplicaSet)
    } else {
        uri += "/" + c.cfg.DatabaseName
    }

    return uri
}

func (c *clientImpl) Ping(ctx context.Context) error {
    if c.client == nil {
        return fmt.Errorf("client not connected")
    }
    return c.client.Ping(ctx, nil)
}

// DetectTopology determines MongoDB deployment type
func (c *clientImpl) DetectTopology(ctx context.Context) (TopologyType, error) {
    if c.client == nil {
        return TopologyUnknown, fmt.Errorf("client not connected")
    }

    result := c.client.Database("admin").RunCommand(ctx, bson.M{"isMaster": 1})
    var isMasterResult struct {
        IsMaster     bool   `bson:"ismaster"`
        SetName      string `bson:"setName,omitempty"`
        Msg          string `bson:"msg,omitempty"`
    }

    if err := result.Decode(&isMasterResult); err != nil {
        return TopologyUnknown, err
    }

    if isMasterResult.Msg == "isdbgrid" {
        return TopologySharded, nil
    } else if isMasterResult.SetName != "" {
        return TopologyReplicaSet, nil
    } else {
        return TopologyStandalone, nil
    }
}

type TopologyType int

const (
    TopologyUnknown TopologyType = iota
    TopologyStandalone  // Single node, no transactions
    TopologyReplicaSet  // Replica set, transactions supported
    TopologySharded     // Sharded cluster, distributed transactions
)
```

---

## 3. Factory Implementation

```go
// common/persistence/mongodb/factory.go
package mongodb

import (
    "context"
    "go.temporal.io/server/common/config"
    "go.temporal.io/server/common/log"
    "go.temporal.io/server/common/persistence"
    "go.temporal.io/server/common/persistence/mongodb/client"
)

type Factory struct {
    cfg                  config.MongoDB
    client               client.Client
    logger               log.Logger
    database             client.Database
    topology             TopologyType
    transactionsEnabled  bool
}

func NewFactory(cfg config.MongoDB, logger log.Logger) (*Factory, error) {
    mongoClient := client.NewClient(&cfg, logger)

    ctx, cancel := context.WithTimeout(context.Background(), cfg.ConnectTimeout)
    defer cancel()

    if err := mongoClient.Connect(ctx); err != nil {
        return nil, fmt.Errorf("failed to connect to MongoDB: %w", err)
    }

    // Detect MongoDB topology for transaction support
    topology, err := mongoClient.DetectTopology(ctx)
    if err != nil {
        logger.Warn("Failed to detect MongoDB topology", tag.Error(err))
        topology = TopologyUnknown
    }

    // Transactions are supported only in replica sets and sharded clusters
    transactionsEnabled := topology == TopologyReplicaSet || topology == TopologySharded

    logger.Info("MongoDB topology detected",
        tag.Value(topologyToString(topology)),
        tag.Bool("transactionsEnabled", transactionsEnabled))

    database := mongoClient.Database(cfg.DatabaseName)

    factory := &Factory{
        cfg:                 cfg,
        client:              mongoClient,
        logger:              logger,
        database:            database,
        topology:            topology,
        transactionsEnabled: transactionsEnabled,
    }

    // Initialize schema/indexes if needed
    if err := factory.initializeSchema(ctx); err != nil {
        return nil, fmt.Errorf("failed to initialize schema: %w", err)
    }

    return factory, nil
}

func (f *Factory) Close() {
    ctx := context.Background()
    if f.client != nil {
        if err := f.client.Close(ctx); err != nil {
            f.logger.Error("Failed to close MongoDB client", tag.Error(err))
        }
    }
}

func (f *Factory) NewShardStore() (persistence.ShardStore, error) {
    return NewShardStore(f.database, f.cfg, f.logger, f.transactionsEnabled)
}

func (f *Factory) NewExecutionStore() (persistence.ExecutionStore, error) {
    return NewExecutionStore(f.database, f.cfg, f.logger, f.transactionsEnabled)
}

func (f *Factory) NewTaskStore() (persistence.TaskStore, error) {
    return NewTaskStore(f.database, f.cfg, f.logger, f.transactionsEnabled)
}

func (f *Factory) NewFairTaskStore() (persistence.TaskStore, error) {
    // Fair task store implementation with fairness guarantees
    return NewTaskStore(f.database, f.cfg, f.logger, f.transactionsEnabled)
}

func (f *Factory) NewMetadataStore() (persistence.MetadataStore, error) {
    return NewMetadataStore(f.database, f.cfg, f.logger, f.transactionsEnabled)
}

func (f *Factory) NewClusterMetadataStore() (persistence.ClusterMetadataStore, error) {
    return NewClusterMetadataStore(f.database, f.cfg, f.logger, f.transactionsEnabled)
}

func (f *Factory) NewQueue(queueType persistence.QueueType) (persistence.Queue, error) {
    return NewQueue(f.database, queueType, f.cfg, f.logger, f.transactionsEnabled)
}

func (f *Factory) NewQueueV2() (persistence.QueueV2, error) {
    return NewQueueV2(f.database, f.cfg, f.logger, f.transactionsEnabled)
}

func (f *Factory) NewNexusEndpointStore() (persistence.NexusEndpointStore, error) {
    return NewNexusEndpointStore(f.database, f.cfg, f.logger, f.transactionsEnabled)
}

func topologyToString(t TopologyType) string {
    switch t {
    case TopologyStandalone:
        return "standalone"
    case TopologyReplicaSet:
        return "replicaset"
    case TopologySharded:
        return "sharded"
    default:
        return "unknown"
    }
}

func (f *Factory) initializeSchema(ctx context.Context) error {
    // Create indexes for efficient queries
    collections := []string{
        collectionShards,
        collectionExecutions,
        collectionExecutionTasks,
        collectionHistory,
        collectionTasks,
        collectionTaskQueues,
        collectionNamespaces,
        collectionClusterMetadata,
        collectionQueues,
        collectionQueueMessages,
        collectionNexusEndpoints,
    }

    for _, collName := range collections {
        if err := f.createIndexesForCollection(ctx, collName); err != nil {
            return fmt.Errorf("failed to create indexes for collection %s: %w", collName, err)
        }
    }

    return nil
}
```

---

## 4. ShardStore Implementation

```go
// common/persistence/mongodb/shard_store.go
package mongodb

import (
    "context"
    "time"

    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    enumspb "go.temporal.io/api/enums/v1"
    "go.temporal.io/server/common/config"
    "go.temporal.io/server/common/log"
    "go.temporal.io/server/common/log/tag"
    "go.temporal.io/server/common/persistence"
    "go.temporal.io/server/common/persistence/mongodb/client"
)

const (
    collectionShards = "shards"
)

type shardStore struct {
    db                  client.Database
    logger              log.Logger
    cfg                 config.MongoDB
    transactionsEnabled bool
}

type shardDocument struct {
    ShardID                int32  `bson:"_id"`
    RangeID                int64  `bson:"range_id"`
    Data                   []byte `bson:"data"`
    DataEncoding           string `bson:"data_encoding"`
    UpdatedAt              int64  `bson:"updated_at"`
}

func NewShardStore(db client.Database, cfg config.MongoDB, logger log.Logger, transactionsEnabled bool) (persistence.ShardStore, error) {
    return &shardStore{
        db:                  db,
        cfg:                 cfg,
        logger:              logger.WithTags(tag.ComponentShardStore),
        transactionsEnabled: transactionsEnabled,
    }, nil
}

func (s *shardStore) GetOrCreateShard(
    ctx context.Context,
    request *persistence.InternalGetOrCreateShardRequest,
) (*persistence.InternalGetOrCreateShardResponse, error) {
    collection := s.db.Collection(collectionShards)

    filter := bson.M{"_id": request.ShardID}

    var doc shardDocument
    err := collection.FindOne(ctx, filter).Decode(&doc)

    if err == mongo.ErrNoDocuments {
        // Create new shard
        newDoc := shardDocument{
            ShardID:      request.ShardID,
            RangeID:      request.InitialFailoverVersion,
            Data:         []byte{}, // Initialize with empty mutable state
            DataEncoding: enumspb.ENCODING_TYPE_PROTO3.String(),
            UpdatedAt:    time.Now().UnixNano(),
        }

        _, err := collection.InsertOne(ctx, newDoc)
        if err != nil {
            return nil, convertError(err)
        }

        return &persistence.InternalGetOrCreateShardResponse{
            ShardInfo: &persistence.InternalShardInfo{
                ShardID:  request.ShardID,
                RangeID:  newDoc.RangeID,
                Data:     newDoc.Data,
                Encoding: newDoc.DataEncoding,
            },
        }, nil
    } else if err != nil {
        return nil, convertError(err)
    }

    return &persistence.InternalGetOrCreateShardResponse{
        ShardInfo: &persistence.InternalShardInfo{
            ShardID:  doc.ShardID,
            RangeID:  doc.RangeID,
            Data:     doc.Data,
            Encoding: doc.DataEncoding,
        },
    }, nil
}

func (s *shardStore) UpdateShard(
    ctx context.Context,
    request *persistence.InternalUpdateShardRequest,
) error {
    collection := s.db.Collection(collectionShards)

    // Optimistic concurrency control using range_id
    filter := bson.M{
        "_id":      request.ShardID,
        "range_id": request.PreviousRangeID,
    }

    update := bson.M{
        "$set": bson.M{
            "range_id":      request.RangeID,
            "data":          request.Data,
            "data_encoding": request.Encoding,
            "updated_at":    time.Now().UnixNano(),
        },
    }

    result, err := collection.UpdateOne(ctx, filter, update)
    if err != nil {
        return convertError(err)
    }

    if result.MatchedCount == 0 {
        return &persistence.ShardOwnershipLostError{
            ShardID: request.ShardID,
            Msg:     "shard ownership lost or range_id mismatch",
        }
    }

    return nil
}

func (s *shardStore) AssertShardOwnership(
    ctx context.Context,
    request *persistence.AssertShardOwnershipRequest,
) error {
    collection := s.db.Collection(collectionShards)

    filter := bson.M{
        "_id":      request.ShardID,
        "range_id": request.RangeID,
    }

    count, err := collection.CountDocuments(ctx, filter)
    if err != nil {
        return convertError(err)
    }

    if count == 0 {
        return &persistence.ShardOwnershipLostError{
            ShardID: request.ShardID,
            Msg:     "shard ownership assertion failed",
        }
    }

    return nil
}
```

---

## 5. ExecutionStore Implementation

ExecutionStore is the most complex store. Here's a simplified version focusing on key methods:

```go
// common/persistence/mongodb/execution_store.go
package mongodb

import (
    "context"
    "time"

    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "go.temporal.io/api/serviceerror"
    "go.temporal.io/server/common/config"
    "go.temporal.io/server/common/log"
    "go.temporal.io/server/common/log/tag"
    "go.temporal.io/server/common/persistence"
    "go.temporal.io/server/common/persistence/mongodb/client"
)

const (
    collectionExecutions     = "executions"
    collectionExecutionTasks = "execution_tasks"
    collectionHistory        = "history"
)

type executionStore struct {
    db                  client.Database
    logger              log.Logger
    cfg                 config.MongoDB
    transactionsEnabled bool
}

type executionDocument struct {
    ShardID           int32  `bson:"shard_id"`
    NamespaceID       string `bson:"namespace_id"`
    WorkflowID        string `bson:"workflow_id"`
    RunID             string `bson:"run_id"`
    Data              []byte `bson:"data"`
    DataEncoding      string `bson:"data_encoding"`
    State             []byte `bson:"state"`
    StateEncoding     string `bson:"state_encoding"`
    NextEventID       int64  `bson:"next_event_id"`
    DBRecordVersion   int64  `bson:"db_record_version"`
    UpdatedAt         int64  `bson:"updated_at"`
}

func NewExecutionStore(db client.Database, cfg config.MongoDB, logger log.Logger, transactionsEnabled bool) (persistence.ExecutionStore, error) {
    return &executionStore{
        db:                  db,
        cfg:                 cfg,
        logger:              logger.WithTags(tag.ComponentExecutionStore),
        transactionsEnabled: transactionsEnabled,
    }, nil
}

func (e *executionStore) CreateWorkflowExecution(
    ctx context.Context,
    request *persistence.InternalCreateWorkflowExecutionRequest,
) (*persistence.InternalCreateWorkflowExecutionResponse, error) {
    session, err := e.db.Client().StartSession(ctx)
    if err != nil {
        return nil, convertError(err)
    }
    defer session.EndSession(ctx)

    var response *persistence.InternalCreateWorkflowExecutionResponse

    _, err = session.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
        // Create workflow execution
        if err := e.createExecution(ctx, request); err != nil {
            return nil, err
        }

        // Create history events
        if err := e.appendHistoryEvents(ctx, request); err != nil {
            return nil, err
        }

        // Create transfer/timer tasks
        if err := e.createTasks(ctx, request); err != nil {
            return nil, err
        }

        response = &persistence.InternalCreateWorkflowExecutionResponse{}
        return nil, nil
    })

    if err != nil {
        return nil, convertError(err)
    }

    return response, nil
}

func (e *executionStore) GetWorkflowExecution(
    ctx context.Context,
    request *persistence.GetWorkflowExecutionRequest,
) (*persistence.InternalGetWorkflowExecutionResponse, error) {
    collection := e.db.Collection(collectionExecutions)

    filter := bson.M{
        "shard_id":     request.ShardID,
        "namespace_id": request.NamespaceID,
        "workflow_id":  request.WorkflowID,
        "run_id":       request.RunID,
    }

    var doc executionDocument
    err := collection.FindOne(ctx, filter).Decode(&doc)
    if err == mongo.ErrNoDocuments {
        return nil, serviceerror.NewNotFound("workflow execution not found")
    } else if err != nil {
        return nil, convertError(err)
    }

    return &persistence.InternalGetWorkflowExecutionResponse{
        State: &persistence.InternalWorkflowMutableState{
            ExecutionInfo:    &persistence.WorkflowExecutionInfo{...},
            ExecutionState:   &persistence.WorkflowExecutionState{...},
            NextEventID:      doc.NextEventID,
            DBRecordVersion:  doc.DBRecordVersion,
        },
    }, nil
}

func (e *executionStore) UpdateWorkflowExecution(
    ctx context.Context,
    request *persistence.InternalUpdateWorkflowExecutionRequest,
) error {
    if e.transactionsEnabled {
        return e.updateWorkflowExecutionWithTransaction(ctx, request)
    } else {
        return e.updateWorkflowExecutionWithoutTransaction(ctx, request)
    }
}

func (e *executionStore) updateWorkflowExecutionWithTransaction(
    ctx context.Context,
    request *persistence.InternalUpdateWorkflowExecutionRequest,
) error {
    session, err := e.db.Client().StartSession(ctx)
    if err != nil {
        return convertError(err)
    }
    defer session.EndSession(ctx)

    _, err = session.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
        // Update mutable state with optimistic concurrency
        if err := e.updateExecution(ctx, request); err != nil {
            return nil, err
        }

        // Append history events
        if err := e.appendHistoryEvents(ctx, request); err != nil {
            return nil, err
        }

        // Create/complete/delete tasks
        if err := e.updateTasks(ctx, request); err != nil {
            return nil, err
        }

        return nil, nil
    })

    return convertError(err)
}

func (e *executionStore) updateWorkflowExecutionWithoutTransaction(
    ctx context.Context,
    request *persistence.InternalUpdateWorkflowExecutionRequest,
) error {
    // For standalone MongoDB, execute operations sequentially
    // Note: This may have consistency implications in case of failures

    // Update mutable state with optimistic concurrency
    if err := e.updateExecution(ctx, request); err != nil {
        return err
    }

    // Append history events
    if err := e.appendHistoryEvents(ctx, request); err != nil {
        e.logger.Warn("Failed to append history events", tag.Error(err))
        return err
    }

    // Create/complete/delete tasks
    if err := e.updateTasks(ctx, request); err != nil {
        e.logger.Warn("Failed to update tasks", tag.Error(err))
        return err
    }

    return nil
}

func (e *executionStore) updateExecution(
    ctx context.Context,
    request *persistence.InternalUpdateWorkflowExecutionRequest,
) error {
    collection := e.db.Collection(collectionExecutions)

    // Optimistic concurrency with db_record_version
    filter := bson.M{
        "shard_id":          request.ShardID,
        "namespace_id":      request.ExecutionInfo.NamespaceID,
        "workflow_id":       request.ExecutionInfo.WorkflowID,
        "run_id":            request.ExecutionState.RunID,
        "db_record_version": request.Condition.DBRecordVersion,
    }

    update := bson.M{
        "$set": bson.M{
            "data":              request.MutableStateData,
            "data_encoding":     request.MutableStateDataEncoding,
            "state":             request.ExecutionStateData,
            "state_encoding":    request.ExecutionStateDataEncoding,
            "next_event_id":     request.NextEventID,
            "db_record_version": request.DBRecordVersion,
            "updated_at":        time.Now().UnixNano(),
        },
    }

    result, err := collection.UpdateOne(ctx, filter, update)
    if err != nil {
        return err
    }

    if result.MatchedCount == 0 {
        return &persistence.ConditionFailedError{
            Msg: "workflow execution condition failed - version mismatch",
        }
    }

    return nil
}
```

---

## 6. TaskStore Implementation

```go
// common/persistence/mongodb/task_store.go
package mongodb

import (
    "context"
    "time"

    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo/options"
    "go.temporal.io/server/common/config"
    "go.temporal.io/server/common/log"
    "go.temporal.io/server/common/log/tag"
    "go.temporal.io/server/common/persistence"
    "go.temporal.io/server/common/persistence/mongodb/client"
)

const (
    collectionTasks         = "tasks"
    collectionTaskQueues    = "task_queues"
    collectionTaskQueueUserData = "task_queue_user_data"
)

type taskDocument struct {
    NamespaceID      string `bson:"namespace_id"`
    TaskQueue        string `bson:"task_queue"`
    TaskType         int32  `bson:"task_type"`
    TaskID           int64  `bson:"_id"`
    Data             []byte `bson:"data"`
    DataEncoding     string `bson:"data_encoding"`
    CreateTime       int64  `bson:"create_time"`
}

type taskStore struct {
    db     client.Database
    logger log.Logger
    cfg    config.MongoDB
}

func NewTaskStore(db client.Database, cfg config.MongoDB, logger log.Logger) (persistence.TaskStore, error) {
    return &taskStore{
        db:     db,
        cfg:    cfg,
        logger: logger.WithTags(tag.ComponentTaskStore),
    }, nil
}

func (t *taskStore) CreateTasks(
    ctx context.Context,
    request *persistence.InternalCreateTasksRequest,
) (*persistence.CreateTasksResponse, error) {
    collection := t.db.Collection(collectionTasks)

    var docs []interface{}
    for _, task := range request.Tasks {
        doc := taskDocument{
            NamespaceID:  task.NamespaceID,
            TaskQueue:    task.TaskQueue,
            TaskType:     int32(task.TaskType),
            TaskID:       task.TaskID,
            Data:         task.Data,
            DataEncoding: task.Encoding,
            CreateTime:   time.Now().UnixNano(),
        }
        docs = append(docs, doc)
    }

    _, err := collection.InsertMany(ctx, docs)
    if err != nil {
        return nil, convertError(err)
    }

    return &persistence.CreateTasksResponse{}, nil
}

func (t *taskStore) GetTasks(
    ctx context.Context,
    request *persistence.GetTasksRequest,
) (*persistence.InternalGetTasksResponse, error) {
    collection := t.db.Collection(collectionTasks)

    filter := bson.M{
        "namespace_id": request.NamespaceID,
        "task_queue":   request.TaskQueue,
        "task_type":    int32(request.TaskType),
    }

    if request.ReadLevel != nil {
        filter["_id"] = bson.M{"$gt": *request.ReadLevel}
    }

    opts := options.Find().
        SetLimit(int64(request.BatchSize)).
        SetSort(bson.M{"_id": 1})

    cursor, err := collection.Find(ctx, filter, opts)
    if err != nil {
        return nil, convertError(err)
    }
    defer cursor.Close(ctx)

    var tasks []*persistence.InternalTaskInfo
    for cursor.Next(ctx) {
        var doc taskDocument
        if err := cursor.Decode(&doc); err != nil {
            return nil, convertError(err)
        }

        tasks = append(tasks, &persistence.InternalTaskInfo{
            NamespaceID:  doc.NamespaceID,
            WorkflowID:   "", // Extract from data if needed
            RunID:        "", // Extract from data if needed
            TaskID:       doc.TaskID,
            Data:         doc.Data,
            Encoding:     doc.DataEncoding,
        })
    }

    return &persistence.InternalGetTasksResponse{
        Tasks: tasks,
    }, nil
}
```

---

## 7. MetadataStore Implementation

```go
// common/persistence/mongodb/metadata_store.go
package mongodb

import (
    "context"
    "fmt"
    "time"

    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "go.temporal.io/api/serviceerror"
    "go.temporal.io/server/common/config"
    "go.temporal.io/server/common/log"
    "go.temporal.io/server/common/log/tag"
    "go.temporal.io/server/common/persistence"
    "go.temporal.io/server/common/persistence/mongodb/client"
)

const collectionNamespaces = "namespaces"

type namespaceDocument struct {
    ID                  string `bson:"_id"`
    Name                string `bson:"name"`
    Data                []byte `bson:"data"`
    DataEncoding        string `bson:"data_encoding"`
    IsGlobalNamespace   bool   `bson:"is_global_namespace"`
    NotificationVersion int64  `bson:"notification_version"`
    UpdatedAt           int64  `bson:"updated_at"`
}

type metadataStore struct {
    db     client.Database
    logger log.Logger
    cfg    config.MongoDB
}

func NewMetadataStore(db client.Database, cfg config.MongoDB, logger log.Logger) (persistence.MetadataStore, error) {
    return &metadataStore{
        db:     db,
        cfg:    cfg,
        logger: logger.WithTags(tag.ComponentMetadataStore),
    }, nil
}

func (m *metadataStore) CreateNamespace(
    ctx context.Context,
    request *persistence.InternalCreateNamespaceRequest,
) (*persistence.CreateNamespaceResponse, error) {
    collection := m.db.Collection(collectionNamespaces)

    doc := namespaceDocument{
        ID:                  request.ID,
        Name:                request.Name,
        Data:                request.Data,
        DataEncoding:        request.Encoding,
        IsGlobalNamespace:   request.IsGlobalNamespace,
        NotificationVersion: request.NotificationVersion,
        UpdatedAt:           time.Now().UnixNano(),
    }

    _, err := collection.InsertOne(ctx, doc)
    if err != nil {
        if isDuplicateKeyError(err) {
            return nil, &persistence.NamespaceAlreadyExistsError{
                Msg: fmt.Sprintf("namespace %s already exists", request.Name),
            }
        }
        return nil, convertError(err)
    }

    return &persistence.CreateNamespaceResponse{ID: request.ID}, nil
}

func (m *metadataStore) GetNamespace(
    ctx context.Context,
    request *persistence.GetNamespaceRequest,
) (*persistence.InternalGetNamespaceResponse, error) {
    collection := m.db.Collection(collectionNamespaces)

    var filter bson.M
    if request.ID != "" {
        filter = bson.M{"_id": request.ID}
    } else {
        filter = bson.M{"name": request.Name}
    }

    var doc namespaceDocument
    err := collection.FindOne(ctx, filter).Decode(&doc)
    if err == mongo.ErrNoDocuments {
        return nil, serviceerror.NewNamespaceNotFound(request.Name)
    } else if err != nil {
        return nil, convertError(err)
    }

    return &persistence.InternalGetNamespaceResponse{
        Namespace: &persistence.InternalNamespaceDetail{
            Info: &persistence.NamespaceInfo{
                ID:   doc.ID,
                Name: doc.Name,
            },
            Config:              &persistence.NamespaceConfig{...},
            ReplicationConfig:   &persistence.NamespaceReplicationConfig{...},
            ConfigVersion:       doc.NotificationVersion,
            FailoverVersion:     doc.NotificationVersion,
        },
        IsGlobalNamespace:   doc.IsGlobalNamespace,
        NotificationVersion: doc.NotificationVersion,
    }, nil
}
```

---

## 8. Schema Design

### 8.1 MongoDB Schema Files

The bootstrap script now lives at `schema/mongodb/temporal/versioned/v1.0/init.js`. It executes idempotent `ensureCollection` calls to create collections lazily and backs them with indexes aligned with the Mongo stores implemented so far.

Key additions since the queue/Nexus milestone:

- `queue_metadata` — unique `_id` (`queue_metadata_lookup`) storing blob + ack metadata for legacy queues and DLQs.
- `queue_messages` — ascending/descending `{ queue_type, message_id }` indexes for pagination (`queue_messages_lookup`, `queue_messages_lookup_desc`).
- `queue_v2_metadata` — unique `{ queue_type, queue_name }` index for named queue metadata.
- `queue_v2_messages` — indexes `{ queue_type, queue_name, partition, message_id }` and descending counterpart to support fan-out scans.
- `nexus_endpoints_metadata` — singleton document tracking the Nexus table version.
- `nexus_endpoints` — unique `_id` index for endpoints (payload + version).

See `init.js` for the full list (it still ensures namespaces, shards, executions, history, tasks, visibility, etc.). The script only runs inserts when a collection or index is missing, so re-running it after deployments is safe.

### 8.2 Document Models vs Wide Column

**Cassandra (Wide Column Model)**:

```sql
PRIMARY KEY (shard_id, type, namespace_id, workflow_id, run_id, visibility_ts, task_id)
```

**MongoDB (Document Model)**:

```javascript
{
  "_id": ObjectId("..."),
  "shard_id": 1,
  "namespace_id": "default",
  "workflow_id": "my-workflow",
  "run_id": "uuid",
  "data": BinData(...),
  // Compound index: {shard_id: 1, namespace_id: 1, workflow_id: 1, run_id: 1}
}
```

---

## 9. Configuration

### 9.1 Add MongoDB Config

```go
// common/config/persistence.go
type MongoDB struct {
    Hosts           []string      `yaml:"hosts"`
    Port            int           `yaml:"port"`
    DatabaseName    string        `yaml:"databaseName"`
    User            string        `yaml:"user"`
    Password        string        `yaml:"password"`
    MaxConns        int           `yaml:"maxConns"`
    ConnectTimeout  time.Duration `yaml:"connectTimeout"`
    TLS             *TLS          `yaml:"tls"`
    ReplicaSet      string        `yaml:"replicaSet"`      // For transaction support
    ReadPreference  string        `yaml:"readPreference"`  // primary, secondary, etc.
    WriteConcern    string        `yaml:"writeConcern"`    // majority, w1, etc.
}

// Add to DataStore struct
type DataStore struct {
    SQL                   *SQL                   `yaml:"sql"`
    Cassandra             *Cassandra             `yaml:"cassandra"`
    MongoDB               *MongoDB               `yaml:"mongodb"`  // NEW
    Elasticsearch         *Elasticsearch         `yaml:"elasticsearch"`
    CustomDataStoreConfig *CustomDatastoreConfig `yaml:"customDataStore"`
}
```

### 9.2 Configuration Examples for Different Topologies

#### Standalone MongoDB (Development/Testing)

```yaml
persistence:
  defaultStore: mongodb-store
  numHistoryShards: 4
  datastores:
    mongodb-store:
      mongodb:
        hosts:
          - "localhost:27017"
        databaseName: "temporal"
        user: "temporal"
        password: "password"
        maxConns: 20
        connectTimeout: 10s
        # No replicaSet specified = standalone mode
        # Transactions will be disabled automatically
```

#### Replica Set MongoDB (Production - High Availability)

```yaml
persistence:
  defaultStore: mongodb-store
  numHistoryShards: 16
  datastores:
    mongodb-store:
      mongodb:
        hosts:
          - "mongo-rs0-1:27017"
          - "mongo-rs0-2:27017"
          - "mongo-rs0-3:27017"
        databaseName: "temporal"
        user: "temporal"
        password: "password"
        maxConns: 100
        connectTimeout: 10s
        replicaSet: "rs0" # Enables transaction support
        readPreference: "primary"
        writeConcern: "majority"
        tls:
          enabled: true
          certFile: "/path/to/client.pem"
          keyFile: "/path/to/client-key.pem"
          caFile: "/path/to/ca.pem"
```

#### Sharded MongoDB (Production - Scale Out)

```yaml
persistence:
  defaultStore: mongodb-store
  numHistoryShards: 64
  datastores:
    mongodb-store:
      mongodb:
        hosts:
          - "mongos-1:27017"
          - "mongos-2:27017" # Multiple mongos for HA
        databaseName: "temporal"
        user: "temporal"
        password: "password"
        maxConns: 200
        connectTimeout: 15s
        # No replicaSet for sharded clusters
        # Transactions supported across shards (MongoDB 4.2+)
        readPreference: "primary"
        writeConcern: "majority"
                tls:
                    enabled: true
```

### 9.3 Schema Initialization

Temporal provides an idempotent helper script under `schema/mongodb/temporal/versioned/v1.0` that creates the collections and indexes required by the stores implemented so far (shards, namespaces, task queues, executions, etc.).

```bash
export MONGO_URI="mongodb://localhost:27017"
export TEMPORAL_MONGO_DB="temporal"
# Optional: keep visibility data in a separate database
# export TEMPORAL_MONGO_VISIBILITY_DB="temporal_visibility"

mongosh "$MONGO_URI" schema/mongodb/temporal/versioned/v1.0/init.js
```

The script only creates missing collections and indexes, so it is safe to rerun. When additional persistence features land (e.g., metrics collections or future Nexus extensions), extend the script or add a new versioned directory to capture those changes.

### 9.4 Topology-Aware Configuration

The plugin automatically detects MongoDB topology and adjusts behavior:

- **Standalone**: Transactions disabled, operations executed sequentially
- **Replica Set**: Transactions enabled, ACID guarantees for multi-document operations
- **Sharded**: Distributed transactions enabled, scale-out support

---

## 10. Testing

### 10.0 Running the shared persistence test suites

Temporal has a shared persistence integration test harness under `common/persistence/tests`. The MongoDB implementation is wired into this harness and runs against an already-running MongoDB **replica set** (transactions require replica set or sharded deployments).

The harness uses the following environment variables (all optional, with sensible defaults):

- `MONGODB_SEEDS`: MongoDB host (default: `LOCALHOST_IP` / `127.0.0.1`)
- `MONGODB_PORT`: MongoDB port (default: `27017`)
- `MONGODB_REPLICA_SET`: replica set name (default: `rs0`)

For the development dependencies started via `make start-dependencies`, MongoDB is started as a single-node replica set (`rs0`) and the default credentials are `temporal` / `temporal`.

Note: with the official MongoDB Docker image, the root user created via `MONGO_INITDB_ROOT_USERNAME` / `MONGO_INITDB_ROOT_PASSWORD` authenticates against the `admin` database. If you configure credentials in Temporal’s MongoDB datastore config, you may need `authSource: admin`.

### 10.1 Multi-Topology Test Suite

To ensure the MongoDB plugin works correctly across all deployment types, we need comprehensive tests:

```go
// common/persistence/tests/mongodb_test.go
package tests

import (
    "context"
    "fmt"
    "testing"
    "time"

    "github.com/stretchr/testify/require"
    "github.com/stretchr/testify/suite"
    "github.com/testcontainers/testcontainers-go"
    "github.com/testcontainers/testcontainers-go/wait"
    "go.mongodb.org/mongo-driver/bson"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    "go.temporal.io/server/common/config"
    "go.temporal.io/server/common/log"
    "go.temporal.io/server/common/persistence"
    "go.temporal.io/server/common/persistence/mongodb"
    "go.temporal.io/server/common/persistence/serialization"
)

type mongoTestData struct {
    Factory persistence.DataStoreFactory
    Logger  log.Logger
    Config  config.MongoDB
    Cleanup func()
}

// Test all stores with standalone MongoDB (no transactions)
func TestMongoDBStandaloneSuite(t *testing.T) {
    testData, tearDown := setUpStandaloneMongoDB(t)
    defer tearDown()

    // Verify topology detection
    factory := testData.Factory.(*mongodb.Factory)
    require.False(t, factory.IsTransactionsEnabled(), "Transactions should be disabled for standalone")

    runAllStoreTests(t, testData)
}

// Test all stores with replica set MongoDB (transactions enabled)
func TestMongoDBReplicaSetSuite(t *testing.T) {
    testData, tearDown := setUpReplicaSetMongoDB(t)
    defer tearDown()

    // Verify topology detection
    factory := testData.Factory.(*mongodb.Factory)
    require.True(t, factory.IsTransactionsEnabled(), "Transactions should be enabled for replica set")

    runAllStoreTests(t, testData)

    // Additional transaction-specific tests
    runTransactionTests(t, testData)
}

// Test with sharded MongoDB cluster (distributed transactions)
func TestMongoDBShardedSuite(t *testing.T) {
    if testing.Short() {
        t.Skip("Skipping sharded MongoDB tests in short mode")
    }

    testData, tearDown := setUpShardedMongoDB(t)
    defer tearDown()

    // Verify topology detection
    factory := testData.Factory.(*mongodb.Factory)
    require.True(t, factory.IsTransactionsEnabled(), "Transactions should be enabled for sharded cluster")

    runAllStoreTests(t, testData)
    runShardedTests(t, testData)
}

func runAllStoreTests(t *testing.T, testData *mongoTestData) {
    // Test ShardStore
    shardStore, err := testData.Factory.NewShardStore()
    require.NoError(t, err)
    s := NewShardSuite(t, shardStore, serialization.NewSerializer(), testData.Logger)
    suite.Run(t, s)

    // Test ExecutionStore
    executionStore, err := testData.Factory.NewExecutionStore()
    require.NoError(t, err)
    e := NewExecutionSuite(t, executionStore, serialization.NewSerializer(), testData.Logger)
    suite.Run(t, e)

    // Test MetadataStore
    metadataStore, err := testData.Factory.NewMetadataStore()
    require.NoError(t, err)
    m := NewMetadataSuite(t, metadataStore, serialization.NewSerializer(), testData.Logger)
    suite.Run(t, m)

    // Test TaskStore
    taskStore, err := testData.Factory.NewTaskStore()
    require.NoError(t, err)
    ts := NewTaskSuite(t, taskStore, serialization.NewSerializer(), testData.Logger)
    suite.Run(t, ts)
}

### 10.2 Standalone MongoDB Setup

func setUpStandaloneMongoDB(t *testing.T) (*mongoTestData, func()) {
    ctx := context.Background()

    mongoContainer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
        ContainerRequest: testcontainers.ContainerRequest{
            Image:        "mongo:7.0.24",
            ExposedPorts: []string{"27017/tcp"},
            Env: map[string]string{
                "MONGO_INITDB_ROOT_USERNAME": "root",
                "MONGO_INITDB_ROOT_PASSWORD": "password",
            },
            WaitingFor: wait.ForLog("Waiting for connections").WithStartupTimeout(30 * time.Second),
        },
        Started: true,
    })
    require.NoError(t, err)

    host, err := mongoContainer.Host(ctx)
    require.NoError(t, err)

    port, err := mongoContainer.MappedPort(ctx, "27017")
    require.NoError(t, err)

    cfg := config.MongoDB{
        Hosts:          []string{fmt.Sprintf("%s:%s", host, port.Port())},
        DatabaseName:   "temporal_test_standalone",
        User:           "root",
        Password:       "password",
        MaxConns:       10,
        ConnectTimeout: 10 * time.Second,
        // No ReplicaSet = Standalone mode
    }

    factory, err := mongodb.NewFactory(cfg, log.NewTestLogger())
    require.NoError(t, err)

    tearDown := func() {
        factory.Close()
        mongoContainer.Terminate(ctx)
    }

    return &mongoTestData{
        Factory: factory,
        Logger:  log.NewTestLogger(),
        Config:  cfg,
        Cleanup: tearDown,
    }, tearDown
}

### 10.3 Replica Set MongoDB Setup

func setUpReplicaSetMongoDB(t *testing.T) (*mongoTestData, func()) {
    ctx := context.Background()

    // Start 3 MongoDB containers for replica set
    containers := make([]testcontainers.Container, 3)
    ports := make([]string, 3)

    network, err := testcontainers.GenericNetwork(ctx, testcontainers.GenericNetworkRequest{
        NetworkRequest: testcontainers.NetworkRequest{
            Name: "mongo-rs-network",
        },
    })
    require.NoError(t, err)

    // Start MongoDB instances
    for i := 0; i < 3; i++ {
        container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
            ContainerRequest: testcontainers.ContainerRequest{
                Image:        "mongo:7.0.24",
                ExposedPorts: []string{"27017/tcp"},
                Networks:     []string{"mongo-rs-network"},
                NetworkAliases: map[string][]string{
                    "mongo-rs-network": {fmt.Sprintf("mongo%d", i+1)},
                },
                Env: map[string]string{
                    "MONGO_INITDB_ROOT_USERNAME": "root",
                    "MONGO_INITDB_ROOT_PASSWORD": "password",
                },
                Cmd: []string{
                    "mongod",
                    "--replSet", "rs0",
                    "--bind_ip_all",
                    "--port", "27017",
                },
                WaitingFor: wait.ForLog("Waiting for connections").WithStartupTimeout(60 * time.Second),
            },
            Started: true,
        })
        require.NoError(t, err)

        containers[i] = container

        host, err := container.Host(ctx)
        require.NoError(t, err)
        port, err := container.MappedPort(ctx, "27017")
        require.NoError(t, err)
        ports[i] = fmt.Sprintf("%s:%s", host, port.Port())
    }

    // Initialize replica set
    time.Sleep(5 * time.Second) // Wait for containers to be ready
    err = initializeReplicaSet(ctx, ports[0])
    require.NoError(t, err)

    cfg := config.MongoDB{
        Hosts:          ports,
        DatabaseName:   "temporal_test_replica",
        User:           "root",
        Password:       "password",
        ReplicaSet:     "rs0", // This enables transactions
        MaxConns:       10,
        ConnectTimeout: 15 * time.Second,
    }

    factory, err := mongodb.NewFactory(cfg, log.NewTestLogger())
    require.NoError(t, err)

    tearDown := func() {
        factory.Close()
        for _, container := range containers {
            container.Terminate(ctx)
        }
        network.Remove(ctx)
    }

    return &mongoTestData{
        Factory: factory,
        Logger:  log.NewTestLogger(),
        Config:  cfg,
        Cleanup: tearDown,
    }, tearDown
}

func initializeReplicaSet(ctx context.Context, primaryHost string) error {
    // Connect to primary and initialize replica set
    client, err := mongo.Connect(ctx, options.Client().ApplyURI(
        fmt.Sprintf("mongodb://root:password@%s/?directConnection=true", primaryHost)))
    if err != nil {
        return err
    }
    defer client.Disconnect(ctx)

    // Initialize replica set configuration
    config := bson.M{
        "_id": "rs0",
        "members": []bson.M{
            {"_id": 0, "host": "mongo1:27017"},
            {"_id": 1, "host": "mongo2:27017"},
            {"_id": 2, "host": "mongo3:27017"},
        },
    }

    result := client.Database("admin").RunCommand(ctx, bson.M{"replSetInitiate": config})
    return result.Err()
}

### 10.4 Sharded MongoDB Setup

func setUpShardedMongoDB(t *testing.T) (*mongoTestData, func()) {
    ctx := context.Background()

    network, err := testcontainers.GenericNetwork(ctx, testcontainers.GenericNetworkRequest{
        NetworkRequest: testcontainers.NetworkRequest{
            Name: "mongo-sharded-network",
        },
    })
    require.NoError(t, err)

    var containers []testcontainers.Container

    // Start Config Server
    configServer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
        ContainerRequest: testcontainers.ContainerRequest{
            Image:        "mongo:7.0.24",
            ExposedPorts: []string{"27017/tcp"},
            Networks:     []string{"mongo-sharded-network"},
            NetworkAliases: map[string][]string{
                "mongo-sharded-network": {"configserver"},
            },
            Env: map[string]string{
                "MONGO_INITDB_ROOT_USERNAME": "root",
                "MONGO_INITDB_ROOT_PASSWORD": "password",
            },
            Cmd: []string{
                "mongod",
                "--configsvr",
                "--replSet", "configReplSet",
                "--bind_ip_all",
                "--port", "27017",
            },
            WaitingFor: wait.ForLog("Waiting for connections").WithStartupTimeout(60 * time.Second),
        },
        Started: true,
    })
    require.NoError(t, err)
    containers = append(containers, configServer)

    // Start Shard Servers (2 shards)
    for i := 0; i < 2; i++ {
        shardServer, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
            ContainerRequest: testcontainers.ContainerRequest{
                Image:        "mongo:7.0.24",
                ExposedPorts: []string{"27017/tcp"},
                Networks:     []string{"mongo-sharded-network"},
                NetworkAliases: map[string][]string{
                    "mongo-sharded-network": {fmt.Sprintf("shard%d", i+1)},
                },
                Env: map[string]string{
                    "MONGO_INITDB_ROOT_USERNAME": "root",
                    "MONGO_INITDB_ROOT_PASSWORD": "password",
                },
                Cmd: []string{
                    "mongod",
                    "--shardsvr",
                    "--replSet", fmt.Sprintf("shard%dReplSet", i+1),
                    "--bind_ip_all",
                    "--port", "27017",
                },
                WaitingFor: wait.ForLog("Waiting for connections").WithStartupTimeout(60 * time.Second),
            },
            Started: true,
        })
        require.NoError(t, err)
        containers = append(containers, shardServer)
    }

    // Start Mongos Router
    mongos, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
        ContainerRequest: testcontainers.ContainerRequest{
            Image:        "mongo:7.0.24",
            ExposedPorts: []string{"27017/tcp"},
            Networks:     []string{"mongo-sharded-network"},
            NetworkAliases: map[string][]string{
                "mongo-sharded-network": {"mongos"},
            },
            Env: map[string]string{
                "MONGO_INITDB_ROOT_USERNAME": "root",
                "MONGO_INITDB_ROOT_PASSWORD": "password",
            },
            Cmd: []string{
                "mongos",
                "--configdb", "configReplSet/configserver:27017",
                "--bind_ip_all",
                "--port", "27017",
            },
            WaitingFor: wait.ForLog("Waiting for connections").WithStartupTimeout(60 * time.Second),
        },
        Started: true,
    })
    require.NoError(t, err)
    containers = append(containers, mongos)

    // Initialize sharded cluster
    time.Sleep(10 * time.Second)
    err = initializeShardedCluster(ctx, mongos)
    require.NoError(t, err)

    host, err := mongos.Host(ctx)
    require.NoError(t, err)
    port, err := mongos.MappedPort(ctx, "27017")
    require.NoError(t, err)

    cfg := config.MongoDB{
        Hosts:          []string{fmt.Sprintf("%s:%s", host, port.Port())},
        DatabaseName:   "temporal_test_sharded",
        User:           "root",
        Password:       "password",
        MaxConns:       20,
        ConnectTimeout: 20 * time.Second,
        // No ReplicaSet for sharded (connected via mongos)
    }

    factory, err := mongodb.NewFactory(cfg, log.NewTestLogger())
    require.NoError(t, err)

    tearDown := func() {
        factory.Close()
        for _, container := range containers {
            container.Terminate(ctx)
        }
        network.Remove(ctx)
    }

    return &mongoTestData{
        Factory: factory,
        Logger:  log.NewTestLogger(),
        Config:  cfg,
        Cleanup: tearDown,
    }, tearDown
}

### 10.5 Transaction-Specific Tests

func runTransactionTests(t *testing.T, testData *mongoTestData) {
    // Test atomic workflow execution creation
    executionStore, err := testData.Factory.NewExecutionStore()
    require.NoError(t, err)

    // Test that failure in history creation rolls back execution creation
    // This should only work with transactions enabled
    t.Run("AtomicWorkflowCreation", func(t *testing.T) {
        // Implementation of transaction rollback test
    })
}

func runShardedTests(t *testing.T, testData *mongoTestData) {
    // Test shard key distribution
    // Test cross-shard queries
    // Test distributed transactions
    t.Run("CrossShardOperations", func(t *testing.T) {
        // Implementation of cross-shard test
    })
}
```

---

## 11. Performance Considerations

### 11.1 Transaction Support

MongoDB supports multi-document ACID transactions with replica sets:

```go
func (s *ShardStore) UpdateShardWithTransaction(ctx context.Context, ...) error {
    session, err := s.client.StartSession()
    if err != nil {
        return err
    }
    defer session.EndSession(ctx)

    _, err = session.WithTransaction(ctx, func(ctx context.Context) (interface{}, error) {
        // Perform multiple operations atomically
        return nil, nil
    })
    return err
}
```

### 11.2 Optimistic Concurrency

Use `findOneAndUpdate` with filter conditions:

```go
filter := bson.M{
    "_id":      request.ShardID,
    "range_id": request.PreviousRangeID, // Optimistic lock
}
update := bson.M{"$set": bson.M{...}}
opts := options.FindOneAndUpdate().SetReturnDocument(options.After)
```

### 11.3 Index Strategy

- **Compound indexes** for multi-field queries
- **Partial indexes** to reduce size
- **TTL indexes** for automatic cleanup
- **Text indexes** for search functionality

### 11.4 Connection Pooling

```go
clientOpts := options.Client().
    SetMaxPoolSize(100).
    SetMinPoolSize(10).
    SetMaxConnIdleTime(30 * time.Second).
    SetHeartbeatInterval(10 * time.Second)
```

### 11.5 Read Preferences

- **Primary**: Consistency over performance
- **Secondary**: Performance over consistency (eventual consistency)
- **PrimaryPreferred**: Primary with secondary fallback

### 11.6 Write Concerns

- **Majority**: Wait for majority acknowledgment (safer)
- **W1**: Wait for primary acknowledgment (faster)
- **Journaled**: Wait for journal sync (durability)

MongoDB plugin provides a complete persistence solution for Temporal Server with ACID transactions, flexible schema, and rich querying capabilities.

---

## 12. Metrics and Monitoring

- **Prometheus metrics**: `persistence_mongo_transactions_started`, `persistence_mongo_transactions_failed`, `persistence_mongo_sessions_in_progress`, and `persistence_mongo_max_pool_size` are emitted by the Mongo persistence layer. Use these counters and gauges to track transaction volume, failures, and pool utilization.
- **Grafana dashboard**: `develop/docker-compose/grafana/provisioning/temporalio-dashboards/temporal-mongodb.json` is provisioned automatically when you run `make start-dependencies`. The dashboard visualizes transaction rates, session usage, and pool capacity.
- **Alerting**: Prometheus rule files (`develop/docker-compose/prometheus-darwin/mongodb-alerts.yml` and `develop/docker-compose/prometheus-linux/mongodb-alerts.yml`) raise alerts for sustained transaction failures and high connection pool utilization. Adjust thresholds to match your production expectations.

## 13. Future Work

- **High-load validation**: Full MongoDB persistence load testing is deferred until after functional development milestones land. Track this in the release backlog and schedule stress/chaos runs before GA sign-off.
