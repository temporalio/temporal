# Temporal Persistence Plugin Development Guide

This guide covers the architecture and implementation patterns for developing new persistence plugins for Temporal Server.

---

## Table of Contents

1. [General Architecture](#1-general-architecture)
2. [DataStoreFactory Interface](#2-datastorefactory-interface)
3. [Store Interfaces](#3-store-interfaces)
4. [SQL Plugin Architecture](#4-sql-plugin-architecture)
5. [NoSQL Plugin Architecture](#5-nosql-plugin-architecture)
6. [Configuration Integration](#6-configuration-integration)
7. [Schema Management](#7-schema-management)
8. [Testing](#8-testing)
9. [Implementation Checklist](#9-implementation-checklist)
10. [Development Roadmap](#10-development-roadmap)

---

## 1. General Architecture

Temporal's persistence layer uses a three-tier abstraction:

```text
┌─────────────────────────────────────────────────────────────┐
│                    Services Layer                           │
│              (Frontend, History, Matching)                  │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                    Manager Layer                            │
│              (persistence.Manager)                          │
│         Handles serialization, metrics, retries             │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                     Store Layer                             │
│    (ShardStore, ExecutionStore, TaskStore, etc.)            │
│         Plugin implementations live here                    │
└───────────────────────────┬─────────────────────────────────┘
                            │
┌───────────────────────────▼─────────────────────────────────┐
│                    Database Layer                           │
│           (MySQL, PostgreSQL, Cassandra, etc.)              │
└─────────────────────────────────────────────────────────────┘
```

### Plugin Implementation Approaches

| Approach              | Used By                   | Key Characteristics                                                                           |
| --------------------- | ------------------------- | --------------------------------------------------------------------------------------------- |
| **SQL Plugin System** | MySQL, PostgreSQL, SQLite | Plugin registration via `sql.RegisterPlugin()`, shared SQL factory, database-specific drivers |
| **Direct Factory**    | Cassandra                 | Direct `DataStoreFactory` implementation, no plugin registry                                  |

---

## 2. DataStoreFactory Interface

Every persistence plugin must implement the `DataStoreFactory` interface defined in `common/persistence/persistence_interface.go`:

```go
type DataStoreFactory interface {
    Close()
    NewShardStore() (ShardStore, error)
    NewExecutionStore() (ExecutionStore, error)
    NewTaskStore() (TaskStore, error)
    NewFairTaskStore() (TaskStore, error)  // Task store with fairness enabled
    NewMetadataStore() (MetadataStore, error)
    NewClusterMetadataStore() (ClusterMetadataStore, error)
    NewQueue(queueType QueueType) (Queue, error)
    NewQueueV2() (QueueV2, error)
    NewNexusEndpointStore() (NexusEndpointStore, error)
}
```

### Factory Selection

Factory selection is handled in `common/persistence/client/fx.go`:

```go
func DataStoreFactoryProvider(...) DataStoreFactory {
    switch {
    case ds.SQL != nil:
        // SQL plugin path
    case ds.Cassandra != nil:
        // Cassandra factory
    case ds.CustomDataStoreConfig != nil:
        // Custom implementation
    // Add new database type here
    }
}
```

---

## 3. Store Interfaces

### 3.1 ShardStore

Manages shard ownership and range IDs for partitioning.

```go
type ShardStore interface {
    GetOrCreateShard(ctx context.Context, request *InternalGetOrCreateShardRequest) (*InternalGetOrCreateShardResponse, error)
    UpdateShard(ctx context.Context, request *InternalUpdateShardRequest) error
    AssertShardOwnership(ctx context.Context, request *AssertShardOwnershipRequest) error
}
```

**Key Responsibilities:**

- Shard ownership management
- Optimistic concurrency via range ID
- Cluster-aware shard assignment

### 3.2 ExecutionStore

The most complex store—manages workflow execution state.

```go
type ExecutionStore interface {
    CreateWorkflowExecution(ctx context.Context, request *InternalCreateWorkflowExecutionRequest) (*InternalCreateWorkflowExecutionResponse, error)
    UpdateWorkflowExecution(ctx context.Context, request *InternalUpdateWorkflowExecutionRequest) error
    DeleteWorkflowExecution(ctx context.Context, request *DeleteWorkflowExecutionRequest) error
    GetWorkflowExecution(ctx context.Context, request *GetWorkflowExecutionRequest) (*InternalGetWorkflowExecutionResponse, error)
    // ... many more methods for tasks, history, etc.
}
```

**Key Responsibilities:**

- Workflow state (mutable state)
- History events
- Transfer tasks, timer tasks, replication tasks
- Immediate tasks

### 3.3 TaskStore

Manages task queues for workflow task distribution.

```go
type TaskStore interface {
    CreateTasks(ctx context.Context, request *InternalCreateTasksRequest) (*CreateTasksResponse, error)
    GetTasks(ctx context.Context, request *GetTasksRequest) (*InternalGetTasksResponse, error)
    CompleteTask(ctx context.Context, request *CompleteTaskRequest) error
    // ...
}
```

### 3.4 MetadataStore

Stores namespace definitions and metadata.

```go
type MetadataStore interface {
    CreateNamespace(ctx context.Context, request *InternalCreateNamespaceRequest) (*CreateNamespaceResponse, error)
    GetNamespace(ctx context.Context, request *GetNamespaceRequest) (*InternalGetNamespaceResponse, error)
    UpdateNamespace(ctx context.Context, request *InternalUpdateNamespaceRequest) error
    // ...
}
```

### 3.5 ClusterMetadataStore

Manages multi-cluster configuration and cluster membership.

```go
type ClusterMetadataStore interface {
    GetClusterMetadata(ctx context.Context, request *InternalGetClusterMetadataRequest) (*InternalGetClusterMetadataResponse, error)
    SaveClusterMetadata(ctx context.Context, request *InternalSaveClusterMetadataRequest) (bool, error)
    ListClusterMetadata(ctx context.Context, request *InternalListClusterMetadataRequest) (*InternalListClusterMetadataResponse, error)
    // ...
}
```

### 3.6 Queue and QueueV2

Message queue for internal communication and DLQ support.

```go
type Queue interface {
    EnqueueMessage(ctx context.Context, blob *commonpb.DataBlob) error
    ReadMessages(ctx context.Context, lastMessageID int64, maxCount int) ([]*QueueMessage, error)
    DeleteMessagesBefore(ctx context.Context, messageID int64) error
    // ...
}

type QueueV2 interface {
    EnqueueMessage(ctx context.Context, request *InternalEnqueueMessageRequest) (*InternalEnqueueMessageResponse, error)
    ReadMessages(ctx context.Context, request *InternalReadMessagesRequest) (*InternalReadMessagesResponse, error)
    CreateQueue(ctx context.Context, request *InternalCreateQueueRequest) (*InternalCreateQueueResponse, error)
    // ...
}
```

### 3.7 NexusEndpointStore

Manages Nexus service endpoints.

```go
type NexusEndpointStore interface {
    GetNexusEndpoint(ctx context.Context, request *GetNexusEndpointRequest) (*InternalNexusEndpoint, error)
    ListNexusEndpoints(ctx context.Context, request *ListNexusEndpointsRequest) (*InternalListNexusEndpointsResponse, error)
    CreateOrUpdateNexusEndpoint(ctx context.Context, request *InternalCreateOrUpdateNexusEndpointRequest) error
    DeleteNexusEndpoint(ctx context.Context, request *DeleteNexusEndpointRequest) error
}
```

---

## 4. SQL Plugin Architecture

SQL databases use a plugin registration system that shares a common factory implementation.

### 4.1 Plugin Registration

Each SQL database registers itself via `init()`:

```go
// common/persistence/sql/sqlplugin/mysql/plugin.go
func init() {
    sql.RegisterPlugin(PluginName, &plugin{})
}
```

### 4.2 Plugin Interface

```go
// common/persistence/sql/sqlplugin/interfaces.go
type Plugin interface {
    CreateDB(cfg *config.SQL, r resolver.ServiceResolver, dc *dynconfig.Collection) (DB, error)
    CreateAdminDB(cfg *config.SQL, r resolver.ServiceResolver, dc *dynconfig.Collection) (AdminDB, error)
}
```

### 4.3 DB Interface

The `DB` interface defines SQL-specific operations:

```go
type DB interface {
    TableCRUD
    PluginName() string
    DbName() string
    IsDupEntryError(err error) bool
    Close() error
}

type TableCRUD interface {
    shardCRUD
    executionCRUD
    taskCRUD
    metadataCRUD
    clusterMetadataCRUD
    queueCRUD
    nexusEndpointsCRUD
}
```

### 4.4 Table CRUD Interfaces

Each store maps to specific CRUD operations:

```go
type shardCRUD interface {
    InsertIntoShards(ctx context.Context, row *ShardsRow) (sql.Result, error)
    UpdateShards(ctx context.Context, row *ShardsRow) (sql.Result, error)
    SelectFromShards(ctx context.Context, filter ShardsFilter) (*ShardsRow, error)
    ReadLockShards(ctx context.Context, filter ShardsFilter) (int64, error)
    WriteLockShards(ctx context.Context, filter ShardsFilter) (int64, error)
}
```

### 4.5 Directory Structure for SQL Plugin

```text
common/persistence/sql/sqlplugin/<database>/
├── plugin.go           # Plugin registration and factory
├── db.go               # DB interface implementation
├── shard.go            # shardCRUD implementation
├── execution.go        # executionCRUD implementation
├── task.go             # taskCRUD implementation
├── metadata.go         # metadataCRUD implementation
├── cluster_metadata.go # clusterMetadataCRUD implementation
├── queue.go            # queueCRUD implementation
├── nexus_endpoints.go  # nexusEndpointsCRUD implementation
├── typeconv.go         # Type conversion utilities
└── tests/              # Database-specific tests
```

### 4.6 Implementing a New SQL Plugin

1. Create directory under `common/persistence/sql/sqlplugin/<database>/`
2. Implement `Plugin` interface with driver initialization
3. Implement `DB` interface and all `TableCRUD` methods
4. Register plugin in `init()` function
5. Create schema files under `schema/<database>/`
6. Add configuration support in `common/config/persistence.go`

---

## 5. NoSQL Plugin Architecture

NoSQL databases implement `DataStoreFactory` directly without the plugin registry.

### 5.1 Direct Factory Pattern

```go
// common/persistence/cassandra/factory.go
func NewFactory(cfg config.Cassandra, ...) *Factory {
    return &Factory{
        cfg:      cfg,
        clusterSession: clusterSession,
        // ...
    }
}

func (f *Factory) NewShardStore() (persistence.ShardStore, error) {
    return NewShardStore(f.clusterSession, f.cfg, f.logger)
}
```

### 5.2 Client Abstraction Layer

NoSQL plugins typically define a client abstraction:

```go
// Example client interface for a NoSQL database
type Client interface {
    Connect(ctx context.Context) error
    Close() error
    Database(name string) Database
}

type Database interface {
    Collection(name string) Collection
}

type Collection interface {
    FindOne(ctx context.Context, filter interface{}) SingleResult
    Find(ctx context.Context, filter interface{}) (Cursor, error)
    InsertOne(ctx context.Context, document interface{}) error
    UpdateOne(ctx context.Context, filter interface{}, update interface{}) error
    DeleteOne(ctx context.Context, filter interface{}) error
}
```

### 5.3 Directory Structure for NoSQL Plugin

```text
common/persistence/<database>/
├── client/
│   ├── interfaces.go      # Client interface definitions
│   ├── client.go          # Client implementation
│   └── session.go         # Session/transaction support
├── factory.go             # DataStoreFactory implementation
├── shard_store.go         # ShardStore implementation
├── execution_store.go     # ExecutionStore implementation
├── task_store.go          # TaskStore implementation
├── metadata_store.go      # MetadataStore implementation
├── cluster_metadata_store.go
├── queue.go               # Queue implementations
├── nexus_endpoint_store.go
├── errors.go              # Error handling utilities
└── common.go              # Shared constants
```

### 5.4 Key Implementation Considerations

#### Error Handling

Each database has unique error types. Create mapping functions:

```go
func convertError(err error) error {
    if err == nil {
        return nil
    }
    switch {
    case isDuplicateKeyError(err):
        return &persistence.ConditionFailedError{Msg: err.Error()}
    case isNotFoundError(err):
        return serviceerror.NewNotFound(err.Error())
    default:
        return serviceerror.NewUnavailable(err.Error())
    }
}
```

#### Optimistic Concurrency

Use conditional updates for consistency:

```go
// Example: Update with version check
func (s *ShardStore) UpdateShard(ctx context.Context, request *persistence.InternalUpdateShardRequest) error {
    // Filter includes current version/range_id for optimistic locking
    filter := buildFilterWithVersion(request.ShardID, request.PreviousRangeID)
    update := buildUpdateDocument(request)

    result, err := s.collection.UpdateOne(ctx, filter, update)
    if result.MatchedCount == 0 {
        return &persistence.ShardOwnershipLostError{...}
    }
    return err
}
```

#### Transaction Support

For databases supporting transactions:

```go
func (s *Store) ExecuteInTransaction(ctx context.Context, fn func(ctx context.Context) error) error {
    session, err := s.client.StartSession(ctx)
    if err != nil {
        return err
    }
    defer session.EndSession(ctx)

    return session.WithTransaction(ctx, func(ctx context.Context) error {
        return fn(ctx)
    })
}
```

---

## 6. Configuration Integration

### 6.1 Define Configuration Structure

Add your database configuration to `common/config/persistence.go`:

```go
// Example: New database configuration
type NewDatabase struct {
    Hosts           []string      `yaml:"hosts"`
    Port            int           `yaml:"port"`
    DatabaseName    string        `yaml:"databaseName"`
    User            string        `yaml:"user"`
    Password        string        `yaml:"password"`
    MaxConns        int           `yaml:"maxConns"`
    ConnectTimeout  time.Duration `yaml:"connectTimeout"`
    TLS             *TLS          `yaml:"tls"`
    // Database-specific options...
}

// Add to DataStore struct
type DataStore struct {
    SQL                   *SQL                   `yaml:"sql"`
    Cassandra             *Cassandra             `yaml:"cassandra"`
    NewDatabase           *NewDatabase           `yaml:"newDatabase"`  // Add here
    Elasticsearch         *Elasticsearch         `yaml:"elasticsearch"`
    CustomDataStoreConfig *CustomDatastoreConfig `yaml:"customDataStore"`
}
```

### 6.2 Update Factory Provider

Add your database to `common/persistence/client/fx.go`:

```go
func DataStoreFactoryProvider(...) DataStoreFactory {
    switch {
    case ds.SQL != nil:
        return sql.NewFactory(...)
    case ds.Cassandra != nil:
        return cassandra.NewFactory(...)
    case ds.NewDatabase != nil:          // Add this case
        return newdatabase.NewFactory(...)
    case ds.CustomDataStoreConfig != nil:
        return abstractDataStoreFactory.NewFactory(...)
    }
    // ...
}
```

### 6.3 Example Configuration File

```yaml
persistence:
  defaultStore: default-store
  numHistoryShards: 4
  datastores:
    default-store:
      newDatabase:
        hosts:
          - "localhost:27017"
        databaseName: "temporal"
        user: "temporal"
        password: "password"
        maxConns: 100
        connectTimeout: 10s
        tls:
          enabled: false
```

---

## 7. Schema Management

### 7.1 Schema Directory Structure

```text
schema/<database>/temporal/
├── versioned/
│   ├── v1.0/
│   │   └── schema.sql (or schema.js, schema.cql, etc.)
│   ├── v1.1/
│   │   └── schema.sql
│   └── ...
└── setup.sh (optional setup script)
```

### 7.2 Core Tables/Collections

At minimum, implement schema for:

| Store             | Purpose                            |
| ----------------- | ---------------------------------- |
| Shards            | Shard ownership and range IDs      |
| Executions        | Workflow execution state           |
| ExecutionTasks    | Transfer, timer, replication tasks |
| History           | Workflow history events            |
| Tasks             | Task queue items                   |
| TaskQueues        | Task queue metadata                |
| TaskQueueUserData | Task queue user data               |
| Namespaces        | Namespace definitions              |
| ClusterMetadata   | Cluster configuration              |
| Queues            | Internal message queues            |
| QueueMessages     | Queue message content              |
| NexusEndpoints    | Nexus service endpoints            |

### 7.3 Schema Requirements

- **Unique Constraints**: Ensure proper uniqueness for shard IDs, workflow IDs, etc.
- **Indexes**: Create indexes for efficient queries (shard_id, namespace_id, workflow_id, task_id, etc.)
- **Version Column**: Include schema version tracking for migrations

---

## 8. Testing

### 8.1 Reuse Existing Test Suites

Temporal provides test suites in `common/persistence/tests/` that can be reused:

```go
// common/persistence/tests/<database>_test.go
func TestShardStoreSuite(t *testing.T) {
    testData, tearDown := setUpTest(t)
    defer tearDown()

    shardStore, err := testData.Factory.NewShardStore()
    require.NoError(t, err)

    s := NewShardSuite(t, shardStore, serialization.NewSerializer(), testData.Logger)
    suite.Run(t, s)
}

func TestExecutionStoreSuite(t *testing.T) {
    testData, tearDown := setUpTest(t)
    defer tearDown()

    executionStore, err := testData.Factory.NewExecutionStore()
    require.NoError(t, err)

    s := NewExecutionSuite(t, executionStore, serialization.NewSerializer(), testData.Logger)
    suite.Run(t, s)
}
```

### 8.2 Test Setup Pattern

```go
type testData struct {
    Factory persistence.DataStoreFactory
    Logger  log.Logger
}

func setUpTest(t *testing.T) (*testData, func()) {
    logger := log.NewTestLogger()

    // Initialize database connection
    // Use testcontainers or embedded database for isolation

    factory := newdatabase.NewFactory(cfg, logger)

    tearDown := func() {
        factory.Close()
        // Cleanup database
    }

    return &testData{
        Factory: factory,
        Logger:  logger,
    }, tearDown
}
```

### 8.3 Testing Requirements

1. **Unit Tests**: Mock database interactions
2. **Integration Tests**: Use testcontainers or embedded database
3. **Persistence Test Suites**: Reuse existing test suites in `common/persistence/tests/`

---

## 9. Implementation Checklist

### 9.1 Pre-Implementation

- [ ] Review existing implementations (Cassandra for NoSQL, MySQL/PostgreSQL for SQL)
- [ ] Understand all store interfaces in `persistence_interface.go`
- [ ] Understand data structures in `data_interfaces.go`
- [ ] Plan client abstraction layer
- [ ] Plan error handling strategy

### 9.2 Core Implementation

| Component            | Interface/Location                 | Priority |
| -------------------- | ---------------------------------- | -------- |
| Configuration        | `common/config/persistence.go`     | Critical |
| Factory              | `DataStoreFactory`                 | Critical |
| ShardStore           | `persistence.ShardStore`           | Critical |
| MetadataStore        | `persistence.MetadataStore`        | Critical |
| ClusterMetadataStore | `persistence.ClusterMetadataStore` | High     |
| ExecutionStore       | `persistence.ExecutionStore`       | Critical |
| TaskStore            | `persistence.TaskStore`            | High     |
| Queue                | `persistence.Queue`                | Medium   |
| QueueV2              | `persistence.QueueV2`              | Medium   |
| NexusEndpointStore   | `persistence.NexusEndpointStore`   | Medium   |

### 9.3 Schema Files

- [ ] Create schema directory under `schema/<database>/temporal/`
- [ ] Implement versioned schema files
- [ ] Create indexes for efficient queries

### 9.4 Integration

- [ ] Add factory case to `common/persistence/client/fx.go`
- [ ] Add configuration struct to `common/config/persistence.go`

### 9.5 Testing

- [ ] Unit tests for each store
- [ ] Integration tests with test containers
- [ ] Reuse persistence test suites

---

## 10. Development Roadmap

### Phase 1: Foundation (Week 1-2)

#### Step 1.1: Project Setup

Create directory structure:

```text
common/persistence/<database>/
├── client/
│   ├── interfaces.go
│   ├── client.go
│   └── session.go
├── factory.go
├── errors.go
├── common.go
└── README.md
```

#### Step 1.2: Client Layer

1. Define client interfaces
2. Implement client wrapper with connection pooling
3. Implement error conversion utilities
4. Add TLS/SSL support
5. Add metrics integration

#### Step 1.3: Factory Implementation

1. Implement `NewFactory()` function
2. Implement all `DataStoreFactory` interface methods
3. Add connection lifecycle management

### Phase 2: Core Stores (Week 3-5)

| Priority | Store                | Notes                                   |
| -------- | -------------------- | --------------------------------------- |
| 1        | ShardStore           | Simplest store, validates client layer  |
| 2        | MetadataStore        | Required for server startup             |
| 3        | ClusterMetadataStore | Multi-cluster support                   |
| 4        | ExecutionStore       | Most complex, break into sub-components |

### Phase 3: Task Management (Week 6-7)

| Priority | Store     | Notes                    |
| -------- | --------- | ------------------------ |
| 1        | TaskStore | Task queue management    |
| 2        | Queue     | Message queue operations |
| 3        | QueueV2   | Enhanced queue with DLQ  |

### Phase 4: Additional Features (Week 8-9)

| Priority | Store              | Notes                           |
| -------- | ------------------ | ------------------------------- |
| 1        | NexusEndpointStore | Nexus service endpoints         |
| 2        | Visibility Store   | Optional if using Elasticsearch |

### Phase 5: Testing (Week 10-12)

1. Unit tests for each store
2. Integration tests with test containers
3. Performance benchmarks
4. Stress testing

### Phase 6: Documentation and Release (Week 13-14)

1. Configuration documentation
2. Deployment guides
3. Schema migration procedures
4. Performance tuning guide

---

## Quick Reference

### Key Files

| File                                          | Purpose                  |
| --------------------------------------------- | ------------------------ |
| `common/persistence/persistence_interface.go` | Store interfaces         |
| `common/persistence/data_interfaces.go`       | Request/Response types   |
| `common/persistence/client/fx.go`             | Factory selection        |
| `common/config/persistence.go`                | Configuration structures |
| `common/persistence/tests/`                   | Test suites              |

### Reference Implementations

| Type                    | Location                                       |
| ----------------------- | ---------------------------------------------- |
| SQL Plugin (MySQL)      | `common/persistence/sql/sqlplugin/mysql/`      |
| SQL Plugin (PostgreSQL) | `common/persistence/sql/sqlplugin/postgresql/` |
| NoSQL (Cassandra)       | `common/persistence/cassandra/`                |

### Schema Locations

| Database   | Path                              |
| ---------- | --------------------------------- |
| Cassandra  | `schema/cassandra/temporal/`      |
| MySQL      | `schema/mysql/v8/temporal/`       |
| PostgreSQL | `schema/postgresql/v12/temporal/` |
