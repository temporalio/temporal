# Temporal Persistence Layer: SQL vs MongoDB Comparison

This document provides a comprehensive comparison of Temporal's SQL-based and MongoDB-based persistence implementations, serving as both an introduction to Temporal's persistence layer and a detailed mapping between the two storage backends.

## Table of Contents

- [Temporal Persistence Layer: SQL vs MongoDB Comparison](#temporal-persistence-layer-sql-vs-mongodb-comparison)
  - [Table of Contents](#table-of-contents)
  - [Introduction to Temporal Persistence](#introduction-to-temporal-persistence)
  - [Architecture Overview](#architecture-overview)
  - [Data Store Categories](#data-store-categories)
  - [Detailed Schema Comparison](#detailed-schema-comparison)
    - [1. Shard Store](#1-shard-store)
      - [SQL Table: `shards`](#sql-table-shards)
      - [MongoDB Collection: `shards`](#mongodb-collection-shards)
    - [2. Namespace Store (Metadata)](#2-namespace-store-metadata)
      - [SQL Tables](#sql-tables)
      - [MongoDB Collections](#mongodb-collections)
    - [3. Execution Store](#3-execution-store)
      - [SQL Tables (Execution-related)](#sql-tables-execution-related)
      - [Additional SQL Tables for Execution](#additional-sql-tables-for-execution)
      - [MongoDB Collections (Execution-related)](#mongodb-collections-execution-related)
    - [4. Task Store](#4-task-store)
      - [SQL Tables](#sql-tables-1)
      - [MongoDB Collections](#mongodb-collections-1)
    - [5. Queue Store](#5-queue-store)
      - [SQL Tables](#sql-tables-2)
      - [MongoDB Collections](#mongodb-collections-2)
    - [6. Visibility Store](#6-visibility-store)
      - [SQL Table: `executions_visibility`](#sql-table-executions_visibility)
      - [SQL Table: `custom_search_attributes`](#sql-table-custom_search_attributes)
      - [MongoDB Collection: `visibility_executions`](#mongodb-collection-visibility_executions)
    - [7. Cluster Metadata Store](#7-cluster-metadata-store)
      - [SQL Tables](#sql-tables-3)
      - [MongoDB Collections](#mongodb-collections-3)
    - [8. Nexus Endpoint Store](#8-nexus-endpoint-store)
      - [SQL Tables](#sql-tables-4)
      - [MongoDB Collections](#mongodb-collections-4)
  - [Design Decisions: SQL to MongoDB Adaptation](#design-decisions-sql-to-mongodb-adaptation)
    - [From Foreign Keys to Document Embedding (Denormalization)](#from-foreign-keys-to-document-embedding-denormalization)
      - [The SQL Model: Normalized with Implicit Foreign Keys](#the-sql-model-normalized-with-implicit-foreign-keys)
      - [The MongoDB Model: Denormalized with Embedded Arrays](#the-mongodb-model-denormalized-with-embedded-arrays)
    - [Impact Analysis: Denormalization Trade-offs](#impact-analysis-denormalization-trade-offs)
      - [✅ Advantages](#-advantages)
      - [❌ Disadvantages](#-disadvantages)
    - [Specific Challenges and Solutions](#specific-challenges-and-solutions)
      - [Challenge 1: Workflow Execution Updates](#challenge-1-workflow-execution-updates)
      - [Challenge 2: Optimistic Concurrency Control](#challenge-2-optimistic-concurrency-control)
      - [Challenge 3: History Event Pagination](#challenge-3-history-event-pagination)
      - [Challenge 4: Visibility Store Search Attributes](#challenge-4-visibility-store-search-attributes)
      - [Challenge 5: CHASM Visibility (Standalone Activities)](#challenge-5-chasm-visibility-standalone-activities)
    - [Foreign Key Behavior Comparison](#foreign-key-behavior-comparison)
    - [When NOT to Denormalize](#when-not-to-denormalize)
    - [Performance Implications](#performance-implications)
      - [Read Patterns](#read-patterns)
      - [Write Patterns](#write-patterns)
    - [Migration Considerations](#migration-considerations)
  - [Key Design Differences](#key-design-differences)
  - [Transaction Support](#transaction-support)
    - [SQL](#sql)
    - [MongoDB](#mongodb)
  - [Summary](#summary)

---

## Introduction to Temporal Persistence

Temporal Server requires a durable persistence layer to store workflow execution state, task queues, visibility data, and cluster metadata. The persistence layer is designed to be pluggable, supporting multiple backends:

- **SQL Databases**: MySQL, PostgreSQL, SQLite
- **NoSQL Databases**: Cassandra, MongoDB

This document focuses on comparing SQL (MySQL) and MongoDB implementations.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────┐
│                      Temporal Services                          │
│  (Frontend, History, Matching, Worker)                          │
└───────────────────────────┬─────────────────────────────────────┘
                            │
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│               Persistence Interface Layer                       │
│  (common/persistence/*.go - Abstract interfaces)                │
└───────────────────────────┬─────────────────────────────────────┘
                            │
            ┌───────────────┼───────────────┐
            ▼               ▼               ▼
    ┌───────────┐   ┌───────────┐   ┌───────────┐
    │    SQL    │   │  MongoDB  │   │ Cassandra │
    │   Store   │   │   Store   │   │   Store   │
    └───────────┘   └───────────┘   └───────────┘
```

## Data Store Categories

| Store Type               | Purpose                                    | SQL Implementation                            | MongoDB Implementation                               |
| ------------------------ | ------------------------------------------ | --------------------------------------------- | ---------------------------------------------------- |
| **ShardStore**           | Manages shard ownership and range IDs      | `shards` table                                | `shards` collection                                  |
| **MetadataStore**        | Namespace definitions and metadata         | `namespaces`, `namespace_metadata` tables     | `namespaces`, `namespace_metadata` collections       |
| **ExecutionStore**       | Workflow execution state, history          | Multiple tables (see below)                   | Multiple collections (see below)                     |
| **TaskStore**            | Task queue management                      | `tasks`, `task_queues`, etc.                  | `tasks`, `task_queues` collections                   |
| **QueueStore**           | Internal message queues (DLQ, replication) | `queue`, `queue_metadata` tables              | `queue_messages`, `queue_metadata` collections       |
| **VisibilityStore**      | Workflow search and listing                | `executions_visibility` table                 | `visibility_executions` collection                   |
| **ClusterMetadataStore** | Multi-cluster configuration                | `cluster_metadata_info`, `cluster_membership` | `cluster_metadata`, `cluster_membership` collections |
| **NexusEndpointStore**   | Nexus service endpoints                    | `nexus_endpoints` table                       | `nexus_endpoints` collection                         |

---

## Detailed Schema Comparison

### 1. Shard Store

Shards are used to partition workflow execution data for horizontal scalability. Each shard is owned by a single History service instance.

#### SQL Table: `shards`

| Column          | Type        | Description                                              |
| --------------- | ----------- | -------------------------------------------------------- |
| `shard_id`      | INT         | **Primary Key** - Shard identifier (0 to N-1)            |
| `range_id`      | BIGINT      | Monotonically increasing ID for ownership verification   |
| `data`          | MEDIUMBLOB  | Protobuf-encoded shard info (`persistencespb.ShardInfo`) |
| `data_encoding` | VARCHAR(16) | Encoding type (typically "proto3")                       |

#### MongoDB Collection: `shards`

```javascript
{
  _id: Int32,           // shard_id (same as SQL)
  range_id: Int64,      // ownership verification
  data: Binary,         // protobuf blob
  data_encoding: String // "proto3"
}
```

**Key Difference**: MongoDB uses `_id` as the natural primary key, eliminating the need for a separate index.

---

### 2. Namespace Store (Metadata)

Namespaces (formerly "domains") provide logical isolation for workflows.

#### SQL Tables

**`namespaces`**

| Column                 | Type         | Description                                 |
| ---------------------- | ------------ | ------------------------------------------- |
| `partition_id`         | INT          | Partition key (always 54321)                |
| `id`                   | BINARY(16)   | **Primary Key (with partition_id)** - UUID  |
| `name`                 | VARCHAR(255) | **Unique** - Human-readable name            |
| `notification_version` | BIGINT       | Change notification version                 |
| `data`                 | MEDIUMBLOB   | Protobuf-encoded namespace data             |
| `data_encoding`        | VARCHAR(16)  | Encoding type                               |
| `is_global`            | TINYINT(1)   | Whether namespace is global (multi-cluster) |

**`namespace_metadata`**

| Column                 | Type   | Description                         |
| ---------------------- | ------ | ----------------------------------- |
| `partition_id`         | INT    | **Primary Key**                     |
| `notification_version` | BIGINT | Global notification version counter |

#### MongoDB Collections

**`namespaces`**

```javascript
{
  _id: String,              // namespace UUID
  name: String,             // unique index
  data: Binary,             // protobuf blob
  data_encoding: String,
  is_global: Boolean,
  notification_version: Int64,
  updated_at: Date
}
// Index: { name: 1 } UNIQUE
```

**`namespace_metadata`**

```javascript
{
  _id: "temporal-namespace-metadata",  // singleton document
  notification_version: Int64
}
```

**Key Differences**:

- SQL uses composite primary key (`partition_id`, `id`); MongoDB uses `_id` directly
- MongoDB adds `updated_at` timestamp for debugging
- MongoDB uses a singleton document pattern for metadata

---

### 3. Execution Store

The execution store is the heart of Temporal's persistence, storing workflow state, history events, and internal tasks.

#### SQL Tables (Execution-related)

**`executions`** - Mutable workflow state

| Column               | Type         | Description                    |
| -------------------- | ------------ | ------------------------------ |
| `shard_id`           | INT          | Shard identifier               |
| `namespace_id`       | BINARY(16)   | Namespace UUID                 |
| `workflow_id`        | VARCHAR(255) | Workflow ID                    |
| `run_id`             | BINARY(16)   | Run UUID                       |
| `next_event_id`      | BIGINT       | Next history event ID          |
| `last_write_version` | BIGINT       | Conflict resolution version    |
| `data`               | MEDIUMBLOB   | Protobuf execution info        |
| `data_encoding`      | VARCHAR(16)  | Encoding                       |
| `state`              | MEDIUMBLOB   | Protobuf execution state       |
| `state_encoding`     | VARCHAR(16)  | State encoding                 |
| `db_record_version`  | BIGINT       | Optimistic concurrency control |

**Primary Key**: `(shard_id, namespace_id, workflow_id, run_id)`

**`current_executions`** - Points to the current run for a workflow

| Column               | Type         | Description           |
| -------------------- | ------------ | --------------------- |
| `shard_id`           | INT          | Shard identifier      |
| `namespace_id`       | BINARY(16)   | Namespace UUID        |
| `workflow_id`        | VARCHAR(255) | Workflow ID           |
| `run_id`             | BINARY(16)   | Current run UUID      |
| `create_request_id`  | VARCHAR(255) | Deduplication ID      |
| `state`              | INT          | Workflow state enum   |
| `status`             | INT          | Workflow status enum  |
| `start_version`      | BIGINT       | Start version for CDC |
| `last_write_version` | BIGINT       | Latest write version  |

**Primary Key**: `(shard_id, namespace_id, workflow_id)`

**`history_node`** - History events storage

| Column          | Type        | Description                    |
| --------------- | ----------- | ------------------------------ |
| `shard_id`      | INT         | Shard identifier               |
| `tree_id`       | BINARY(16)  | History tree UUID              |
| `branch_id`     | BINARY(16)  | Branch UUID                    |
| `node_id`       | BIGINT      | Event batch starting ID        |
| `txn_id`        | BIGINT      | Transaction ID for ordering    |
| `prev_txn_id`   | BIGINT      | Previous transaction (linking) |
| `data`          | MEDIUMBLOB  | Serialized history events      |
| `data_encoding` | VARCHAR(16) | Encoding                       |

**Primary Key**: `(shard_id, tree_id, branch_id, node_id, txn_id)`

**`history_tree`** - Branch metadata

| Column          | Type        | Description       |
| --------------- | ----------- | ----------------- |
| `shard_id`      | INT         | Shard identifier  |
| `tree_id`       | BINARY(16)  | History tree UUID |
| `branch_id`     | BINARY(16)  | Branch UUID       |
| `data`          | MEDIUMBLOB  | Branch info       |
| `data_encoding` | VARCHAR(16) | Encoding          |

**Primary Key**: `(shard_id, tree_id, branch_id)`

#### Additional SQL Tables for Execution

| Table                       | Purpose                         |
| --------------------------- | ------------------------------- |
| `buffered_events`           | Events waiting to be applied    |
| `activity_info_maps`        | Active activities per execution |
| `timer_info_maps`           | Active timers per execution     |
| `child_execution_info_maps` | Child workflow info             |
| `request_cancel_info_maps`  | Pending cancellation requests   |
| `signal_info_maps`          | Pending external signals        |
| `signals_requested_sets`    | Signal deduplication            |
| `chasm_node_maps`           | CHASM state machine nodes       |

#### MongoDB Collections (Execution-related)

**`executions`**

```javascript
{
  _id: String,              // "{shard_id}:{namespace_id}:{workflow_id}:{run_id}"
  shard_id: Int32,
  namespace_id: String,
  workflow_id: String,
  run_id: String,
  next_event_id: Int64,
  db_record_version: Int64,
  last_write_version: Int64,

  // Embedded protobuf blobs
  execution_info: { data: Binary, encoding: String },
  execution_state: { data: Binary, encoding: String },

  // Embedded maps (denormalized from SQL's separate tables)
  activity_infos: [{ id: Int64, blob: { data: Binary, encoding: String } }],
  timer_infos: [{ key: String, blob: { data: Binary, encoding: String } }],
  child_execution_infos: [{ id: Int64, blob: { data: Binary, encoding: String } }],
  request_cancel_infos: [{ id: Int64, blob: { data: Binary, encoding: String } }],
  signal_infos: [{ id: Int64, blob: { data: Binary, encoding: String } }],
  signal_requested_ids: [String],
  chasm_nodes: [{ key: String, metadata: Binary, data: Binary }],

  buffered_events: [{ data: Binary, encoding: String }],
  checksum: { data: Binary, encoding: String }
}
```

**`current_executions`**

```javascript
{
  _id: String,              // "{shard_id}:{namespace_id}:{workflow_id}"
  shard_id: Int32,
  namespace_id: String,
  workflow_id: String,
  run_id: String,
  create_request_id: String,
  state: Int32,
  status: Int32,
  start_version: Int64,
  start_time: Date,
  last_write_version: Int64,
  execution_state: { data: Binary, encoding: String }
}
```

**`history_nodes`**

```javascript
{
  _id: String,              // "{shard_id}:{tree_id}:{branch_id}:{node_id}:{txn_id}"
  shard_id: Int32,
  tree_id: String,
  branch_id: String,
  node_id: Int64,
  txn_id: Int64,
  prev_txn_id: Int64,
  events: { data: Binary, encoding: String }
}
```

**`history_branches`**

```javascript
{
  _id: String,              // "{shard_id}:{tree_id}:{branch_id}"
  shard_id: Int32,
  tree_id: String,
  branch_id: String,
  info: String,             // optional info string
  tree_info: { data: Binary, encoding: String }
}
```

**Key Differences**:

- **Denormalization**: MongoDB embeds activity_infos, timer_infos, etc. as arrays within the execution document, while SQL uses separate tables with foreign key relationships
- **Composite ID**: MongoDB uses string-concatenated `_id` fields; SQL uses multi-column primary keys
- **Single Document Updates**: MongoDB can update execution state atomically with a single document update

---

### 4. Task Store

Task queues hold workflow and activity tasks waiting to be picked up by workers.

#### SQL Tables

**`task_queues`** / **`task_queues_v2`**

| Column          | Type           | Description              |
| --------------- | -------------- | ------------------------ |
| `range_hash`    | INT UNSIGNED   | Hash for sharding        |
| `task_queue_id` | VARBINARY(272) | Encoded queue identifier |
| `range_id`      | BIGINT         | Ownership verification   |
| `data`          | MEDIUMBLOB     | Queue metadata           |
| `data_encoding` | VARCHAR(16)    | Encoding                 |

**`tasks`** / **`tasks_v2`** (with fairness)

| Column          | Type           | Description                        |
| --------------- | -------------- | ---------------------------------- |
| `range_hash`    | INT UNSIGNED   | Hash for sharding                  |
| `task_queue_id` | VARBINARY(272) | Queue identifier                   |
| `pass`          | BIGINT         | Fairness scheduling pass (v2 only) |
| `task_id`       | BIGINT         | Task sequence ID                   |
| `data`          | MEDIUMBLOB     | Task data                          |
| `data_encoding` | VARCHAR(16)    | Encoding                           |

**`task_queue_user_data`** - Worker versioning data

| Column            | Type         | Description            |
| ----------------- | ------------ | ---------------------- |
| `namespace_id`    | BINARY(16)   | Namespace UUID         |
| `task_queue_name` | VARCHAR(255) | Queue name             |
| `data`            | MEDIUMBLOB   | User data              |
| `version`         | BIGINT       | Optimistic concurrency |

**`build_id_to_task_queue`** - Build ID mapping

| Column            | Type         | Description     |
| ----------------- | ------------ | --------------- |
| `namespace_id`    | BINARY(16)   | Namespace UUID  |
| `build_id`        | VARCHAR(255) | Worker build ID |
| `task_queue_name` | VARCHAR(255) | Queue name      |

#### MongoDB Collections

**`task_queues`**

```javascript
{
  _id: String,              // "{namespace_id}:{task_queue}:{task_type}:{subqueue}:v1" or "v2"
  namespace_id: String,
  task_queue: String,
  task_type: Int32,         // WORKFLOW_TASK, ACTIVITY_TASK, etc.
  subqueue: Int32,
  range_id: Int64,
  data: Binary,
  data_encoding: String,
  updated_at: Date
}
// Index: (namespace_id, task_queue, task_type, subqueue)
```

**`tasks`**

```javascript
{
  _id: String,              // "{namespace_id}:{task_queue}:{task_type}:{subqueue}:{task_pass}:{task_id}"
  namespace_id: String,
  task_queue: String,
  task_type: Int32,
  subqueue: Int32,
  task_pass: Int64,         // for fairness scheduling
  task_id: Int64,
  task: Binary,
  task_encoding: String,
  expiry_time: Date,        // TTL index for auto-deletion
  created_at: Date
}
// Index: (namespace_id, task_queue, task_type, subqueue, task_pass, task_id)
// TTL Index: (expiry_time) with expireAfterSeconds: 0
```

**`task_queue_user_data`**

```javascript
{
  _id: String,              // generated unique ID
  namespace_id: String,
  task_queue: String,
  version: Int64,
  data: Binary,
  data_encoding: String,
  updated_at: Date
}
// Index: (namespace_id, task_queue) UNIQUE
```

**`task_queue_build_id_map`**

```javascript
{
  _id: String,
  namespace_id: String,
  build_id: String,
  task_queue: String
}
// Index: (namespace_id, build_id, task_queue) UNIQUE
// Index: (namespace_id, build_id)
```

**Key Differences**:

- **TTL Index**: MongoDB uses native TTL indexes for task expiration; SQL requires manual cleanup
- **Range Hash**: SQL uses `range_hash` for sharding; MongoDB doesn't need this due to different sharding model
- **Version Suffix**: MongoDB `_id` includes version suffix (v1/v2) to support migration between fairness modes

---

### 5. Queue Store

Internal queues for DLQ messages and replication.

#### SQL Tables

**`queue`** / **`queue_metadata`** (Legacy)

| Column            | Type       | Description      |
| ----------------- | ---------- | ---------------- |
| `queue_type`      | INT        | Queue type enum  |
| `message_id`      | BIGINT     | Message sequence |
| `message_payload` | MEDIUMBLOB | Message data     |

**`queues`** / **`queue_messages`** (V2)

| Column            | Type         | Description  |
| ----------------- | ------------ | ------------ |
| `queue_type`      | INT          | Queue type   |
| `queue_name`      | VARCHAR(255) | Queue name   |
| `queue_partition` | BIGINT       | Partition ID |
| `message_id`      | BIGINT       | Message ID   |
| `message_payload` | MEDIUMBLOB   | Payload      |

#### MongoDB Collections

**`queue_messages`** (Legacy)

```javascript
{
  _id: String,              // "{queue_type}:{message_id}"
  queue_type: Int32,
  message_id: Int64,
  payload: Binary,
  encoding: String,
  created_at: Date
}
// Indexes: (queue_type, message_id) ASC and DESC
```

**`queue_metadata`** (Legacy)

```javascript
{
  _id: String,              // "{queue_type}"
  queue_type: Int32,
  blob: Binary,
  encoding: String,
  version: Int64,
  last_message_id: Int64,
  updated_at: Date
}
```

**`queue_v2_messages`** / **`queue_v2_metadata`** (V2)

Similar structure with additional `queue_name` and `partition` fields.

---

### 6. Visibility Store

The visibility store enables workflow listing and search functionality.

#### SQL Table: `executions_visibility`

| Column                   | Type         | Description                         |
| ------------------------ | ------------ | ----------------------------------- |
| `namespace_id`           | CHAR(64)     | Namespace UUID                      |
| `run_id`                 | CHAR(64)     | **Primary Key (with namespace_id)** |
| `_version`               | BIGINT       | Out-of-order update rejection       |
| `start_time`             | DATETIME(6)  | Workflow start time                 |
| `execution_time`         | DATETIME(6)  | First workflow task time            |
| `workflow_id`            | VARCHAR(255) | Workflow ID                         |
| `workflow_type_name`     | VARCHAR(255) | Workflow type                       |
| `status`                 | INT          | Execution status enum               |
| `close_time`             | DATETIME(6)  | Workflow close time (nullable)      |
| `history_length`         | BIGINT       | Number of history events            |
| `history_size_bytes`     | BIGINT       | History size                        |
| `execution_duration`     | BIGINT       | Duration in nanoseconds             |
| `state_transition_count` | BIGINT       | State transitions                   |
| `memo`                   | BLOB         | User memo                           |
| `encoding`               | VARCHAR(64)  | Memo encoding                       |
| `task_queue`             | VARCHAR(255) | Task queue name                     |
| `search_attributes`      | JSON         | Custom search attributes            |
| `parent_workflow_id`     | VARCHAR(255) | Parent workflow                     |
| `parent_run_id`          | VARCHAR(255) | Parent run                          |
| `root_workflow_id`       | VARCHAR(255) | Root workflow                       |
| `root_run_id`            | VARCHAR(255) | Root run                            |

**Generated Columns** (SQL specific):

- `TemporalChangeVersion`, `BinaryChecksums`, `BatcherUser`, `BuildIds`, etc.
- These are virtual columns generated from `search_attributes` JSON for indexing

**Indexes**: ~25+ indexes for various search patterns

#### SQL Table: `custom_search_attributes`

Separate table for user-defined search attributes with pre-allocated slots:

- `Bool01-03`, `Datetime01-03`, `Double01-03`, `Int01-03`
- `Keyword01-10`, `Text01-03`, `KeywordList01-03`

#### MongoDB Collection: `visibility_executions`

```javascript
{
  _id: String,                    // "{namespace_id}:{run_id}"
  namespace_id: String,
  workflow_id: String,
  run_id: String,
  workflow_type_name: String,
  task_queue: String,
  start_time: Date,
  execution_time: Date,
  visibility_time: Date,          // for sorting (close_time or start_time)
  close_time: Date,               // nullable
  execution_duration: Int64,      // nanoseconds
  history_length: Int64,
  history_size_bytes: Int64,
  state_transition_count: Int64,
  status: Int32,
  memo: Binary,
  memo_encoding: String,
  search_attributes_data: Binary, // raw protobuf for reconstruction
  search_attributes_encoding: String,
  search_attributes: Object,      // denormalized map for querying
  parent_workflow_id: String,
  parent_run_id: String,
  root_workflow_id: String,
  root_run_id: String,
  version: Int64
}
```

**Indexes**:

```javascript
// Core indexes
{ namespace_id: 1, workflow_id: 1, run_id: 1 } UNIQUE
{ namespace_id: 1, start_time: -1, run_id: 1 }
{ namespace_id: 1, close_time: -1, run_id: 1 }
{ namespace_id: 1, visibility_time: -1, run_id: 1 }
{ namespace_id: 1, workflow_type_name: 1, visibility_time: -1 }
{ namespace_id: 1, status: 1, visibility_time: -1 }
```

**`visibility_search_attributes`** (for dynamic schema)

```javascript
{
  _id: ObjectId,
  name: String,           // attribute name (UNIQUE)
  type: Int32,            // attribute type enum
  created_at: Date
}
```

**Key Differences**:

- **Generated Columns**: SQL uses computed columns from JSON; MongoDB stores denormalized `search_attributes` map
- **Custom Attributes**: SQL pre-allocates slots (Keyword01-10); MongoDB uses dynamic schema
- **Index Strategy**: SQL requires many explicit indexes; MongoDB uses flexible compound indexes
- **Query Language**: SQL uses WHERE clauses; MongoDB uses aggregation pipeline with `$match`

---

### 7. Cluster Metadata Store

Stores multi-cluster configuration and cluster membership.

#### SQL Tables

**`cluster_metadata_info`**

| Column               | Type         | Description                      |
| -------------------- | ------------ | -------------------------------- |
| `metadata_partition` | INT          | Partition key                    |
| `cluster_name`       | VARCHAR(255) | **Primary Key (with partition)** |
| `data`               | MEDIUMBLOB   | Cluster metadata                 |
| `version`            | BIGINT       | Optimistic concurrency           |

**`cluster_membership`**

| Column                 | Type         | Description                      |
| ---------------------- | ------------ | -------------------------------- |
| `membership_partition` | INT          | Partition key                    |
| `host_id`              | BINARY(16)   | **Primary Key (with partition)** |
| `rpc_address`          | VARCHAR(128) | Host address                     |
| `rpc_port`             | SMALLINT     | Port                             |
| `role`                 | TINYINT      | Service role                     |
| `session_start`        | TIMESTAMP    | Session start                    |
| `last_heartbeat`       | TIMESTAMP    | Last heartbeat                   |
| `record_expiry`        | TIMESTAMP    | Expiration time                  |

#### MongoDB Collections

**`cluster_metadata`**

```javascript
{
  _id: String,              // cluster_name
  data: Binary,
  data_encoding: String,
  version: Int64
}
```

**`cluster_membership`**

```javascript
{
  _id: Binary,              // host_id (16 bytes)
  rpc_address: String,
  rpc_port: Int32,
  role: Int32,
  session_start: Date,
  last_heartbeat: Date,
  record_expiry: Date
}
```

---

### 8. Nexus Endpoint Store

Stores Nexus service endpoint configurations.

#### SQL Tables

**`nexus_endpoints`**

| Column    | Type       | Description            |
| --------- | ---------- | ---------------------- |
| `id`      | BINARY(16) | **Primary Key**        |
| `data`    | MEDIUMBLOB | Endpoint configuration |
| `version` | BIGINT     | Optimistic concurrency |

**`nexus_endpoints_partition_status`**

| Column    | Type   | Description                |
| --------- | ------ | -------------------------- |
| `id`      | INT    | **Primary Key** (always 0) |
| `version` | BIGINT | Table-wide version         |

#### MongoDB Collections

**`nexus_endpoints`**

```javascript
{
  _id: String,              // endpoint UUID
  version: Int64,
  data: Binary,
  data_encoding: String,
  updated_at: Date
}
```

**`nexus_endpoints_metadata`**

```javascript
{
  _id: "table_version",     // singleton
  version: Int64,
  updated_at: Date
}
```

---

## Design Decisions: SQL to MongoDB Adaptation

This section discusses the key architectural decisions made when adapting Temporal's SQL-based persistence layer to MongoDB, including the trade-offs, challenges, and solutions developed during the migration.

### From Foreign Keys to Document Embedding (Denormalization)

#### The SQL Model: Normalized with Implicit Foreign Keys

In SQL, Temporal uses a **normalized schema** where related data is split across multiple tables. While there are no explicit `FOREIGN KEY` constraints (for performance reasons), the tables maintain implicit relationships:

```
executions (parent)
    │
    ├── activity_info_maps (child) ──── FK: (shard_id, namespace_id, workflow_id, run_id)
    ├── timer_info_maps (child) ─────── FK: (shard_id, namespace_id, workflow_id, run_id)
    ├── child_execution_info_maps ───── FK: (shard_id, namespace_id, workflow_id, run_id)
    ├── request_cancel_info_maps ────── FK: (shard_id, namespace_id, workflow_id, run_id)
    ├── signal_info_maps ────────────── FK: (shard_id, namespace_id, workflow_id, run_id)
    ├── signals_requested_sets ──────── FK: (shard_id, namespace_id, workflow_id, run_id)
    ├── chasm_node_maps ─────────────── FK: (shard_id, namespace_id, workflow_id, run_id)
    └── buffered_events ─────────────── FK: (shard_id, namespace_id, workflow_id, run_id)
```

**How SQL operations work:**

```sql
-- Reading execution state requires multiple queries or JOINs
SELECT * FROM executions WHERE shard_id=? AND namespace_id=? AND workflow_id=? AND run_id=?;
SELECT * FROM activity_info_maps WHERE shard_id=? AND namespace_id=? AND workflow_id=? AND run_id=?;
SELECT * FROM timer_info_maps WHERE shard_id=? AND namespace_id=? AND workflow_id=? AND run_id=?;
-- ... and so on for each related table

-- Updating requires multiple statements in a transaction
BEGIN;
UPDATE executions SET ... WHERE ...;
DELETE FROM activity_info_maps WHERE ... AND schedule_id IN (?);
INSERT INTO activity_info_maps (...) VALUES (...);
DELETE FROM timer_info_maps WHERE ... AND timer_id IN (?);
INSERT INTO timer_info_maps (...) VALUES (...);
COMMIT;
```

#### The MongoDB Model: Denormalized with Embedded Arrays

MongoDB uses **document embedding** to store all related data in a single document:

```javascript
// Single document contains everything
{
  _id: "1:namespace-uuid:workflow-id:run-uuid",
  shard_id: 1,
  namespace_id: "namespace-uuid",
  workflow_id: "workflow-id",
  run_id: "run-uuid",

  // Core execution data
  execution_info: { data: Binary, encoding: "proto3" },
  execution_state: { data: Binary, encoding: "proto3" },
  next_event_id: 150,
  db_record_version: 42,

  // Embedded arrays (previously separate tables)
  activity_infos: [
    { id: 5, blob: { data: Binary, encoding: "proto3" } },
    { id: 8, blob: { data: Binary, encoding: "proto3" } },
    { id: 12, blob: { data: Binary, encoding: "proto3" } }
  ],
  timer_infos: [
    { key: "timer-1", blob: { data: Binary, encoding: "proto3" } }
  ],
  child_execution_infos: [...],
  request_cancel_infos: [...],
  signal_infos: [...],
  signal_requested_ids: ["signal-1", "signal-2"],
  chasm_nodes: [...],
  buffered_events: [...]
}
```

**How MongoDB operations work:**

```javascript
// Single read gets everything
db.executions.findOne({ _id: "1:namespace-uuid:workflow-id:run-uuid" });

// Single atomic update modifies everything
db.executions.updateOne(
  { _id: "1:namespace-uuid:workflow-id:run-uuid", db_record_version: 42 },
  {
    $set: {
      execution_info: newExecutionInfo,
      db_record_version: 43,
    },
    $pull: {
      activity_infos: { id: { $in: [5, 8] } }, // Remove completed activities
      timer_infos: { key: { $in: ["timer-1"] } }, // Remove fired timers
    },
    $push: {
      activity_infos: { $each: [newActivity1, newActivity2] },
      buffered_events: { $each: [newEvent1] },
    },
  }
);
```

### Impact Analysis: Denormalization Trade-offs

#### ✅ Advantages

| Aspect                   | Benefit                                                | Impact on Temporal                                                           |
| ------------------------ | ------------------------------------------------------ | ---------------------------------------------------------------------------- |
| **Atomic Updates**       | Single document update is atomic without transactions  | Workflow state changes (add activity, fire timer, etc.) are naturally atomic |
| **Read Performance**     | One round-trip fetches complete state                  | `GetWorkflowExecution` is faster - no JOINs or multiple queries              |
| **Data Locality**        | Related data stored together on disk                   | Better cache utilization, reduced I/O                                        |
| **Simpler Transactions** | Many operations don't need multi-document transactions | Reduced transaction overhead for common operations                           |
| **No Orphan Data**       | Deleting execution removes all related data            | No need for cascading deletes or cleanup jobs                                |

#### ❌ Disadvantages

| Aspect                   | Challenge                                              | Mitigation Strategy                                                         |
| ------------------------ | ------------------------------------------------------ | --------------------------------------------------------------------------- |
| **Document Size**        | Large workflows with many activities can grow large    | MongoDB 16MB limit is sufficient; activities store references, not payloads |
| **Partial Updates**      | Can't update single activity without fetching document | Use `$push`, `$pull`, `$set` with array filters for targeted updates        |
| **Storage Overhead**     | Repeated keys in embedded documents                    | BSON is binary; compression at storage layer helps                          |
| **Index Limitations**    | Can't efficiently index individual array elements      | Use separate collections where needed (e.g., visibility)                    |
| **Migration Complexity** | Schema changes affect entire document                  | Careful versioning and backward compatibility                               |

### Specific Challenges and Solutions

#### Challenge 1: Workflow Execution Updates

**Problem**: SQL updates specific rows in child tables; MongoDB needs to update arrays within a document.

**SQL approach:**

```sql
-- Delete specific activity
DELETE FROM activity_info_maps
WHERE shard_id=1 AND namespace_id=? AND workflow_id=? AND run_id=? AND schedule_id=5;

-- Insert new activity
INSERT INTO activity_info_maps (shard_id, namespace_id, workflow_id, run_id, schedule_id, data, data_encoding)
VALUES (1, ?, ?, ?, 10, ?, 'proto3');
```

**MongoDB solution:**

The key insight is that MongoDB provides **atomic array operators** (`$push`, `$pull`, `$set`) that can modify embedded arrays without reading the entire document first. This is crucial because:

1. **No read-modify-write cycle**: SQL's approach of DELETE + INSERT maps to MongoDB's `$pull` + `$push` in a single update operation
2. **Atomic guarantees**: All array modifications happen atomically within the document update
3. **Selective updates**: We don't need to replace the entire `activity_infos` array; we surgically remove completed activities and add new ones

The implementation builds an update document with multiple operators:

```go
// In execution_store.go - buildExecutionUpdates()
updates := bson.M{
    "$pull": bson.M{
        "activity_infos": bson.M{"id": bson.M{"$in": deletedActivityIDs}},
    },
    "$push": bson.M{
        "activity_infos": bson.M{"$each": newActivityDocs},
    },
    "$set": bson.M{
        "execution_info": updatedExecutionInfo,
        "db_record_version": newVersion,
    },
}
```

**Performance impact:**

- ✅ Single round-trip to database (vs. multiple statements in SQL)
- ✅ No transaction overhead for single-document updates
- ⚠️ Large arrays (1000+ activities) may see slower `$pull` operations due to array scanning
- ⚠️ MongoDB must rewrite the entire document if it grows beyond its allocated space (document relocation)

#### Challenge 2: Optimistic Concurrency Control

**Problem**: SQL uses row-level locking and version checks per table; MongoDB needs document-level versioning.

**SQL approach:**

```sql
-- Each table may have its own version or rely on execution's version
UPDATE executions
SET data=?, db_record_version=db_record_version+1
WHERE shard_id=? AND namespace_id=? AND workflow_id=? AND run_id=?
  AND db_record_version=?;  -- Optimistic lock check

-- If no rows updated, concurrent modification detected
```

**MongoDB solution:**

MongoDB doesn't have row-level locking like SQL databases. Instead, we implement **optimistic concurrency control (OCC)** by including the expected version in the query filter. The solution works as follows:

1. **Version in filter**: The `db_record_version` is part of the query filter, not just the document
2. **Atomic check-and-update**: If the version doesn't match, the query matches zero documents
3. **Conflict detection**: We check `result.MatchedCount == 0` to detect concurrent modifications
4. **No explicit locks**: This approach scales better than pessimistic locking in distributed systems

```go
// In execution_store.go
filter := bson.M{
    "_id":               executionID,
    "db_record_version": expectedVersion,  // Only match if version is exactly what we expect
}
update := bson.M{
    "$set": bson.M{
        "db_record_version": expectedVersion + 1,  // Increment version
        // ... other updates
    },
}
result, err := collection.UpdateOne(ctx, filter, update)
if result.MatchedCount == 0 {
    // Another process modified the document - caller should retry
    return &persistence.ConditionFailedError{Msg: "version mismatch"}
}
```

**Performance impact:**

- ✅ No lock contention - multiple readers can proceed simultaneously
- ✅ Better scalability for distributed History service instances
- ✅ Failed updates are cheap (no rollback needed)
- ⚠️ High-contention workflows may see more retries than with pessimistic locking
- ⚠️ Requires application-level retry logic (already present in Temporal)

#### Challenge 3: History Event Pagination

**Problem**: SQL can efficiently paginate with `WHERE node_id > ? ORDER BY node_id LIMIT ?`; MongoDB compound key requires different approach.

**SQL approach:**

```sql
SELECT * FROM history_node
WHERE shard_id=? AND tree_id=? AND branch_id=?
  AND node_id >= ? AND node_id < ?
ORDER BY node_id, txn_id
LIMIT 1000;
```

**MongoDB solution:**

History events are stored in separate documents (not embedded) because they can grow unboundedly and need efficient range queries. The challenge is that MongoDB's `_id` is a concatenated string (`{shard}:{tree}:{branch}:{node}:{txn}`), so we can't use simple `_id` comparison for pagination.

The solution uses **indexed field queries** instead of `_id` ordering:

1. **Compound index**: Create index on `(shard_id, tree_id, branch_id, node_id, txn_id)`
2. **Range filter on node_id**: Use `$gte` and `$lt` operators for the numeric `node_id` field
3. **Explicit sort**: Sort by `node_id` then `txn_id` to ensure correct event ordering
4. **MaxNodeID constraint**: Critical for pagination - without the upper bound (`$lt: maxNodeID`), queries could return events from future pages

```go
// In execution_store.go - ReadHistoryBranch()
filter := bson.M{
    "shard_id":  shardID,
    "tree_id":   treeID,
    "branch_id": branchID,
    "node_id": bson.M{
        "$gte": minNodeID,  // Start from this event batch
        "$lt":  maxNodeID,  // Don't go beyond requested range
    },
}
opts := options.Find().
    SetSort(bson.D{{Key: "node_id", Value: 1}, {Key: "txn_id", Value: 1}}).
    SetLimit(pageSize)
```

**A bug we discovered**: Initially, the `MaxNodeID` filter was missing, causing pagination to return incorrect results. When reading page N, we'd sometimes get events from page N+1 because MongoDB would happily return all matching documents up to the limit. The fix was adding the explicit upper bound filter.

**Performance impact:**

- ✅ Uses compound index efficiently - no collection scan
- ✅ Pagination is O(log n) with proper indexes
- ⚠️ String `_id` concatenation adds ~50 bytes overhead per document
- ⚠️ Sort operation requires index; without index, MongoDB must sort in memory

#### Challenge 4: Visibility Store Search Attributes

**Problem**: SQL uses generated columns and separate `custom_search_attributes` table; MongoDB needs queryable document structure.

**SQL approach:**

```sql
-- Generated columns in executions_visibility
TemporalChangeVersion JSON GENERATED ALWAYS AS (search_attributes->"$.TemporalChangeVersion"),
BatcherUser VARCHAR(255) GENERATED ALWAYS AS (search_attributes->>"$.BatcherUser"),

-- Separate table for custom attributes with pre-allocated slots
CREATE TABLE custom_search_attributes (
  Keyword01 VARCHAR(255) GENERATED ALWAYS AS (search_attributes->>"$.Keyword01"),
  Keyword02 VARCHAR(255) GENERATED ALWAYS AS (search_attributes->>"$.Keyword02"),
  -- ... up to Keyword10, Int01-03, Bool01-03, etc.
);

-- Query using generated columns
SELECT * FROM executions_visibility
WHERE namespace_id=? AND BatcherUser='user@example.com'
ORDER BY close_time DESC;
```

SQL's approach uses **generated columns** - a MySQL feature where columns are automatically computed from JSON fields. This allows indexing of JSON values without storing them twice. However, custom search attributes are limited by pre-allocated slot columns (`Keyword01`-`Keyword10`, etc.).

**MongoDB solution:**

MongoDB's schema-less nature makes this much simpler. Instead of pre-defined columns, we store search attributes as a **nested document with dynamic keys**:

1. **Dual storage**: Keep raw protobuf bytes (for exact reconstruction) AND a denormalized map (for querying)
2. **Dynamic schema**: Any search attribute name can be added without schema migration
3. **Native types**: Convert protobuf payloads to MongoDB-native types (string, int, bool, datetime) for proper comparison semantics
4. **Dot notation queries**: Access nested fields with `search_attributes.CustomField` syntax

```go
type visibilityExecutionDocument struct {
    // Raw protobuf for exact reconstruction
    SearchAttributes    []byte         `bson:"search_attributes_data,omitempty"`
    SearchAttributesEnc string         `bson:"search_attributes_encoding,omitempty"`

    // Denormalized map for querying - no schema constraints
    SearchAttributesMap map[string]any `bson:"search_attributes"`
}

// Querying uses MongoDB's flexible document queries
filter := bson.M{
    "namespace_id": namespaceID,
    "search_attributes.BatcherUser": "user@example.com",
}
```

**Query Language Translation** (`visibility_query_converter.go`):

The visibility store must accept SQL-like query strings and convert them to MongoDB queries:

```go
// Input SQL-like query:
// `BatcherUser = 'user@example.com' AND CloseTime > '2024-01-01'`

// Output MongoDB filter:
bson.M{
    "$and": []bson.M{
        {"search_attributes.BatcherUser": "user@example.com"},
        {"close_time": bson.M{"$gt": time.Parse(...)}},
    },
}
```

The query converter handles:

- Field name mapping (predefined fields like `WorkflowId` → `workflow_id`)
- Search attribute prefix (custom fields → `search_attributes.<name>`)
- Operator translation (`=` → implicit, `>` → `$gt`, `IN` → `$in`, etc.)
- Type coercion (string timestamps → time.Time for proper comparison)

**Performance impact:**

- ✅ No slot limitations - unlimited custom search attributes
- ✅ Simpler schema - no generated column complexity
- ⚠️ Indexing dynamic fields requires **wildcard indexes** (`{"search_attributes.$**": 1}`)
- ⚠️ Wildcard indexes less efficient than specific indexes - consider creating targeted indexes for frequently-queried attributes

#### Challenge 5: CHASM Visibility (Standalone Activities)

**Problem**: CHASM (Component-Hosted Activity State Machines) requires listing activities by archetype. SQL visibility uses `WorkflowId` field; MongoDB needs special handling for the field mapping.

**Background**: CHASM enables "standalone activities" - activities that exist independently without a parent workflow. These are stored in the visibility table but with special semantics:

- `workflow_type_name` = archetype name (e.g., `"temporal.api.activity.v1.ActivityInfo"`)
- `workflow_id` = activity ID (reused field)
- User queries may use `ActivityId` search attribute which must map to `workflow_id`

**SQL approach:**

```sql
-- CHASM stores archetype in WorkflowTypeName, activity ID in WorkflowId
SELECT * FROM executions_visibility
WHERE namespace_id=? AND workflow_type_name='temporal.api.activity.v1.ActivityInfo'
ORDER BY start_time DESC;

-- SQL query converter maps ActivityId → WorkflowId column automatically
```

**MongoDB solution:**

The challenge was twofold:

1. **Archetype lookup**: Convert integer archetype ID to string archetype name
2. **Field mapping**: Ensure `ActivityId` queries correctly target `workflow_id` field

```go
func (s *visibilityStore) ListChasmExecutions(
    ctx context.Context,
    request *store.ListChasmExecutionsRequest,
) (*store.InternalListChasmExecutionsResponse, error) {
    // Step 1: Convert archetype ID to name using CHASM registry
    comp, ok := s.chasmRegistry.ComponentByID(request.ArchetypeID)
    if !ok {
        return nil, serviceerror.NewInvalidArgument("unknown archetype")
    }

    // Step 2: Build base filter on archetype
    filter := bson.M{
        "namespace_id":       request.NamespaceID.String(),
        "workflow_type_name": comp.ArchetypeName(),
    }

    // Step 3: Apply user's query filter (ActivityId → workflow_id mapping)
    if request.Query != "" {
        queryFilter, err := s.convertQuery(request.Query)
        filter = bson.M{"$and": []bson.M{filter, queryFilter}}
    }

    // ... execute query
}
```

**The bug we discovered**: The query converter wasn't handling `ActivityId` correctly for CHASM queries. When a user searched for `ActivityId = 'my-activity'`, the converter needed to translate this to `workflow_id: 'my-activity'` in the MongoDB filter. The fix was adding explicit mapping in the query converter:

```go
// In visibility_query_converter.go
func (c *queryConverter) getFieldName(searchAttr string) string {
    switch searchAttr {
    case "WorkflowId", "ActivityId":  // ActivityId maps to same field
        return "workflow_id"
    case "WorkflowType":
        return "workflow_type_name"
    // ... other predefined fields
    default:
        return "search_attributes." + searchAttr
    }
}
```

**Performance impact:**

- ✅ Same index as regular visibility queries - no new indexes needed
- ✅ Archetype lookup is O(1) - uses in-memory registry
- ⚠️ CHASM registry dependency: Each service (frontend, history, matching) has its own registry instance, which is why visibility store cannot be cached globally (see Factory section)

### Foreign Key Behavior Comparison

| Behavior                    | SQL (Implicit FK)                  | MongoDB (Embedded)                               |
| --------------------------- | ---------------------------------- | ------------------------------------------------ |
| **Referential Integrity**   | Application-enforced               | Automatic (embedded in parent)                   |
| **Cascade Delete**          | Manual cleanup required            | Automatic (delete parent = delete children)      |
| **Orphan Prevention**       | Possible if app bugs               | Impossible (no separate storage)                 |
| **Partial Reads**           | Can read just child table          | Must read entire document (can use projection)   |
| **Bulk Child Updates**      | Efficient with batch INSERT/DELETE | Requires careful array manipulation              |
| **Child Count Queries**     | `SELECT COUNT(*) FROM child_table` | `$size` operator or application logic            |
| **Cross-Execution Queries** | Easy with JOINs                    | Requires separate collections (e.g., visibility) |

### When NOT to Denormalize

Not all SQL tables were denormalized in MongoDB. Some remain as separate collections:

| Data Type            | Reason for Separate Collection                   |
| -------------------- | ------------------------------------------------ |
| **History Nodes**    | Can grow very large; need independent pagination |
| **History Branches** | Shared across executions (branch forking)        |
| **Tasks**            | Need efficient queue operations (pop, expire)    |
| **Visibility**       | Need complex queries with indexes                |
| **Replication DLQ**  | Cross-shard data, independent lifecycle          |

### Performance Implications

#### Read Patterns

| Operation                     | SQL                    | MongoDB                     | Winner         |
| ----------------------------- | ---------------------- | --------------------------- | -------------- |
| Get full execution state      | Multiple queries/JOINs | Single document read        | **MongoDB**    |
| Get only execution info       | Single row read        | Document read (can project) | SQL (slightly) |
| List activities for execution | Child table query      | Extract from document       | **MongoDB**    |
| Search across executions      | Indexed query          | Indexed query               | Tie            |

#### Write Patterns

| Operation                     | SQL                         | MongoDB              | Winner         |
| ----------------------------- | --------------------------- | -------------------- | -------------- |
| Update execution + activities | Multi-statement transaction | Single atomic update | **MongoDB**    |
| Add single activity           | Single INSERT               | Array push           | SQL (slightly) |
| Delete execution cascade      | Multiple DELETEs            | Single delete        | **MongoDB**    |
| Bulk activity updates         | Batch INSERT/DELETE         | Complex $push/$pull  | SQL            |

### Migration Considerations

When working on the MongoDB implementation, developers familiar with SQL should:

1. **Think in documents, not rows**: Group related data that's always accessed together
2. **Design for access patterns**: MongoDB optimizes for document-level operations
3. **Use atomic operators**: `$set`, `$push`, `$pull`, `$inc` instead of read-modify-write
4. **Avoid unbounded arrays**: If a list can grow indefinitely, consider a separate collection
5. **Index strategically**: Compound indexes in MongoDB work differently than SQL
6. **Plan for transactions**: Multi-document transactions need replica set and have overhead

---

## Key Design Differences

| Aspect                 | SQL                                | MongoDB                           |
| ---------------------- | ---------------------------------- | --------------------------------- |
| **Primary Keys**       | Multi-column composite keys        | String-concatenated `_id`         |
| **Relationships**      | Separate tables with implicit FK   | Embedded documents (denormalized) |
| **Sharding**           | `range_hash` column                | Native MongoDB sharding           |
| **Transactions**       | Native ACID                        | Replica set transactions required |
| **Schema**             | Fixed schema with migrations       | Flexible schema                   |
| **Expiration**         | Manual cleanup jobs                | TTL indexes                       |
| **Search Attributes**  | Generated columns + separate table | Dynamic schema in document        |
| **Optimistic Locking** | `version`/`range_id` columns       | Same pattern with `$set`          |

## Transaction Support

### SQL

- Native transaction support with BEGIN/COMMIT/ROLLBACK
- Row-level locking
- Works with single database instance

### MongoDB

- Requires **replica set** deployment for multi-document transactions
- Uses `withTransaction()` session API
- All stores validate `transactionsEnabled` flag
- Standalone MongoDB instances are **not supported** for production

```go
// MongoDB transaction pattern
err := s.withTransaction(ctx, func(sessCtx mongo.SessionContext) error {
    // Multiple operations within transaction
    _, err := collection.UpdateOne(sessCtx, filter, update)
    return err
})
```

---

## Summary

MongoDB provides a document-oriented alternative to SQL with:

- **Simpler schema**: Fewer tables through denormalization
- **Flexible indexing**: Dynamic indexes without schema changes
- **Native TTL**: Automatic document expiration
- **Horizontal scaling**: Built-in sharding support

Trade-offs:

- **Transaction requirement**: Must use replica set
- **Query complexity**: Aggregation pipeline vs SQL
- **Storage overhead**: Some denormalization increases document size

Both implementations provide full feature parity for Temporal Server operations.
