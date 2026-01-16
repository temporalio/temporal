// Temporal MongoDB schema bootstrap (v1.0)
//
// Usage:
//   env TEMPORAL_MONGO_DB="temporal" \
//   env TEMPORAL_MONGO_VISIBILITY_DB="temporal_visibility" \
//   mongosh "mongodb://localhost:27017" init.js
//
// The script is idempotent: collections are created when missing and indexes are
// ensured. Existing collections/indexes are left untouched.

(function () {
  const primaryDbName = (typeof process !== "undefined" && process.env.TEMPORAL_MONGO_DB) || "temporal";
  const visibilityDbName =
    (typeof process !== "undefined" && process.env.TEMPORAL_MONGO_VISIBILITY_DB) || primaryDbName;

  function ensureCollection(database, spec) {
    const { name, options, indexes } = spec;
    const existingCollections = new Set(database.getCollectionNames());
    if (!existingCollections.has(name)) {
      database.createCollection(name, options || {});
      print(`Created collection ${database.getName()}.${name}`);
    }
    if (indexes && indexes.length) {
      const collection = database.getCollection(name);
      indexes.forEach((idx) => ensureIndex(collection, idx));
    }
  }

  function ensureIndex(collection, spec) {
    const options = Object.assign({}, spec.options || {}, { name: spec.name });
    try {
      const resultName = collection.createIndex(spec.keys, options);
      if (resultName === spec.name) {
        print(`Ensured index ${collection.getFullName()}.${spec.name}`);
      }
    } catch (err) {
      const ignorable = [
        85, // IndexOptionsConflict
        11000, // duplicate key error when unique index already exists
      ];
      if (!ignorable.includes(err.code)) {
        throw err;
      }
    }
  }

  const primaryDb = db.getSiblingDB(primaryDbName);

  const primaryCollections = [
    { name: "shards" },
    {
      name: "namespaces",
      indexes: [{ name: "namespaces_name_unique", keys: { name: 1 }, options: { unique: true } }],
    },
    { name: "namespace_metadata" },
    { name: "cluster_metadata" },
    { name: "cluster_membership" },
    { name: "executions" },
    { name: "current_executions" },
    { name: "history_branches" },
    { name: "history_nodes" },
    { name: "transfer_tasks" },
    { name: "timer_tasks" },
    { name: "replication_tasks" },
    { name: "visibility_tasks" },
    { name: "replication_dlq_tasks" },
    {
      name: "task_queues",
      indexes: [
        {
          name: "task_queue_unique",
          keys: {
            namespace_id: 1,
            task_queue: 1,
            task_type: 1,
            subqueue: 1,
          },
          options: { unique: true },
        },
      ],
    },
    {
      name: "tasks",
      indexes: [
        {
          name: "tasks_lookup",
          keys: {
            namespace_id: 1,
            task_queue: 1,
            task_type: 1,
            subqueue: 1,
            task_pass: 1,
            task_id: 1,
          },
        },
        {
          name: "tasks_ttl",
          keys: { expiry_time: 1 },
          options: { expireAfterSeconds: 0 },
        },
      ],
    },
    {
      name: "task_queue_user_data",
      indexes: [
        {
          name: "task_queue_user_data_unique",
          keys: { namespace_id: 1, task_queue: 1 },
          options: { unique: true },
        },
      ],
    },
    {
      name: "task_queue_build_id_map",
      indexes: [
        {
          name: "task_queue_build_id_unique",
          keys: { namespace_id: 1, build_id: 1, task_queue: 1 },
          options: { unique: true },
        },
        {
          name: "task_queue_build_id_lookup",
          keys: { namespace_id: 1, build_id: 1 },
        },
      ],
    },
    {
      name: "queue_metadata",
      indexes: [
        {
          name: "queue_metadata_lookup",
          keys: { queue_type: 1 },
          options: { unique: true },
        },
      ],
    },
    {
      name: "queue_messages",
      indexes: [
        {
          name: "queue_messages_lookup",
          keys: { queue_type: 1, message_id: 1 },
        },
        {
          name: "queue_messages_lookup_desc",
          keys: { queue_type: 1, message_id: -1 },
        },
      ],
    },
    {
      name: "queue_v2_metadata",
      indexes: [
        {
          name: "queue_v2_metadata_lookup",
          keys: { queue_type: 1, queue_name: 1 },
          options: { unique: true },
        },
      ],
    },
    {
      name: "queue_v2_messages",
      indexes: [
        {
          name: "queue_v2_messages_lookup",
          keys: { queue_type: 1, queue_name: 1, partition: 1, message_id: 1 },
        },
        {
          name: "queue_v2_messages_lookup_desc",
          keys: { queue_type: 1, queue_name: 1, partition: 1, message_id: -1 },
        },
      ],
    },
    {
      name: "nexus_endpoints_metadata",
      indexes: [
        {
          name: "nexus_endpoints_metadata_unique",
          keys: { _id: 1 },
          options: { unique: true },
        },
      ],
    },
    {
      name: "nexus_endpoints",
      indexes: [
        {
          name: "nexus_endpoints_id_unique",
          keys: { _id: 1 },
          options: { unique: true },
        },
      ],
    },
  ];

  primaryCollections.forEach((spec) => ensureCollection(primaryDb, spec));

  const visibilityDb = db.getSiblingDB(visibilityDbName);
  const visibilityCollections = [
    { name: "visibility_executions" },
    {
      name: "visibility_search_attributes",
      indexes: [{ name: "visibility_sa_name_unique", keys: { name: 1 }, options: { unique: true } }],
    },
  ];

  visibilityCollections.forEach((spec) => ensureCollection(visibilityDb, spec));

  print(`Temporal MongoDB schema v1.0 applied to ${primaryDbName} (primary) and ${visibilityDbName} (visibility)`);
})();
