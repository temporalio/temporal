CREATE TABLE nexus_endpoints
(
    partition       int, -- constant for all rows (using a single partition for efficient list queries)
    type            int, -- enum RowType { PartitionStatus, NexusEndpoint }
    id              uuid,
    data            blob,
    data_encoding   text,
    -- When type=PartitionStatus contains the partition version.
    --      Partition version is used to guarantee latest versions when listing all endpoints.
    -- When type=NexusEndpoint contains the endpoint version used for optimistic concurrency.
    version         bigint,
    PRIMARY KEY ((partition), type, id)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
    };
