CREATE TABLE nexus_incoming_services
(
    partition       int, -- constant for all rows (using a single partition for efficient list queries)
    type            int, -- enum RowType { PartitionStatus, NexusIncomingService }
    service_id      uuid,
    data            blob,
    data_encoding   text,
    -- When type=PartitionStatus contains the partition version.
    --      Partition version is used to guarantee latest versions when listing all services.
    -- When type=NexusIncomingService contains the service version used for optimistic concurrency
    version         bigint,
    PRIMARY KEY ((partition), type, service_id)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
    };
