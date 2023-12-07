CREATE TABLE nexus_services
(
    services_partition  int, -- constant for all rows (using a single partition for efficient list queries)
    service_id          uuid,
    type                int, -- enum RowType { PartitionStatus, NexusIncomingService }
    service_name        text,
    namespace_id        uuid,
    task_queue_name     text,
    metadata            blob,
    metadata_encoding   text,
    -- When type=PartitionStatus contains the partition version.
    --      Partition version is used to guarantee latest versions when listing all services.
    -- When type=NexusIncomingService contains the service version used for optimistic concurrency
    version             bigint,
    PRIMARY KEY ((services_partition), type, service_id)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
    };