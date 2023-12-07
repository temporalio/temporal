CREATE TABLE nexus_services
(
    service_id          uuid,
    type                int, -- enum RowType { PartitionStatus, NexusIncomingService }
    service_name        text,
    namespace_id        uuid,
    task_queue_name     text,
    metadata            blob,
    -- When type=PartitionStatus contains the partition version.
    --      Partition version is used to guarantee latest versions when listing all services.
    -- When type=NexusIncomingService contains the service version used for optimistic concurrency
    version             bigint,
    PRIMARY KEY ((service_id), type, service_name)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
    };