CREATE TABLE nexus_services
(
    services_partition  int, -- constant for all rows (using a single partition)
    type                int, -- enum RowType { PartitionStatus, NexusService }
    service_name        text,
    namespace_id        UUID,
    task_queue_name     text,
    metadata            blob,
    -- When type=PartitionStatus contains the partition version.
    --      Partition version is used to guarantee latest versions when listing all services.
    -- When type=NexusService contains the service version used for optimistic concurrency
    version             bigint,
    PRIMARY KEY ((services_partition), type, service_name)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
    };