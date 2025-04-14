CREATE TABLE queues
(
    queue_type        int,
    queue_name        text,
    metadata_payload  blob,
    metadata_encoding text,
    version           bigint,
    PRIMARY KEY ((queue_type, queue_name))
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
    };

CREATE TABLE queue_messages
(
    queue_type       int,
    queue_name       text,
    queue_partition  int,
    message_id       bigint,
    message_payload  blob,
    message_encoding text,
    PRIMARY KEY ((queue_type, queue_name, queue_partition), message_id)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
    };
