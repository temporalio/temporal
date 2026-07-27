CREATE TABLE IF NOT EXISTS queue_message_id_range
(
    queue_type      int,
    next_message_id bigint,
    version         bigint,
    PRIMARY KEY (queue_type)
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
    };

CREATE TABLE IF NOT EXISTS queue_message_id_ranges
(
    queue_type      int,
    queue_name      text,
    next_message_id bigint,
    version         bigint,
    PRIMARY KEY ((queue_type, queue_name))
) WITH COMPACTION = {
    'class': 'org.apache.cassandra.db.compaction.LeveledCompactionStrategy'
    };
