-- Native streams: stream_segments persistence facet.  See
-- native-streams/persistence-abi.html Approach C and
-- native-streams/cassandra-operability.html for table options.
--
-- One Cassandra partition per stream: ((shard_id, namespace_id,
-- stream_id)) per the closed decision in
-- native-streams/open-questions.html and persistence-abi.html.
--
-- Compaction: TWCS with 1-hour windows.
-- gc_grace_seconds: 172800 (48 h) — paired with the 12-hour
-- incremental-repair operator commitment.
-- See native-streams/cassandra-operability.html for rationale.

CREATE TABLE stream_segments (
  shard_id        INT,
  namespace_id    UUID,
  stream_id       TEXT,
  segment_id      BIGINT,
  txn_id          BIGINT,
  first_offset    BIGINT,
  last_offset     BIGINT,
  item_count      INT,
  payload_hash    BLOB,
  data            BLOB,
  data_encoding   TEXT,
  PRIMARY KEY ((shard_id, namespace_id, stream_id), segment_id, txn_id)
) WITH compaction = {
    'class':                  'TimeWindowCompactionStrategy',
    'compaction_window_unit': 'HOURS',
    'compaction_window_size': 1,
    'unchecked_tombstone_compaction': 'true'
  }
  AND gc_grace_seconds = 172800;
