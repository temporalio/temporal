-- Native streams: stream_segments persistence facet.  See
-- native-streams/persistence-abi.html Approach C.

CREATE TABLE stream_segments (
  shard_id        INTEGER NOT NULL,
  namespace_id    BLOB NOT NULL,
  stream_id       TEXT NOT NULL,
  segment_id      INTEGER NOT NULL,
  txn_id          INTEGER NOT NULL,
  --
  first_offset    INTEGER NOT NULL,
  last_offset     INTEGER NOT NULL,
  item_count      INTEGER NOT NULL,
  payload_hash    BLOB NOT NULL,
  data            BLOB NOT NULL,
  data_encoding   TEXT NOT NULL,
  PRIMARY KEY (shard_id, namespace_id, stream_id, segment_id, txn_id)
);
