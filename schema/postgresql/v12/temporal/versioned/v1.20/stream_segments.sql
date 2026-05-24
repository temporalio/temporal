-- Native streams: stream_segments persistence facet.
--
-- See native-streams/persistence-abi.html Approach C and
-- native-streams/exactly-once.html §6 cross-facet commit protocol.
--
-- One row per (segment_id, txn_id) under (shard_id, namespace_id, stream_id).
-- Multiple tentative versions of the same offset range coexist under
-- different txn_ids; the read path filters by txn_id <= committed_txn_id
-- which lives on the Stream chasm component.

CREATE TABLE stream_segments (
  shard_id        INTEGER NOT NULL,
  namespace_id    BYTEA NOT NULL,
  stream_id       VARCHAR(255) NOT NULL,
  segment_id      BIGINT NOT NULL,
  txn_id          BIGINT NOT NULL,
  --
  first_offset    BIGINT NOT NULL,
  last_offset     BIGINT NOT NULL,
  item_count      INTEGER NOT NULL,
  payload_hash    BYTEA NOT NULL,
  data            BYTEA NOT NULL,
  data_encoding   VARCHAR(16) NOT NULL,
  PRIMARY KEY (shard_id, namespace_id, stream_id, segment_id, txn_id)
);
