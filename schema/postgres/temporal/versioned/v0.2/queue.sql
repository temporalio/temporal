CREATE TABLE queue (
  queue_type INTEGER NOT NULL,
  message_id BIGINT NOT NULL,
  message_payload BYTEA NOT NULL,
  PRIMARY KEY(queue_type, message_id)
);

CREATE TABLE queue_metadata (
  queue_type INTEGER NOT NULL,
  data BYTEA NOT NULL,
  PRIMARY KEY(queue_type)
);
