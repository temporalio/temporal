CREATE TABLE queue (
  queue_type INT NOT NULL,
  message_id BIGINT NOT NULL,
  message_payload BLOB NOT NULL,
  PRIMARY KEY(queue_type, message_id)
);

CREATE TABLE queue_metadata (
  queue_type INT NOT NULL,
  data BLOB NOT NULL,
  PRIMARY KEY(queue_type)
);
