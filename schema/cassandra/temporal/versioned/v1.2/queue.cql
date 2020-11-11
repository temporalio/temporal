ALTER TABLE queue ADD message_encoding text;
ALTER TABLE queue_metadata ADD data blob;
ALTER TABLE queue_metadata ADD data_encoding text;