ALTER TABLE queue ADD message_encoding VARCHAR(16) NOT NULL DEFAULT 'Json';
ALTER TABLE queue_metadata ADD data_encoding VARCHAR(16) NOT NULL DEFAULT 'Json';
