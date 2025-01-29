ALTER TABLE current_executions
  ADD COLUMN attached_request_ids MEDIUMBLOB NULL,
  ADD COLUMN attached_request_ids_encoding VARCHAR(16) NULL;
