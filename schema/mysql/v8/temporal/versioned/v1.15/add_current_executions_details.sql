ALTER TABLE current_executions
  ADD COLUMN details MEDIUMBLOB NULL,
  ADD COLUMN details_encoding VARCHAR(16) NULL;
