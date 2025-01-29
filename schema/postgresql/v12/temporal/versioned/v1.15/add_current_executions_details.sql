ALTER TABLE current_executions
  ADD COLUMN details BYTEA NULL,
  ADD COLUMN details_encoding VARCHAR(16) NULL;
