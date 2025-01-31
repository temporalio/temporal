ALTER TABLE current_executions
  ADD COLUMN data BYTEA NULL,
  ADD COLUMN data_encoding VARCHAR(16) NULL;
