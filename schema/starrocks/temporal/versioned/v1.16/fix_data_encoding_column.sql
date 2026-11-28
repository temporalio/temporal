ALTER TABLE current_executions DROP COLUMN data_encoding;
ALTER TABLE current_executions ADD COLUMN data_encoding VARCHAR(16) NOT NULL DEFAULT '';
