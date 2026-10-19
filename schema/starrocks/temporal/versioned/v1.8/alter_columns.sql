ALTER TABLE current_executions MODIFY COLUMN create_request_id VARCHAR(255);
ALTER TABLE signals_requested_sets MODIFY COLUMN signal_id VARCHAR(255);
