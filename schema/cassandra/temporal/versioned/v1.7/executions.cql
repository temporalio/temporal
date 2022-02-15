ALTER TABLE executions DROP tiered_storage_task_data;
ALTER TABLE executions DROP tiered_storage_task_encoding;
ALTER TABLE executions ADD task_data blob;
ALTER TABLE executions ADD task_encoding text;