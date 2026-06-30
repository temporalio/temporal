ALTER TABLE executions_visibility ALTER COLUMN workflow_id TYPE VARCHAR(1000);
ALTER TABLE executions_visibility ALTER COLUMN parent_workflow_id TYPE VARCHAR(1000);
ALTER TABLE executions_visibility ALTER COLUMN root_workflow_id TYPE VARCHAR(1000);
