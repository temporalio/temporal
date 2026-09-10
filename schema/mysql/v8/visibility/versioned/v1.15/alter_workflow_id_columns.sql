ALTER TABLE executions_visibility MODIFY workflow_id VARBINARY(1000) NOT NULL;
ALTER TABLE executions_visibility MODIFY parent_workflow_id VARBINARY(1000) NULL;
ALTER TABLE executions_visibility MODIFY root_workflow_id VARBINARY(1000) NOT NULL DEFAULT '';
