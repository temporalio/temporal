ALTER TYPE activity_info ADD  scheduled_event_batch_id bigint;
ALTER TYPE child_execution_info ADD  initiated_event_batch_id bigint;
ALTER TYPE child_execution_info ADD  started_workflow_id text;
ALTER TYPE child_execution_info ADD  started_run_id uuid;
ALTER TYPE child_execution_info ADD  domain_name text;
ALTER TYPE child_execution_info ADD  workflow_type_name text;
ALTER TYPE workflow_execution ADD  completion_event_batch_id bigint;