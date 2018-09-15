ALTER TYPE workflow_execution ADD decision_attempt bigint;
ALTER TYPE workflow_execution ADD decision_timestamp bigint;

ALTER TYPE timer_task ADD schedule_attempt bigint;