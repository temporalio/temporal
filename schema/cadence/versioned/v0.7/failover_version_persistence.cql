-- the failover version when this task is created, used to compare against the mutable state, in case the events got overwritten
ALTER TYPE transfer_task ADD version bigint;

-- the failover version when this task is created, used to compare against the mutable state, in case the events got overwritten
ALTER TYPE timer_task ADD version bigint;

-- the failover version when this attr is created, used to compare against the mutable state, in case the events got overwritten
ALTER TYPE workflow_execution ADD decision_version bigint;
ALTER TYPE activity_info ADD version bigint;
ALTER TYPE timer_info ADD version bigint;
ALTER TYPE child_execution_info ADD version bigint;
ALTER TYPE request_cancel_info ADD version bigint;
ALTER TYPE signal_info ADD version bigint;