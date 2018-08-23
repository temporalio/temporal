ALTER TYPE workflow_execution ADD attempt              int;
ALTER TYPE workflow_execution ADD has_retry_policy     boolean;
ALTER TYPE workflow_execution ADD init_interval        int;
ALTER TYPE workflow_execution ADD backoff_coefficient  double;
ALTER TYPE workflow_execution ADD max_interval         int;
ALTER TYPE workflow_execution ADD expiration_time      timestamp;
ALTER TYPE workflow_execution ADD max_attempts         int;
ALTER TYPE workflow_execution ADD non_retriable_errors list<text>;