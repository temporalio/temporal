ALTER TYPE workflow_execution ADD cancel_requested  boolean;
ALTER TYPE workflow_execution ADD cancel_request_id text;

-- External workflow cancellation in progress mutable state
CREATE TYPE request_cancel_info (
  initiated_id      bigint,
  cancel_request_id text,
);

ALTER TABLE executions ADD request_cancel_map map<bigint, frozen<request_cancel_info>>;
