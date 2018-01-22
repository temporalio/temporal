CREATE TYPE signal_info (
  initiated_id      bigint,
  signal_request_id uuid,
  signal_name       text,
  input             blob,
  control           blob,
);

ALTER TABLE executions ADD signal_map map<bigint, frozen<signal_info>>;
ALTER TABLE executions ADD signal_requested set<uuid>;