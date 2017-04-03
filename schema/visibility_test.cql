CREATE TABLE open_executions (
  domain_id            uuid,
  domain_partition     int,
  workflow_id          text,
  run_id               uuid,
  start_time           timestamp,
  workflow_type_name   text,
  PRIMARY KEY  ((domain_id, domain_partition), workflow_id, run_id)
);

CREATE INDEX open_by_start_time ON open_executions (start_time);
CREATE INDEX open_by_type ON open_executions (workflow_type_name);

CREATE TABLE closed_executions (
  domain_id            uuid,
  domain_partition     int,
  workflow_id          text,
  run_id               uuid,
  start_time           timestamp,
  close_time           timestamp,
  workflow_type_name   text,
  PRIMARY KEY  ((domain_id, domain_partition), workflow_id, run_id)
);

CREATE INDEX closed_by_start_time ON closed_executions (start_time);
CREATE INDEX closed_by_close_time ON closed_executions (close_time);
CREATE INDEX closed_by_type ON closed_executions (workflow_type_name);