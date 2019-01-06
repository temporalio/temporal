ALTER TYPE transfer_task ADD record_visibility          boolean;

ALTER TYPE replication_task ADD  reset_workflow                  boolean;
ALTER TYPE replication_task ADD  new_run_first_event_id          bigint;
ALTER TYPE replication_task ADD  new_run_next_event_id           bigint;

ALTER TABLE history_tree ADD   fork_time         timestamp;
ALTER TABLE history_tree ADD   info              text;