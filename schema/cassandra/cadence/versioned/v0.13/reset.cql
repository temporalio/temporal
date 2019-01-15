ALTER TYPE transfer_task ADD record_visibility          boolean;

ALTER TYPE replication_task ADD  reset_workflow                  boolean;

ALTER TABLE history_tree ADD   fork_time         timestamp;
ALTER TABLE history_tree ADD   info              text;