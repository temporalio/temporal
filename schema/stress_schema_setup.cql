---- insert shard

insert into executions (shard_id, type, workflow_id, run_id, task_id, shard, range_id) VALUES (1, 0, '3fe89dad-8326-fac5-fd40-fe08cfa25dec', 228ce20b-af54-fe2f-ff17-be728a00f785, 23, { shard_id: 1, range_id: 1, transfer_ack_level: 0 }, 1) IF NOT EXISTS;