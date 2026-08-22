CREATE TABLE tiered_storage_tasks (
    shard_id      INT         NOT NULL,
    task_id       BIGINT      NOT NULL,
    --
    data          MEDIUMBLOB  NOT NULL,
    data_encoding VARCHAR(16) NOT NULL,
    PRIMARY KEY (shard_id, task_id)
);