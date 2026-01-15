CREATE TABLE current_chasm_executions(
	shard_id INT NOT NULL,
	namespace_id BINARY(16) NOT NULL,
	business_id VARCHAR(255) NOT NULL,
	archetype_id BIGINT NOT NULL,
	--
	run_id BINARY(16) NOT NULL,
	create_request_id VARCHAR(255) NOT NULL,
	state INT NOT NULL,
	status INT NOT NULL,
	start_time TIMESTAMP NULL,
	last_write_version BIGINT NOT NULL,
	-- `data` contains the ExecutionState (same as in `executions.state` above)
	data MEDIUMBLOB NOT NULL,
	data_encoding VARCHAR(16) NOT NULL DEFAULT '',
	PRIMARY KEY (shard_id, namespace_id, business_id, archetype_id)
);
