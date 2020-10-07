ALTER TABLE cluster_metadata ADD data BYTEA NOT NULL;
ALTER TABLE cluster_metadata ADD data_encoding VARCHAR(16) NOT NULL;
ALTER TABLE cluster_metadata ADD version BIGINT NOT NULL;
ALTER TABLE cluster_metadata DROP immutable_data;
ALTER TABLE cluster_metadata DROP immutable_data_encoding;