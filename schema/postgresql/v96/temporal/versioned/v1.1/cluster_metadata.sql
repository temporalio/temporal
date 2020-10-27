ALTER TABLE cluster_metadata ADD data BYTEA NOT NULL DEFAULT '';
ALTER TABLE cluster_metadata ADD data_encoding VARCHAR(16) NOT NULL DEFAULT 'Proto3';
ALTER TABLE cluster_metadata ADD version BIGINT NOT NULL DEFAULT 1;