ALTER TABLE cluster_metadata ADD data BLOB NOT NULL;
ALTER TABLE cluster_metadata ADD data_encoding VARCHAR(16) NOT NULL DEFAULT 'Proto3';
ALTER TABLE cluster_metadata ADD version BIGINT NOT NULL DEFAULT 1;