ALTER TABLE cluster_metadata ADD data BLOB NOT NULL;
ALTER TABLE cluster_metadata ADD data_encoding VARCHAR(16) NOT NULL;
ALTER TABLE cluster_metadata ADD version BIGINT DEFAULT 1 NOT NULL;