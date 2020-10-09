ALTER TABLE cluster_metadata ADD data blob;
ALTER TABLE cluster_metadata ADD data_encoding text;
ALTER TABLE cluster_metadata ADD version bigint;
ALTER TABLE cluster_metadata DROP immutable_data;
ALTER TABLE cluster_metadata DROP immutable_data_encoding;
DELETE FROM cluster_metadata WHERE metadata_partition = 0;