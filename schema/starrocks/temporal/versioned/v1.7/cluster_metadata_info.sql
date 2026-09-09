CREATE TABLE cluster_metadata_info (
  metadata_partition        INT NOT NULL,
  cluster_name              VARCHAR(255) NOT NULL,
  data                      MEDIUMBLOB NOT NULL,
  data_encoding             VARCHAR(16) NOT NULL,
  version                   BIGINT NOT NULL,
  PRIMARY KEY(metadata_partition, cluster_name)
);