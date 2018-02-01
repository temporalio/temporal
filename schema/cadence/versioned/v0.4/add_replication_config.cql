CREATE TYPE cluster_replication_config (
  cluster_name text,
);

CREATE TYPE domain_replication_config (
  active_cluster_name text,
  failover_version    bigint,
  clusters            list<frozen<cluster_replication_config>>
);

ALTER TABLE domains_by_name ADD replication_config frozen<domain_replication_config>;
ALTER TABLE domains_by_name ADD version bigint;