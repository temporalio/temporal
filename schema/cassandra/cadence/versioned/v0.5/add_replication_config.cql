CREATE TYPE cluster_replication_config (
  cluster_name text,
);

CREATE TYPE domain_replication_config (
  active_cluster_name text,
  clusters            list<frozen<cluster_replication_config>>
);

ALTER TABLE domains_by_name ADD replication_config frozen<domain_replication_config>;
ALTER TABLE domains_by_name ADD failover_version bigint;
ALTER TABLE domains_by_name ADD db_version bigint;