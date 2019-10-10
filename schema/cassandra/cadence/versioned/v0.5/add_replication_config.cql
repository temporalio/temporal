CREATE TYPE cluster_replication_config (
  cluster_name text,
);

CREATE TYPE domain_replication_config (
  active_cluster_name text,
  clusters            list<frozen<cluster_replication_config>>
);
